use std::{
    collections::VecDeque,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::{Buf, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    runtime::Handle,
    sync::mpsc,
    time::{self, Instant},
};

use crate::{
    Correlation, HandshakeStatus, NetError, RequestTarget, RequestToken, ResponseFuture, Result,
    SessionError, SessionProtocol,
    completion::{CompletionTable, MAX_IN_FLIGHT},
};

const DEFAULT_BUFFER_CAPACITY: usize = 8 * 1024;

type ResponseResult<P> = std::result::Result<
    <P as SessionProtocol>::Response,
    SessionError<<P as SessionProtocol>::Error>,
>;

/// Runtime policy for one physical node and its one persistent connection.
#[derive(Clone, Copy, Debug)]
pub struct NodeOptions {
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub reconnect_delay: Duration,
    pub max_reconnect_delay: Duration,
    pub tcp_nodelay: bool,
    pub write_batch: usize,
    pub read_buffer_capacity: usize,
    pub write_buffer_capacity: usize,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_millis(500),
            connect_timeout: Duration::from_secs(2),
            reconnect_delay: Duration::from_millis(50),
            max_reconnect_delay: Duration::from_secs(2),
            tcp_nodelay: true,
            write_batch: 4,
            read_buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            write_buffer_capacity: DEFAULT_BUFFER_CAPACITY,
        }
    }
}

impl NodeOptions {
    fn validate(self) -> Result<Self> {
        if self.request_timeout.is_zero() {
            return Err(NetError::InvalidConfig(
                "request_timeout must be greater than zero".into(),
            ));
        }
        if self.connect_timeout.is_zero() {
            return Err(NetError::InvalidConfig(
                "connect_timeout must be greater than zero".into(),
            ));
        }
        if self.reconnect_delay.is_zero() {
            return Err(NetError::InvalidConfig(
                "reconnect_delay must be greater than zero".into(),
            ));
        }
        if self.max_reconnect_delay < self.reconnect_delay {
            return Err(NetError::InvalidConfig(
                "max_reconnect_delay must be at least reconnect_delay".into(),
            ));
        }
        if self.write_batch == 0 {
            return Err(NetError::InvalidConfig(
                "write_batch must be greater than zero".into(),
            ));
        }
        if self.read_buffer_capacity == 0 || self.write_buffer_capacity == 0 {
            return Err(NetError::InvalidConfig(
                "session buffer capacities must be greater than zero".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodeStats {
    pub connected: bool,
    pub active_requests: usize,
    pub successful_connections: u64,
    pub disconnects: u64,
}

/// Cloneable, fail-fast request handle for one physical endpoint.
///
/// Exactly one background task owns exactly one TCP stream for this node.
pub struct Node<P: SessionProtocol> {
    inner: Arc<NodeInner<P>>,
}

struct NodeInner<P: SessionProtocol> {
    requests: mpsc::Sender<Envelope<P::Request>>,
    shared: Arc<Shared<P>>,
    endpoint: std::net::SocketAddr,
}

struct Shared<P: SessionProtocol> {
    completions: Arc<CompletionTable<ResponseResult<P>>>,
    connected: AtomicBool,
    connection_generation: AtomicU64,
    successful_connections: AtomicU64,
    disconnects: AtomicU64,
}

impl<P: SessionProtocol> Clone for Node<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P: SessionProtocol> Node<P> {
    pub fn new(endpoint: std::net::SocketAddr, protocol: P, options: NodeOptions) -> Result<Self> {
        let options = options.validate()?;
        let runtime = Handle::try_current().map_err(|_| NetError::NoRuntime)?;
        let completions = CompletionTable::new();
        let shared = Arc::new(Shared {
            completions,
            connected: AtomicBool::new(false),
            connection_generation: AtomicU64::new(0),
            successful_connections: AtomicU64::new(0),
            disconnects: AtomicU64::new(0),
        });
        let (requests, request_rx) = mpsc::channel(MAX_IN_FLIGHT);
        let driver = ConnectionDriver::new(endpoint, protocol, options, request_rx, shared.clone());
        runtime.spawn(driver.run());

        Ok(Self {
            inner: Arc::new(NodeInner {
                requests,
                shared,
                endpoint,
            }),
        })
    }

    /// Submit without waiting for queue capacity. The returned Future only
    /// waits for the response already admitted by this call.
    #[inline]
    pub fn request(
        &self,
        request: P::Request,
    ) -> std::result::Result<ResponseFuture<ResponseResult<P>>, SessionError<P::Error>> {
        if !self.inner.shared.connected.load(Ordering::Acquire) {
            return Err(SessionError::Unavailable);
        }
        let connection_generation = self
            .inner
            .shared
            .connection_generation
            .load(Ordering::Acquire);

        let Some((token, future)) = self.inner.shared.completions.reserve() else {
            return Err(SessionError::Busy);
        };

        // Close the race with a disconnect between the first availability
        // check and slot reservation.
        if !self.inner.shared.connected.load(Ordering::Acquire)
            || self
                .inner
                .shared
                .connection_generation
                .load(Ordering::Acquire)
                != connection_generation
        {
            future.abort();
            return Err(SessionError::Unavailable);
        }

        let envelope = Envelope {
            token,
            connection_generation,
            request,
        };
        match self.inner.requests.try_send(envelope) {
            Ok(()) => Ok(future),
            Err(mpsc::error::TrySendError::Full(_)) => {
                future.abort();
                Err(SessionError::Busy)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                future.abort();
                Err(SessionError::Closed)
            }
        }
    }

    #[inline]
    pub fn is_connected(&self) -> bool {
        self.inner.shared.connected.load(Ordering::Acquire)
    }

    #[inline]
    pub fn endpoint(&self) -> std::net::SocketAddr {
        self.inner.endpoint
    }

    pub fn stats(&self) -> NodeStats {
        NodeStats {
            connected: self.is_connected(),
            active_requests: self.inner.shared.completions.active(),
            successful_connections: self
                .inner
                .shared
                .successful_connections
                .load(Ordering::Relaxed),
            disconnects: self.inner.shared.disconnects.load(Ordering::Relaxed),
        }
    }
}

impl<P: SessionProtocol> fmt::Debug for Node<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Node")
            .field("endpoint", &self.endpoint())
            .field("stats", &self.stats())
            .finish()
    }
}

impl<K: ?Sized, P: SessionProtocol> RequestTarget<K> for Node<P> {
    type Request = P::Request;
    type Response = P::Response;
    type Error = P::Error;
    type Future = ResponseFuture<ResponseResult<P>>;

    #[inline]
    fn request_for(
        &self,
        _key: &K,
        request: Self::Request,
    ) -> std::result::Result<Self::Future, SessionError<Self::Error>> {
        self.request(request)
    }
}

struct Envelope<R> {
    token: RequestToken,
    connection_generation: u64,
    request: R,
}

#[derive(Clone, Copy)]
struct Pending {
    token: RequestToken,
    deadline: Instant,
}

enum DriveError<E> {
    Io(std::io::Error),
    Protocol(E),
    Timeout,
    Closed,
    UnexpectedResponse,
}

struct ConnectionDriver<P: SessionProtocol> {
    endpoint: std::net::SocketAddr,
    protocol: P,
    options: NodeOptions,
    requests: mpsc::Receiver<Envelope<P::Request>>,
    shared: Arc<Shared<P>>,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    batch: VecDeque<Envelope<P::Request>>,
}

impl<P: SessionProtocol> ConnectionDriver<P> {
    fn new(
        endpoint: std::net::SocketAddr,
        protocol: P,
        options: NodeOptions,
        requests: mpsc::Receiver<Envelope<P::Request>>,
        shared: Arc<Shared<P>>,
    ) -> Self {
        Self {
            endpoint,
            protocol,
            options,
            requests,
            shared,
            read_buffer: BytesMut::with_capacity(options.read_buffer_capacity),
            write_buffer: BytesMut::with_capacity(options.write_buffer_capacity),
            batch: VecDeque::with_capacity(options.write_batch),
        }
    }

    async fn run(mut self) {
        let mut reconnect_delay = self.options.reconnect_delay;
        loop {
            let mut stream = match Self::connect(self.endpoint, self.options).await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::debug!(endpoint = %self.endpoint, %error, "session connect failed");
                    if !self.wait_disconnected(reconnect_delay).await {
                        return;
                    }
                    reconnect_delay = next_delay(reconnect_delay, self.options.max_reconnect_delay);
                    continue;
                }
            };

            self.protocol.reset();
            self.read_buffer.clear();
            self.write_buffer.clear();
            if let Err(error) = self.handshake(&mut stream).await {
                self.log_establish_error(&error);
                let _ = stream.shutdown().await;
                if !self.wait_disconnected(reconnect_delay).await {
                    return;
                }
                reconnect_delay = next_delay(reconnect_delay, self.options.max_reconnect_delay);
                continue;
            }

            reconnect_delay = self.options.reconnect_delay;
            self.shared
                .connection_generation
                .fetch_add(1, Ordering::AcqRel);
            self.shared
                .successful_connections
                .fetch_add(1, Ordering::Relaxed);
            self.shared.connected.store(true, Ordering::Release);

            let mut pending = VecDeque::with_capacity(MAX_IN_FLIGHT);
            let result = self.drive(&mut stream, &mut pending).await;
            self.shared.connected.store(false, Ordering::Release);

            let closed = matches!(result, DriveError::Closed);
            let error = match result {
                DriveError::Io(error) => SessionError::Io(Arc::new(error)),
                DriveError::Protocol(error) => SessionError::Protocol(Arc::new(error)),
                DriveError::Timeout => SessionError::Timeout {
                    timeout: self.options.request_timeout,
                },
                DriveError::Closed => SessionError::Closed,
                DriveError::UnexpectedResponse => SessionError::UnexpectedResponse,
            };

            self.fail_pending(&mut pending, error.clone());
            self.fail_queued(error);
            let _ = stream.shutdown().await;

            if closed && self.requests.is_closed() {
                return;
            }
            self.shared.disconnects.fetch_add(1, Ordering::Relaxed);
            if !self.wait_disconnected(reconnect_delay).await {
                return;
            }
        }
    }

    async fn connect(
        endpoint: std::net::SocketAddr,
        options: NodeOptions,
    ) -> std::io::Result<TcpStream> {
        let stream = time::timeout(options.connect_timeout, TcpStream::connect(endpoint))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "TCP connect timed out")
            })??;
        stream.set_nodelay(options.tcp_nodelay)?;
        Ok(stream)
    }

    async fn handshake(
        &mut self,
        stream: &mut TcpStream,
    ) -> std::result::Result<(), DriveError<P::Error>> {
        let mut status = self
            .protocol
            .begin_handshake(&mut self.write_buffer)
            .map_err(DriveError::Protocol)?;
        if status == HandshakeStatus::Ready && self.write_buffer.is_empty() {
            return Ok(());
        }

        let (mut reader, mut writer) = stream.split();
        let timeout = time::sleep(self.options.connect_timeout);
        tokio::pin!(timeout);
        loop {
            if status == HandshakeStatus::Ready && self.write_buffer.is_empty() {
                return Ok(());
            }

            tokio::select! {
                biased;

                _ = &mut timeout => return Err(DriveError::Timeout),

                read = reader.read_buf(&mut self.read_buffer), if status == HandshakeStatus::Pending => {
                    match read {
                        Ok(0) => return Err(DriveError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))),
                        Ok(_) => {
                            status = self
                                .protocol
                                .decode_handshake(&mut self.read_buffer, &mut self.write_buffer)
                                .map_err(DriveError::Protocol)?;
                        }
                        Err(error) => return Err(DriveError::Io(error)),
                    }
                }

                write = writer.write(&self.write_buffer), if !self.write_buffer.is_empty() => {
                    match write {
                        Ok(0) => return Err(DriveError::Io(std::io::Error::from(std::io::ErrorKind::WriteZero))),
                        Ok(length) => self.write_buffer.advance(length),
                        Err(error) => return Err(DriveError::Io(error)),
                    }
                }
            }
        }
    }

    fn log_establish_error(&self, error: &DriveError<P::Error>) {
        match error {
            DriveError::Io(error) => {
                tracing::debug!(endpoint = %self.endpoint, %error, "session handshake I/O failed");
            }
            DriveError::Protocol(error) => {
                tracing::debug!(endpoint = %self.endpoint, %error, "session handshake protocol failed");
            }
            DriveError::Timeout => {
                tracing::debug!(endpoint = %self.endpoint, "session handshake timed out");
            }
            DriveError::Closed | DriveError::UnexpectedResponse => {
                unreachable!("handshake cannot produce a request-stream state error");
            }
        }
    }

    async fn drive(
        &mut self,
        stream: &mut TcpStream,
        pending: &mut VecDeque<Pending>,
    ) -> DriveError<P::Error> {
        let (mut reader, mut writer) = stream.split();
        let timeout = time::sleep_until(far_future());
        tokio::pin!(timeout);

        loop {
            prune_resolved::<P>(&self.shared.completions, pending);
            if let Some(deadline) = pending.front().map(|pending| pending.deadline)
                && timeout.deadline() != deadline
            {
                timeout.as_mut().reset(deadline);
            }

            tokio::select! {
                biased;

                _ = &mut timeout, if !pending.is_empty() => {
                    return DriveError::Timeout;
                }

                read = reader.read_buf(&mut self.read_buffer), if !pending.is_empty() => {
                    match read {
                        Ok(0) => return DriveError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof)),
                        Ok(_) => {
                            if let Err(error) = self.decode_responses(pending) {
                                return error;
                            }
                        }
                        Err(error) => return DriveError::Io(error),
                    }
                }

                write = writer.write(&self.write_buffer), if !self.write_buffer.is_empty() => {
                    match write {
                        Ok(0) => return DriveError::Io(std::io::Error::from(std::io::ErrorKind::WriteZero)),
                        Ok(length) => self.write_buffer.advance(length),
                        Err(error) => return DriveError::Io(error),
                    }
                }

                request = self.requests.recv() => {
                    let Some(request) = request else {
                        return DriveError::Closed;
                    };
                    self.batch.push_back(request);
                    while self.batch.len() < self.options.write_batch {
                        match self.requests.try_recv() {
                            Ok(request) => self.batch.push_back(request),
                            Err(mpsc::error::TryRecvError::Empty) => break,
                            Err(mpsc::error::TryRecvError::Disconnected) => break,
                        }
                    }
                    self.encode_batch(pending);
                }

            }
        }
    }

    fn encode_batch(&mut self, pending: &mut VecDeque<Pending>) {
        while let Some(envelope) = self.batch.pop_front() {
            if envelope.connection_generation
                != self.shared.connection_generation.load(Ordering::Acquire)
            {
                self.shared
                    .completions
                    .complete(envelope.token, Err(SessionError::Unavailable));
                continue;
            }
            let original_length = self.write_buffer.len();
            match self
                .protocol
                .encode(&envelope.request, envelope.token, &mut self.write_buffer)
            {
                Ok(()) => pending.push_back(Pending {
                    token: envelope.token,
                    deadline: Instant::now() + self.options.request_timeout,
                }),
                Err(error) => {
                    self.write_buffer.truncate(original_length);
                    self.shared
                        .completions
                        .complete(envelope.token, Err(SessionError::Protocol(Arc::new(error))));
                }
            }
        }
    }

    fn decode_responses(
        &mut self,
        pending: &mut VecDeque<Pending>,
    ) -> std::result::Result<(), DriveError<P::Error>> {
        loop {
            let decoded = self
                .protocol
                .decode(&mut self.read_buffer)
                .map_err(DriveError::Protocol)?;
            let Some(decoded) = decoded else {
                return Ok(());
            };

            match decoded.correlation {
                Correlation::Fifo => {
                    prune_resolved::<P>(&self.shared.completions, pending);
                    let Some(request) = pending.pop_front() else {
                        return Err(DriveError::UnexpectedResponse);
                    };
                    self.shared
                        .completions
                        .complete(request.token, Ok(decoded.response));
                }
                Correlation::Tagged(request_id) => {
                    // A false result is a timed-out, cancelled, duplicate, or
                    // otherwise late response and is intentionally ignored.
                    self.shared
                        .completions
                        .complete(request_id, Ok(decoded.response));
                }
            }
        }
    }

    fn fail_pending(&self, pending: &mut VecDeque<Pending>, error: SessionError<P::Error>) {
        while let Some(request) = pending.pop_front() {
            self.shared
                .completions
                .complete(request.token, Err(error.clone()));
        }
    }

    fn fail_queued(&mut self, error: SessionError<P::Error>) {
        while let Ok(request) = self.requests.try_recv() {
            self.shared
                .completions
                .complete(request.token, Err(error.clone()));
        }
    }

    async fn wait_disconnected(&mut self, delay: Duration) -> bool {
        let sleep = time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => return true,
                request = self.requests.recv() => match request {
                    Some(request) => {
                        self.shared
                            .completions
                            .complete(request.token, Err(SessionError::Unavailable));
                    }
                    None => return false,
                }
            }
        }
    }
}

fn prune_resolved<P: SessionProtocol>(
    completions: &CompletionTable<ResponseResult<P>>,
    pending: &mut VecDeque<Pending>,
) {
    while pending
        .front()
        .is_some_and(|request| !completions.is_driver_pending(request.token))
    {
        pending.pop_front();
    }
}

fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(365 * 24 * 60 * 60)
}

fn next_delay(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}
