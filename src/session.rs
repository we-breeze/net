use std::{
    collections::VecDeque,
    fmt,
    future::poll_fn,
    io::IoSlice,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, ready},
    time::Duration,
};

use bytes::{Buf, BufMut, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, tcp::ReadHalf},
    runtime::Handle,
    sync::mpsc,
    time::{self, Instant, MissedTickBehavior},
};

use crate::{
    Correlation, HandshakeStatus, NetError, RequestTarget, RequestToken, ResponseFuture, Result,
    RxBuffer, SessionError, SessionProtocol,
    completion::{CompletionTable, MAX_IN_FLIGHT},
    rx::DEFAULT_MAX_RX_BUFFER_CAPACITY,
};

const DEFAULT_READ_BUFFER_CAPACITY: usize = 2 * 1024;
const RX_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
const MAX_WRITE_VECTORS: usize = 4;

type ResponseResult<P> = std::result::Result<
    <P as SessionProtocol>::Response,
    SessionError<<P as SessionProtocol>::Error>,
>;

/// Aborts a response slot unless its request has been published to the driver.
/// This also covers unwinding from a caller-provided request builder.
struct ResponseReservation<T> {
    future: Option<ResponseFuture<T>>,
}

impl<T> ResponseReservation<T> {
    fn new(future: ResponseFuture<T>) -> Self {
        Self {
            future: Some(future),
        }
    }

    fn publish(mut self) -> ResponseFuture<T> {
        self.future.take().expect("response reservation is present")
    }
}

impl<T> Drop for ResponseReservation<T> {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            future.abort();
        }
    }
}

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
    pub max_read_buffer_capacity: usize,
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
            read_buffer_capacity: DEFAULT_READ_BUFFER_CAPACITY,
            max_read_buffer_capacity: DEFAULT_MAX_RX_BUFFER_CAPACITY,
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
        if !(1..=MAX_WRITE_VECTORS).contains(&self.write_batch) {
            return Err(NetError::InvalidConfig(format!(
                "write_batch must be between 1 and {MAX_WRITE_VECTORS}"
            )));
        }
        if self.read_buffer_capacity == 0 {
            return Err(NetError::InvalidConfig(
                "read_buffer_capacity must be greater than zero".into(),
            ));
        }
        if !self.read_buffer_capacity.is_power_of_two()
            || !self.max_read_buffer_capacity.is_power_of_two()
            || self.max_read_buffer_capacity < self.read_buffer_capacity
        {
            return Err(NetError::InvalidConfig(
                "receive buffer capacities must be powers of two and max must be at least initial"
                    .into(),
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
        self.request_with(|| request)
    }

    /// Reserve bounded request capacity before constructing the request.
    ///
    /// `build` runs synchronously only after this node is connected and its
    /// MPSC queue has capacity. This lets protocol clients postpone their final
    /// serialization until the request has passed the fail-fast admission
    /// checks.
    #[inline]
    pub fn request_with<F>(
        &self,
        build: F,
    ) -> std::result::Result<ResponseFuture<ResponseResult<P>>, SessionError<P::Error>>
    where
        F: FnOnce() -> P::Request,
    {
        if !self.inner.shared.connected.load(Ordering::Acquire) {
            return Err(SessionError::Unavailable);
        }
        let connection_generation = self
            .inner
            .shared
            .connection_generation
            .load(Ordering::Acquire);

        let permit = match self.inner.requests.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => return Err(SessionError::Busy),
            Err(mpsc::error::TrySendError::Closed(())) => return Err(SessionError::Closed),
        };

        let Some((token, future)) = self.inner.shared.completions.reserve() else {
            return Err(SessionError::Busy);
        };
        let future = ResponseReservation::new(future);

        // Close the race with a disconnect between the first availability
        // check and queue reservation. In particular, do not serialize a
        // request for a connection generation that can no longer send it.
        if !self.inner.shared.connected.load(Ordering::Acquire)
            || self
                .inner
                .shared
                .connection_generation
                .load(Ordering::Acquire)
                != connection_generation
        {
            return Err(SessionError::Unavailable);
        }

        let request = build();

        // Reservation and request construction are lock-free, but a disconnect
        // may still win between them. Never publish that frame to a new stream.
        if !self.inner.shared.connected.load(Ordering::Acquire)
            || self
                .inner
                .shared
                .connection_generation
                .load(Ordering::Acquire)
                != connection_generation
        {
            return Err(SessionError::Unavailable);
        }

        let envelope = Envelope {
            token,
            connection_generation,
            request,
        };
        permit.send(envelope);
        Ok(future.publish())
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

struct WriteQueue<F> {
    frames: VecDeque<F>,
    front_offset: usize,
    max_vectors: usize,
}

impl<F: AsRef<[u8]>> WriteQueue<F> {
    fn new(max_vectors: usize) -> Self {
        let max_vectors = max_vectors.clamp(1, MAX_WRITE_VECTORS);
        Self {
            frames: VecDeque::with_capacity(max_vectors),
            front_offset: 0,
            max_vectors,
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    #[inline]
    fn push(&mut self, frame: F) -> bool {
        if frame.as_ref().is_empty() {
            return false;
        }
        self.frames.push_back(frame);
        true
    }

    fn poll_write<W: AsyncWrite + Unpin>(
        &mut self,
        cx: &mut Context<'_>,
        writer: &mut W,
    ) -> Poll<std::io::Result<usize>> {
        let count = self.frames.len().min(self.max_vectors);
        let written = if writer.is_write_vectored() && count > 1 {
            let mut slices = [IoSlice::new(&[]); MAX_WRITE_VECTORS];
            for (index, frame) in self.frames.iter().take(count).enumerate() {
                let bytes = frame.as_ref();
                slices[index] = if index == 0 {
                    IoSlice::new(&bytes[self.front_offset..])
                } else {
                    IoSlice::new(bytes)
                };
            }
            ready!(Pin::new(&mut *writer).poll_write_vectored(cx, &slices[..count]))?
        } else {
            let frame = self.frames.front().expect("write queue is not empty");
            ready!(Pin::new(&mut *writer).poll_write(cx, &frame.as_ref()[self.front_offset..]))?
        };
        self.advance(written);
        Poll::Ready(Ok(written))
    }

    fn advance(&mut self, mut written: usize) {
        while written > 0 {
            let frame = self.frames.front().expect("socket wrote queued bytes");
            let remaining = frame.as_ref().len() - self.front_offset;
            if written < remaining {
                self.front_offset += written;
                return;
            }
            written -= remaining;
            self.frames.pop_front();
            self.front_offset = 0;
        }
    }

    fn clear(&mut self) {
        self.frames.clear();
        self.front_offset = 0;
    }
}

struct ConnectionDriver<P: SessionProtocol> {
    endpoint: std::net::SocketAddr,
    protocol: P,
    options: NodeOptions,
    requests: mpsc::Receiver<Envelope<P::Request>>,
    shared: Arc<Shared<P>>,
    read_buffer: RxBuffer,
    writes: WriteQueue<P::Frame>,
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
            read_buffer: RxBuffer::new(
                options.read_buffer_capacity,
                options.max_read_buffer_capacity,
            ),
            writes: WriteQueue::new(options.write_batch),
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
            self.read_buffer.reset();
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
            self.writes.clear();
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
        let mut write_buffer = BytesMut::new();
        let mut status = self
            .protocol
            .begin_handshake(&mut write_buffer)
            .map_err(DriveError::Protocol)?;
        if status == HandshakeStatus::Ready && write_buffer.is_empty() {
            return Ok(());
        }

        let mut read_buffer = BytesMut::new();
        let (mut reader, mut writer) = stream.split();
        let timeout = time::sleep(self.options.connect_timeout);
        tokio::pin!(timeout);
        loop {
            if status == HandshakeStatus::Ready && write_buffer.is_empty() {
                return Ok(());
            }

            tokio::select! {
                biased;

                _ = &mut timeout => return Err(DriveError::Timeout),

                read = reader.read_buf(&mut read_buffer), if status == HandshakeStatus::Pending => {
                    match read {
                        Ok(0) => return Err(DriveError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))),
                        Ok(_) => {
                            status = self
                                .protocol
                                .decode_handshake(&mut read_buffer, &mut write_buffer)
                                .map_err(DriveError::Protocol)?;
                        }
                        Err(error) => return Err(DriveError::Io(error)),
                    }
                }

                write = writer.write(&write_buffer), if !write_buffer.is_empty() => {
                    match write {
                        Ok(0) => return Err(DriveError::Io(std::io::Error::from(std::io::ErrorKind::WriteZero))),
                        Ok(length) => write_buffer.advance(length),
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
        let first_maintenance = Instant::now() + maintenance_jitter(self.endpoint);
        let mut maintenance = time::interval_at(first_maintenance, RX_MAINTENANCE_INTERVAL);
        maintenance.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
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

                read = read_socket(&mut reader, &mut self.read_buffer, !pending.is_empty()) => {
                    match read {
                        Ok(SocketRead::Response(outcome)) => {
                            if let Err(error) = self.decode_responses(pending) {
                                return error;
                            }
                            if self.read_buffer.is_full()
                                && let Err(error) = self.read_buffer.reserve(1)
                            {
                                return DriveError::Io(invalid_data(error));
                            }
                            if outcome == DrainOutcome::Eof {
                                return DriveError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                            }
                        }
                        Ok(SocketRead::IdleEof) => {
                            return DriveError::Io(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                        }
                        Ok(SocketRead::IdleData) => return DriveError::UnexpectedResponse,
                        Err(error) => return DriveError::Io(error),
                    }
                }

                write = poll_fn(|cx| self.writes.poll_write(cx, &mut writer)), if !self.writes.is_empty() => {
                    match write {
                        Ok(0) => return DriveError::Io(std::io::Error::from(std::io::ErrorKind::WriteZero)),
                        Ok(_) => {}
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

                _ = maintenance.tick() => {
                    self.read_buffer.shrink();
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
            match self.protocol.encode(envelope.request, envelope.token) {
                Ok(frame) => {
                    if !self.writes.push(frame) {
                        self.shared
                            .completions
                            .complete(envelope.token, Err(SessionError::EmptyRequestFrame));
                        continue;
                    }
                    pending.push_back(Pending {
                        token: envelope.token,
                        deadline: Instant::now() + self.options.request_timeout,
                    });
                }
                Err(error) => {
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
                    let Some(request) = pending.pop_front() else {
                        return Err(DriveError::UnexpectedResponse);
                    };
                    self.shared
                        .completions
                        .complete(request.token, Ok(decoded.response));
                }
                Correlation::Tagged(request_id) => {
                    if let Some(index) = pending
                        .iter()
                        .position(|pending| pending.token == request_id)
                    {
                        pending.remove(index);
                    }
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

fn far_future() -> Instant {
    Instant::now() + Duration::from_secs(365 * 24 * 60 * 60)
}

/// Spread connection-local maintenance across one interval. This is computed
/// once per connection and avoids a burst when DNS publishes many endpoints
/// together.
fn maintenance_jitter(endpoint: std::net::SocketAddr) -> Duration {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET;
    let mut mix = |byte: u8| {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    };
    match endpoint.ip() {
        std::net::IpAddr::V4(ip) => {
            mix(4);
            for byte in ip.octets() {
                mix(byte);
            }
        }
        std::net::IpAddr::V6(ip) => {
            mix(6);
            for byte in ip.octets() {
                mix(byte);
            }
        }
    }
    for byte in endpoint.port().to_be_bytes() {
        mix(byte);
    }

    let interval_millis = RX_MAINTENANCE_INTERVAL.as_millis() as u64;
    Duration::from_millis(hash % interval_millis)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DrainOutcome {
    Drained,
    Full,
    Eof,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SocketRead {
    Response(DrainOutcome),
    IdleEof,
    IdleData,
}

async fn read_socket(
    reader: &mut ReadHalf<'_>,
    buffer: &mut RxBuffer,
    response_pending: bool,
) -> std::io::Result<SocketRead> {
    if response_pending {
        return drain_socket(reader, buffer).await.map(SocketRead::Response);
    }

    wait_idle_socket(reader).await
}

/// Keep an idle connection registered with the runtime reactor without
/// allocating the protocol receive ring. FIN/RST wakes this future immediately;
/// any payload is invalid because there is no request awaiting a response.
async fn wait_idle_socket(reader: &ReadHalf<'_>) -> std::io::Result<SocketRead> {
    let mut byte = [0_u8; 1];
    loop {
        reader.readable().await?;
        match reader.try_read(&mut byte) {
            Ok(0) => return Ok(SocketRead::IdleEof),
            Ok(_) => return Ok(SocketRead::IdleData),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
    }
}

/// Wait for one readable event, then consume everything currently queued by
/// the kernel. A full ring returns control to the protocol so a decoded length
/// prefix can reserve the final frame size before reading resumes.
async fn drain_socket(
    reader: &mut ReadHalf<'_>,
    buffer: &mut RxBuffer,
) -> std::io::Result<DrainOutcome> {
    buffer.prepare_read().map_err(invalid_data)?;
    if buffer.remaining_mut() == 0 {
        return Ok(DrainOutcome::Full);
    }

    let first = reader.read_buf(buffer).await?;
    if first == 0 {
        return Ok(DrainOutcome::Eof);
    }
    loop {
        if buffer.remaining_mut() == 0 {
            return Ok(DrainOutcome::Full);
        }
        match reader.try_read_buf(buffer) {
            Ok(0) => return Ok(DrainOutcome::Eof),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(DrainOutcome::Drained);
            }
            Err(error) => return Err(error),
        }
    }
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}

fn next_delay(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

#[cfg(test)]
mod tests {
    use std::{
        io::IoSlice,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    use tokio::io::AsyncWrite;

    use super::{
        MAX_WRITE_VECTORS, NodeOptions, RX_MAINTENANCE_INTERVAL, WriteQueue, maintenance_jitter,
    };

    #[derive(Default)]
    struct CountingWriter {
        direct_calls: usize,
        vectored_calls: usize,
        maximum_write: usize,
    }

    impl AsyncWrite for CountingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.direct_calls += 1;
            Poll::Ready(Ok(bytes.len().min(self.maximum_write)))
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            slices: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            self.vectored_calls += 1;
            let length = slices.iter().map(|slice| slice.len()).sum::<usize>();
            Poll::Ready(Ok(length.min(self.maximum_write)))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn one_frame_uses_direct_write_and_multiple_frames_use_writev() {
        let mut cx = Context::from_waker(Waker::noop());
        let mut writer = CountingWriter {
            maximum_write: usize::MAX,
            ..CountingWriter::default()
        };
        let mut writes = WriteQueue::new(MAX_WRITE_VECTORS);

        assert!(writes.push(&b"one"[..]));
        assert!(matches!(
            writes.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(3))
        ));
        assert_eq!(writer.direct_calls, 1);
        assert_eq!(writer.vectored_calls, 0);

        assert!(writes.push(&b"two"[..]));
        assert!(writes.push(&b"three"[..]));
        assert!(matches!(
            writes.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(8))
        ));
        assert_eq!(writer.direct_calls, 1);
        assert_eq!(writer.vectored_calls, 1);
        assert!(writes.is_empty());
    }

    #[test]
    fn partial_write_preserves_the_remaining_frame_offset() {
        let mut cx = Context::from_waker(Waker::noop());
        let mut writer = CountingWriter {
            maximum_write: 4,
            ..CountingWriter::default()
        };
        let mut writes = WriteQueue::new(MAX_WRITE_VECTORS);
        assert!(writes.push(&b"abc"[..]));
        assert!(writes.push(&b"def"[..]));

        assert!(matches!(
            writes.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(4))
        ));
        assert_eq!(writer.vectored_calls, 1);
        assert_eq!(writes.frames.len(), 1);
        assert_eq!(writes.front_offset, 1);

        assert!(matches!(
            writes.poll_write(&mut cx, &mut writer),
            Poll::Ready(Ok(2))
        ));
        assert_eq!(writer.direct_calls, 1);
        assert!(writes.is_empty());
    }

    #[test]
    fn defaults_use_a_two_kib_receive_ring_and_four_frame_write_batch() {
        let options = NodeOptions::default();

        assert_eq!(options.read_buffer_capacity, 2 * 1024);
        assert_eq!(options.write_batch, MAX_WRITE_VECTORS);
        assert!(options.validate().is_ok());
    }

    #[test]
    fn write_batch_is_bounded_by_the_stack_iovec_array() {
        let options = NodeOptions {
            write_batch: MAX_WRITE_VECTORS + 1,
            ..NodeOptions::default()
        };

        assert!(options.validate().is_err());
    }

    #[test]
    fn maintenance_jitter_is_stable_and_bounded() {
        let first = "127.0.0.1:6379".parse().unwrap();
        let second = "127.0.0.2:6379".parse().unwrap();

        assert_eq!(maintenance_jitter(first), maintenance_jitter(first));
        assert!(maintenance_jitter(first) < RX_MAINTENANCE_INTERVAL);
        assert!(maintenance_jitter(second) < RX_MAINTENANCE_INTERVAL);
        assert_ne!(maintenance_jitter(first), maintenance_jitter(second));
    }
}
