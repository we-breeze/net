use std::{
    collections::VecDeque,
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::{net::TcpStream, runtime::Handle};

use crate::{
    BoxFuture, BrzTcpStream, DnsOptions, DnsResolver, DnsSource, EndpointSet, EndpointSource,
    NetError, Result, StreamProvider, maintenance, stream::ManagedConnection,
};

static NEXT_MAINTENANCE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug)]
pub struct NodePoolOptions {
    /// Minimum total number of physical connections maintained in the background.
    pub min_connections: usize,
    /// Hard limit across idle, checked-out, and currently connecting sockets.
    pub max_connections: usize,
    /// How long an excess idle connection may remain unused.
    pub idle_timeout: Duration,
    /// Disable Nagle aggregation to prioritize latency for small requests.
    pub tcp_nodelay: bool,
}

impl Default for NodePoolOptions {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 256,
            idle_timeout: Duration::from_secs(300),
            tcp_nodelay: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePoolStats {
    pub total_connections: usize,
    pub idle_connections: usize,
    pub checked_out_connections: usize,
    pub connecting_connections: usize,
}

/// Connection pool for one logical node.
///
/// The endpoint source may contain multiple addresses (for example, DNS A
/// records), but all pooled connections belong to the same logical node.
#[derive(Clone)]
pub struct NodePool {
    inner: Arc<NodePoolInner>,
}

pub(crate) struct NodePoolInner {
    maintenance_id: u64,
    runtime: Handle,
    source: Arc<dyn EndpointSource>,
    state: Mutex<ConnectionState>,
    next_endpoint: AtomicUsize,
    options: NodePoolOptions,
}

#[derive(Default)]
struct ConnectionState {
    idle: VecDeque<IdleConnection>,
    checked_out: usize,
    connecting: usize,
}

impl ConnectionState {
    fn total(&self) -> usize {
        self.idle.len() + self.checked_out + self.connecting
    }
}

struct IdleConnection {
    connection: ManagedConnection,
    since: Instant,
}

impl NodePool {
    pub fn new(source: impl EndpointSource + 'static, options: NodePoolOptions) -> Result<Self> {
        validate_options(options)?;
        let runtime = Handle::try_current().map_err(|_| {
            NetError::InvalidConfig("NodePool must be created inside a Tokio runtime".into())
        })?;
        let source: Arc<dyn EndpointSource> = Arc::new(source);
        let inner = Arc::new(NodePoolInner {
            maintenance_id: NEXT_MAINTENANCE_ID.fetch_add(1, Ordering::Relaxed),
            runtime,
            source,
            state: Mutex::new(ConnectionState::default()),
            next_endpoint: AtomicUsize::new(0),
            options,
        });
        maintenance::register(&inner)?;
        Ok(Self { inner })
    }

    pub fn from_endpoints(
        endpoints: impl IntoIterator<Item = SocketAddr>,
        options: NodePoolOptions,
    ) -> Result<Self> {
        Self::new(EndpointSet::new(endpoints), options)
    }

    pub async fn from_dns(
        names: impl IntoIterator<Item = impl Into<String>>,
        dns_options: DnsOptions,
        pool_options: NodePoolOptions,
    ) -> Result<Self> {
        Self::new(DnsSource::new(names, dns_options).await?, pool_options)
    }

    /// Build from DNS using an explicitly shared resolver.
    pub async fn from_dns_with_resolver(
        resolver: &DnsResolver,
        names: impl IntoIterator<Item = impl Into<String>>,
        dns_options: DnsOptions,
        pool_options: NodePoolOptions,
    ) -> Result<Self> {
        Self::new(
            DnsSource::with_resolver(resolver, names, dns_options).await?,
            pool_options,
        )
    }

    pub fn stats(&self) -> NodePoolStats {
        let state = self.inner.state.lock();
        NodePoolStats {
            total_connections: state.total(),
            idle_connections: state.idle.len(),
            checked_out_connections: state.checked_out,
            connecting_connections: state.connecting,
        }
    }

    async fn acquire_stream(&self) -> Result<BrzTcpStream> {
        let endpoints = self.inner.source.snapshot();
        let action = {
            let mut state = self.inner.state.lock();
            if endpoints.is_empty() {
                AcquireAction::NoEndpoints
            } else if let Some(idle) = state.idle.pop_back() {
                state.checked_out += 1;
                AcquireAction::Reuse(idle.connection)
            } else if state.total() < self.inner.options.max_connections {
                state.connecting += 1;
                AcquireAction::Connect(endpoints)
            } else {
                AcquireAction::Exhausted
            }
        };

        match action {
            AcquireAction::NoEndpoints => Err(NetError::NoEndpoints),
            AcquireAction::Reuse(connection) => {
                Ok(BrzTcpStream::new(connection, Arc::downgrade(&self.inner)))
            }
            AcquireAction::Connect(endpoints) => {
                let mut reservation = ConnectionReservation::new(self.inner.clone());
                let connection = self.inner.connect(endpoints).await?;
                reservation.commit();
                Ok(BrzTcpStream::new(connection, Arc::downgrade(&self.inner)))
            }
            AcquireAction::Exhausted => Err(NetError::PoolExhausted {
                max_connections: self.inner.options.max_connections,
            }),
        }
    }
}

impl fmt::Debug for NodePool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodePool")
            .field("endpoints", &self.inner.source.snapshot())
            .field("options", &self.inner.options)
            .field("stats", &self.stats())
            .finish()
    }
}

impl<K: ?Sized + Sync> StreamProvider<K> for NodePool {
    fn acquire<'a>(&'a self, _key: &'a K) -> BoxFuture<'a, Result<BrzTcpStream>> {
        Box::pin(async move { self.acquire_stream().await })
    }
}

impl NodePoolInner {
    pub(crate) fn maintenance_id(&self) -> u64 {
        self.maintenance_id
    }

    pub(crate) fn runtime(&self) -> Handle {
        self.runtime.clone()
    }

    async fn connect(&self, endpoints: Arc<[SocketAddr]>) -> Result<ManagedConnection> {
        let start = self.next_endpoint.fetch_add(1, Ordering::Relaxed);
        let mut last_error = None;

        for offset in 0..endpoints.len() {
            let endpoint = endpoints[(start + offset) % endpoints.len()];
            match TcpStream::connect(endpoint).await {
                Ok(stream) => {
                    if let Err(source) = stream.set_nodelay(self.options.tcp_nodelay) {
                        last_error = Some(NetError::Connect { endpoint, source });
                        continue;
                    }
                    return Ok(ManagedConnection {
                        io: stream,
                        endpoint,
                    });
                }
                Err(source) => {
                    last_error = Some(NetError::Connect { endpoint, source });
                }
            }
        }

        Err(last_error.unwrap_or(NetError::NoEndpoints))
    }

    pub(crate) fn recycle(&self, connection: ManagedConnection) {
        let mut state = self.state.lock();
        debug_assert!(state.checked_out > 0);
        state.checked_out = state.checked_out.saturating_sub(1);
        state.idle.push_back(IdleConnection {
            connection,
            since: Instant::now(),
        });
    }

    pub(crate) fn discard_connection(&self, connection: ManagedConnection) {
        drop(connection);
        let mut state = self.state.lock();
        debug_assert!(state.checked_out > 0);
        state.checked_out = state.checked_out.saturating_sub(1);
    }

    fn finish_request_connection(&self) {
        let mut state = self.state.lock();
        debug_assert!(state.connecting > 0);
        state.connecting = state.connecting.saturating_sub(1);
        state.checked_out += 1;
    }

    fn release_request_reservation(&self) {
        let mut state = self.state.lock();
        debug_assert!(state.connecting > 0);
        state.connecting = state.connecting.saturating_sub(1);
    }

    /// Perform housekeeping without making request acquisition pay its scan cost.
    /// A contended state lock is skipped to keep hot-path contention bounded.
    pub(crate) fn maintenance_tick(&self) -> bool {
        let endpoints = self.source.snapshot();
        let Some(mut state) = self.state.try_lock() else {
            return false;
        };

        state
            .idle
            .retain(|idle| endpoints.contains(&idle.connection.endpoint));

        let idle_needed_for_min = self
            .options
            .min_connections
            .saturating_sub(state.checked_out + state.connecting);
        let now = Instant::now();
        while state.idle.len() > idle_needed_for_min
            && state
                .idle
                .front()
                .is_some_and(|idle| now.duration_since(idle.since) >= self.options.idle_timeout)
        {
            state.idle.pop_front();
        }

        !endpoints.is_empty() && state.total() < self.options.min_connections
    }

    pub(crate) fn reserve_background_connection(
        self: &Arc<Self>,
        completion: std::sync::mpsc::Sender<maintenance::Command>,
    ) -> Option<(Arc<[SocketAddr]>, BackgroundReservation)> {
        let endpoints = self.source.snapshot();
        if endpoints.is_empty() {
            return None;
        }

        let mut state = self.state.try_lock()?;
        if state.total() >= self.options.min_connections
            || state.total() >= self.options.max_connections
        {
            return None;
        }
        state.connecting += 1;
        drop(state);

        Some((
            endpoints,
            BackgroundReservation {
                owner: self.clone(),
                completion,
                active: true,
            },
        ))
    }
}

enum AcquireAction {
    NoEndpoints,
    Reuse(ManagedConnection),
    Connect(Arc<[SocketAddr]>),
    Exhausted,
}

struct ConnectionReservation {
    owner: Arc<NodePoolInner>,
    active: bool,
}

impl ConnectionReservation {
    fn new(owner: Arc<NodePoolInner>) -> Self {
        Self {
            owner,
            active: true,
        }
    }

    fn commit(&mut self) {
        self.owner.finish_request_connection();
        self.active = false;
    }
}

impl Drop for ConnectionReservation {
    fn drop(&mut self) {
        if self.active {
            self.owner.release_request_reservation();
        }
    }
}

pub(crate) struct BackgroundReservation {
    owner: Arc<NodePoolInner>,
    completion: std::sync::mpsc::Sender<maintenance::Command>,
    active: bool,
}

impl BackgroundReservation {
    pub(crate) async fn connect(mut self, endpoints: Arc<[SocketAddr]>) {
        let connection = match self.owner.connect(endpoints).await {
            Ok(connection) => Some(connection),
            Err(error) => {
                tracing::debug!(
                    maintenance_id = self.owner.maintenance_id,
                    %error,
                    "background node connection failed"
                );
                None
            }
        };
        let mut retry_now = false;
        let mut connection = connection;

        {
            let current_endpoints = self.owner.source.snapshot();
            let mut state = self.owner.state.lock();
            debug_assert!(state.connecting > 0);
            state.connecting = state.connecting.saturating_sub(1);

            if connection
                .as_ref()
                .is_some_and(|value| current_endpoints.contains(&value.endpoint))
            {
                state.idle.push_back(IdleConnection {
                    connection: connection.take().expect("connection was checked above"),
                    since: Instant::now(),
                });
                retry_now = state.total() < self.owner.options.min_connections;
            } else if connection.is_some() {
                retry_now = true;
            }
        }

        drop(connection);
        self.active = false;
        let _ = self.completion.send(maintenance::Command::Finished {
            id: self.owner.maintenance_id,
            retry_now,
        });
    }
}

impl Drop for BackgroundReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }

        {
            let mut state = self.owner.state.lock();
            debug_assert!(state.connecting > 0);
            state.connecting = state.connecting.saturating_sub(1);
        }
        let _ = self.completion.send(maintenance::Command::Finished {
            id: self.owner.maintenance_id,
            retry_now: false,
        });
    }
}

fn validate_options(options: NodePoolOptions) -> Result<()> {
    if options.max_connections == 0 {
        return Err(NetError::InvalidConfig(
            "max_connections must be greater than zero".into(),
        ));
    }
    if options.min_connections > options.max_connections {
        return Err(NetError::InvalidConfig(
            "min_connections cannot exceed max_connections".into(),
        ));
    }
    if options.idle_timeout.is_zero() {
        return Err(NetError::InvalidConfig(
            "idle_timeout must be greater than zero".into(),
        ));
    }
    Ok(())
}
