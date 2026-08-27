use std::{
    collections::VecDeque,
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use tokio::{net::TcpStream, sync::Notify};

use crate::{
    BoxFuture, BrzTcpStream, DnsOptions, DnsSource, EndpointSet, EndpointSource, NetError, Result,
    StreamProvider, stream::ManagedConnection,
};

#[derive(Clone, Copy, Debug)]
pub struct NodePoolOptions {
    pub max_connections: usize,
    pub max_idle_connections: usize,
    pub idle_timeout: Duration,
    pub tcp_nodelay: bool,
}

impl Default for NodePoolOptions {
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_idle_connections: 32,
            idle_timeout: Duration::from_secs(60),
            tcp_nodelay: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePoolStats {
    pub total_connections: usize,
    pub idle_connections: usize,
    pub checked_out_connections: usize,
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
    _source: Arc<dyn EndpointSource>,
    endpoints: tokio::sync::watch::Receiver<Arc<[SocketAddr]>>,
    state: Mutex<ConnectionState>,
    notify: Notify,
    next_endpoint: AtomicUsize,
    options: NodePoolOptions,
}

#[derive(Default)]
struct ConnectionState {
    idle: VecDeque<IdleConnection>,
    total: usize,
}

struct IdleConnection {
    connection: ManagedConnection,
    since: Instant,
}

impl NodePool {
    pub fn new(source: impl EndpointSource + 'static, options: NodePoolOptions) -> Result<Self> {
        validate_options(options)?;
        let source: Arc<dyn EndpointSource> = Arc::new(source);
        let endpoints = source.subscribe();
        Ok(Self {
            inner: Arc::new(NodePoolInner {
                _source: source,
                endpoints,
                state: Mutex::new(ConnectionState::default()),
                notify: Notify::new(),
                next_endpoint: AtomicUsize::new(0),
                options,
            }),
        })
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

    pub fn stats(&self) -> NodePoolStats {
        let state = self.inner.state.lock();
        NodePoolStats {
            total_connections: state.total,
            idle_connections: state.idle.len(),
            checked_out_connections: state.total.saturating_sub(state.idle.len()),
        }
    }

    async fn acquire_stream(&self) -> Result<BrzTcpStream> {
        loop {
            let endpoints = self.inner.endpoints.borrow().clone();
            let notified = self.inner.notify.notified();

            let (action, freed) = {
                let mut state = self.inner.state.lock();
                let freed = prune_idle(&mut state, &endpoints, self.inner.options.idle_timeout);

                let action = if endpoints.is_empty() {
                    AcquireAction::NoEndpoints
                } else if let Some(idle) = state.idle.pop_back() {
                    AcquireAction::Reuse(idle.connection)
                } else if state.total < self.inner.options.max_connections {
                    state.total += 1;
                    AcquireAction::Connect(endpoints)
                } else {
                    AcquireAction::Wait
                };
                (action, freed)
            };

            if freed > 0 {
                self.inner.notify.notify_waiters();
            }

            match action {
                AcquireAction::NoEndpoints => return Err(NetError::NoEndpoints),
                AcquireAction::Reuse(connection) => {
                    return Ok(BrzTcpStream::new(connection, Arc::downgrade(&self.inner)));
                }
                AcquireAction::Connect(endpoints) => {
                    let mut reservation = ConnectionReservation::new(self.inner.clone());
                    let connection = self.inner.connect(endpoints).await?;
                    reservation.commit();
                    return Ok(BrzTcpStream::new(connection, Arc::downgrade(&self.inner)));
                }
                AcquireAction::Wait => notified.await,
            }
        }
    }
}

impl fmt::Debug for NodePool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodePool")
            .field("endpoints", &self.inner.endpoints.borrow().as_ref())
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
        let endpoints = self.endpoints.borrow().clone();
        let mut connection = Some(connection);
        let mut state = self.state.lock();
        let freed = prune_idle(&mut state, &endpoints, self.options.idle_timeout);
        let active =
            endpoints.contains(&connection.as_ref().expect("connection is present").endpoint);

        if active && state.idle.len() < self.options.max_idle_connections {
            state.idle.push_back(IdleConnection {
                connection: connection.take().expect("connection is present"),
                since: Instant::now(),
            });
        } else {
            state.total = state.total.saturating_sub(1);
        }
        drop(state);
        drop(connection);

        if freed > 0 {
            self.notify.notify_waiters();
        } else {
            self.notify.notify_one();
        }
    }

    pub(crate) fn discard_connection(&self, connection: ManagedConnection) {
        drop(connection);
        let mut state = self.state.lock();
        state.total = state.total.saturating_sub(1);
        drop(state);
        self.notify.notify_one();
    }

    fn release_reservation(&self) {
        let mut state = self.state.lock();
        state.total = state.total.saturating_sub(1);
        drop(state);
        self.notify.notify_one();
    }
}

enum AcquireAction {
    NoEndpoints,
    Reuse(ManagedConnection),
    Connect(Arc<[SocketAddr]>),
    Wait,
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
        self.active = false;
    }
}

impl Drop for ConnectionReservation {
    fn drop(&mut self) {
        if self.active {
            self.owner.release_reservation();
        }
    }
}

fn validate_options(options: NodePoolOptions) -> Result<()> {
    if options.max_connections == 0 {
        return Err(NetError::InvalidConfig(
            "max_connections must be greater than zero".into(),
        ));
    }
    if options.max_idle_connections > options.max_connections {
        return Err(NetError::InvalidConfig(
            "max_idle_connections cannot exceed max_connections".into(),
        ));
    }
    Ok(())
}

fn prune_idle(
    state: &mut ConnectionState,
    endpoints: &[SocketAddr],
    idle_timeout: Duration,
) -> usize {
    let now = Instant::now();
    let before = state.idle.len();
    state.idle.retain(|idle| {
        endpoints.contains(&idle.connection.endpoint)
            && now.duration_since(idle.since) < idle_timeout
    });
    let removed = before - state.idle.len();
    state.total = state.total.saturating_sub(removed);
    removed
}
