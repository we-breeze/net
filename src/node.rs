use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crossbeam_queue::ArrayQueue;
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
pub struct NodePool<S = EndpointSet> {
    source: S,
    core: Arc<NodePoolCore>,
}

impl<S: Clone> Clone for NodePool<S> {
    fn clone(&self) -> Self {
        Self {
            source: self.source.clone(),
            core: self.core.clone(),
        }
    }
}

pub(crate) struct NodePoolCore {
    maintenance_id: u64,
    runtime: Handle,
    idle: ArrayQueue<IdleConnection>,
    total: AtomicUsize,
    connecting: AtomicUsize,
    maintenance_owned: AtomicUsize,
    next_endpoint: AtomicUsize,
    options: NodePoolOptions,
}

struct IdleConnection {
    connection: ManagedConnection,
    since: Instant,
}

impl<S: EndpointSource> NodePool<S> {
    pub fn new(source: S, options: NodePoolOptions) -> Result<Self> {
        validate_options(options)?;
        let runtime = Handle::try_current().map_err(|_| {
            NetError::InvalidConfig("NodePool must be created inside a Tokio runtime".into())
        })?;
        let core = Arc::new(NodePoolCore {
            maintenance_id: NEXT_MAINTENANCE_ID.fetch_add(1, Ordering::Relaxed),
            runtime,
            idle: ArrayQueue::new(options.max_connections),
            total: AtomicUsize::new(0),
            connecting: AtomicUsize::new(0),
            maintenance_owned: AtomicUsize::new(0),
            next_endpoint: AtomicUsize::new(0),
            options,
        });
        maintenance::register(&core, source.endpoint_set().clone())?;
        Ok(Self { source, core })
    }

    pub fn stats(&self) -> NodePoolStats {
        self.core.stats()
    }

    async fn acquire_stream(&self) -> Result<BrzTcpStream> {
        let endpoints = self.source.snapshot();
        if endpoints.is_empty() {
            return Err(NetError::NoEndpoints);
        }

        if let Some(connection) = self.core.pop_idle() {
            return Ok(BrzTcpStream::new(connection, Arc::downgrade(&self.core)));
        }
        if self
            .core
            .reserve_connection(self.core.options.max_connections)
        {
            return self.connect_stream(endpoints).await;
        }

        // Close the race with a concurrent recycle after the first empty pop.
        if let Some(connection) = self.core.pop_idle() {
            return Ok(BrzTcpStream::new(connection, Arc::downgrade(&self.core)));
        }
        // Close the equivalent race with background removal freeing capacity.
        if self
            .core
            .reserve_connection(self.core.options.max_connections)
        {
            return self.connect_stream(endpoints).await;
        }

        Err(NetError::PoolExhausted {
            max_connections: self.core.options.max_connections,
        })
    }

    async fn connect_stream(&self, endpoints: Arc<[SocketAddr]>) -> Result<BrzTcpStream> {
        let mut reservation = ConnectionReservation::new(self.core.clone());
        let connection = self.core.connect(endpoints).await?;
        reservation.commit();
        Ok(BrzTcpStream::new(connection, Arc::downgrade(&self.core)))
    }
}

impl NodePool<EndpointSet> {
    pub fn from_endpoints(
        endpoints: impl IntoIterator<Item = SocketAddr>,
        options: NodePoolOptions,
    ) -> Result<Self> {
        Self::new(EndpointSet::new(endpoints), options)
    }
}

impl NodePool<DnsSource> {
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
}

impl<S: EndpointSource> fmt::Debug for NodePool<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodePool")
            .field("endpoints", &self.source.snapshot())
            .field("options", &self.core.options)
            .field("stats", &self.stats())
            .finish()
    }
}

impl<K: ?Sized + Sync, S: EndpointSource> StreamProvider<K> for NodePool<S> {
    fn acquire<'a>(&'a self, _key: &'a K) -> BoxFuture<'a, Result<BrzTcpStream>> {
        Box::pin(async move { self.acquire_stream().await })
    }
}

impl NodePoolCore {
    pub(crate) fn maintenance_id(&self) -> u64 {
        self.maintenance_id
    }

    pub(crate) fn runtime(&self) -> Handle {
        self.runtime.clone()
    }

    fn stats(&self) -> NodePoolStats {
        let total = self.total.load(Ordering::Acquire);
        let idle = self.idle.len();
        let connecting = self.connecting.load(Ordering::Acquire);
        let maintenance_owned = self.maintenance_owned.load(Ordering::Acquire);
        NodePoolStats {
            total_connections: total,
            idle_connections: idle,
            checked_out_connections: total
                .saturating_sub(idle)
                .saturating_sub(connecting)
                .saturating_sub(maintenance_owned),
            connecting_connections: connecting,
        }
    }

    #[inline]
    fn pop_idle(&self) -> Option<ManagedConnection> {
        self.idle.pop().map(|idle| idle.connection)
    }

    #[inline]
    fn reserve_connection(&self, limit: usize) -> bool {
        let reserved = self
            .total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |total| {
                (total < limit).then_some(total + 1)
            })
            .is_ok();
        if reserved {
            self.connecting.fetch_add(1, Ordering::Relaxed);
        }
        reserved
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
        self.push_idle(IdleConnection {
            connection,
            since: Instant::now(),
        });
    }

    pub(crate) fn discard_connection(&self, connection: ManagedConnection) {
        self.release_total();
        drop(connection);
    }

    fn push_idle(&self, idle: IdleConnection) -> bool {
        match self.idle.push(idle) {
            Ok(()) => true,
            Err(idle) => {
                tracing::error!(
                    maintenance_id = self.maintenance_id,
                    max_connections = self.options.max_connections,
                    "idle connection queue reached an impossible full state; discarding connection"
                );
                self.release_total();
                drop(idle);
                false
            }
        }
    }

    fn finish_connection(&self) {
        self.release_connecting();
    }

    fn release_reservation(&self) {
        self.release_connecting();
        self.release_total();
    }

    fn release_connecting(&self) {
        let previous = self.connecting.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0);
    }

    fn release_total(&self) {
        let result = self
            .total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |total| {
                total.checked_sub(1)
            });
        debug_assert!(result.is_ok());
    }

    fn release_expired_above_min(&self) -> bool {
        self.total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |total| {
                (total > self.options.min_connections).then_some(total - 1)
            })
            .is_ok()
    }

    /// Perform endpoint and idle-timeout cleanup outside request acquisition.
    pub(crate) fn maintenance_tick(&self, source: &EndpointSet) -> bool {
        let endpoints = source.snapshot();
        let scan_count = self.idle.len();
        let now = Instant::now();

        for _ in 0..scan_count {
            let Some(idle) = self.idle.pop() else {
                break;
            };
            self.maintenance_owned.fetch_add(1, Ordering::Relaxed);

            let active = endpoints.contains(&idle.connection.endpoint);
            let expired = now.duration_since(idle.since) >= self.options.idle_timeout;
            if !active {
                self.release_total();
                drop(idle);
            } else if expired && self.release_expired_above_min() {
                drop(idle);
            } else {
                self.push_idle(idle);
            }

            let previous = self.maintenance_owned.fetch_sub(1, Ordering::Relaxed);
            debug_assert!(previous > 0);
        }

        !endpoints.is_empty() && self.total.load(Ordering::Acquire) < self.options.min_connections
    }

    pub(crate) fn reserve_background_connection(
        self: &Arc<Self>,
        source: EndpointSet,
        completion: std::sync::mpsc::Sender<maintenance::Command>,
    ) -> Option<(Arc<[SocketAddr]>, BackgroundReservation)> {
        let endpoints = source.snapshot();
        if endpoints.is_empty() || !self.reserve_connection(self.options.min_connections) {
            return None;
        }

        Some((
            endpoints,
            BackgroundReservation {
                owner: self.clone(),
                source,
                completion,
                active: true,
            },
        ))
    }
}

struct ConnectionReservation {
    owner: Arc<NodePoolCore>,
    active: bool,
}

impl ConnectionReservation {
    fn new(owner: Arc<NodePoolCore>) -> Self {
        Self {
            owner,
            active: true,
        }
    }

    fn commit(&mut self) {
        self.owner.finish_connection();
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

pub(crate) struct BackgroundReservation {
    owner: Arc<NodePoolCore>,
    source: EndpointSet,
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
        self.owner.release_connecting();

        let mut retry_now = false;
        if let Some(connection) = connection {
            if self.source.snapshot().contains(&connection.endpoint) {
                retry_now = !self.owner.push_idle(IdleConnection {
                    connection,
                    since: Instant::now(),
                }) || self.owner.total.load(Ordering::Acquire)
                    < self.owner.options.min_connections;
            } else {
                self.owner.release_total();
                drop(connection);
                retry_now = true;
            }
        } else {
            self.owner.release_total();
        }

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

        self.owner.release_reservation();
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
