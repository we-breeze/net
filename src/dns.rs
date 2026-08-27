use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    future::Future,
    hash::{Hash, Hasher},
    io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Duration,
};

use parking_lot::Mutex;
use tokio::{
    net::lookup_host,
    sync::{mpsc, oneshot},
    time::{self, Instant, MissedTickBehavior},
};

use crate::{EndpointSet, EndpointSource, NetError, Result, source::normalize};

/// Refresh policy for the names used by one [`DnsSource`].
#[derive(Clone, Copy, Debug)]
pub struct DnsOptions {
    pub refresh_interval: Duration,
}

impl Default for DnsOptions {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(30),
        }
    }
}

/// Process-level DNS scheduler limits.
#[derive(Clone, Copy, Debug)]
pub struct DnsResolverOptions {
    /// Maximum number of system resolver calls running at once.
    pub max_concurrent_lookups: usize,
    /// New-host batching and change-coalescing cadence.
    pub scheduler_tick: Duration,
    /// Upper bound applied to one asynchronous resolver call.
    pub lookup_timeout: Duration,
}

impl Default for DnsResolverOptions {
    fn default() -> Self {
        Self {
            max_concurrent_lookups: 32,
            scheduler_tick: Duration::from_secs(1),
            lookup_timeout: Duration::from_secs(5),
        }
    }
}

/// A shared, IPv4-only DNS resolver.
///
/// One actor owns the hostname registry and schedules bounded lookups. The
/// request path never talks to that actor: it reads immutable endpoint
/// snapshots published through [`EndpointSet`]'s copy-on-write storage.
#[derive(Clone)]
pub struct DnsResolver {
    commands: mpsc::UnboundedSender<Command>,
}

impl DnsResolver {
    /// Start an independent resolver on the current Tokio runtime.
    pub fn new(options: DnsResolverOptions) -> Result<Self> {
        validate_resolver_options(options)?;
        let lookup: LookupFn = Arc::new(|host| Box::pin(lookup_ipv4(host)));
        Self::start(options, lookup)
    }

    /// Return the process-wide resolver used by [`DnsSource::new`].
    ///
    /// A stopped resolver is recreated. This matters in tests and embedding
    /// environments that create more than one Tokio runtime sequentially.
    pub fn shared() -> Result<Self> {
        static SHARED: OnceLock<Mutex<Option<DnsResolver>>> = OnceLock::new();

        let mut shared = SHARED.get_or_init(|| Mutex::new(None)).lock();
        if let Some(resolver) = shared.as_ref()
            && !resolver.commands.is_closed()
        {
            return Ok(resolver.clone());
        }

        let resolver = Self::new(DnsResolverOptions::default())?;
        *shared = Some(resolver.clone());
        Ok(resolver)
    }

    /// Resolve and watch one or more `host:port` authorities through this
    /// resolver.
    pub async fn source(
        &self,
        names: impl IntoIterator<Item = impl Into<String>>,
        options: DnsOptions,
    ) -> Result<DnsSource> {
        DnsSource::with_resolver(self, names, options).await
    }

    fn start(options: DnsResolverOptions, lookup: LookupFn) -> Result<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            NetError::InvalidConfig("DnsResolver must be created inside a Tokio runtime".into())
        })?;
        let (commands, command_rx) = mpsc::unbounded_channel();
        let actor = ResolverActor::new(options, lookup, command_rx);
        runtime.spawn(actor.run());
        Ok(Self { commands })
    }

    async fn register(
        &self,
        authorities: Vec<Authority>,
        refresh_interval: Duration,
    ) -> Result<RegistrationParts> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Register {
                authorities,
                refresh_interval,
                reply,
            })
            .map_err(|_| NetError::DnsResolverStopped)?;
        response.await.map_err(|_| NetError::DnsResolverStopped)
    }

    #[cfg(test)]
    fn with_lookup(options: DnsResolverOptions, lookup: LookupFn) -> Result<Self> {
        validate_resolver_options(options)?;
        Self::start(options, lookup)
    }
}

/// Resolves one or more `host:port` names and follows their IPv4 changes.
///
/// Clones share one registration. Dropping the last clone unregisters its
/// hostnames from the resolver when no other source is using them.
#[derive(Clone)]
pub struct DnsSource {
    endpoints: EndpointSet,
    _registration: Arc<DnsRegistration>,
}

impl DnsSource {
    /// Resolve through the process-wide [`DnsResolver`].
    pub async fn new(
        names: impl IntoIterator<Item = impl Into<String>>,
        options: DnsOptions,
    ) -> Result<Self> {
        let resolver = DnsResolver::shared()?;
        Self::with_resolver(&resolver, names, options).await
    }

    /// Resolve through an explicitly supplied shared resolver.
    pub async fn with_resolver(
        resolver: &DnsResolver,
        names: impl IntoIterator<Item = impl Into<String>>,
        options: DnsOptions,
    ) -> Result<Self> {
        if options.refresh_interval.is_zero() {
            return Err(NetError::InvalidConfig(
                "DNS refresh interval must be greater than zero".into(),
            ));
        }

        let mut authorities = names
            .into_iter()
            .map(|name| Authority::parse(name.into()))
            .collect::<Result<Vec<_>>>()?;
        authorities.sort();
        authorities.dedup();
        if authorities.is_empty() {
            return Err(NetError::NoEndpoints);
        }

        let parts = resolver
            .register(authorities, options.refresh_interval)
            .await?;
        let source = Self {
            endpoints: parts.endpoints,
            _registration: Arc::new(DnsRegistration {
                id: parts.id,
                commands: resolver.commands.clone(),
            }),
        };

        match parts.initial.await {
            Ok(Ok(())) => Ok(source),
            Ok(Err(error)) => Err(error.into_net_error()),
            Err(_) => Err(NetError::DnsResolverStopped),
        }
    }

    pub fn snapshot(&self) -> Arc<[SocketAddr]> {
        self.endpoints.snapshot()
    }
}

impl std::fmt::Debug for DnsSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DnsSource")
            .field("endpoints", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl EndpointSource for DnsSource {
    fn snapshot(&self) -> Arc<[SocketAddr]> {
        DnsSource::snapshot(self)
    }
}

struct DnsRegistration {
    id: u64,
    commands: mpsc::UnboundedSender<Command>,
}

impl Drop for DnsRegistration {
    fn drop(&mut self) {
        let _ = self.commands.send(Command::Unregister { id: self.id });
    }
}

struct RegistrationParts {
    id: u64,
    endpoints: EndpointSet,
    initial: oneshot::Receiver<std::result::Result<(), InitialError>>,
}

enum Command {
    Register {
        authorities: Vec<Authority>,
        refresh_interval: Duration,
        reply: oneshot::Sender<RegistrationParts>,
    },
    Unregister {
        id: u64,
    },
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Authority {
    host: Arc<str>,
    port: u16,
}

impl Authority {
    fn parse(name: String) -> Result<Self> {
        let name = name.trim();
        let Some((host, port)) = name.rsplit_once(':') else {
            return Err(invalid_authority(name));
        };
        let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
        if host.is_empty() || host.contains(':') {
            return Err(invalid_authority(name));
        }
        let port = port
            .trim()
            .parse::<u16>()
            .ok()
            .filter(|port| *port > 0)
            .ok_or_else(|| invalid_authority(name))?;
        Ok(Self {
            host: Arc::from(host),
            port,
        })
    }

    fn name(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

fn invalid_authority(name: &str) -> NetError {
    NetError::InvalidConfig(format!(
        "invalid DNS authority '{name}', expected an IPv4 hostname and port"
    ))
}

#[derive(Clone)]
struct Ipv4Snapshot {
    addresses: Arc<[Ipv4Addr]>,
    fingerprint: Ipv4Fingerprint,
}

impl Ipv4Snapshot {
    fn new(mut addresses: Vec<Ipv4Addr>) -> io::Result<Self> {
        addresses.sort_unstable();
        addresses.dedup();
        if addresses.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "DNS answer contains no IPv4 addresses",
            ));
        }
        let fingerprint = Ipv4Fingerprint::from(addresses.as_slice());
        Ok(Self {
            addresses: addresses.into(),
            fingerprint,
        })
    }

    fn same_set(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint && self.addresses == other.addresses
    }
}

/// Cheaply rejects changed answers before the exact slice comparison.
///
/// The original Breeze implementation used only length plus sum. Retaining an
/// exact comparison after this fingerprint removes collision-based missed
/// updates; DNS answers are normally tiny, so its cost is negligible compared
/// with the system resolver call.
#[derive(Clone, Copy, Eq, PartialEq)]
struct Ipv4Fingerprint {
    len: u32,
    sum: u64,
    xor: u32,
}

impl From<&[Ipv4Addr]> for Ipv4Fingerprint {
    fn from(addresses: &[Ipv4Addr]) -> Self {
        let (sum, xor) = addresses.iter().fold((0_u64, 0_u32), |(sum, xor), ip| {
            let value = u32::from(*ip);
            (sum.wrapping_add(value as u64), xor ^ value)
        });
        Self {
            len: addresses.len() as u32,
            sum,
            xor,
        }
    }
}

type LookupFuture = Pin<Box<dyn Future<Output = io::Result<Ipv4Snapshot>> + Send + 'static>>;
type LookupFn = Arc<dyn Fn(Arc<str>) -> LookupFuture + Send + Sync>;

async fn lookup_ipv4(host: Arc<str>) -> io::Result<Ipv4Snapshot> {
    let addresses = lookup_host((host.as_ref(), 0)).await?;
    Ipv4Snapshot::new(
        addresses
            .filter_map(|address| match address {
                SocketAddr::V4(address) => Some(*address.ip()),
                SocketAddr::V6(_) => None,
            })
            .collect(),
    )
}

struct ResolverActor {
    options: DnsResolverOptions,
    lookup: LookupFn,
    commands: mpsc::UnboundedReceiver<Command>,
    results: mpsc::UnboundedReceiver<LookupResult>,
    result_sender: mpsc::UnboundedSender<LookupResult>,
    hosts: HashMap<Arc<str>, HostRecord>,
    subscribers: HashMap<u64, Subscriber>,
    scheduled: BinaryHeap<Reverse<ScheduledLookup>>,
    initial_ready: VecDeque<Arc<str>>,
    ready: VecDeque<Arc<str>>,
    pending_initial_hosts: HashSet<Arc<str>>,
    dirty_subscribers: HashSet<u64>,
    next_subscriber_id: u64,
    next_query_id: u64,
    inflight: usize,
}

impl ResolverActor {
    fn new(
        options: DnsResolverOptions,
        lookup: LookupFn,
        commands: mpsc::UnboundedReceiver<Command>,
    ) -> Self {
        let (result_sender, results) = mpsc::unbounded_channel();
        Self {
            options,
            lookup,
            commands,
            results,
            result_sender,
            hosts: HashMap::new(),
            subscribers: HashMap::new(),
            scheduled: BinaryHeap::new(),
            initial_ready: VecDeque::new(),
            ready: VecDeque::new(),
            pending_initial_hosts: HashSet::new(),
            dirty_subscribers: HashSet::new(),
            next_subscriber_id: 1,
            next_query_id: 1,
            inflight: 0,
        }
    }

    async fn run(mut self) {
        let mut tick = time::interval_at(
            Instant::now() + self.options.scheduler_tick,
            self.options.scheduler_tick,
        );
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            self.dispatch_due();
            self.dispatch_ready();

            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        return;
                    };
                    self.handle_command(command);
                }
                result = self.results.recv() => {
                    if let Some(result) = result {
                        self.handle_result(result);
                    }
                }
                _ = tick.tick() => {
                    self.activate_new_hosts();
                    self.publish_dirty();
                }
            }
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::Register {
                authorities,
                refresh_interval,
                reply,
            } => self.register(authorities, refresh_interval, reply),
            Command::Unregister { id } => self.unregister(id),
        }
    }

    fn register(
        &mut self,
        authorities: Vec<Authority>,
        refresh_interval: Duration,
        reply: oneshot::Sender<RegistrationParts>,
    ) {
        let id = self.next_subscriber_id;
        self.next_subscriber_id = self.next_subscriber_id.wrapping_add(1).max(1);
        let endpoints = EndpointSet::new([]);
        let (initial, initial_result) = oneshot::channel();
        let now = Instant::now();
        let mut pending_initial = HashSet::new();
        let mut reschedule = Vec::new();
        for authority in &authorities {
            let record = self.hosts.entry(authority.host.clone()).or_default();
            record.subscribers.insert(id, refresh_interval);
            if !record.attempted {
                pending_initial.insert(authority.host.clone());
            } else if record.attempted && record.inflight.is_none() {
                let proposed = now + refresh_interval;
                if record.next_due.is_none_or(|due| proposed < due) {
                    reschedule.push((authority.host.clone(), proposed));
                }
            }
        }
        let subscriber = Subscriber {
            authorities: authorities.clone().into(),
            endpoints: endpoints.clone(),
            initial: Some(initial),
            pending_initial_hosts: pending_initial.len(),
        };
        self.subscribers.insert(id, subscriber);
        self.pending_initial_hosts.extend(pending_initial);
        for (host, due) in reschedule {
            self.schedule(host, due);
        }

        if self
            .subscribers
            .get(&id)
            .is_some_and(|subscriber| subscriber.pending_initial_hosts == 0)
        {
            self.publish_subscriber(id);
        }

        if reply
            .send(RegistrationParts {
                id,
                endpoints,
                initial: initial_result,
            })
            .is_err()
        {
            self.unregister(id);
        }
    }

    fn unregister(&mut self, id: u64) {
        let Some(subscriber) = self.subscribers.remove(&id) else {
            return;
        };
        self.dirty_subscribers.remove(&id);

        let mut remove = Vec::new();
        let mut reschedule = Vec::new();
        let now = Instant::now();
        for authority in subscriber.authorities.iter() {
            let Some(record) = self.hosts.get_mut(&authority.host) else {
                continue;
            };
            record.subscribers.remove(&id);
            if record.subscribers.is_empty() {
                self.pending_initial_hosts.remove(&authority.host);
                remove.push(authority.host.clone());
            } else if let Some(interval) = record.refresh_interval() {
                let proposed = now + interval;
                if record.next_due.is_none_or(|due| proposed < due) {
                    reschedule.push((authority.host.clone(), proposed));
                }
            }
        }
        for host in remove {
            self.hosts.remove(&host);
        }
        for (host, due) in reschedule {
            self.schedule(host, due);
        }
    }

    fn enqueue(&mut self, host: Arc<str>) {
        let Some(record) = self.hosts.get_mut(&host) else {
            return;
        };
        if record.queued || record.inflight.is_some() || record.subscribers.is_empty() {
            return;
        }
        record.queued = true;
        record.next_due = None;
        record.schedule_version = record.schedule_version.wrapping_add(1);
        self.ready.push_back(host);
    }

    fn activate_new_hosts(&mut self) {
        let hosts = std::mem::take(&mut self.pending_initial_hosts);
        for host in hosts {
            let Some(record) = self.hosts.get_mut(&host) else {
                continue;
            };
            if record.queued || record.inflight.is_some() || record.subscribers.is_empty() {
                continue;
            }
            record.queued = true;
            record.next_due = None;
            record.schedule_version = record.schedule_version.wrapping_add(1);
            self.initial_ready.push_back(host);
        }
    }

    fn schedule(&mut self, host: Arc<str>, due: Instant) {
        let Some(record) = self.hosts.get_mut(&host) else {
            return;
        };
        if record.subscribers.is_empty() {
            return;
        }
        record.schedule_version = record.schedule_version.wrapping_add(1);
        record.next_due = Some(due);
        self.scheduled.push(Reverse(ScheduledLookup {
            due,
            version: record.schedule_version,
            host,
        }));
    }

    fn dispatch_due(&mut self) {
        let now = Instant::now();
        let mut due_hosts = Vec::new();
        while self
            .scheduled
            .peek()
            .is_some_and(|scheduled| scheduled.0.due <= now)
        {
            let Reverse(scheduled) = self.scheduled.pop().expect("peeked scheduled lookup");
            let valid = self.hosts.get(&scheduled.host).is_some_and(|record| {
                record.schedule_version == scheduled.version
                    && record.next_due == Some(scheduled.due)
            });
            if valid {
                due_hosts.push(scheduled.host);
            }
        }
        for host in due_hosts {
            self.enqueue(host);
        }
    }

    fn dispatch_ready(&mut self) {
        while self.inflight < self.options.max_concurrent_lookups {
            let Some(host) = self
                .initial_ready
                .pop_front()
                .or_else(|| self.ready.pop_front())
            else {
                return;
            };
            let Some(record) = self.hosts.get_mut(&host) else {
                continue;
            };
            if !record.queued || record.inflight.is_some() || record.subscribers.is_empty() {
                continue;
            }

            let query_id = self.next_query_id;
            self.next_query_id = self.next_query_id.wrapping_add(1).max(1);
            record.queued = false;
            record.inflight = Some(query_id);
            self.inflight += 1;

            let lookup = self.lookup.clone();
            let result_sender = self.result_sender.clone();
            let timeout = self.options.lookup_timeout;
            tokio::spawn(async move {
                let lookup_host = host.clone();
                let result = match time::timeout(timeout, lookup(lookup_host)).await {
                    Ok(result) => result,
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "DNS lookup timed out",
                    )),
                };
                let _ = result_sender.send(LookupResult {
                    host,
                    query_id,
                    result,
                });
            });
        }
    }

    fn handle_result(&mut self, result: LookupResult) {
        self.inflight = self.inflight.saturating_sub(1);
        let now = Instant::now();
        let mut dirty = Vec::new();

        let Some(record) = self.hosts.get_mut(&result.host) else {
            return;
        };
        if record.inflight != Some(result.query_id) {
            return;
        }
        record.inflight = None;
        let first_attempt = !record.attempted;
        record.attempted = true;
        let initial_subscribers = if first_attempt {
            record.subscribers.keys().copied().collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        let delay = match result.result {
            Ok(snapshot) => {
                let changed = record
                    .snapshot
                    .as_ref()
                    .is_none_or(|current| !current.same_set(&snapshot));
                if changed {
                    record.snapshot = Some(snapshot);
                }
                record.last_error = None;
                record.failures = 0;
                if changed && !first_attempt {
                    dirty.extend(record.subscribers.keys().copied());
                }
                record.refresh_interval().unwrap_or(Duration::from_secs(30))
            }
            Err(error) => {
                record.failures = record.failures.saturating_add(1);
                record.last_error = Some(LookupFailure::from(error));
                retry_delay(
                    record.failures,
                    record.refresh_interval().unwrap_or(Duration::from_secs(30)),
                )
            }
        };
        let has_subscribers = !record.subscribers.is_empty();
        let due = now + jittered(delay, &result.host);
        let _ = record;

        self.dirty_subscribers.extend(dirty);
        let mut initial_ready = Vec::new();
        for id in initial_subscribers {
            let Some(subscriber) = self.subscribers.get_mut(&id) else {
                continue;
            };
            subscriber.pending_initial_hosts = subscriber.pending_initial_hosts.saturating_sub(1);
            if subscriber.pending_initial_hosts == 0 {
                initial_ready.push(id);
            }
        }
        for id in initial_ready {
            self.dirty_subscribers.remove(&id);
            self.publish_subscriber(id);
        }
        if has_subscribers {
            self.schedule(result.host, due);
        }
    }

    fn publish_dirty(&mut self) {
        let dirty = std::mem::take(&mut self.dirty_subscribers);
        for id in dirty {
            if self
                .subscribers
                .get(&id)
                .is_some_and(|subscriber| subscriber.pending_initial_hosts > 0)
            {
                continue;
            }
            self.publish_subscriber(id);
        }
    }

    fn publish_subscriber(&mut self, id: u64) {
        let Some(subscriber) = self.subscribers.get_mut(&id) else {
            return;
        };
        let mut endpoints = Vec::new();
        let mut first_error = None;

        for authority in subscriber.authorities.iter() {
            let Some(record) = self.hosts.get(&authority.host) else {
                continue;
            };
            if let Some(snapshot) = &record.snapshot {
                endpoints.extend(
                    snapshot
                        .addresses
                        .iter()
                        .map(|ip| SocketAddr::V4(SocketAddrV4::new(*ip, authority.port))),
                );
            } else if first_error.is_none()
                && let Some(error) = &record.last_error
            {
                first_error = Some((authority.name(), error.clone()));
            }
        }

        let endpoints = normalize(endpoints);
        subscriber.endpoints.replace(endpoints.iter().copied());
        if let Some(initial) = subscriber.initial.take() {
            let result = if endpoints.is_empty() {
                Err(match first_error {
                    Some((name, failure)) => InitialError::Resolve { name, failure },
                    None => InitialError::NoEndpoints,
                })
            } else {
                Ok(())
            };
            let _ = initial.send(result);
        }
    }
}

#[derive(Default)]
struct HostRecord {
    snapshot: Option<Ipv4Snapshot>,
    attempted: bool,
    last_error: Option<LookupFailure>,
    subscribers: HashMap<u64, Duration>,
    schedule_version: u64,
    next_due: Option<Instant>,
    queued: bool,
    inflight: Option<u64>,
    failures: u32,
}

impl HostRecord {
    fn refresh_interval(&self) -> Option<Duration> {
        self.subscribers.values().copied().min()
    }
}

struct Subscriber {
    authorities: Arc<[Authority]>,
    endpoints: EndpointSet,
    initial: Option<oneshot::Sender<std::result::Result<(), InitialError>>>,
    pending_initial_hosts: usize,
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct ScheduledLookup {
    due: Instant,
    version: u64,
    host: Arc<str>,
}

struct LookupResult {
    host: Arc<str>,
    query_id: u64,
    result: io::Result<Ipv4Snapshot>,
}

#[derive(Clone, Debug)]
struct LookupFailure {
    kind: io::ErrorKind,
    message: Arc<str>,
}

impl From<io::Error> for LookupFailure {
    fn from(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: Arc::from(error.to_string()),
        }
    }
}

#[derive(Debug)]
enum InitialError {
    NoEndpoints,
    Resolve {
        name: String,
        failure: LookupFailure,
    },
}

impl InitialError {
    fn into_net_error(self) -> NetError {
        match self {
            Self::NoEndpoints => NetError::NoEndpoints,
            Self::Resolve { name, failure } => NetError::Resolve {
                name,
                source: io::Error::new(failure.kind, failure.message.to_string()),
            },
        }
    }
}

fn retry_delay(failures: u32, refresh_interval: Duration) -> Duration {
    let shift = failures.saturating_sub(1).min(6);
    Duration::from_secs(1_u64 << shift).min(refresh_interval)
}

fn jittered(delay: Duration, host: &str) -> Duration {
    let jitter_window = delay / 10;
    if jitter_window.is_zero() {
        return delay;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    host.hash(&mut hasher);
    let nanos = jitter_window.as_nanos().min(u64::MAX as u128) as u64;
    delay + Duration::from_nanos(hasher.finish() % nanos.max(1))
}

fn validate_resolver_options(options: DnsResolverOptions) -> Result<()> {
    if options.max_concurrent_lookups == 0 {
        return Err(NetError::InvalidConfig(
            "max_concurrent_lookups must be greater than zero".into(),
        ));
    }
    if options.scheduler_tick.is_zero() {
        return Err(NetError::InvalidConfig(
            "DNS scheduler_tick must be greater than zero".into(),
        ));
    }
    if options.lookup_timeout.is_zero() {
        return Err(NetError::InvalidConfig(
            "DNS lookup_timeout must be greater than zero".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    use tokio::sync::Semaphore;

    use super::*;

    fn test_options(max_concurrent_lookups: usize) -> DnsResolverOptions {
        DnsResolverOptions {
            max_concurrent_lookups,
            scheduler_tick: Duration::from_millis(2),
            lookup_timeout: Duration::from_secs(5),
        }
    }

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        time::timeout(Duration::from_secs(5), async {
            while !condition() {
                time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("condition was not met");
    }

    #[tokio::test(start_paused = true)]
    async fn new_hosts_are_looked_up_on_the_next_tick_not_the_refresh_interval() {
        let calls = Arc::new(AtomicUsize::new(0));
        let lookup: LookupFn = {
            let calls = calls.clone();
            Arc::new(move |_host| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ipv4Snapshot::new(vec![Ipv4Addr::LOCALHOST]) })
            })
        };
        let resolver = DnsResolver::with_lookup(
            DnsResolverOptions {
                max_concurrent_lookups: 1,
                scheduler_tick: Duration::from_secs(1),
                lookup_timeout: Duration::from_secs(5),
            },
            lookup,
        )
        .unwrap();
        let source = tokio::spawn(async move {
            resolver
                .source(
                    ["new.example:80"],
                    DnsOptions {
                        refresh_interval: Duration::from_secs(3600),
                    },
                )
                .await
        });

        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        time::advance(Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        time::advance(Duration::from_millis(1)).await;
        let source = source.await.unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            source.snapshot().as_ref(),
            ["127.0.0.1:80".parse().unwrap()]
        );
    }

    #[tokio::test]
    async fn new_hosts_have_priority_over_the_refresh_backlog() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let lookup: LookupFn = {
            let order = order.clone();
            Arc::new(move |host| {
                order.lock().push(host);
                Box::pin(async { Ipv4Snapshot::new(vec![Ipv4Addr::LOCALHOST]) })
            })
        };
        let (_commands, command_rx) = mpsc::unbounded_channel();
        let mut actor = ResolverActor::new(test_options(1), lookup, command_rx);
        let initial_host: Arc<str> = Arc::from("new.example");
        let refresh_host: Arc<str> = Arc::from("refresh.example");
        for host in [&initial_host, &refresh_host] {
            let mut record = HostRecord {
                queued: true,
                ..HostRecord::default()
            };
            record.subscribers.insert(1, Duration::from_secs(30));
            actor.hosts.insert(host.clone(), record);
        }
        actor.initial_ready.push_back(initial_host.clone());
        actor.ready.push_back(refresh_host);

        actor.dispatch_ready();
        wait_until(|| !order.lock().is_empty()).await;

        assert_eq!(order.lock().as_slice(), [initial_host]);
    }

    #[tokio::test]
    async fn shared_hosts_are_resolved_once_and_publish_ipv4_changes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let address = Arc::new(AtomicU32::new(u32::from(Ipv4Addr::new(192, 0, 2, 1))));
        let lookup: LookupFn = {
            let calls = calls.clone();
            let address = address.clone();
            Arc::new(move |_host| {
                calls.fetch_add(1, Ordering::SeqCst);
                let address = Ipv4Addr::from(address.load(Ordering::SeqCst));
                Box::pin(async move { Ipv4Snapshot::new(vec![address]) })
            })
        };
        let resolver = DnsResolver::with_lookup(test_options(4), lookup).unwrap();

        let (first, second) = tokio::join!(
            resolver.source(
                ["shared.example:6379"],
                DnsOptions {
                    refresh_interval: Duration::from_millis(20),
                },
            ),
            resolver.source(
                ["shared.example:6380"],
                DnsOptions {
                    refresh_interval: Duration::from_millis(20),
                },
            )
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.snapshot()[0], "192.0.2.1:6379".parse().unwrap());
        assert_eq!(second.snapshot()[0], "192.0.2.1:6380".parse().unwrap());

        address.store(u32::from(Ipv4Addr::new(192, 0, 2, 2)), Ordering::SeqCst);
        wait_until(|| first.snapshot()[0] == "192.0.2.2:6379".parse().unwrap()).await;
        assert_eq!(second.snapshot()[0], "192.0.2.2:6380".parse().unwrap());
    }

    #[tokio::test]
    async fn lookup_failures_keep_the_last_good_snapshot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let lookup: LookupFn = {
            let calls = calls.clone();
            Arc::new(move |_host| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if call == 0 {
                        Ipv4Snapshot::new(vec![Ipv4Addr::new(198, 51, 100, 1)])
                    } else {
                        Err(io::Error::new(io::ErrorKind::TimedOut, "resolver down"))
                    }
                })
            })
        };
        let resolver = DnsResolver::with_lookup(test_options(2), lookup).unwrap();
        let source = resolver
            .source(
                ["stable.example:80"],
                DnsOptions {
                    refresh_interval: Duration::from_millis(15),
                },
            )
            .await
            .unwrap();

        wait_until(|| calls.load(Ordering::SeqCst) >= 2).await;
        assert_eq!(source.snapshot().as_ref(), ["198.51.100.1:80".parse().unwrap()]);
    }

    #[tokio::test]
    async fn ten_thousand_names_use_bounded_lookup_concurrency() {
        const NAMES: usize = 10_000;
        const LIMIT: usize = 16;

        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));
        let lookup: LookupFn = {
            let active = active.clone();
            let maximum = maximum.clone();
            let gate = gate.clone();
            Arc::new(move |_host| {
                let active = active.clone();
                let maximum = maximum.clone();
                let gate = gate.clone();
                Box::pin(async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(now, Ordering::SeqCst);
                    let permit = gate.acquire().await.expect("gate open");
                    permit.forget();
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ipv4Snapshot::new(vec![Ipv4Addr::LOCALHOST])
                })
            })
        };
        let resolver = DnsResolver::with_lookup(test_options(LIMIT), lookup).unwrap();
        let mut registrations = Vec::with_capacity(NAMES);
        for index in 0..NAMES {
            let authority = Authority::parse(format!("host-{index}.example:80")).unwrap();
            registrations.push(
                resolver
                    .register(vec![authority], Duration::from_secs(3600))
                    .await
                    .unwrap(),
            );
        }

        wait_until(|| active.load(Ordering::SeqCst) == LIMIT).await;
        assert_eq!(maximum.load(Ordering::SeqCst), LIMIT);
        gate.add_permits(NAMES);

        time::timeout(Duration::from_secs(10), async {
            for registration in registrations {
                registration.initial.await.unwrap().unwrap();
                let _ = resolver.commands.send(Command::Unregister {
                    id: registration.id,
                });
            }
        })
        .await
        .expect("all initial lookups completed");
        assert!(maximum.load(Ordering::SeqCst) <= LIMIT);
    }

    #[test]
    fn fingerprint_never_replaces_exact_set_comparison() {
        let first = Ipv4Snapshot::new(vec![Ipv4Addr::new(192, 0, 2, 4), Ipv4Addr::new(192, 0, 2, 7)])
            .unwrap();
        let colliding =
            Ipv4Snapshot::new(vec![Ipv4Addr::new(192, 0, 2, 5), Ipv4Addr::new(192, 0, 2, 6)])
                .unwrap();

        assert_eq!(first.fingerprint.len, colliding.fingerprint.len);
        assert_eq!(first.fingerprint.sum, colliding.fingerprint.sum);
        assert_eq!(first.fingerprint.xor, colliding.fingerprint.xor);
        assert!(!first.same_set(&colliding));
    }
}
