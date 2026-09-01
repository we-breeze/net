use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{
    NetError, Node, RequestTarget, RequestToken, ResponseFuture, Result, SessionError,
    SessionProtocol,
};

#[derive(Clone, Copy, Debug)]
pub struct QuotaBalancerOptions {
    /// Completed request time consumed before advancing to the next replica.
    pub quota: Duration,
    /// Minimum quota charged for a failed request.
    pub failure_penalty: Duration,
}

impl Default for QuotaBalancerOptions {
    fn default() -> Self {
        Self {
            quota: Duration::from_secs(2),
            failure_penalty: Duration::from_millis(500),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaSnapshot {
    pub used_quota: Duration,
    pub current: bool,
}

/// Lock-free selector that keeps traffic on one replica until its accumulated
/// request-time quota is consumed.
///
/// Selection only touches atomics. A dropped ticket is charged as a failure,
/// so fail-fast admission errors naturally advance an unhealthy replica.
#[derive(Clone)]
pub struct QuotaSelector {
    inner: Arc<BalancerInner>,
}

struct BalancerInner {
    current: AtomicUsize,
    replicas: Box<[ReplicaState]>,
    quota_micros: u64,
    failure_penalty_micros: u64,
}

struct ReplicaState {
    used_micros: AtomicU64,
}

impl QuotaSelector {
    pub fn new(replicas: usize, options: QuotaBalancerOptions) -> Result<Self> {
        Self::with_initial(replicas, options, 0)
    }

    /// Creates a selector whose first choice is `initial % replicas`.
    ///
    /// This is useful when replica order has semantic meaning (for example,
    /// CacheService keeps master at index zero) and therefore cannot be
    /// shuffled merely to randomize the process-wide starting point.
    pub fn with_initial(
        replicas: usize,
        options: QuotaBalancerOptions,
        initial: usize,
    ) -> Result<Self> {
        validate_options(options)?;
        if replicas == 0 {
            return Err(NetError::NoReplicas);
        }

        Ok(Self {
            inner: Arc::new(BalancerInner {
                current: AtomicUsize::new(initial % replicas),
                replicas: (0..replicas)
                    .map(|_| ReplicaState {
                        used_micros: AtomicU64::new(0),
                    })
                    .collect(),
                quota_micros: duration_micros(options.quota),
                failure_penalty_micros: duration_micros(options.failure_penalty),
            }),
        })
    }

    #[inline]
    pub fn select(&self) -> QuotaTicket {
        let count = self.inner.replicas.len();
        let mut index = self.inner.current.load(Ordering::Relaxed);

        if count > 1
            && self.inner.replicas[index]
                .used_micros
                .load(Ordering::Relaxed)
                >= self.inner.quota_micros
        {
            let next = (index + 1) % count;
            if self
                .inner
                .current
                .compare_exchange(index, next, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.inner.replicas[index]
                    .used_micros
                    .store(0, Ordering::Relaxed);
                index = next;
            } else {
                index = self.inner.current.load(Ordering::Acquire);
            }
        }

        self.ticket(index)
    }

    #[inline]
    fn ticket(&self, index: usize) -> QuotaTicket {
        debug_assert!(index < self.inner.replicas.len());
        QuotaTicket {
            balancer: self.inner.clone(),
            index,
            started: Instant::now(),
            finished: false,
        }
    }

    pub fn snapshots(&self) -> Vec<ReplicaSnapshot> {
        let current = self.inner.current.load(Ordering::Relaxed);
        self.inner
            .replicas
            .iter()
            .enumerate()
            .map(|(index, replica)| ReplicaSnapshot {
                used_quota: Duration::from_micros(replica.used_micros.load(Ordering::Relaxed)),
                current: index == current,
            })
            .collect()
    }
}

/// One timed selection from a [`QuotaSelector`].
///
/// Call [`QuotaTicket::success`] for any valid protocol response, including a
/// cache miss. Transport/protocol failure is represented by `failure`, while
/// dropping without finishing is conservatively treated as failure.
pub struct QuotaTicket {
    balancer: Arc<BalancerInner>,
    index: usize,
    started: Instant,
    finished: bool,
}

impl QuotaTicket {
    pub fn index(&self) -> usize {
        self.index
    }

    pub(crate) fn sibling(&self) -> Self {
        Self {
            balancer: self.balancer.clone(),
            index: self.index,
            started: Instant::now(),
            finished: false,
        }
    }

    pub fn success(mut self) {
        let elapsed = self.started.elapsed();
        self.record(elapsed, true);
        self.finished = true;
    }

    pub fn failure(mut self) {
        let elapsed = self.started.elapsed();
        self.record(elapsed, false);
        self.finished = true;
    }

    fn record(&self, elapsed: Duration, succeeded: bool) {
        let elapsed = duration_micros(elapsed);
        let charge = if succeeded {
            elapsed
        } else {
            elapsed.max(self.balancer.failure_penalty_micros)
        };
        self.balancer.replicas[self.index]
            .used_micros
            .fetch_add(charge, Ordering::Relaxed);
    }
}

impl Drop for QuotaTicket {
    fn drop(&mut self) {
        if !self.finished {
            self.record(self.started.elapsed(), false);
        }
    }
}

/// Equivalent physical nodes selected by consumed request-time quota.
#[derive(Clone)]
pub struct ReplicaSet<N> {
    replicas: Arc<[N]>,
    balancer: QuotaSelector,
}

/// One physical session target that can participate in replica selection.
///
/// Protocol crates may wrap a [`Node`] with endpoint-local state such as
/// metrics while keeping selection, retry, and quota accounting in `brz-net`.
/// All dispatch remains static; the request path does not allocate a callback
/// or use dynamic dispatch.
pub trait SessionReplica: Clone + Send + Sync + 'static {
    type Request: Send + 'static;
    type Response: Send + 'static;
    type Error: std::error::Error + Send + Sync + 'static;
    type Future: std::future::Future<Output = std::result::Result<Self::Response, SessionError<Self::Error>>>
        + Send
        + Unpin
        + 'static;

    fn request(
        &self,
        request: Self::Request,
    ) -> std::result::Result<Self::Future, SessionError<Self::Error>>;

    fn request_with<F>(
        &self,
        build: F,
    ) -> std::result::Result<Self::Future, SessionError<Self::Error>>
    where
        F: FnOnce() -> Self::Request;

    fn request_batch(
        &self,
        requests: Vec<Self::Request>,
    ) -> std::result::Result<Vec<Self::Future>, SessionError<Self::Error>>;
}

impl<P: SessionProtocol> SessionReplica for Node<P> {
    type Request = P::Request;
    type Response = P::Response;
    type Error = P::Error;
    type Future = ResponseFuture<NodeReplicaResult<P>>;

    #[inline]
    fn request(
        &self,
        request: Self::Request,
    ) -> std::result::Result<Self::Future, SessionError<Self::Error>> {
        Node::request(self, request)
    }

    #[inline]
    fn request_with<F>(
        &self,
        build: F,
    ) -> std::result::Result<Self::Future, SessionError<Self::Error>>
    where
        F: FnOnce() -> Self::Request,
    {
        Node::request_with(self, build)
    }

    #[inline]
    fn request_batch(
        &self,
        requests: Vec<Self::Request>,
    ) -> std::result::Result<Vec<Self::Future>, SessionError<Self::Error>> {
        Node::request_batch(self, requests)
    }
}

impl<N> ReplicaSet<N> {
    pub fn new(replicas: impl IntoIterator<Item = N>) -> Result<Self> {
        Self::with_options(replicas, QuotaBalancerOptions::default())
    }

    pub fn with_options(
        replicas: impl IntoIterator<Item = N>,
        options: QuotaBalancerOptions,
    ) -> Result<Self> {
        let mut replicas = replicas.into_iter().collect::<Vec<_>>();
        if replicas.is_empty() {
            return Err(NetError::NoReplicas);
        }
        shuffle(&mut replicas);
        Ok(Self {
            balancer: QuotaSelector::new(replicas.len(), options)?,
            replicas: replicas.into(),
        })
    }

    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    pub fn snapshots(&self) -> Vec<ReplicaSnapshot> {
        self.balancer.snapshots()
    }
}

impl<R: SessionReplica> ReplicaSet<R> {
    /// Select one node and submit immediately. Queue-full and disconnected
    /// errors consume the configured failure penalty through `QuotaTicket::drop`.
    #[inline]
    pub fn request(
        &self,
        request: R::Request,
    ) -> std::result::Result<ReplicaSetResponseFuture<R>, SessionError<R::Error>> {
        let guard = self.balancer.select();
        let index = guard.index();
        let response = self.replicas[index].request(request)?;
        Ok(ReplicaResponseFuture {
            response,
            guard: Some(guard),
            output: PhantomData,
        })
    }

    /// Select one node, reserve its bounded queue capacity, and only then
    /// construct the request. This keeps serialization and arena allocation
    /// off the fail-fast overload path.
    #[inline]
    pub fn request_with<F>(
        &self,
        build: F,
    ) -> std::result::Result<ReplicaSetResponseFuture<R>, SessionError<R::Error>>
    where
        F: FnOnce() -> R::Request,
    {
        let guard = self.balancer.select();
        let index = guard.index();
        let response = self.replicas[index].request_with(build)?;
        Ok(ReplicaResponseFuture {
            response,
            guard: Some(guard),
            output: PhantomData,
        })
    }

    /// Submit to the quota-selected replica and retry retryable transport
    /// failures on subsequent replicas in the process-randomized order.
    ///
    /// `retries` counts additional attempts. It is capped at the number of
    /// distinct remaining replicas, so one logical request is never sent to
    /// the same physical node twice. Failed replicas remain in the set; their
    /// failure cost is recorded in the normal quota accounting and the global
    /// selector advances only when that quota is consumed.
    pub async fn request_with_failover<F>(
        &self,
        retries: usize,
        mut build: F,
    ) -> std::result::Result<R::Response, SessionError<R::Error>>
    where
        F: FnMut() -> R::Request,
    {
        let first = self.balancer.select();
        let first_index = first.index();
        let attempts = retries.saturating_add(1).min(self.replicas.len());
        let mut first = Some(first);

        for offset in 0..attempts {
            let index = (first_index + offset) % self.replicas.len();
            let guard = first.take().unwrap_or_else(|| self.balancer.ticket(index));
            let response = match self.replicas[index].request_with(&mut build) {
                Ok(response) => response.await,
                Err(error) => Err(error),
            };

            match response {
                Ok(response) => {
                    guard.success();
                    return Ok(response);
                }
                Err(error) => {
                    guard.failure();
                    if !error.is_retryable_transport() || offset + 1 == attempts {
                        return Err(error);
                    }
                }
            }
        }

        unreachable!("a replica set always performs at least one attempt")
    }

    /// Select one physical replica for a finite pipeline and submit all of its
    /// requests with all-or-nothing admission.
    ///
    /// Every response retains an independent quota timer even though replica
    /// selection happens only once for the whole pipeline.
    pub fn request_batch(
        &self,
        requests: Vec<R::Request>,
    ) -> std::result::Result<Vec<ReplicaSetResponseFuture<R>>, SessionError<R::Error>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let guard = self.balancer.select();
        let index = guard.index();
        let responses = self.replicas[index].request_batch(requests)?;
        let mut guards = Vec::with_capacity(responses.len());
        for _ in 1..responses.len() {
            guards.push(guard.sibling());
        }
        guards.push(guard);

        Ok(responses
            .into_iter()
            .zip(guards)
            .map(|(response, guard)| ReplicaResponseFuture {
                response,
                guard: Some(guard),
                output: PhantomData,
            })
            .collect())
    }
}

impl<N: std::fmt::Debug> std::fmt::Debug for ReplicaSet<N> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplicaSet")
            .field("replicas", &self.replicas)
            .field("snapshots", &self.snapshots())
            .finish()
    }
}

type NodeReplicaResult<P> = std::result::Result<
    <P as SessionProtocol>::Response,
    SessionError<<P as SessionProtocol>::Error>,
>;

/// Response future returned by a [`ReplicaSet`] for any session replica.
pub type ReplicaSetResponseFuture<R> = ReplicaResponseFuture<
    <R as SessionReplica>::Future,
    <R as SessionReplica>::Response,
    <R as SessionReplica>::Error,
>;

/// Response future returned by the direct `ReplicaSet<Node<P>>::request` API.
pub type NodeReplicaResponseFuture<P> = ReplicaSetResponseFuture<Node<P>>;

/// Response Future that charges elapsed time to the selected replica.
pub struct ReplicaResponseFuture<F, T, E> {
    response: F,
    guard: Option<QuotaTicket>,
    output: PhantomData<fn() -> (T, E)>,
}

impl<T, E> ReplicaResponseFuture<ResponseFuture<std::result::Result<T, SessionError<E>>>, T, E> {
    pub fn request_token(&self) -> RequestToken {
        self.response.request_token()
    }
}

impl<F: Unpin, T, E> Unpin for ReplicaResponseFuture<F, T, E> {}

impl<F, T, E> std::future::Future for ReplicaResponseFuture<F, T, E>
where
    F: std::future::Future<Output = std::result::Result<T, SessionError<E>>> + Unpin,
{
    type Output = std::result::Result<T, SessionError<E>>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.response).poll(context) {
            std::task::Poll::Ready(result) => {
                let guard = self
                    .guard
                    .take()
                    .expect("quota guard is present until response completion");
                if result.is_ok() {
                    guard.success();
                } else {
                    guard.failure();
                }
                std::task::Poll::Ready(result)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<K, C> RequestTarget<K> for ReplicaSet<C>
where
    K: ?Sized,
    C: RequestTarget<K>,
{
    type Request = C::Request;
    type Response = C::Response;
    type Error = C::Error;
    type Future = ReplicaResponseFuture<C::Future, C::Response, C::Error>;

    #[inline]
    fn request_for(
        &self,
        key: &K,
        request: Self::Request,
    ) -> std::result::Result<Self::Future, SessionError<Self::Error>> {
        let guard = self.balancer.select();
        let index = guard.index();
        let response = self.replicas[index].request_for(key, request)?;
        Ok(ReplicaResponseFuture {
            response,
            guard: Some(guard),
            output: PhantomData,
        })
    }
}

fn shuffle<T>(values: &mut [T]) {
    use std::{collections::hash_map::RandomState, hash::BuildHasher};

    let random = RandomState::new();
    for upper in (1..values.len()).rev() {
        let index = random.hash_one(upper) as usize % (upper + 1);
        values.swap(upper, index);
    }
}

fn duration_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn validate_options(options: QuotaBalancerOptions) -> Result<()> {
    if duration_micros(options.quota) == 0 {
        return Err(NetError::InvalidConfig(
            "quota must be at least one microsecond".into(),
        ));
    }
    if duration_micros(options.failure_penalty) == 0 {
        return Err(NetError::InvalidConfig(
            "failure_penalty must be at least one microsecond".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{io, sync::Barrier};

    use super::*;

    fn finish(mut guard: QuotaTicket, elapsed: Duration, succeeded: bool) {
        guard.record(elapsed, succeeded);
        guard.finished = true;
    }

    #[test]
    fn requests_stay_on_one_replica_until_its_quota_is_consumed() {
        let balancer = QuotaSelector::new(3, QuotaBalancerOptions::default()).unwrap();

        let first = balancer.select();
        assert_eq!(first.index(), 0);
        finish(first, Duration::from_millis(1_200), true);

        let second = balancer.select();
        assert_eq!(second.index(), 0);
        finish(second, Duration::from_millis(900), true);

        let third = balancer.select();
        assert_eq!(third.index(), 1);
        finish(third, Duration::ZERO, true);

        let snapshots = balancer.snapshots();
        assert_eq!(snapshots[0].used_quota, Duration::ZERO);
        assert!(snapshots[1].current);
    }

    #[test]
    fn failed_requests_consume_at_least_five_hundred_milliseconds() {
        let balancer = QuotaSelector::new(2, QuotaBalancerOptions::default()).unwrap();

        for _ in 0..4 {
            let guard = balancer.select();
            assert_eq!(guard.index(), 0);
            finish(guard, Duration::from_millis(1), false);
        }

        assert_eq!(balancer.snapshots()[0].used_quota, Duration::from_secs(2));
        let next = balancer.select();
        assert_eq!(next.index(), 1);
        finish(next, Duration::ZERO, true);
    }

    #[test]
    fn local_failover_does_not_advance_the_global_selector() {
        let balancer = QuotaSelector::new(2, QuotaBalancerOptions::default()).unwrap();

        let first = balancer.select();
        assert_eq!(first.index(), 0);
        finish(first, Duration::from_millis(1), false);

        let retry = balancer.ticket(1);
        assert_eq!(retry.index(), 1);
        finish(retry, Duration::from_millis(1), true);

        let next_global = balancer.select();
        assert_eq!(next_global.index(), 0);
        finish(next_global, Duration::ZERO, true);
    }

    #[test]
    fn concurrent_selectors_observe_one_atomic_quota_rotation() {
        let balancer = QuotaSelector::new(3, QuotaBalancerOptions::default()).unwrap();
        finish(balancer.select(), Duration::from_secs(2), true);

        let workers = 16;
        let barrier = Arc::new(Barrier::new(workers));
        let handles = (0..workers)
            .map(|_| {
                let balancer = balancer.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let guard = balancer.select();
                    let index = guard.index();
                    finish(guard, Duration::ZERO, true);
                    index
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            assert_eq!(handle.join().unwrap(), 1);
        }
        assert!(balancer.snapshots()[1].current);
    }

    #[test]
    fn invalid_sub_microsecond_options_are_rejected() {
        let error = QuotaSelector::new(
            2,
            QuotaBalancerOptions {
                quota: Duration::from_nanos(999),
                ..QuotaBalancerOptions::default()
            },
        )
        .err()
        .unwrap();
        assert!(matches!(error, NetError::InvalidConfig(_)));
    }

    #[test]
    fn transport_retry_classification_excludes_deterministic_failures() {
        let retryable = [
            SessionError::<io::Error>::Busy,
            SessionError::Unavailable,
            SessionError::Timeout {
                timeout: Duration::from_millis(1),
            },
            SessionError::Closed,
            SessionError::Io(Arc::new(io::Error::other("broken"))),
        ];
        assert!(retryable.iter().all(SessionError::is_retryable_transport));

        let deterministic = [
            SessionError::<io::Error>::Protocol(Arc::new(io::Error::other("protocol"))),
            SessionError::Routing(Arc::new(NetError::NoReplicas)),
            SessionError::UnexpectedResponse,
            SessionError::EmptyRequestFrame,
        ];
        assert!(
            deterministic
                .iter()
                .all(|error| !error.is_retryable_transport())
        );
    }
}
