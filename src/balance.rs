use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use crate::{NetError, Result, stream::StreamObserver};

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

#[derive(Clone)]
pub(crate) struct QuotaBalancer {
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

impl QuotaBalancer {
    pub(crate) fn new(replicas: usize, options: QuotaBalancerOptions) -> Result<Self> {
        validate_options(options)?;
        if replicas == 0 {
            return Err(NetError::NoReplicas);
        }

        Ok(Self {
            inner: Arc::new(BalancerInner {
                current: AtomicUsize::new(0),
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
    pub(crate) fn select(&self) -> QuotaGuard {
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

        QuotaGuard {
            balancer: self.inner.clone(),
            index,
            started: Instant::now(),
            finished: false,
        }
    }

    pub(crate) fn snapshots(&self) -> Vec<ReplicaSnapshot> {
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

pub(crate) struct QuotaGuard {
    balancer: Arc<BalancerInner>,
    index: usize,
    started: Instant,
    finished: bool,
}

impl QuotaGuard {
    pub(crate) fn index(&self) -> usize {
        self.index
    }

    fn finish(mut self, succeeded: bool) {
        let elapsed = self.started.elapsed();
        self.record(elapsed, succeeded);
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

impl StreamObserver for QuotaGuard {
    fn succeeded(self: Box<Self>) {
        (*self).finish(true);
    }
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.record(self.started.elapsed(), false);
        }
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
    use std::sync::Barrier;

    use super::*;

    fn finish(mut guard: QuotaGuard, elapsed: Duration, succeeded: bool) {
        guard.record(elapsed, succeeded);
        guard.finished = true;
    }

    #[test]
    fn requests_stay_on_one_replica_until_its_quota_is_consumed() {
        let balancer = QuotaBalancer::new(3, QuotaBalancerOptions::default()).unwrap();

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
        let balancer = QuotaBalancer::new(2, QuotaBalancerOptions::default()).unwrap();

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
    fn concurrent_selectors_observe_one_atomic_quota_rotation() {
        let balancer = QuotaBalancer::new(3, QuotaBalancerOptions::default()).unwrap();
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
        let error = QuotaBalancer::new(
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
}
