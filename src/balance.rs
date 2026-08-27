use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;

use crate::{NetError, Result, stream::StreamObserver};

#[derive(Clone, Copy, Debug)]
pub struct LatencyBalancerOptions {
    pub initial_latency: Duration,
    pub failure_penalty: Duration,
    pub failure_threshold: u32,
    pub ejection_duration: Duration,
    pub ewma_weight: f64,
}

impl Default for LatencyBalancerOptions {
    fn default() -> Self {
        Self {
            initial_latency: Duration::from_millis(1),
            failure_penalty: Duration::from_millis(100),
            failure_threshold: 3,
            ejection_duration: Duration::from_secs(1),
            ewma_weight: 0.2,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ReplicaSnapshot {
    pub latency: Duration,
    pub inflight: usize,
    pub consecutive_failures: u32,
    pub samples: u64,
    pub ejected: bool,
}

#[derive(Clone)]
pub(crate) struct LatencyBalancer {
    inner: Arc<BalancerInner>,
}

struct BalancerInner {
    state: Mutex<BalancerState>,
    options: LatencyBalancerOptions,
}

struct BalancerState {
    replicas: Vec<ReplicaState>,
    cursor: usize,
}

struct ReplicaState {
    latency_secs: f64,
    inflight: usize,
    consecutive_failures: u32,
    samples: u64,
    ejected_until: Option<Instant>,
}

impl LatencyBalancer {
    pub(crate) fn new(replicas: usize, options: LatencyBalancerOptions) -> Result<Self> {
        validate_options(options)?;
        Ok(Self {
            inner: Arc::new(BalancerInner {
                state: Mutex::new(BalancerState {
                    replicas: (0..replicas)
                        .map(|_| ReplicaState {
                            latency_secs: options.initial_latency.as_secs_f64(),
                            inflight: 0,
                            consecutive_failures: 0,
                            samples: 0,
                            ejected_until: None,
                        })
                        .collect(),
                    cursor: 0,
                }),
                options,
            }),
        })
    }

    pub(crate) fn select(&self) -> Result<BalanceGuard> {
        let now = Instant::now();
        let mut state = self.inner.state.lock();
        let count = state.replicas.len();
        if count == 0 {
            return Err(NetError::NoReplicas);
        }

        for replica in &mut state.replicas {
            if replica.ejected_until.is_some_and(|until| until <= now) {
                replica.ejected_until = None;
            }
        }

        let start = state.cursor % count;
        let unobserved = (0..count)
            .map(|offset| (start + offset) % count)
            .find(|&index| {
                let replica = &state.replicas[index];
                replica.ejected_until.is_none() && replica.samples == 0
            });

        let selected = unobserved.or_else(|| {
            (0..count)
                .map(|offset| (start + offset) % count)
                .filter(|&index| state.replicas[index].ejected_until.is_none())
                .min_by(|&left, &right| {
                    score(&state.replicas[left]).total_cmp(&score(&state.replicas[right]))
                })
        });

        let Some(selected) = selected else {
            return Err(NetError::NoHealthyReplicas);
        };

        state.cursor = (selected + 1) % count;
        state.replicas[selected].inflight += 1;
        drop(state);

        Ok(BalanceGuard {
            balancer: self.inner.clone(),
            index: selected,
            started: Instant::now(),
            finished: false,
        })
    }

    pub(crate) fn snapshots(&self) -> Vec<ReplicaSnapshot> {
        let now = Instant::now();
        self.inner
            .state
            .lock()
            .replicas
            .iter()
            .map(|replica| ReplicaSnapshot {
                latency: Duration::from_secs_f64(replica.latency_secs),
                inflight: replica.inflight,
                consecutive_failures: replica.consecutive_failures,
                samples: replica.samples,
                ejected: replica.ejected_until.is_some_and(|until| until > now),
            })
            .collect()
    }
}

pub(crate) struct BalanceGuard {
    balancer: Arc<BalancerInner>,
    index: usize,
    started: Instant,
    finished: bool,
}

impl BalanceGuard {
    pub(crate) fn index(&self) -> usize {
        self.index
    }

    fn finish(mut self, succeeded: bool) {
        self.record(succeeded);
        self.finished = true;
    }

    fn record(&self, succeeded: bool) {
        let elapsed = self.started.elapsed().as_secs_f64();
        let mut state = self.balancer.state.lock();
        let replica = &mut state.replicas[self.index];
        replica.inflight = replica.inflight.saturating_sub(1);

        let sample = if succeeded {
            elapsed
        } else {
            elapsed.max(self.balancer.options.failure_penalty.as_secs_f64())
        };
        replica.latency_secs = if replica.samples == 0 {
            sample
        } else {
            let weight = self.balancer.options.ewma_weight;
            replica.latency_secs * (1.0 - weight) + sample * weight
        };
        replica.samples += 1;

        if succeeded {
            replica.consecutive_failures = 0;
            replica.ejected_until = None;
        } else {
            replica.consecutive_failures = replica.consecutive_failures.saturating_add(1);
            if replica.consecutive_failures >= self.balancer.options.failure_threshold {
                replica.ejected_until =
                    Some(Instant::now() + self.balancer.options.ejection_duration);
            }
        }
    }
}

impl StreamObserver for BalanceGuard {
    fn succeeded(self: Box<Self>) {
        (*self).finish(true);
    }
}

impl Drop for BalanceGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.record(false);
        }
    }
}

fn score(replica: &ReplicaState) -> f64 {
    replica.latency_secs * (replica.inflight as f64 + 1.0)
}

fn validate_options(options: LatencyBalancerOptions) -> Result<()> {
    if options.failure_threshold == 0 {
        return Err(NetError::InvalidConfig(
            "failure_threshold must be greater than zero".into(),
        ));
    }
    if !(0.0..=1.0).contains(&options.ewma_weight) || options.ewma_weight == 0.0 {
        return Err(NetError::InvalidConfig(
            "ewma_weight must be in the range (0, 1]".into(),
        ));
    }
    Ok(())
}
