use std::{collections::hash_map::RandomState, fmt, hash::BuildHasher, sync::Arc};

use crate::{
    BoxFuture, BrzTcpStream, QuotaBalancerOptions, ReplicaSnapshot, Result, StreamProvider,
    balance::QuotaBalancer,
};

/// Selects one of several equivalent providers using consumed request-time quota.
#[derive(Clone)]
pub struct Pool<C> {
    replicas: Arc<[C]>,
    balancer: QuotaBalancer,
}

impl<C> Pool<C> {
    pub fn new(replicas: impl IntoIterator<Item = C>) -> Result<Self> {
        Self::with_options(replicas, QuotaBalancerOptions::default())
    }

    pub fn with_options(
        replicas: impl IntoIterator<Item = C>,
        options: QuotaBalancerOptions,
    ) -> Result<Self> {
        let mut replicas = replicas.into_iter().collect::<Vec<_>>();
        if replicas.is_empty() {
            return Err(crate::NetError::NoReplicas);
        }
        shuffle(&mut replicas);
        let balancer = QuotaBalancer::new(replicas.len(), options)?;
        let replicas = replicas.into();
        Ok(Self { replicas, balancer })
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn snapshots(&self) -> Vec<ReplicaSnapshot> {
        self.balancer.snapshots()
    }
}

impl<C: fmt::Debug> fmt::Debug for Pool<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Pool")
            .field("replicas", &self.replicas)
            .field("snapshots", &self.snapshots())
            .finish()
    }
}

impl<K, C> StreamProvider<K> for Pool<C>
where
    K: ?Sized + Sync,
    C: StreamProvider<K> + Send + Sync,
{
    fn acquire<'a>(&'a self, key: &'a K) -> BoxFuture<'a, Result<BrzTcpStream>> {
        Box::pin(async move {
            let guard = self.balancer.select();
            let index = guard.index();
            let mut stream = self.replicas[index].acquire(key).await?;
            stream.add_observer(Box::new(guard));
            Ok(stream)
        })
    }
}

fn shuffle<T>(values: &mut [T]) {
    let random = RandomState::new();
    for upper in (1..values.len()).rev() {
        let index = random.hash_one(upper) as usize % (upper + 1);
        values.swap(upper, index);
    }
}
