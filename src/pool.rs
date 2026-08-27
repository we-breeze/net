use std::{fmt, sync::Arc};

use crate::{
    BoxFuture, BrzTcpStream, LatencyBalancerOptions, ReplicaSnapshot, Result, StreamProvider,
    balance::LatencyBalancer,
};

/// Selects one of several equivalent providers using observed request latency.
#[derive(Clone)]
pub struct Pool<C> {
    replicas: Arc<[C]>,
    balancer: LatencyBalancer,
}

impl<C> Pool<C> {
    pub fn new(replicas: impl IntoIterator<Item = C>) -> Result<Self> {
        Self::with_options(replicas, LatencyBalancerOptions::default())
    }

    pub fn with_options(
        replicas: impl IntoIterator<Item = C>,
        options: LatencyBalancerOptions,
    ) -> Result<Self> {
        let replicas: Arc<[C]> = replicas.into_iter().collect::<Vec<_>>().into();
        if replicas.is_empty() {
            return Err(crate::NetError::NoReplicas);
        }
        let balancer = LatencyBalancer::new(replicas.len(), options)?;
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
            let guard = self.balancer.select()?;
            let index = guard.index();
            let mut stream = self.replicas[index].acquire(key).await?;
            stream.add_observer(Box::new(guard));
            Ok(stream)
        })
    }
}
