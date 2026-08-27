use std::{fmt, sync::Arc};

use crate::{BoxFuture, BrzTcpStream, NetError, Result, StreamProvider};

pub trait ShardRouter<K: ?Sized>: Send + Sync {
    fn route(&self, key: &K, shard_count: usize) -> usize;
}

impl<K, F> ShardRouter<K> for F
where
    K: ?Sized,
    F: Fn(&K, usize) -> usize + Send + Sync,
{
    fn route(&self, key: &K, shard_count: usize) -> usize {
        self(key, shard_count)
    }
}

/// Routes a key to one child provider.
#[derive(Clone)]
pub struct Sharded<C, R> {
    shards: Arc<[C]>,
    router: Arc<R>,
}

impl<C, R> Sharded<C, R> {
    pub fn new(router: R, shards: impl IntoIterator<Item = C>) -> Result<Self> {
        let shards: Arc<[C]> = shards.into_iter().collect::<Vec<_>>().into();
        if shards.is_empty() {
            return Err(NetError::NoShards);
        }
        Ok(Self {
            shards,
            router: Arc::new(router),
        })
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }
}

impl<C: fmt::Debug, R> fmt::Debug for Sharded<C, R> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Sharded")
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}

impl<K, C, R> StreamProvider<K> for Sharded<C, R>
where
    K: ?Sized + Sync,
    C: StreamProvider<K> + Send + Sync,
    R: ShardRouter<K>,
{
    fn acquire<'a>(&'a self, key: &'a K) -> BoxFuture<'a, Result<BrzTcpStream>> {
        Box::pin(async move {
            let index = self.router.route(key, self.shards.len());
            let Some(shard) = self.shards.get(index) else {
                return Err(NetError::InvalidShard {
                    index,
                    shard_count: self.shards.len(),
                });
            };
            shard.acquire(key).await
        })
    }
}
