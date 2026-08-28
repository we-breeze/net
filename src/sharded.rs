use std::{fmt, sync::Arc};

use crate::{NetError, RequestTarget, Result, SessionError};

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

    #[inline]
    pub fn get<K: ?Sized>(&self, key: &K) -> Result<&C>
    where
        R: ShardRouter<K>,
    {
        let index = self.router.route(key, self.shards.len());
        self.shards.get(index).ok_or(NetError::InvalidShard {
            index,
            shard_count: self.shards.len(),
        })
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

impl<K, C, R> RequestTarget<K> for Sharded<C, R>
where
    K: ?Sized,
    C: RequestTarget<K>,
    R: ShardRouter<K> + 'static,
{
    type Request = C::Request;
    type Response = C::Response;
    type Error = C::Error;
    type Future = C::Future;

    #[inline]
    fn request_for(
        &self,
        key: &K,
        request: Self::Request,
    ) -> std::result::Result<Self::Future, SessionError<Self::Error>> {
        let shard = self
            .get(key)
            .map_err(|error| SessionError::Routing(Arc::new(error)))?;
        shard.request_for(key, request)
    }
}
