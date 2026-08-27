use std::{net::SocketAddr, sync::Arc};

use arc_swap::ArcSwap;

/// A dynamically replaceable set of addresses for one logical node.
pub trait EndpointSource: Send + Sync {
    /// Load the current immutable endpoint snapshot.
    ///
    /// Implementations publish snapshots with copy-on-write semantics, so this
    /// method is suitable for the connection-acquisition hot path.
    fn snapshot(&self) -> Arc<[SocketAddr]>;
}

/// An endpoint source that can be updated by discovery adapters such as Vintage.
#[derive(Clone)]
pub struct EndpointSet {
    inner: Arc<ArcSwap<EndpointSnapshot>>,
}

#[derive(Debug)]
struct EndpointSnapshot {
    endpoints: Arc<[SocketAddr]>,
}

impl EndpointSet {
    pub fn new(endpoints: impl IntoIterator<Item = SocketAddr>) -> Self {
        let snapshot = EndpointSnapshot {
            endpoints: normalize(endpoints),
        };
        Self {
            inner: Arc::new(ArcSwap::from_pointee(snapshot)),
        }
    }

    /// Atomically replaces the current endpoint snapshot.
    ///
    /// Returns `true` when the snapshot changed.
    pub fn replace(&self, endpoints: impl IntoIterator<Item = SocketAddr>) -> bool {
        let endpoints = normalize(endpoints);
        let current = self.inner.load();
        if current.endpoints.as_ref() == endpoints.as_ref() {
            return false;
        }
        self.inner.store(Arc::new(EndpointSnapshot { endpoints }));
        true
    }

    pub fn snapshot(&self) -> Arc<[SocketAddr]> {
        self.inner.load().endpoints.clone()
    }
}

impl std::fmt::Debug for EndpointSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("EndpointSet")
            .field(&self.snapshot())
            .finish()
    }
}

impl EndpointSource for EndpointSet {
    fn snapshot(&self) -> Arc<[SocketAddr]> {
        EndpointSet::snapshot(self)
    }
}

pub(crate) fn normalize(endpoints: impl IntoIterator<Item = SocketAddr>) -> Arc<[SocketAddr]> {
    let mut endpoints: Vec<_> = endpoints.into_iter().collect();
    endpoints.sort_unstable();
    endpoints.dedup();
    endpoints.into()
}
