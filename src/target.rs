use std::{error::Error, future::Future};

use crate::SessionError;

/// A statically dispatched request target that composes with sharding and
/// replica selection.
///
/// A physical [`crate::Node`] ignores `key`; [`crate::Sharded`] uses it for
/// routing; [`crate::ReplicaSet`] delegates it to its selected child. Both
/// `Sharded<ReplicaSet<Node<_>>, _>` and
/// `ReplicaSet<Sharded<Node<_>, _>>` therefore use the same request API.
pub trait RequestTarget<K: ?Sized>: Send + Sync {
    type Request: Send + 'static;
    type Response: Send + 'static;
    type Error: Error + Send + Sync + 'static;
    type Future: Future<Output = Result<Self::Response, SessionError<Self::Error>>>
        + Send
        + Unpin
        + 'static;

    /// Route and submit immediately without waiting for request capacity.
    fn request_for(
        &self,
        key: &K,
        request: Self::Request,
    ) -> Result<Self::Future, SessionError<Self::Error>>;
}
