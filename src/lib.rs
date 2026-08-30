//! High-performance single-connection transports for Breeze SDK clients.

#[cfg(not(loom))]
mod arena;
#[cfg(not(loom))]
mod balance;
#[cfg(not(loom))]
mod completion;
#[cfg(not(loom))]
mod dns;
#[cfg(not(loom))]
mod error;
#[cfg(not(loom))]
mod protocol;
mod rx;
#[cfg(not(loom))]
mod session;
#[cfg(not(loom))]
mod sharded;
#[cfg(not(loom))]
mod source;
#[cfg(not(loom))]
mod target;

#[cfg(not(loom))]
pub use arena::{
    DEFAULT_REQUEST_ARENA_CHUNK_SIZE, EphemeralBytes, EphemeralBytesArena, EphemeralBytesMut,
    global_request_arena, init_global_request_arena,
};
#[cfg(not(loom))]
pub use balance::{
    NodeReplicaResponseFuture, QuotaBalancerOptions, QuotaSelector, QuotaTicket,
    ReplicaResponseFuture, ReplicaSet, ReplicaSnapshot,
};
#[cfg(not(loom))]
pub use completion::{MAX_IN_FLIGHT, RequestToken, ResponseFuture};
#[cfg(not(loom))]
pub use dns::{DnsOptions, DnsResolver, DnsResolverOptions, DnsSource};
#[cfg(not(loom))]
pub use error::{NetError, Result, SessionError};
#[cfg(not(loom))]
pub use protocol::{Correlation, DecodedResponse, HandshakeStatus, SessionProtocol};
pub use rx::{
    ContiguousRxFrame, DEFAULT_MAX_RX_BUFFER_CAPACITY, RxBuffer, RxCapacityError, RxFrame,
};
#[cfg(not(loom))]
pub use session::{Node, NodeOptions, NodeStats};
#[cfg(not(loom))]
pub use sharded::{ShardRouter, Sharded};
#[cfg(not(loom))]
pub use source::{EndpointSet, EndpointSource};
#[cfg(not(loom))]
pub use target::RequestTarget;
