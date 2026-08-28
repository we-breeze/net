//! High-performance single-connection transports for Breeze SDK clients.

mod balance;
mod completion;
mod dns;
mod error;
mod protocol;
mod session;
mod sharded;
mod source;
mod target;

pub use balance::{
    NodeReplicaResponseFuture, QuotaBalancerOptions, ReplicaResponseFuture, ReplicaSet,
    ReplicaSnapshot,
};
pub use completion::{MAX_IN_FLIGHT, RequestToken, ResponseFuture};
pub use dns::{DnsOptions, DnsResolver, DnsResolverOptions, DnsSource};
pub use error::{NetError, Result, SessionError};
pub use protocol::{Correlation, DecodedResponse, HandshakeStatus, SessionProtocol};
pub use session::{Node, NodeOptions, NodeStats};
pub use sharded::{ShardRouter, Sharded};
pub use source::{EndpointSet, EndpointSource};
pub use target::RequestTarget;
