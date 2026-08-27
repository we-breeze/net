//! Protocol-agnostic async TCP primitives for Breeze SDK clients.

mod balance;
mod dns;
mod error;
mod node;
mod pool;
mod provider;
mod sharded;
mod source;
mod stream;

pub use balance::{LatencyBalancerOptions, ReplicaSnapshot};
pub use dns::{DnsOptions, DnsResolver, DnsResolverOptions, DnsSource};
pub use error::{CallError, NetError, Result};
pub use node::{NodePool, NodePoolOptions, NodePoolStats};
pub use pool::Pool;
pub use provider::{BoxFuture, OperationFuture, StreamProvider};
pub use sharded::{ShardRouter, Sharded};
pub use source::{EndpointSet, EndpointSource};
pub use stream::BrzTcpStream;
