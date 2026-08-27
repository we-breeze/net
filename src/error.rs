use std::{io, net::SocketAddr};

/// Errors produced by the network layer itself.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("endpoint source contains no addresses")]
    NoEndpoints,

    #[error("replica pool contains no replicas")]
    NoReplicas,

    #[error("all replicas are temporarily unavailable")]
    NoHealthyReplicas,

    #[error("sharded provider contains no shards")]
    NoShards,

    #[error("shard router returned index {index}, but only {shard_count} shards exist")]
    InvalidShard { index: usize, shard_count: usize },

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("failed to resolve {name}: {source}")]
    Resolve {
        name: String,
        #[source]
        source: io::Error,
    },

    #[error("shared DNS resolver task stopped")]
    DnsResolverStopped,

    #[error("failed to connect to {endpoint}: {source}")]
    Connect {
        endpoint: SocketAddr,
        #[source]
        source: io::Error,
    },
}

/// The result type used by provider construction and connection acquisition.
pub type Result<T> = std::result::Result<T, NetError>;

/// Separates network-layer failures from errors returned by a protocol operation.
#[derive(Debug, thiserror::Error)]
pub enum CallError<E> {
    #[error(transparent)]
    Net(#[from] NetError),

    #[error("protocol operation failed")]
    Operation(E),
}

impl<E> CallError<E> {
    pub fn into_operation(self) -> Option<E> {
        match self {
            Self::Operation(error) => Some(error),
            Self::Net(_) => None,
        }
    }
}
