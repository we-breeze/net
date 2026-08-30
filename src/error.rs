use std::{io, sync::Arc, time::Duration};

/// Errors produced by the network layer itself.
#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("endpoint source contains no addresses")]
    NoEndpoints,

    #[error("replica pool contains no replicas")]
    NoReplicas,

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

    #[error("session node must be created inside a Tokio runtime")]
    NoRuntime,

    #[error(
        "global request arena already uses chunk size {configured}, cannot change it to {requested}"
    )]
    RequestArenaAlreadyInitialized { configured: usize, requested: usize },
}

/// The result type used while constructing network components.
pub type Result<T> = std::result::Result<T, NetError>;

/// Failure of one request submitted to a single-connection session.
///
/// I/O and protocol errors are reference counted so one connection failure can
/// complete every in-flight request without cloning the underlying error.
#[derive(Debug, thiserror::Error)]
pub enum SessionError<E> {
    #[error("session request capacity is exhausted")]
    Busy,

    #[error("session is not connected")]
    Unavailable,

    #[error("session request timed out after {timeout:?}")]
    Timeout { timeout: Duration },

    #[error("session connection closed")]
    Closed,

    #[error("connection I/O failed: {0}")]
    Io(Arc<io::Error>),

    #[error("protocol failed: {0}")]
    Protocol(Arc<E>),

    #[error("request routing failed: {0}")]
    Routing(Arc<NetError>),

    #[error("received a FIFO response without a pending request")]
    UnexpectedResponse,

    #[error("protocol encoded an empty request frame")]
    EmptyRequestFrame,
}

impl<E> SessionError<E> {
    /// Whether the request may succeed when resubmitted to another physical
    /// replica.
    ///
    /// This only classifies transport admission and connection failures. A
    /// protocol or routing failure is deterministic from the client's point
    /// of view and must not be hidden by replica failover.
    #[inline]
    pub fn is_retryable_transport(&self) -> bool {
        matches!(
            self,
            Self::Busy | Self::Unavailable | Self::Timeout { .. } | Self::Closed | Self::Io(_)
        )
    }
}

impl<E> Clone for SessionError<E> {
    fn clone(&self) -> Self {
        match self {
            Self::Busy => Self::Busy,
            Self::Unavailable => Self::Unavailable,
            Self::Timeout { timeout } => Self::Timeout { timeout: *timeout },
            Self::Closed => Self::Closed,
            Self::Io(error) => Self::Io(error.clone()),
            Self::Protocol(error) => Self::Protocol(error.clone()),
            Self::Routing(error) => Self::Routing(error.clone()),
            Self::UnexpectedResponse => Self::UnexpectedResponse,
            Self::EmptyRequestFrame => Self::EmptyRequestFrame,
        }
    }
}
