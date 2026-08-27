use std::{fmt, time::Duration};

use tokio::time::{Instant, timeout_at};

use crate::{
    BoxFuture, BrzTcpStream, CallError, NetError, OperationFuture, Result, StreamProvider,
};

/// Applies one operation deadline around a fully composed stream provider.
///
/// The deadline covers connection-pool waiting, connection establishment, the
/// protocol write, and the complete response read. A timeout cancels the
/// operation future; the leased [`BrzTcpStream`] is then dropped and cannot be
/// recycled with partially consumed protocol data.
#[derive(Clone)]
pub struct TcpClient<P> {
    provider: P,
    operation_timeout: Duration,
}

impl<P> TcpClient<P> {
    pub fn new(provider: P, operation_timeout: Duration) -> Result<Self> {
        if operation_timeout.is_zero() {
            return Err(NetError::InvalidConfig(
                "operation_timeout must be greater than zero".into(),
            ));
        }
        Ok(Self {
            provider,
            operation_timeout,
        })
    }

    pub fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    pub fn provider(&self) -> &P {
        &self.provider
    }

    pub fn into_inner(self) -> P {
        self.provider
    }

    /// Run one complete protocol operation under a single absolute deadline.
    pub fn with_conn<'a, K, T, E, F>(
        &'a self,
        key: &'a K,
        operation: F,
    ) -> BoxFuture<'a, std::result::Result<T, CallError<E>>>
    where
        K: ?Sized + Sync,
        P: StreamProvider<K>,
        T: Send + 'a,
        E: Send + 'a,
        F: for<'stream> FnOnce(&'stream mut BrzTcpStream) -> OperationFuture<'stream, T, E>
            + Send
            + 'a,
    {
        Box::pin(async move {
            let deadline = Instant::now() + self.operation_timeout;
            match timeout_at(deadline, self.provider.with_conn(key, operation)).await {
                Ok(result) => result,
                Err(_) => Err(CallError::Timeout {
                    timeout: self.operation_timeout,
                }),
            }
        })
    }
}

impl<P: fmt::Debug> fmt::Debug for TcpClient<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpClient")
            .field("provider", &self.provider)
            .field("operation_timeout", &self.operation_timeout)
            .finish()
    }
}
