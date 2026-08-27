use std::{future::Future, pin::Pin};

use crate::{BrzTcpStream, CallError, Result};

/// A boxed, sendable future used at the public async trait boundary.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Future returned by a protocol operation passed to `TcpClient::with_conn`.
pub type OperationFuture<'a, T, E> = BoxFuture<'a, std::result::Result<T, E>>;

/// Something that can select and acquire a physical Breeze TCP stream.
///
/// `NodePool`, `Pool<C>`, and `Sharded<C, R>` all implement this trait, so
/// routing and load balancing can be composed in either order.
pub trait StreamProvider<K: ?Sized + Sync>: Send + Sync {
    fn acquire<'a>(&'a self, key: &'a K) -> BoxFuture<'a, Result<BrzTcpStream>>;

    fn with_conn<'a, T, E, F>(
        &'a self,
        key: &'a K,
        operation: F,
    ) -> BoxFuture<'a, std::result::Result<T, CallError<E>>>
    where
        Self: Sized,
        T: Send + 'a,
        E: Send + 'a,
        F: for<'stream> FnOnce(&'stream mut BrzTcpStream) -> OperationFuture<'stream, T, E>
            + Send
            + 'a,
    {
        Box::pin(async move {
            let mut stream = self.acquire(key).await?;
            match operation(&mut stream).await {
                Ok(value) => {
                    stream.finish_success();
                    Ok(value)
                }
                Err(error) => Err(CallError::Operation(error)),
            }
        })
    }
}
