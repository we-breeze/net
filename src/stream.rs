use std::{
    fmt, io,
    net::SocketAddr,
    pin::Pin,
    sync::Weak,
    task::{Context, Poll},
};

use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

use crate::node::NodePoolCore;

pub(crate) struct ManagedConnection {
    pub(crate) io: TcpStream,
    pub(crate) endpoint: SocketAddr,
}

pub(crate) trait StreamObserver: Send {
    fn succeeded(self: Box<Self>);
}

/// A leased physical TCP connection.
///
/// It implements Tokio's standard async I/O traits. The connection is returned
/// to its owning `NodePool` only when the enclosing `with_conn` operation
/// succeeds; all other drops close it.
pub struct BrzTcpStream {
    connection: Option<ManagedConnection>,
    owner: Weak<NodePoolCore>,
    observers: Vec<Box<dyn StreamObserver>>,
    reusable: bool,
    discarded: bool,
    broken: bool,
}

impl BrzTcpStream {
    pub(crate) fn new(connection: ManagedConnection, owner: Weak<NodePoolCore>) -> Self {
        Self {
            connection: Some(connection),
            owner,
            observers: Vec::new(),
            reusable: false,
            discarded: false,
            broken: false,
        }
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.connection().io.peer_addr()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.connection().io.local_addr()
    }

    /// Prevents this connection from being reused even if the operation succeeds.
    pub fn discard(&mut self) {
        self.discarded = true;
    }

    pub(crate) fn add_observer(&mut self, observer: Box<dyn StreamObserver>) {
        self.observers.push(observer);
    }

    pub(crate) fn finish_success(&mut self) {
        for observer in self.observers.drain(..) {
            observer.succeeded();
        }
        self.reusable = !self.discarded && !self.broken;
    }

    fn connection(&self) -> &ManagedConnection {
        self.connection
            .as_ref()
            .expect("BrzTcpStream connection is present until drop")
    }

    fn connection_mut(&mut self) -> &mut ManagedConnection {
        self.connection
            .as_mut()
            .expect("BrzTcpStream connection is present until drop")
    }

    fn mark_broken(&mut self) {
        self.broken = true;
        self.reusable = false;
    }
}

impl fmt::Debug for BrzTcpStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BrzTcpStream")
            .field("endpoint", &self.connection().endpoint)
            .field("reusable", &self.reusable)
            .field("discarded", &self.discarded)
            .field("broken", &self.broken)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for BrzTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let wanted = buffer.remaining();
        let filled = buffer.filled().len();
        let result = Pin::new(&mut this.connection_mut().io).poll_read(context, buffer);

        match &result {
            Poll::Ready(Err(_)) => this.mark_broken(),
            Poll::Ready(Ok(())) if wanted > 0 && buffer.filled().len() == filled => {
                this.mark_broken();
            }
            _ => {}
        }
        result
    }
}

impl AsyncWrite for BrzTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.connection_mut().io).poll_write(context, buffer);
        match &result {
            Poll::Ready(Err(_)) => this.mark_broken(),
            Poll::Ready(Ok(0)) if !buffer.is_empty() => this.mark_broken(),
            _ => {}
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.connection_mut().io).poll_flush(context);
        if matches!(result, Poll::Ready(Err(_))) {
            this.mark_broken();
        }
        result
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.discarded = true;
        let result = Pin::new(&mut this.connection_mut().io).poll_shutdown(context);
        if matches!(result, Poll::Ready(Err(_))) {
            this.mark_broken();
        }
        result
    }

    fn is_write_vectored(&self) -> bool {
        self.connection().io.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.connection_mut().io).poll_write_vectored(context, buffers);
        if matches!(result, Poll::Ready(Err(_))) {
            this.mark_broken();
        }
        result
    }
}

impl Drop for BrzTcpStream {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };

        if self.reusable
            && !self.discarded
            && !self.broken
            && let Some(owner) = self.owner.upgrade()
        {
            owner.recycle(connection);
            return;
        }

        if let Some(owner) = self.owner.upgrade() {
            owner.discard_connection(connection);
        }
    }
}
