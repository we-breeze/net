use std::error::Error;

use bytes::BytesMut;

use crate::{RequestToken, RxBuffer};

/// Progress of protocol setup on a newly established TCP connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandshakeStatus {
    /// The connection can accept application requests.
    Ready,
    /// More bytes must be read and passed to `decode_handshake`.
    Pending,
}

/// Identifies how one decoded response maps to an in-flight request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Correlation {
    /// The response belongs to the oldest request on this connection.
    Fifo,
    /// The response carries an opaque request identifier.
    Tagged(RequestToken),
}

/// One complete protocol response decoded from the connection read buffer.
#[derive(Debug)]
pub struct DecodedResponse<R> {
    pub correlation: Correlation,
    pub response: R,
}

impl<R> DecodedResponse<R> {
    #[inline]
    pub fn fifo(response: R) -> Self {
        Self {
            correlation: Correlation::Fifo,
            response,
        }
    }

    #[inline]
    pub fn tagged(request_id: u64, response: R) -> Self {
        Self {
            correlation: Correlation::Tagged(RequestToken::from_raw(request_id)),
            response,
        }
    }
}

/// Protocol-specific framing used by the generic single-connection driver.
///
/// Implementations turn each admitted request into one owned wire frame and
/// remove complete responses from the front of the reusable read buffer. The
/// connection task retains frames until the socket has consumed every byte, so
/// callers may use arena-backed storage without copying into another buffer.
pub trait SessionProtocol: Send + 'static {
    type Request: Send + 'static;
    type Frame: AsRef<[u8]> + Send + 'static;
    type Response: Send + 'static;
    type Error: Error + Send + Sync + 'static;

    /// Reset connection-local codec state before a newly connected stream is used.
    fn reset(&mut self) {}

    /// Begin protocol setup for a newly connected stream.
    ///
    /// Protocols such as Redis may append an authentication frame to `dst` and
    /// return [`HandshakeStatus::Pending`]. Protocols with no setup use the
    /// default and become available immediately.
    fn begin_handshake(
        &mut self,
        _dst: &mut BytesMut,
    ) -> std::result::Result<HandshakeStatus, Self::Error> {
        Ok(HandshakeStatus::Ready)
    }

    /// Consume a handshake response and report whether setup is complete.
    ///
    /// This is only called after `begin_handshake` returned `Pending`. A
    /// `Pending` result must mean either that more bytes are needed or that a
    /// further handshake frame was appended to `dst`.
    fn decode_handshake(
        &mut self,
        _src: &mut BytesMut,
        _dst: &mut BytesMut,
    ) -> std::result::Result<HandshakeStatus, Self::Error> {
        Ok(HandshakeStatus::Ready)
    }

    /// Produce one complete, non-empty request frame.
    ///
    /// `request_id` may be ignored by FIFO protocols. Tagged protocols can
    /// encode it into the returned frame without requiring a separate pending
    /// request map.
    fn encode(
        &mut self,
        request: Self::Request,
        request_id: RequestToken,
    ) -> std::result::Result<Self::Frame, Self::Error>;

    /// Decode at most one complete response from the connection's dynamic ring.
    ///
    /// An incomplete decoder that has already observed a frame length should
    /// call [`RxBuffer::reserve`] with the remaining byte count. This lets the
    /// connection grow once before it continues draining the socket.
    fn decode(
        &mut self,
        src: &mut RxBuffer,
    ) -> std::result::Result<Option<DecodedResponse<Self::Response>>, Self::Error>;
}
