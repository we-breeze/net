use std::error::Error;

use bytes::BytesMut;

use crate::RequestToken;

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
/// Implementations append requests directly to the reusable write buffer and
/// remove complete responses from the front of the reusable read buffer.
pub trait SessionProtocol: Send + 'static {
    type Request: Send + 'static;
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

    /// Append one request frame. `request_id` may be ignored by FIFO protocols.
    fn encode(
        &mut self,
        request: &Self::Request,
        request_id: RequestToken,
        dst: &mut BytesMut,
    ) -> std::result::Result<(), Self::Error>;

    /// Decode at most one complete response, consuming its bytes from `src`.
    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<DecodedResponse<Self::Response>>, Self::Error>;
}
