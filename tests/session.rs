use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use brz_net::{
    DecodedResponse, HandshakeStatus, MAX_IN_FLIGHT, Node, NodeConnectionObserver, NodeOptions,
    ReplicaSet, RequestTarget, RequestToken, RxBuffer, SessionError, SessionProtocol, Sharded,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    time::{Instant, sleep},
};

#[derive(Default)]
struct ConnectionTransitions {
    connected: AtomicUsize,
    disconnected: AtomicUsize,
}

impl NodeConnectionObserver for ConnectionTransitions {
    fn on_connection_state_change(&self, connected: bool) {
        if connected {
            self.connected.fetch_add(1, Ordering::Relaxed);
        } else {
            self.disconnected.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy)]
struct ByteFifo;

impl SessionProtocol for ByteFifo {
    type Request = u8;
    type Frame = [u8; 1];
    type Response = u8;
    type Error = Infallible;

    fn encode(
        &mut self,
        request: Self::Request,
        _request_id: RequestToken,
    ) -> Result<Self::Frame, Self::Error> {
        Ok([request])
    }

    fn decode(
        &mut self,
        src: &mut RxBuffer,
    ) -> Result<Option<DecodedResponse<Self::Response>>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let response = src.byte(0).unwrap();
        src.advance(1);
        Ok(Some(DecodedResponse::fifo(response)))
    }
}

#[derive(Clone, Copy)]
struct ByteTagged;

impl SessionProtocol for ByteTagged {
    type Request = u8;
    type Frame = Bytes;
    type Response = u8;
    type Error = Infallible;

    fn encode(
        &mut self,
        request: Self::Request,
        request_id: RequestToken,
    ) -> Result<Self::Frame, Self::Error> {
        let mut dst = BytesMut::with_capacity(9);
        dst.put_u64(request_id.as_u64());
        dst.put_u8(request);
        Ok(dst.freeze())
    }

    fn decode(
        &mut self,
        src: &mut RxBuffer,
    ) -> Result<Option<DecodedResponse<Self::Response>>, Self::Error> {
        if src.len() < 9 {
            return Ok(None);
        }
        let request_id = (0..8).fold(0_u64, |value, index| {
            (value << 8) | u64::from(src.byte(index).unwrap())
        });
        let response = src.byte(8).unwrap();
        src.advance(9);
        Ok(Some(DecodedResponse::tagged(request_id, response)))
    }
}

#[derive(Clone, Copy)]
struct AuthenticatedByteFifo;

impl SessionProtocol for AuthenticatedByteFifo {
    type Request = u8;
    type Frame = [u8; 1];
    type Response = u8;
    type Error = Infallible;

    fn begin_handshake(&mut self, dst: &mut BytesMut) -> Result<HandshakeStatus, Self::Error> {
        dst.put_u8(0xaa);
        Ok(HandshakeStatus::Pending)
    }

    fn decode_handshake(
        &mut self,
        src: &mut BytesMut,
        _dst: &mut BytesMut,
    ) -> Result<HandshakeStatus, Self::Error> {
        if src.is_empty() {
            return Ok(HandshakeStatus::Pending);
        }
        assert_eq!(src.get_u8(), 0xbb);
        Ok(HandshakeStatus::Ready)
    }

    fn encode(
        &mut self,
        request: Self::Request,
        _request_id: RequestToken,
    ) -> Result<Self::Frame, Self::Error> {
        Ok([request])
    }

    fn decode(
        &mut self,
        src: &mut RxBuffer,
    ) -> Result<Option<DecodedResponse<Self::Response>>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let response = src.byte(0).unwrap();
        src.advance(1);
        Ok(Some(DecodedResponse::fifo(response)))
    }
}

struct DropFrame {
    byte: [u8; 1],
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for DropFrame {
    fn as_ref(&self) -> &[u8] {
        &self.byte
    }
}

impl Drop for DropFrame {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

struct DropTrackedFifo {
    drops: Arc<AtomicUsize>,
}

impl SessionProtocol for DropTrackedFifo {
    type Request = u8;
    type Frame = DropFrame;
    type Response = u8;
    type Error = Infallible;

    fn encode(
        &mut self,
        request: Self::Request,
        _request_id: RequestToken,
    ) -> Result<Self::Frame, Self::Error> {
        Ok(DropFrame {
            byte: [request],
            drops: self.drops.clone(),
        })
    }

    fn decode(
        &mut self,
        src: &mut RxBuffer,
    ) -> Result<Option<DecodedResponse<Self::Response>>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let response = src.byte(0).unwrap();
        src.advance(1);
        Ok(Some(DecodedResponse::fifo(response)))
    }
}

fn test_options() -> NodeOptions {
    NodeOptions {
        request_timeout: Duration::from_secs(2),
        connect_timeout: Duration::from_millis(200),
        reconnect_delay: Duration::from_millis(10),
        max_reconnect_delay: Duration::from_millis(40),
        ..NodeOptions::default()
    }
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(Instant::now() < deadline, "condition did not become true");
        sleep(Duration::from_millis(2)).await;
    }
}

async fn spawn_fifo_echo() -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let server_accepts = accepts.clone();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        server_accepts.fetch_add(1, Ordering::Relaxed);
        let mut buffer = [0_u8; 4096];
        loop {
            let length = match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(length) => length,
            };
            if stream.write_all(&buffer[..length]).await.is_err() {
                return;
            }
        }
    });
    (address, accepts)
}

#[tokio::test]
async fn many_fifo_requests_share_exactly_one_tcp_connection() {
    let (address, accepts) = spawn_fifo_echo().await;
    let node = Node::new(address, ByteFifo, test_options()).unwrap();
    wait_until(|| node.is_connected()).await;

    let responses = (0_u8..128)
        .map(|value| (value, node.request(value).unwrap()))
        .collect::<Vec<_>>();
    for (expected, response) in responses {
        assert_eq!(response.await.unwrap(), expected);
    }

    assert_eq!(accepts.load(Ordering::Relaxed), 1);
    assert_eq!(node.stats().successful_connections, 1);
    assert_eq!(node.stats().active_requests, 0);
}

#[tokio::test]
async fn timed_request_reports_only_the_post_send_attempt_duration() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = stream.read_u8().await.unwrap();
        sleep(Duration::from_millis(10)).await;
        stream.write_u8(request).await.unwrap();
    });

    let node = Node::new(address, ByteFifo, test_options()).unwrap();
    wait_until(|| node.is_connected()).await;
    let response = node.request_with_timing(|| 7).unwrap().await;

    assert_eq!(response.value.unwrap(), 7);
    let real = response
        .timing
        .sent_to_completed
        .expect("encoded non-empty frame must reach the send stage");
    assert!(real >= Duration::from_millis(5));
    assert!(real < Duration::from_secs(1));
}

#[tokio::test]
async fn finite_batch_submits_independent_fifo_requests_in_order() {
    let (address, accepts) = spawn_fifo_echo().await;
    let node = Node::new(address, ByteFifo, test_options()).unwrap();
    wait_until(|| node.is_connected()).await;

    let responses = node.request_batch((0_u8..128).collect()).unwrap();
    assert_eq!(responses.len(), 128);
    for (expected, response) in (0_u8..128).zip(responses) {
        assert_eq!(response.await.unwrap(), expected);
    }

    assert_eq!(accepts.load(Ordering::Relaxed), 1);
    assert_eq!(node.stats().active_requests, 0);
}

#[tokio::test]
async fn tagged_responses_may_arrive_out_of_order() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut frames = [[0_u8; 9]; 2];
        stream.read_exact(&mut frames[0]).await.unwrap();
        stream.read_exact(&mut frames[1]).await.unwrap();
        stream.write_all(&frames[1]).await.unwrap();
        stream.write_all(&frames[0]).await.unwrap();
    });

    let node = Node::new(address, ByteTagged, test_options()).unwrap();
    wait_until(|| node.is_connected()).await;
    let first = node.request(11).unwrap();
    let second = node.request(22).unwrap();

    assert_eq!(first.await.unwrap(), 11);
    assert_eq!(second.await.unwrap(), 22);
}

#[tokio::test]
async fn cancelling_a_fifo_future_does_not_shift_later_responses() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut requests = [0_u8; 2];
        stream.read_exact(&mut requests).await.unwrap();
        stream.write_all(&requests).await.unwrap();
    });

    let node = Node::new(address, ByteFifo, test_options()).unwrap();
    wait_until(|| node.is_connected()).await;
    let cancelled = node.request(11).unwrap();
    let retained = node.request(22).unwrap();
    drop(cancelled);

    assert_eq!(retained.await.unwrap(), 22);
}

#[tokio::test]
async fn node_becomes_available_only_after_protocol_handshake() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        assert_eq!(stream.read_u8().await.unwrap(), 0xaa);
        sleep(Duration::from_millis(20)).await;
        stream.write_u8(0xbb).await.unwrap();

        let request = stream.read_u8().await.unwrap();
        stream.write_u8(request).await.unwrap();
    });

    let node = Node::new(address, AuthenticatedByteFifo, test_options()).unwrap();
    assert!(!node.is_connected());
    assert!(matches!(node.request(7), Err(SessionError::Unavailable)));
    wait_until(|| node.is_connected()).await;
    assert_eq!(node.request(7).unwrap().await.unwrap(), 7);
    assert_eq!(node.stats().successful_connections, 1);
}

#[tokio::test]
async fn capacity_is_strictly_bounded_and_fails_fast() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = [0_u8; 4096];
        while stream.read(&mut buffer).await.unwrap_or(0) != 0 {}
    });

    let mut options = test_options();
    options.request_timeout = Duration::from_secs(10);
    let node = Node::new(address, ByteFifo, options).unwrap();
    wait_until(|| node.is_connected()).await;

    let requests = (0..MAX_IN_FLIGHT)
        .map(|value| node.request(value as u8).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(node.stats().active_requests, MAX_IN_FLIGHT);
    assert!(matches!(node.request(0), Err(SessionError::Busy)));

    let builds = AtomicUsize::new(0);
    assert!(matches!(
        node.request_with(|| {
            builds.fetch_add(1, Ordering::Relaxed);
            0
        }),
        Err(SessionError::Busy)
    ));
    assert_eq!(builds.load(Ordering::Relaxed), 0);

    drop(requests);
    drop(node);
}

#[tokio::test]
async fn panicking_request_builder_releases_its_admission() {
    let (address, _) = spawn_fifo_echo().await;
    let node = Node::new(address, ByteFifo, test_options()).unwrap();
    wait_until(|| node.is_connected()).await;

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = node.request_with(|| -> u8 { panic!("builder failed") });
    }));
    assert!(panic.is_err());
    assert_eq!(node.stats().active_requests, 0);
    assert_eq!(node.request(9).unwrap().await.unwrap(), 9);
}

#[tokio::test]
async fn owned_frame_is_released_after_socket_write_not_response() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let request_read = Arc::new(Notify::new());
    let allow_response = Arc::new(Notify::new());
    let server_request_read = request_read.clone();
    let server_allow_response = allow_response.clone();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = stream.read_u8().await.unwrap();
        server_request_read.notify_one();
        server_allow_response.notified().await;
        stream.write_u8(request).await.unwrap();
    });

    let drops = Arc::new(AtomicUsize::new(0));
    let node = Node::new(
        address,
        DropTrackedFifo {
            drops: drops.clone(),
        },
        test_options(),
    )
    .unwrap();
    wait_until(|| node.is_connected()).await;

    let response = node.request(17).unwrap();
    request_read.notified().await;
    wait_until(|| drops.load(Ordering::Relaxed) == 1).await;
    allow_response.notify_one();

    assert_eq!(response.await.unwrap(), 17);
}

#[tokio::test]
async fn oldest_timeout_closes_connection_fails_all_and_reconnects() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let server_accepts = accepts.clone();
    tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        server_accepts.fetch_add(1, Ordering::Relaxed);
        let mut request = [0_u8; 2];
        first.read_exact(&mut request).await.unwrap();
        let mut eof = [0_u8; 1];
        let _ = first.read(&mut eof).await;

        let (mut second, _) = listener.accept().await.unwrap();
        server_accepts.fetch_add(1, Ordering::Relaxed);
        let mut buffer = [0_u8; 32];
        loop {
            let length = match second.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(length) => length,
            };
            if second.write_all(&buffer[..length]).await.is_err() {
                return;
            }
        }
    });

    let mut options = test_options();
    options.request_timeout = Duration::from_millis(40);
    let node = Node::new(address, ByteFifo, options).unwrap();
    wait_until(|| node.is_connected()).await;

    let first = node.request(1).unwrap();
    let second = node.request(2).unwrap();
    assert!(matches!(first.await, Err(SessionError::Timeout { .. })));
    assert!(matches!(second.await, Err(SessionError::Timeout { .. })));

    wait_until(|| accepts.load(Ordering::Relaxed) >= 2 && node.is_connected()).await;
    assert_eq!(node.request(3).unwrap().await.unwrap(), 3);
    assert_eq!(node.stats().disconnects, 1);
}

#[tokio::test]
async fn idle_peer_close_is_detected_and_reconnected_before_the_next_request() {
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let allow_idle_close = Arc::new(Notify::new());
    let server_accepts = accepts.clone();
    let server_allow_idle_close = allow_idle_close.clone();
    tokio::spawn(async move {
        let (first, _) = listener.accept().await.unwrap();
        server_accepts.fetch_add(1, Ordering::Relaxed);
        server_allow_idle_close.notified().await;
        drop(first);

        let (mut second, _) = listener.accept().await.unwrap();
        server_accepts.fetch_add(1, Ordering::Relaxed);
        let mut buffer = [0_u8; 32];
        loop {
            let length = match second.read(&mut buffer).await {
                Ok(0) | Err(_) => return,
                Ok(length) => length,
            };
            if second.write_all(&buffer[..length]).await.is_err() {
                return;
            }
        }
    });

    let transitions = Arc::new(ConnectionTransitions::default());
    let node = Node::new_observed(address, ByteFifo, test_options(), transitions.clone()).unwrap();
    wait_until(|| node.is_connected() && accepts.load(Ordering::Relaxed) == 1).await;
    assert_eq!(node.stats().active_requests, 0);

    allow_idle_close.notify_one();
    wait_until(|| {
        node.is_connected()
            && node.stats().successful_connections >= 2
            && accepts.load(Ordering::Relaxed) >= 2
            && transitions.connected.load(Ordering::Relaxed) >= 2
    })
    .await;

    assert_eq!(node.request(23).unwrap().await.unwrap(), 23);
    assert_eq!(node.stats().disconnects, 1);
    assert_eq!(transitions.connected.load(Ordering::Relaxed), 2);
    assert_eq!(transitions.disconnected.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn sharding_and_replica_selection_compose_in_either_order() {
    let (address, _) = spawn_fifo_echo().await;
    let node = Node::new(address, ByteFifo, test_options()).unwrap();
    wait_until(|| node.is_connected()).await;

    let router = |key: &u64, shard_count: usize| *key as usize % shard_count;
    let redis_order = Sharded::new(
        router,
        [ReplicaSet::new([node.clone(), node.clone()]).unwrap()],
    )
    .unwrap();
    assert_eq!(redis_order.request_for(&7, 31).unwrap().await.unwrap(), 31);

    let mc_order = ReplicaSet::new([
        Sharded::new(router, [node.clone()]).unwrap(),
        Sharded::new(router, [node.clone()]).unwrap(),
    ])
    .unwrap();
    assert_eq!(mc_order.request_for(&9, 47).unwrap().await.unwrap(), 47);
}
