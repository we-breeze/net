use std::{
    future::pending,
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use net::{
    CallError, EndpointSet, NetError, NodePool, NodePoolOptions, Pool, QuotaBalancerOptions,
    Sharded, StreamProvider, TcpClient,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
};

static NO_KEY: () = ();

struct TestServer {
    address: SocketAddr,
    accepts: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(identity: u8) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = accepts.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut request = [0_u8; 1];
                    loop {
                        if stream.read_exact(&mut request).await.is_err() {
                            return;
                        }
                        if stream.write_all(&[identity]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        Self {
            address,
            accepts,
            task,
        }
    }

    fn accept_count(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }

    async fn start_with_first_partial_response(identity: u8) -> (Self, oneshot::Receiver<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let counter = accepts.clone();
        let (first_request, first_request_received) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut first_request = Some(first_request);
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let accepted = counter.fetch_add(1, Ordering::SeqCst);
                let first_request = if accepted == 0 {
                    first_request.take()
                } else {
                    None
                };
                tokio::spawn(async move {
                    let mut request = [0_u8; 1];
                    if stream.read_exact(&mut request).await.is_err() {
                        return;
                    }
                    if let Some(first_request) = first_request {
                        let _ = stream.write_all(&[identity]).await;
                        let _ = first_request.send(());
                        pending::<()>().await;
                    }
                    loop {
                        if stream.write_all(&[identity]).await.is_err()
                            || stream.read_exact(&mut request).await.is_err()
                        {
                            return;
                        }
                    }
                });
            }
        });
        (
            Self {
                address,
                accepts,
                task,
            },
            first_request_received,
        )
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn node(address: SocketAddr) -> NodePool {
    NodePool::from_endpoints(
        [address],
        NodePoolOptions {
            min_connections: 0,
            ..NodePoolOptions::default()
        },
    )
    .unwrap()
}

async fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(timeout, async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition was not met before timeout");
}

#[test]
fn endpoint_updates_publish_immutable_cow_snapshots() {
    let first = "127.0.0.1:1001".parse().unwrap();
    let second = "127.0.0.1:1002".parse().unwrap();
    let endpoints = EndpointSet::new([first]);

    let before = endpoints.snapshot();
    assert!(endpoints.replace([second]));
    let after = endpoints.snapshot();

    assert_eq!(before.as_ref(), [first]);
    assert_eq!(after.as_ref(), [second]);
}

async fn request<K, P>(provider: &P, key: &K) -> Result<u8, CallError<io::Error>>
where
    K: ?Sized + Sync,
    P: StreamProvider<K>,
{
    provider
        .with_conn(key, |stream| {
            Box::pin(async move {
                stream.write_all(&[1]).await?;
                let mut response = [0_u8; 1];
                stream.read_exact(&mut response).await?;
                Ok(response[0])
            })
        })
        .await
}

#[test]
fn brz_stream_has_standard_async_io_traits() {
    fn assert_traits<T: AsyncRead + AsyncWrite + Unpin + Send>() {}
    assert_traits::<net::BrzTcpStream>();
}

#[test]
fn node_pool_defaults_prioritize_ready_low_latency_connections() {
    let options = NodePoolOptions::default();
    assert_eq!(options.min_connections, 1);
    assert_eq!(options.max_connections, 256);
    assert_eq!(options.idle_timeout, Duration::from_secs(300));
    assert!(options.tcp_nodelay);
}

#[tokio::test]
async fn zero_operation_timeout_is_rejected() {
    let node =
        NodePool::from_endpoints(["127.0.0.1:1".parse().unwrap()], NodePoolOptions::default())
            .unwrap();
    let error = TcpClient::new(node, Duration::ZERO).unwrap_err();
    assert!(matches!(error, NetError::InvalidConfig(_)));
}

#[tokio::test]
async fn read_timeout_discards_connection_and_releases_the_node_slot() {
    let (server, first_request_received) = TestServer::start_with_first_partial_response(70).await;
    let options = NodePoolOptions {
        min_connections: 0,
        max_connections: 1,
        ..NodePoolOptions::default()
    };
    let node = NodePool::from_endpoints([server.address], options).unwrap();
    let client = TcpClient::new(node.clone(), Duration::from_millis(100)).unwrap();

    let first = tokio::spawn(async move {
        client
            .with_conn(&NO_KEY, |stream| {
                Box::pin(async move {
                    stream.write_all(&[1]).await?;
                    let mut response = [0_u8; 2];
                    stream.read_exact(&mut response).await?;
                    Ok::<_, io::Error>(response)
                })
            })
            .await
    });
    first_request_received.await.unwrap();
    assert_eq!(node.stats().total_connections, 1);
    assert_eq!(node.stats().checked_out_connections, 1);

    let result = request(&node, &NO_KEY).await;
    assert!(matches!(
        result,
        Err(CallError::Net(NetError::PoolExhausted {
            max_connections: 1
        }))
    ));

    let first = first.await.unwrap();
    assert!(matches!(
        first,
        Err(CallError::Timeout { timeout }) if timeout == Duration::from_millis(100)
    ));
    assert_eq!(request(&node, &NO_KEY).await.unwrap(), 70);
    assert_eq!(server.accept_count(), 2);
    assert_eq!(node.stats().total_connections, 1);
    assert_eq!(node.stats().idle_connections, 1);
}

#[tokio::test]
async fn exhausted_pool_fails_without_invoking_the_operation() {
    let server = TestServer::start(71).await;
    let options = NodePoolOptions {
        min_connections: 0,
        max_connections: 1,
        ..NodePoolOptions::default()
    };
    let node = NodePool::from_endpoints([server.address], options).unwrap();
    let first_node = node.clone();
    let (entered, entered_rx) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let first = tokio::spawn(async move {
        first_node
            .with_conn(&NO_KEY, move |_stream| {
                Box::pin(async move {
                    let _ = entered.send(());
                    let _ = release_rx.await;
                    Ok::<_, io::Error>(())
                })
            })
            .await
    });
    entered_rx.await.unwrap();

    let called = Arc::new(AtomicBool::new(false));
    let operation_called = called.clone();
    let client = TcpClient::new(node.clone(), Duration::from_secs(1)).unwrap();
    let result: Result<(), CallError<io::Error>> = client
        .with_conn(&NO_KEY, move |_stream| {
            operation_called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        })
        .await;

    assert!(matches!(
        result,
        Err(CallError::Net(NetError::PoolExhausted {
            max_connections: 1
        }))
    ));
    assert!(!called.load(Ordering::SeqCst));
    assert_eq!(node.stats().total_connections, 1);
    assert_eq!(node.stats().checked_out_connections, 1);

    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    assert_eq!(node.stats().total_connections, 1);
    assert_eq!(node.stats().idle_connections, 1);
}

#[tokio::test]
async fn successful_operations_reuse_the_same_connection() {
    let server = TestServer::start(7).await;
    let node = node(server.address);

    assert_eq!(request(&node, &NO_KEY).await.unwrap(), 7);
    assert_eq!(request(&node, &NO_KEY).await.unwrap(), 7);
    assert_eq!(server.accept_count(), 1);
    assert_eq!(node.stats().total_connections, 1);
    assert_eq!(node.stats().idle_connections, 1);
}

#[tokio::test]
async fn operation_errors_discard_the_connection() {
    let server = TestServer::start(8).await;
    let node = node(server.address);

    let result: Result<(), CallError<&'static str>> = node
        .with_conn(&NO_KEY, |_stream| Box::pin(async { Err("failed") }))
        .await;
    assert!(matches!(result, Err(CallError::Operation("failed"))));
    assert_eq!(node.stats().total_connections, 0);

    assert_eq!(request(&node, &NO_KEY).await.unwrap(), 8);
    tokio::task::yield_now().await;
    assert_eq!(server.accept_count(), 2);
}

#[tokio::test]
async fn cancellation_discards_a_checked_out_connection() {
    let server = TestServer::start(9).await;
    let node = node(server.address);
    let task_node = node.clone();
    let (entered_tx, entered_rx) = oneshot::channel();

    let task = tokio::spawn(async move {
        task_node
            .with_conn(&NO_KEY, move |_stream| {
                Box::pin(async move {
                    let _ = entered_tx.send(());
                    pending::<Result<(), io::Error>>().await
                })
            })
            .await
    });

    entered_rx.await.unwrap();
    assert_eq!(node.stats().checked_out_connections, 1);
    task.abort();
    let _ = task.await;
    assert_eq!(node.stats().total_connections, 0);
}

#[tokio::test]
async fn default_min_connection_is_created_in_the_background() {
    let server = TestServer::start(10).await;
    let node = NodePool::from_endpoints([server.address], NodePoolOptions::default()).unwrap();

    wait_until(Duration::from_secs(2), || {
        node.stats().idle_connections == 1
    })
    .await;
    assert_eq!(node.stats().total_connections, 1);
    assert_eq!(server.accept_count(), 1);
}

#[tokio::test]
async fn background_maintenance_reaps_idle_connections_but_preserves_minimum() {
    let server = TestServer::start(13).await;
    let node = NodePool::from_endpoints(
        [server.address],
        NodePoolOptions {
            min_connections: 1,
            max_connections: 2,
            idle_timeout: Duration::from_millis(50),
            ..NodePoolOptions::default()
        },
    )
    .unwrap();
    wait_until(Duration::from_secs(2), || {
        node.stats().idle_connections == 1
    })
    .await;

    let first_node = node.clone();
    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let first = tokio::spawn(async move {
        first_node
            .with_conn(&NO_KEY, move |_stream| {
                Box::pin(async move {
                    let _ = entered_tx.send(());
                    let _ = release_rx.await;
                    Ok::<_, io::Error>(())
                })
            })
            .await
    });
    entered_rx.await.unwrap();
    assert_eq!(request(&node, &NO_KEY).await.unwrap(), 13);
    release_tx.send(()).unwrap();
    first.await.unwrap().unwrap();
    assert_eq!(node.stats().idle_connections, 2);

    wait_until(Duration::from_secs(2), || {
        node.stats().total_connections == 1 && node.stats().idle_connections == 1
    })
    .await;
}

#[tokio::test]
async fn endpoint_updates_stop_reusing_removed_addresses() {
    let first = TestServer::start(11).await;
    let second = TestServer::start(12).await;
    let endpoints = EndpointSet::new([first.address]);
    let node = NodePool::new(endpoints.clone(), NodePoolOptions::default()).unwrap();

    wait_until(Duration::from_secs(2), || {
        node.stats().idle_connections == 1
    })
    .await;
    assert_eq!(request(&node, &NO_KEY).await.unwrap(), 11);
    assert!(endpoints.replace([second.address]));
    wait_until(Duration::from_secs(3), || {
        second.accept_count() == 1 && node.stats().idle_connections == 1
    })
    .await;
    assert_eq!(request(&node, &NO_KEY).await.unwrap(), 12);
    assert_eq!(node.stats().total_connections, 1);
}

fn modulo(key: &u64, shard_count: usize) -> usize {
    *key as usize % shard_count
}

fn quick_quota() -> QuotaBalancerOptions {
    QuotaBalancerOptions {
        quota: Duration::from_micros(1),
        ..QuotaBalancerOptions::default()
    }
}

#[tokio::test]
async fn redis_composition_shards_before_balancing_replicas() {
    let shard_zero_a = TestServer::start(20).await;
    let shard_zero_b = TestServer::start(21).await;
    let shard_one = TestServer::start(30).await;

    let shard_zero = Pool::with_options(
        [node(shard_zero_a.address), node(shard_zero_b.address)],
        quick_quota(),
    )
    .unwrap();
    let shard_one = Pool::new([node(shard_one.address)]).unwrap();
    let redis = Sharded::new(modulo as fn(&u64, usize) -> usize, [shard_zero, shard_one]).unwrap();

    assert_eq!(request(&redis, &1).await.unwrap(), 30);
    let first = request(&redis, &0).await.unwrap();
    let second = request(&redis, &0).await.unwrap();
    let mut identities = [first, second];
    identities.sort_unstable();
    assert_eq!(identities, [20, 21]);
}

#[tokio::test]
async fn mc_composition_balances_before_each_replica_shards() {
    let a0 = TestServer::start(40).await;
    let a1 = TestServer::start(41).await;
    let b0 = TestServer::start(50).await;
    let b1 = TestServer::start(51).await;
    let b2 = TestServer::start(52).await;

    let replica_a = Sharded::new(
        modulo as fn(&u64, usize) -> usize,
        [node(a0.address), node(a1.address)],
    )
    .unwrap();
    let replica_b = Sharded::new(
        modulo as fn(&u64, usize) -> usize,
        [node(b0.address), node(b1.address), node(b2.address)],
    )
    .unwrap();
    let mc = Pool::with_options([replica_a, replica_b], quick_quota()).unwrap();

    // key 4 maps to shard 0 in the two-node replica and shard 1 in the
    // three-node replica. The first two calls explore both full replicas.
    let mut identities = [
        request(&mc, &4).await.unwrap(),
        request(&mc, &4).await.unwrap(),
    ];
    identities.sort_unstable();
    assert_eq!(identities, [40, 51]);
}

#[tokio::test]
async fn quota_pool_rotates_only_after_the_current_replica_consumes_its_quota() {
    let first = TestServer::start(60).await;
    let second = TestServer::start(61).await;
    let pool =
        Pool::with_options([node(first.address), node(second.address)], quick_quota()).unwrap();

    let first_identity = request(&pool, &NO_KEY).await.unwrap();
    let second_identity = request(&pool, &NO_KEY).await.unwrap();
    let third_identity = request(&pool, &NO_KEY).await.unwrap();

    assert_ne!(first_identity, second_identity);
    assert_eq!(third_identity, first_identity);
}
