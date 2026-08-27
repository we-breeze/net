# net

`net` 是 Breeze SDK 共用的、协议无关的异步 TCP 基础层。消费方通常将
Cargo package 重命名为 `brz-net`，在 Rust 代码中通过 `brz_net` 使用。

```toml
[dependencies]
brz-net = { package = "net", git = "https://github.com/we-breeze/net.git" }
```

## 核心模型

- `BrzTcpStream`：持有一条物理连接，实现 Tokio `AsyncRead`、
  `AsyncWrite`、`Unpin` 和 `Send`。
- `NodePool`：管理一个逻辑节点的 endpoint 发现、建连、空闲连接和回收。
- `Pool<C>`：在多个等价 `C` 之间按观测耗时负载均衡并快速隔离连续失败者。
- `Sharded<C, R>`：使用 key 和 `ShardRouter` 选择一个 `C`。
- `StreamProvider<K>`：以上组件共同实现的组合接口。
- `TcpClient<P>`：包裹最终组合，在最外层实施统一 operation deadline。

组合顺序就是执行顺序：

```rust,ignore
// Redis：key -> shard -> slave replica -> physical connection
type RedisNet<R> = Sharded<Pool<NodePool>, R>;

// MC：complete replica -> key -> shard -> physical connection
type McNet<R> = Pool<Sharded<NodePool, R>>;

// 普通的多副本服务
type MotanNet = Pool<NodePool>;
```

每个 `Sharded` 都持有自己的 router 和 shard 数量，因此 MC 的不同完整副本
可以具有不同的分片数。

## 使用连接

`TcpClient::with_conn` 是主要入口。回调成功时连接才会回收到原 `NodePool`；回调
错误、Future 被取消、发生异步 I/O 错误、operation 超时或显式调用 `discard()`
时，连接都会关闭。

```rust,ignore
use std::time::Duration;

use brz_net::{NodePool, NodePoolOptions, TcpClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

let node = NodePool::from_endpoints(
    ["127.0.0.1:11211".parse()?],
    NodePoolOptions::default(),
)?;
let client = TcpClient::new(node, Duration::from_millis(400))?;

let value = client
    .with_conn(&(), |stream| {
        Box::pin(async move {
            stream.write_all(b"version\r\n").await?;
            let mut response = vec![0; 128];
            let length = stream.read(&mut response).await?;
            Ok::<_, std::io::Error>(response[..length].to_vec())
        })
    })
    .await?;
```

协议实现必须在返回 `Ok` 前完成一次完整的请求/响应交换，不能把留有未消费
数据的连接标记为成功。

每次调用只创建一个 Tokio `timeout_at`，使用同一个绝对 deadline 覆盖等待连接池、
建连、写请求和读取完整响应。超时取消 Future 后，可能已经部分读写的物理连接会被
丢弃，并自动归还 `NodePool` 的连接计数。

## 动态 endpoint

`EndpointSet` 使用 copy-on-write 不可变快照并原子发布更新，可由 Vintage 等发现
适配器更新。连接获取路径只读取快照，不会与发现更新争用共享锁。新请求总是使用
最新快照，已移除 endpoint 的空闲连接不会再复用。

`DnsSource` 只接受 IPv4 hostname，默认复用进程级 `DnsResolver`。解析器按 hostname
去重（端口由订阅者各自保留），不会为每个 `NodePool` 创建定时任务：新注册域名在
下一个 1 秒调度 tick 执行首次解析，之后才按 `refresh_interval` 分散刷新。查询并发
默认限制为 1，用单个 lookup task 平滑万级域名解析对系统 resolver 的压力；解析
失败保留最后一次成功结果，只有地址集合实际变化时才发布新
快照。

通常不需要调整并发；确认系统能够承受且需要加快解析时，可以显式共享一个带自定义
上限的解析器：

```rust,ignore
use brz_net::{DnsOptions, DnsResolver, DnsResolverOptions, NodePool, NodePoolOptions};

let resolver = DnsResolver::new(DnsResolverOptions {
    max_concurrent_lookups: 4,
    ..DnsResolverOptions::default()
})?;
let node = NodePool::from_dns_with_resolver(
    &resolver,
    ["redis.example:6379"],
    DnsOptions::default(),
    NodePoolOptions::default(),
)
.await?;
```

## 验证

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```
