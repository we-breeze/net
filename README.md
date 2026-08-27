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
- `NodePool<S>`：管理一个逻辑节点的 endpoint 发现、建连、空闲连接和回收；
  `S: EndpointSource` 使用静态分发，不在请求路径保存 `dyn` trait object。
- `Pool<C>`：在多个等价 `C` 之间按累计请求耗时 quota 轮转。
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

## 副本选择

`Pool` 初始化时随机排列副本，之后从第一个副本开始选择。当前副本累计完成的请求
耗时达到默认 2 秒 quota 后，下一个请求原子切换到下一个副本，并清空原副本 quota。
因此相同 quota 内，耗时越高的副本获得的请求数量越少。

成功请求按实际耗时累加；失败、取消或连接获取错误至少消耗 500ms quota。选择和
计数热路径只使用原子操作，不遍历副本。高并发时已经发出的请求可能让 quota 略微
超出 2 秒，这是有意保留的近似行为。本层目前不自动重试失败请求。

可通过 `QuotaBalancerOptions` 调整 quota 和失败计费：

```rust,ignore
use std::time::Duration;

use brz_net::{Pool, QuotaBalancerOptions};

let pool = Pool::with_options(
    replicas,
    QuotaBalancerOptions {
        quota: Duration::from_secs(2),
        failure_penalty: Duration::from_millis(500),
    },
)?;
```

`NodePoolOptions::default()` 面向低延迟请求：

- `min_connections = 1`，后台保持至少一条可用连接；
- `max_connections = 256`；
- `idle_timeout = 300s`；
- `tcp_nodelay = true`，关闭 Nagle 聚合，优先降低小包请求延迟。

没有空闲连接时，请求会在未达到 `max_connections` 的前提下直接建立新连接；
容量已满则立即返回 `NetError::PoolExhausted`，不会排队等待。调用方可以据此重试、
选择其他副本或快速失败。

连接获取和成功归还的热路径使用有界 MPMC 队列及原子容量计数，不获取互斥锁：
借用已有连接只执行队列 `pop`，归还只执行队列 `push`，新建连接通过原子操作预占
总连接名额。`total_connections` 是严格容量边界；`NodePoolStats` 的各字段是分别读取
的瞬时观测值，并发过程中不保证来自同一个时刻。

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

每次调用只创建一个 Tokio `timeout_at`，使用同一个绝对 deadline 覆盖连接获取、
建连、写请求和读取完整响应。超时取消 Future 后，可能已经部分读写的物理连接会被
丢弃，并自动归还 `NodePool` 的连接计数。

只有完整操作返回 `Ok` 的连接才会回池。协议错误、I/O 错误、Future 取消和超时的
连接都会直接关闭，不进入空闲池。

## 连接维护

请求路径不扫描过期连接，也不检查每条空闲连接是否仍属于最新 DNS/endpoint 集合。
进程级共享维护器每秒扫描一次所有存活的 `NodePool`，通过同一无锁队列在后台完成：

- 清除已经从 endpoint 集合移除的空闲连接；
- 清除超过 `idle_timeout` 的空闲连接，但保留 `min_connections`；
- 异步补足 `min_connections`。

后台补连接全进程同时只发起一个，避免大量节点启动时形成建连尖峰；请求触发的按需
建连不受这个后台限制。`NodePool` 需要在 Tokio runtime 内创建。默认每个逻辑节点
保持一条连接，因此万级节点场景会相应保留万级基础连接；不需要预热时可显式配置
`min_connections = 0`。

## 动态 endpoint

`EndpointSet` 使用 copy-on-write 不可变快照并原子发布更新，可由 Vintage 等发现
适配器更新。连接获取路径只读取快照，不会与发现更新争用共享锁。已移除 endpoint
的空闲连接由共享维护器在下一个维护 tick 清除，避免把集合遍历放到请求路径。

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
