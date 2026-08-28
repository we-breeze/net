# net

`net` 是 Breeze SDK 共用的、协议无关的单连接异步传输层。Cargo package
直接叫 `net`；消费方可重命名为 `brz-net`，在 Rust 代码中通过 `brz_net`
使用。

```toml
[dependencies]
brz-net = { package = "net", path = "crates/breeze/net" }
```

## 核心模型

- `Node<P>` 对应一个物理 endpoint，后台只有一个 task，并且任何时刻最多持有
  一条持久 TCP 连接；没有连接池、borrow/lease、idle connection 或连接归还。
- 调用方提交结构化的 `P::Request`。`SessionProtocol` 直接将请求编码进连接 task
  复用的 `BytesMut`，并从复用的读取 buffer 解码完整响应。
- 请求通过容量为 256 的有界 MPSC `try_send` 进入连接 task。容量耗尽立即返回
  `SessionError::Busy`，不会等待。
- 每条连接固定分配 256 个 response slot。每个 slot 使用 `AtomicWaker`，不会为
  每次请求创建 `oneshot`；request ID 低 8 位是 slot index，高位是 generation，
  防止迟到响应命中已复用的 slot。
- `ReplicaSet<T>` 在等价副本间按耗时 quota 选择，`Sharded<T, R>` 按 key 选择
  分片。二者通过纯泛型 `RequestTarget<K>` 任意组合，请求热路径没有
  `Mutex`/`RwLock` 或 `dyn` dispatch。

典型组合如下：

```rust,ignore
use brz_net::{Node, ReplicaSet, Sharded};

// Redis slave：先按 key 找 shard，再在该 shard 的 slave 间选择副本。
type RedisSlaves<P, R> = Sharded<ReplicaSet<Node<P>>, R>;

// CacheService：先选一个完整副本，再按该副本自己的 shard 数进行路由。
type CacheService<P, R> = ReplicaSet<Sharded<Node<P>, R>>;

// 普通多副本服务，例如 Motan。
type Motan<P> = ReplicaSet<Node<P>>;
```

没有分片时直接使用 `Node<P>` 或 `ReplicaSet<Node<P>>`，不需要创建假的 shard。

## 协议和响应关联

`SessionProtocol` 只负责协议相关的三件事：连接握手、请求编码、响应解码。

- Redis/MC 返回 `DecodedResponse::fifo(response)`，响应与连接中最早的 pending
  request 对应。
- Motan 一类允许乱序返回的协议，将 `RequestToken::as_u64()` 写入 request ID，
  解码时返回 `DecodedResponse::tagged(request_id, response)`，直接定位固定 slot。
- Redis AUTH 等连接级初始化由 `begin_handshake`/`decode_handshake` 完成。握手成功
  前 `Node` 不对调用方标记为 available；重连后会重新握手。

简化的 FIFO codec：

```rust,ignore
use bytes::{Buf, BufMut, BytesMut};
use brz_net::{DecodedResponse, RequestToken, SessionProtocol};

struct Codec;

impl SessionProtocol for Codec {
    type Request = u8;
    type Response = u8;
    type Error = std::convert::Infallible;

    fn encode(
        &mut self,
        request: &u8,
        _: RequestToken,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        dst.put_u8(*request);
        Ok(())
    }

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<DecodedResponse<u8>>, Self::Error> {
        Ok((!src.is_empty()).then(|| DecodedResponse::fifo(src.get_u8())))
    }
}
```

提交本身不需要 `.await`，只有已经接纳的 response future 需要等待：

```rust,ignore
use brz_net::RequestTarget;

let response = redis_slaves.request_for(key, command)?.await?;
```

## 超时和连接失效

`NodeOptions::request_timeout` 默认 500ms。连接 task 只维护一个可重置的 Tokio
timer，始终指向最老 pending request 的 deadline；不会为每个请求创建
`timeout`/`timeout_at` future。

读、写和超时位于同一个事件循环中，因此 socket 写阻塞也不会遮蔽 timeout。最老
请求超时、I/O 错误、响应解码错误或异常 FIFO 响应都会关闭整条物理连接，并完成该连接上
的所有 queued/in-flight request。上层决定是否切换副本或重试，本层不自动重试。

TCP connect 默认超时 2s，协议握手使用同样的 2s 上限；重连从 50ms 指数退避到
2s。`tcp_nodelay` 默认开启。

## 副本选择

`ReplicaSet` 创建或重建时随机排列副本，然后从第一个开始。当前副本累计完成请求
耗时达到默认 2s quota 后，下一个请求原子切换到下一个副本；失败或被取消的请求
至少计入 500ms。耗时高或持续失败的节点因此获得更少请求。

选择、计时和切换只访问当前 index 与对应 quota 的原子变量，不扫描所有副本。
高并发下已经发出的请求可能让 quota 略微超过阈值，这是预期的近似行为。

## DNS 和动态 endpoint

`DnsResolver`/`DnsSource` 保留为独立的控制面原语：

- 仅保留 IPv4；
- hostname 去重，默认最多一个 lookup 同时执行，以平滑万级域名的解析负载；
- 新注册 hostname 在下一个默认 1s scheduler tick 处理；
- 解析失败保留最后一次成功结果；
- 只有 IP 集合真实变化时才通过 `EndpointSet` 的 ArcSwap COW 快照原子发布。

请求路径不会执行 DNS、扫描 endpoint 或管理连接。当前版本尚未把 `DnsSource`
快照自动 reconcile 为增删后的 `Node`/`ReplicaSet`；这层适配应在首个 Redis/MC
迁移时结合其配置所有权补上，避免先固化错误的生命周期接口。

## 当前边界

- 本层不做自动重试。
- 本层没有连接池；MySQL 的独占连接/事务语义以后单独增加，不复用这里的
  multiplexed `Node`。
- 暂未迁移 Redis、MC、Motan SDK；当前代码只提供并验证底层框架。

## 验证

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```
