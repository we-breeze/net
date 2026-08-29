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
- 调用方提交结构化的 `P::Request`。`SessionProtocol` 将其转换成一个拥有所有权的
  最终 `P::Frame`；连接 task 通过 vectored write 直接写这些帧，不再复制到中间
  `BytesMut`。响应从连接级动态 ring buffer 解码。
- 请求通过容量为 256 的有界 MPSC 进入连接 task。`request_with` 先用
  `try_reserve` 做无等待准入，再调用请求构造闭包；容量耗尽立即返回
  `SessionError::Busy`，不会执行序列化。
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

## 全局请求 Arena

Redis、MC、Motan 等协议编码共享一份进程级 `EphemeralBytesArena`。默认包含两个
64 MiB chunk，总计保留 128 MiB。应用若需要覆盖默认值，必须在创建任何 SDK client
之前初始化；参数是单个 chunk 的大小：

```rust,ignore
brz_net::init_global_request_arena(32 * 1024 * 1024)?; // 总计 64 MiB
```

初始化后不支持 resize。重复设置相同大小会返回同一个实例；设置不同大小会返回配置
错误。未显式初始化时，第一次调用 `global_request_arena()` 会采用编译期默认值。

## 协议和响应关联

`SessionProtocol` 只负责协议相关的三件事：连接握手、请求编码、响应解码。

- Redis/MC 返回 `DecodedResponse::fifo(response)`，响应与连接中最早的 pending
  request 对应。
- Motan 一类允许乱序返回的协议，将 `RequestToken::as_u64()` 写入 request ID，
  解码时返回 `DecodedResponse::tagged(request_id, response)`，直接定位固定 slot。
- Redis AUTH 等连接级初始化由 `begin_handshake`/`decode_handshake` 完成。握手成功
  前 `Node` 不对调用方标记为 available；重连后会重新握手。

## 动态接收 Ring Buffer

每个物理连接惰性创建一个默认 8 KiB 的 `RxBuffer`，最大默认 64 MiB。socket
readiness 到达后，连接 task 会持续读取到 `WouldBlock`；如果 ring 先写满，则先让
协议解析已有数据。协议读到 frame/body 长度后通过 `reserve` 一次预留剩余容量，再
继续读取。因此大响应不需要切换独立 buffer，body、协议尾部和后续 pipeline response
始终处于同一个逻辑字节流。

`read <= taken <= write` 中，`taken` 之前的完整 response 由 `RxFrame` guard 保活。
guard 未释放而 ring 需要扩缩容时，只复制尚未解析的数据到新 backing；旧 backing
由 guard 继续持有，不阻塞连接读取。完整帧连续时可以交给 `Bytes::from_owner` 零复制；
帧跨 ring 末尾时由具体协议决定复制或按两段消费。缩容由连接 task 的周期维护分支
执行，不进入请求准入或响应查找热路径。

简化的 FIFO codec：

```rust,ignore
use brz_net::{DecodedResponse, RequestToken, RxBuffer, SessionProtocol};

struct Codec;

impl SessionProtocol for Codec {
    type Request = u8;
    type Frame = [u8; 1];
    type Response = u8;
    type Error = std::convert::Infallible;

    fn encode(
        &mut self,
        request: u8,
        _: RequestToken,
    ) -> Result<Self::Frame, Self::Error> {
        Ok([request])
    }

    fn decode(
        &mut self,
        src: &mut RxBuffer,
    ) -> Result<Option<DecodedResponse<u8>>, Self::Error> {
        let Some(response) = src.byte(0) else {
            return Ok(None);
        };
        src.advance(1);
        Ok(Some(DecodedResponse::fifo(response)))
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
的所有 queued/in-flight request。尚未完全写入 socket 的 owned frame 会直接释放；
上层决定是否切换副本或重试，本层不自动重试。

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

请求路径不会执行 DNS、扫描 endpoint 或管理连接。具体 SDK 在控制面把 `DnsSource`
快照 reconcile 成新的 `Node`/`ReplicaSet`；RedisService 已采用这一模式，并复用未变化
地址对应的 Node。

## 当前边界

- 本层不做自动重试。
- 本层没有连接池；MySQL 的独占连接/事务语义以后单独增加，不复用这里的
  multiplexed `Node`。
- RedisService 已迁移；MC、Motan 尚未迁移。

## 验证

```bash
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings

RUSTFLAGS="--cfg loom -C debug-assertions" \
  cargo test --release loom_ --lib -- --test-threads=1
```
