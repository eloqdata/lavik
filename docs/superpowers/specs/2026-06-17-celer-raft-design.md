# celer-raft 设计文档

**日期**: 2026-06-17
**状态**: 设计草案,待评审
**目标**: 在 celer 协程运行时之上实现一个生产级 Raft 库 (`celer-raft`),供 keylane 这类基于 celer 的数据库做分布式多副本一致性。以 braft 为算法与工程参照,但**不移植** braft 的线程/锁模型 —— 改为利用 celer 的 shared-nothing thread-per-core 模型做无锁实现。

---

## 1. 背景与约束

### 1.1 参照物:braft

- braft 源码约 22,890 行,68 个源文件中 61 个依赖 brpc/bthread/butil/bvar,深度耦合。
- braft 的并发模型:单个 raft node 摊在所有核上,brpc 每个请求派发到一个 bthread,`FSMCaller` / `LogManager` 各用一个 `ExecutionQueue` 串行化有序路径,node 状态用 38+26 处 `bthread_id_lock/unlock` 保护。
- braft 的端口模型:**全进程一个 `brpc::Server`、一个端口**;`global_node_manager` 单例,所有 group 的 node 按 `NodeId=(group_id, peer_id)` 注册;请求体带 `group_id`,`raft_service` 用它查到 node 再处理。
- braft 的传输:基于 brpc protobuf service(8 个 `.proto`),日志复制 / 快照传输用 brpc streaming + attachment。

**结论**:直接"完全复刻/移植" braft 不可行 —— 等于先在 celer 上重造 brpc 的 streaming/异步 closure/bthread_id 一整套,工作量比写 raft 还大。可行路径是**以 braft 为算法参照,在 celer 协程模型上重新实现**。

### 1.2 运行时:celer

- thread-per-core、shared-nothing。每个 `Worker` 独占 io_uring backend、ready 队列、连接表、cross-core 邮箱。
- C++20 协程 `Task<T>`;协程内不能阻塞。
- `Service` 由 `Server` 在**所有 worker** 上各起一份(`TcpService` 每 worker 一个 SO_REUSEPORT listener);连接经内核哈希落在随机 worker。
- `celer::rpc`(517 行)是一个轻量 RPC:16 字节 `WireHeader` + `verb`(uint16)分发 + `req_id` 关联响应。当前定位为"same-host benchmark transport"(native 字节序),`Handler` 是**同步 inline、不能阻塞**的。
- io 层当前**纯网络**:io_uring 只接了 send/recv/accept,**没有文件/磁盘 io**。

### 1.3 关键约束

1. Raft 要求 term/vote、每批日志在响应 peer **之前**必须 `fsync` 落盘 —— 但 celer 当前无磁盘 io,且 `rpc::Handler` 不能挂起。**这两项是 raft 的前置地基。**
2. celer 仓库应保持纯净(运行时职责),不应内置 raft。

---

## 2. 核心架构决策

### 2.1 分层与打包(纯 submodule)

celer-raft 是**独立仓库**,作为 **keylane 的 submodule**(建在 keylane 下,递归 clone 一把拉全)。celer-raft **自带 celer 作为它自己的嵌套 submodule**,以便在该目录独立开发与测试。

```
keylane/
├── celer/                   (submodule)
├── celer-raft/              (submodule)
│   ├── celer/               (celer-raft 自己的 submodule,供独立开发)
│   ├── include/celer/raft/
│   ├── src/  test/
│   └── CMakeLists.txt
└── CMakeLists.txt
```

CMake 用守卫避免 celer 被 `add_subdirectory` 两次(重复 target 报错):

```cmake
# celer-raft/CMakeLists.txt
if (NOT TARGET celer::rpc)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/celer
                   ${CMAKE_CURRENT_BINARY_DIR}/celer)   # 独立构建时用自带 celer
endif()
add_library(celer_raft src/...)
add_library(celer::raft ALIAS celer_raft)
target_link_libraries(celer_raft PUBLIC celer::rpc)     # 头/库经 PUBLIC usage requirement 传递继承
```

```cmake
# keylane/CMakeLists.txt
add_subdirectory(celer)        # 先定义 celer::core / celer::rpc
add_subdirectory(celer-raft)   # 守卫见 celer::rpc 已存在 → 跳过嵌套 celer,复用同一份
target_link_libraries(keylane_module PUBLIC celer::raft)
```

- **谁让 celer target 存在** → `add_subdirectory`,守卫保证只发生一次(独立时 celer-raft 自己,集成时 keylane)。
- **如何找到 celer 的头/库** → `target_link_libraries(... celer::rpc)`,include 路径经 PUBLIC 传递自动继承,无需手写。
- 不引入 install/find_package(纯 submodule)。

### 2.2 节点放置模型:owning worker,零锁,multi-raft 扩展

利用 celer shared-nothing:**每个 raft group 钉在固定的一个 worker 上**,`owning_worker = group_id % num_workers`(round-robin)。该 group 的全部状态(① raft 共识状态:term/vote/log/commitIndex/role/peers;② 应用 FSM)只许 owning worker 碰 → **无需任何锁**(braft 那 64 处 bthread_id 锁全部消除)。

- 没有任何 worker 独占所有 group;每个 worker 均摊 ~G/N 个 group。
- 单 group 吞吐受单核约束,但:I/O(fsync/网络)是 io_uring 异步,owning worker `co_await` 期间去跑同 worker 的别的 group,核不空转;有序路径在 braft 同样是单线程(`ExecutionQueue`)。
- **横向扩展靠 multi-raft**:多 group 分散到所有核,聚合吞吐随核数线性扩展(TiKV/Cockroach 的标准做法)。
- 与 keylane 契合:shard S 已钉在 worker W,其 raft group 也钉在 W → 客户端写入路径(含 FSM apply 写 hash 表)零跨核。
- 超热单 group 的缓解:拆 shard(更多 group);CPU 重活(快照生成、大块序列化、checksum)可 offload 到别的 worker,owning worker 只留有序临界区。

### 2.3 端口与请求路由:单端口 + cross-core 转交(A 方案)

与 braft 一致:**全节点一个 raft 端口**,所有 group 复用;请求体带 `group_id`。

- RpcServer 在所有 worker 收包(celer 模型);worker 解出 `group_id`,算 `owning_worker = group_id % N`。
- 落点 worker ≠ owning worker 时,用 **cross-core 邮箱把请求转交给 owning worker**,owning worker 串行处理(改 log、可能触发 apply),响应经 cross-core 转回落点 worker 发出。
- 这是 braft 单端口 + group_id 多路复用的 celer 版;差别仅在 braft 当场加锁碰 node,我们转交到 owning worker 无锁。**cross-core 一跳是换取零锁的代价**(braft 用满地锁换)。
- 两类流量:客户端→本节点写,keylane 已路由到 owning worker,**零跨核**;peer→本节点 raft RPC 才需要转交。

### 2.4 消息序列化

celer::rpc 是裸字节 + 手填 verb,序列化要自己做(brpc 用 protobuf 自动做掉)。raft 在 verb handler 内**手动 encode/decode** `AppendEntries`/`RequestVote` 等结构。具体编码方案(手写紧凑编码 vs 引入 protobuf/flatbuffers)在 **M1 决定**,M0 不涉及。

### 2.5 测试策略

- braft 纯算法测试可作**行为规约**低成本移植:`test_ballot.cpp`(0 处 brpc 依赖)、`test_configuration.cpp`(1)、`test_log_entry.cpp`(3)、`test_ballot_box.cpp`(5)。注意它们 `#include "braft/ballot.h"` 测的是 braft 的类,移植前提是 celer-raft 的类结构/接口与之对齐。
- 重度耦合的集成测试 `test_log.cpp`(57)、`test_node.cpp`(225)无法直接抄;其分区/重启/成员变更**场景语义**照搬,用 celer 传输在 celer-raft 上重写。
- 测试框架:gtest + ctest(与 braft 一致,便于移植)。

---

## 3. 里程碑分解

每个里程碑独立走一遍 spec → plan → 实现。

| 里程碑 | 内容 | 仓库 |
|---|---|---|
| **M0 地基** | ① celer 异步文件 io(io_uring write/fsync/read);② celer::rpc 异步 handler + 跨机字节序 | celer |
| **M1 单 group 共识** | 持久化 term/vote;log store(内存+落盘 fsync);选举;AppendEntries 复制;commit;FSM apply;owning-worker + cross-core 转交;消息序列化方案定案 | celer-raft |
| **M2 快照** | StateMachine 快照存/载;InstallSnapshot(需流式传输);日志截断压缩 | celer-raft (+celer 流式) |
| **M3 成员变更** | add_peer/remove_peer(braft 式单节点变更 + catch-up) | celer-raft |
| **M4 multi-raft + keylane 接入** | 多 group/节点;group→worker 映射;keylane ShardFsm;写入走 Propose | celer-raft + keylane |
| **M5 生产加固** | lease/线性一致读;pre-vote;metrics;CLI | celer-raft |

**先做 M0** —— 没有异步 handler 和磁盘 io,raft 一行跑不起来;且 M0 是对 celer 改动最集中的一块,先稳定 celer 能力,M1-M5 基本只在 celer-raft 内写。

---

## 4. M0 详细设计(本轮实施目标)

M0 全部落在 **celer 仓库**,为上层 raft 铺地基。两块互相独立,可并行。

### 4.1 M0.1 — celer 异步磁盘 io

对称复刻现有 send/recv 的异步模式,加文件 io。

**公开接口**(新增 `celer/fs/file.h`,实现 `src/fs/file.cpp`):

```cpp
namespace celer {
class File {
 public:
  static StatusOr<File> Open(std::string_view path, int flags, mode_t mode = 0644);  // 冷路径,阻塞 open
  Task<StatusOr<std::size_t>> WriteAt(std::span<const std::byte> buf, std::int64_t off);  // IORING_OP_WRITE
  Task<Status>                Fsync(bool datasync = true);                                 // IORING_OP_FSYNC
  Task<StatusOr<std::size_t>> ReadAt(std::span<std::byte> buf, std::int64_t off);          // IORING_OP_READ(重放/读快照)
  int fd() const noexcept;
  void Close() noexcept;
};
}  // namespace celer
```

**backend 扩展**(`io/io_uring_backend`,经 `Worker` 转发,与 `SubmitSend` 同构):

```cpp
Status SubmitWrite(int fd, std::span<const std::byte> buf, std::int64_t off, IoCompletion* tag);
Status SubmitFsync(int fd, bool datasync, IoCompletion* tag);
Status SubmitRead (int fd, std::span<std::byte> buf, std::int64_t off, IoCompletion* tag);
```

完成时 backend 经 `Enqueue` resume 等待的协程(与现有 send 完成处理一致)。

**默认决策**:
- 文件用裸 fd(registered file 优化留后)。
- 文件 io 是 **per-worker**:一个 group 的 log 文件只被 owning worker 碰 → 无锁,延续 shared-nothing。
- `Open` 走阻塞 syscall(冷路径,可接受);数据路径全异步。

**验收**:一个 worker 上的协程能 `co_await file.WriteAt(...)` 后 `co_await file.Fsync()`,期间该 worker 可调度其他协程;写入数据落盘后可由 `ReadAt` 读回校验。

### 4.2 M0.2 — celer::rpc 异步 handler + 跨机字节序

**异步 handler**(与同步 `OnVerb` 并存,不动同步快路径):

```cpp
using AsyncHandler = std::function<Task<Bytes>(BytesView)>;
void OnVerbAsync(std::uint16_t verb, AsyncHandler handler);
```

`Serve()` 分发逻辑:先查异步表,命中则 `co_await handler(payload)`(于是 handler 可落盘 / cross-core 转交再返回);否则走原同步 inline 路径。**echo/bench 基准不受影响。**

**跨机字节序**:`WireHeader` 不再 raw `memcpy`,改为显式**小端**编解码(常见 LE 主机零成本,BE 主机 byteswap),移除"same-host / native byte order"限制。

**默认决策**:
- 服务端**每连接串行**处理(handler 挂起时不流水线收下一个请求)—— M0 求简,owning worker 本就串行;流水线留后续优化。
- 字节序选小端显式编码。

**验收**:注册一个 `OnVerbAsync` handler,其内部 `co_await` 一个文件 fsync(用 M0.1)后再返回响应,客户端 `Call` 能正确拿到响应且 `req_id` 对齐;`WireHeader` 经显式编解码跨(模拟)字节序往返正确。

---

## 5. 非目标(本设计明确不做)

- 不移植 braft 的 bthread/brpc/bvar 任何代码。
- 不在 celer 内置 raft。
- M0 不做 raft 算法本身、不做消息序列化方案选型、不做流式传输(快照流式在 M2)。
- 不做单 group 跨多核(用 multi-raft 替代)。
- 不上 CMake install/find_package(纯 submodule)。

---

## 6. 风险与开放问题

- **R1**:celer io_uring backend 当前只为网络设计,混入文件 op 可能需要区分 SQE 类型 / 完成路由。实现 M0.1 时需先读 `io_uring_backend` 现状确认改动面。
- **R2**:每连接串行的服务端在 raft 高吞吐下可能成为瓶颈(一个 peer 连接上的请求排队)。M0 接受此简化,M1/M5 视压测结果决定是否上流水线。
- **R3(开放,M1 决)**:raft 消息序列化 —— 手写紧凑编码(轻、零依赖)vs protobuf/flatbuffers(省事、与 braft .proto 可对照)。
- **R4**:braft 纯算法测试移植依赖 celer-raft 类接口与 braft 对齐的程度;若接口分歧大,移植收益下降,转为"语义参照重写"。
