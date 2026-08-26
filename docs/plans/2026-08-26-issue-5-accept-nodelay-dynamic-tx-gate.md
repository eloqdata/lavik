# Issue #5 改动计划：accept 路径 TCP_NODELAY + 复制事务排序门动态收窄

- Issue: https://github.com/thweetkomputer/keylane/issues/5
- 基线: `4494c27`（main HEAD），worktree: `keylane-issue-5`，分支 `perf/issue-5-accept-nodelay-dynamic-tx-gate`
- 设计依据: 复制事务排序门一次性源码调查（任务/PR 上下文，未入仓；其结论已融入 docs/architecture/05-replication.md 与本计划，下称"调查文档"）
- 两个改动相互独立，但同属一个 issue，落在同一分支。

## 改动 1：accept 路径为客户端连接设置 TCP_NODELAY

### 问题

celer accept 路径（`celer/src/net/tcp_listener.cpp`）只对监听 fd 设了
SO_REUSEADDR/SO_REUSEPORT/IPV6_V6ONLY，accept 出的客户端 fd 从未设
TCP_NODELAY。pipeline 批次的回复需要多次 flush 时，后续小包触发
Nagle × 延迟 ACK 死锁，loopback 上每批稳定 +40ms。仓库内 TCP_NODELAY
目前只设在外联连接上（`celer/src/rpc/rpc.cpp:168`、
`src/replication/replication.cpp:1521`）。

### 修法

在 `TcpListener::AcceptUnregistered()`（`celer/src/net/tcp_listener.cpp`）
拿到 accepted fd 后、构造 `Connection` 前执行：

```cpp
int one = 1;
setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
```

- **best-effort、非致命**：失败不拒绝连接（与 Redis `anetSetTcpNoDelay`
  的语义一致；该 fd 一定是 TCP，失败近乎不可能，按 celer 现有日志/忽略
  惯例处理）。
- 放在 `AcceptUnregistered()` 而非某个特定 listener 的回调里，保证所有经
  TcpListener accept 的入站连接（客户端、以及任何其他入站路径）行为一致。
- 实施时确认 keylane 客户端监听确实走 `TcpListener::Accept*`（检查服务
  端接线，如 `app/keylane.cpp` / `src/redis/` 的 listen 路径）。

### 测试

确定性单测（不做 CI 里的时延断言，避免 flaky）：

- 在 keylane 测试侧（`tests/`，`keylane_unit_tests` 或独立的已注册/新注册
  e2e 目标）启动 celer `Worker` + `TcpListener` 绑定 `127.0.0.1:0`，用普通
  客户端 socket 连接，**直接对 `AcceptUnregistered()` 返回的 `Connection`
  的 fd** 做 `getsockopt(TCP_NODELAY)` 断言为 1（在连接注册/所有权转移前
  检查，不依赖 worker 连接表）。
- 若存在已注册的、直接驱动 celer runtime 的测试文件（如
  `tests/cross_core_active_sender_test.cpp` 一类），优先扩展现有文件；否则
  新增测试文件并在根 `CMakeLists.txt` 注册。

pipeline ×100 批时延从 ~44ms 降到 ~1ms 的效果由性能验证阶段（见下）实测
确认，不写成 CI 阈值断言。

## 改动 2：复制事务排序门按动态 participant 收窄

### 问题

source 挂 ONLINE replica（replication log active）后，所有静态带
`kCmdWrite | kCmdMultiShard` 且不带 `kCmdMayBlock` 的命令在**整个命令体**
期间持有进程级全局排序门 `g_replication_transaction_order`（CAS + 固定
1ms 轮询，`src/redis/command.cpp:6495-6503`；调用点
`ExecuteCommandBody` 内 `src/redis/command.cpp:7221-7244`）。判定用的是
命令表静态标志，不是本次请求的实际 participant 集合：单 key DEL、同 shard
MSET 都会进全局门，尽管单 participant 不可能形成跨 flow rendezvous 环。
64 连接并发 MSET-100 实测吞吐钉在 ~740 MSET/s、单笔 ~86ms。

### 修法

设计要点：**不用"危险命令例外表"（漏列即不安全），改用"已证明安全的命令
能力标志"（漏标只是维持进门，失败方向保守）**。

1. 在 `include/keylane/command_table.h` 的 `CommandFlag` 新增
   `kCmdKeyViewComplete = 1u << 10`，语义注释为："`DetermineKeys(spec, args)`
   返回的 key 视图恰好覆盖本命令可能成为事务 participant / envelope flow
   的全部 key；只有带此标志的命令才允许对复制全局排序门做动态单 shard
   收窄"。
2. 逐条审计后，只给**已证明 key 视图完整**的命令种类打上该标志（预期名单：
   `kDel`/`kUnlink`/`kMSet`/`kMSetNx`/`kRename`/`kRenameNx`/`kCopy` 等走
   标准 key 视图构建事务的命令；审计范围见下节）。已确认视图不完整的
   `kSort`（BY/GET pattern 按数据扩展，sort_command.cpp:485-526）、
   `kGeoRadius`/`kGeoRadiusByMember`（STORE 目的地在视图外，
   command_table.cpp:436-437 + zset_command.cpp:2589-2603）、
   `kZDiffStore`/`kZInterStore`/`kZUnionStore`（aggregate STORE 分支只返回
   源 key，目的地 arg1 在视图外，command_table.cpp:442-477 +
   zset_command.cpp:2509-2515）等一律不打标志，维持进门。
3. 在 `ExecuteCommandBody` 现有进门点之前，按请求的动态 key 视图计算
   participant shard 数，仅带标志且确定单 shard 的请求跳过全局门：

```cpp
// 仅当请求"确定只落在单个 shard"时放行；任何不确定都保守返回 true（维持进门）。
bool RequestSpansMultipleShards(const CommandRequest& request) {
  if (request.spec_ == nullptr) return true;
  // 未证明 key 视图覆盖全部 participant 的命令种类：维持进门（保守）。
  if ((request.spec_->flags_ & kCmdKeyViewComplete) == 0) return true;
  auto keys = DetermineKeys(*request.spec_, request.args_);
  if (!keys.ok() || keys->count() == 0) return true;  // arity/语法错误等维持现状
  const unsigned first = ShardForKey(request.args_[keys->first_]);
  for (std::size_t i = keys->first_ + keys->step_; i <= keys->last_;
       i += keys->step_) {
    if (ShardForKey(request.args_[i]) != first) return true;
  }
  return false;
}
```

`ExecuteCommandBody` 改为：

```cpp
const bool snapshot_transaction = <现有静态条件，不变>;
ReplicationTransactionOrderGuard replication_order_guard;
if (snapshot_transaction && RequestSpansMultipleShards(request)) {
  co_await BeginReplicationTransactionOrder(&replication_order_guard);  // 同前
}
SnapshotTransactionOperationGuard snapshot_transaction_guard;
if (snapshot_transaction) {
  co_await BeginSnapshotTransaction(&snapshot_transaction_guard);       // 不变
}
```

要点：

- **snapshot 门保留不变**（仍按静态标志）：FULLSYNC_CUT 的 close/drain
  语义完全不受影响（调查文档 §4：防半事务是 snapshot 门的职责，不是全局
  bool 的职责）。
- 动态收窄的适用性由命令表上的能力标志表达：新命令默认没有该标志，
  自动落在保守侧；不存在"忘了加例外就不安全"的脆弱性。
- 决策**只按"确定 ≤1 participant"放行**，绝不按"本次无冲突"放行（issue
  正确性约束）。

### 必须做的正确性审计（实施的一部分）

枚举命令表中所有 `(kCmdWrite|kCmdMultiShard) && !kCmdMayBlock` 的命令
（DEL/UNLINK、MSET、MSETNX、RENAME/RENAMENX、COPY、
SDIFFSTORE/SINTERSTORE/SUNIONSTORE、ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE、
GEOSEARCHSTORE、GEORADIUS 系、SORT 系、SPOP 等），逐条确认其执行路径的
txn participant / envelope flow 集合恰好由 `DetermineKeys(spec, args)`
覆盖的 key 决定。**只有证明完整的种类才打 `kCmdKeyViewComplete` 标志；
证明不了、或确认不完整（kSort / kGeoRadius / kGeoRadiusByMember /
kZDiffStore / kZInterStore / kZUnionStore 已确认不完整），就不打标志、
维持进门。**审计结论（每条命令为什么能/不能打标）以注释形式留在命令表
相应行旁或测试里。

### 正确性论证（写进代码注释）

1. 全局门防的是 ONLINE 复制多 flow FIFO 的乱序环
   （{A,B}/{B,C}/{C,A} rendezvous/ACK cycle，调查文档 §3；
   `include/keylane/command.h:435-439`）。
2. participant ≤1 shard 的请求最多产生单 flow envelope；单 flow marker
   不参与任何跨 flow rendezvous 环（调查文档 §7），跳过全局门不会引入环。
3. FLUSH 仍持全局门并发布 all-flow control barrier；跳过门的单 shard 写
   只向一个 flow 放 marker，其 target 侧 apply 不等任何其他 flow，不可能
   与 FLUSH 的 all-flow barrier 构成等待环（调查文档 §7 的反例如前提是
   多 flow 事务，多 flow 事务仍然进门，语义不变）。
4. snapshot 门保留 → FULLSYNC_CUT 行为不变（见上）。

### 明确不在本次范围（记录为后续项）

- EXEC 路径的全局门（`src/redis/command.cpp:5860-5870`）维持现状：EXEC
  的 key 预计算发生在进门之后，动态收窄需要重排 EXEC 流程，超出本 issue
  范围。
- blocking list/zset 每次 attempt 的 `global -> snapshot` 获取
  （`src/redis/list_command.cpp:596-617`、`src/redis/zset_command.cpp:603-648`）
  维持现状。
- 不替换 1ms 轮询为事件驱动 waiter（调查文档建议 2），不移除多 shard 写
  的全局序列化（调查文档 §7 的进一步方向）。
- 调查文档 §6 发现的 gate 顺序互等是独立的 correctness 问题，本 issue
  不改 gate 获取顺序，不在此修。

### 测试

1. **单元级**：对收窄决策的直接断言——单 key DEL → 跳过；hashtag 聚合到
   单 shard 的 MSET → 跳过；跨 shard MSET → 进门；MSETNX 单 key-value 对
   → 跳过；RENAME 同 shard → 跳过 / 跨 shard → 进门；无法确定 key → 进门；
   **未打标志的种类即使显示 key 同 shard 也仍进门**：SORT ... STORE、
   GEORADIUS ... STORE、ZUNIONSTORE（源全在 shard A、目的地在 shard B，
   判定必须进门）。测试访问方式遵循现有模式
   （`tests/replication_log_e2e_test.cpp:1104-1134` 已直接断言门函数，
   helper 按同等方式暴露）。
2. **命令表完备性测试**：枚举命令表中全部
   `(kCmdWrite|kCmdMultiShard) && !kCmdMayBlock` 的 kind，断言每个要么带
   `kCmdKeyViewComplete` 标志、要么在测试内显式列出的"已确认视图不完整"
   名单里；新增门 eligible 命令而未做审计归类时该测试失败，强制未来作者
   做出显式分类。
3. **e2e 回归**（扩展 `tests/replication_log_e2e_test.cpp`，避免 CMake 变动）：
   source + ONLINE replica；用现有 `KEYLANE_TX_ACTIVE_BLOCK_PAUSE_MS`
   hook（`src/storage/engine/write.cpp:649-670`）让一笔跨 shard MSET 在持门
   期间停顿 10 秒；**先用可观测信号确认该事务已进入 pause hook**（如 hook
   日志/计数器；若无现成信号，加 test-only 的观测点，不用纯时间猜测），
   然后并发断言：
   - 同 shard（hashtag）MSET 在远小于停顿的宽松上界（如 5s）内完成
     （不再互斥）；
   - 另一笔跨 shard MSET 在 5s 检查点仍未完成、停顿结束后才完成
     （全局门语义保留）。
   停顿 10s 与检查点 5s 留足分离度，避免时序 flaky。

### 文档同步

- `include/keylane/command.h:435-439` 附近描述全局门动机的注释若仍按静态
  admission 描述，更新为动态收窄后的语义。
- `docs/replication-design.md` 中 ONLINE 顺序门相关段落（约 :322-336）若
  描述静态按命令表进门，同步更新。
- 调查文档是历史调查记录，不改。

## 实施与验证流程

1. 本计划先过 Codex adversarial gate（Kimi 专项：concurrency、
   distributed-state）。
2. 并行实施：改动 1（celer + 其测试）与改动 2（command.cpp + 其测试）由
   不同 subagent 负责；根 `CMakeLists.txt` 只允许改动 1 的 agent 在需要新增
   测试文件时编辑，改动 2 一律扩展现有测试文件，避免冲突。
3. 集成验证（单一 agent）：
   - worktree 内完整 Debug 构建（`scripts/build_debug.sh`，`build_debug/`
     已被 gitignore）。
   - 跑相关 ctest：unit tests、replication_log_e2e、multikey_e2e、
     multi_exec_e2e、list_e2e（含三 flow 事务复制 E2E）、
     atomicity_stress_e2e 等；可行则全量。
   - 性能验证**全部在 `/mnt/local_nvme/` 下进行**（数据文件、服务端工作
     目录、bench 脚本、原始产物放 `/mnt/local_nvme/keylane_issue5/`）：
     基线 build（干净 HEAD 的源码快照放在 worktree 外）与改动后 build
     对比，场景与 issue 参考数据对齐——1 连接 pipeline SET ×100/×1000 批
     时延（预期 ~44ms → ~1ms）；source+ONLINE replica 下 64 连接
     MSET-100 聚合吞吐与单笔延迟（预期远超 ~740 MSET/s、无 ~86ms 排队）；
     hashtag 同 shard MSET 并发。结果与 issue 的 before 表对照。
4. 全部改动完成后过 Codex code gate（`--base 4494c27`，同一 session key，
   同一组 Kimi risk）；NEEDS REVISION 则修复后用 `--since` 增量复跑，直到
   PASS。不自行 git commit/push/开 PR。

## 评审记录

- Adversarial 第 1 轮（session key `keylane-issue-5`）：NEEDS REVISION。
  - P0：`DetermineKeys` 视图不等于真实 envelope participant 集合——
    SORT...STORE 的 BY/GET pattern 按数据扩展 participant
    （sort_command.cpp:485-526）；GEORADIUS/GEORADIUSBYMEMBER 的
    STORE/STOREDIST 目的地在视图外（command_table.cpp:436-437、
    zset_command.cpp:2589-2603）。→ 计划已改为对这些 kind 无条件进门
    （`KindMayExpandParticipantsBeyondKeyView` 例外表），并补对应单测。
  - 残余 1：e2e 3s 停顿配 2s/3s 阈值太贴边。→ 改为 10s 停顿 + 进入 hook
    的可观测确认 + 5s 检查点。
  - 残余 2：TCP_NODELAY 测试直接查 `AcceptUnregistered()` 返回的
    `Connection`，不依赖注册后的连接表。→ 已纳入。
- Adversarial 第 2 轮：NEEDS REVISION。
  - P0：例外表仍不完整——`kZDiffStore`/`kZInterStore`/`kZUnionStore` 的
    aggregate STORE 分支只返回源 key（command_table.cpp:442-477），目的地
    arg1 由执行单独加入事务（zset_command.cpp:2509-2515）。→ 设计改向：
    放弃手工例外表，改为命令表能力标志 `kCmdKeyViewComplete`（未证明完整
    ⇒ 默认保守进门），并新增命令表完备性测试强制未来命令显式分类。
  - 残余：手工例外表对未来新增命令脆弱。→ 同上，标志 + 完备性测试解决。
