# Replication transaction order 与 1 ms 轮询调查

调查基线：`4494c27`。本文只把截图当作待核对的问题描述；结论来自当前仓库源码、测试和 Git 历史。代码行号均指该基线。

## 结论先行

1. 这里不是会增长等待时间的指数 backoff，而是**固定周期轮询**：一次 CAS 失败后无条件提交一个 1 ms 的 `io_uring` timeout，醒来再抢。释放方没有唤醒 waiter，也没有 FIFO/fairness。因此一次竞争至少增加一次请求的 1 ms sleep，实际还会叠加调度延迟；源码不保证恰好 1.000 ms。
2. “需要顺序”的原始问题是真实的，但它来自 **ONLINE 复制的多 flow FIFO + target 阻塞 rendezvous + ACK 反压**，不是来自 full-sync 快照。如果不同 flow 以不一致顺序看到重叠 participant 集合，target 可以形成 rendezvous/ACK 环。
3. 当前解决方式明显过宽：它不是只序列化真正跨 shard 的写，而是按静态 `kCmdMultiShard` 标志序列化整个命令体。单 key `DEL missing-key`、同 shard 的 MSET、单 participant envelope 都会进全局门，尽管这些请求不可能形成跨 flow rendezvous 环。
4. 不能只在执行结束时 `fetch_add` 一个“类似 LSN”的数就删除门。已有 replication tx id 已经单调递增，但它只是身份；flow LSN 又彼此独立。新序号必须由 source publisher 或 target 真正执行排序/缓冲协议，才能改变每条 flow FIFO 的可观察顺序。
5. 目前还有一个独立于性能的 correctness 风险：普通多分片写按 `DB -> global -> snapshot` 获取 gate，FLUSH 按 `global -> DB drain`，FULLSYNC_CUT 按 `snapshot close/drain -> DB close/drain`。两组都存在可达的持有—等待环。相关注释彼此矛盾；这应先于“是否彻底去掉全局门”修复并加回归测试。

## 1. 这把门到底是什么

全局状态只是一个进程级 `std::atomic<bool>`，与每 worker 的 DB/snapshot 计数门分开定义（`src/redis/command.cpp:1270-1283`）。抢占是一次 strong CAS，释放是 release-store（`src/redis/command.cpp:6495-6503`）：

```text
false --CAS--> true       // 唯一 owner
true  --失败--> SleepFor(1 ms) --再 CAS
owner --store(false)--> false
```

等待循环位于 `src/redis/command.cpp:1638-1647`。它没有 waiter 队列、condition/notification、unlock handoff 或公平性信息。假设 owner 在 contender 睡下 0.1 ms 后释放，contender 仍要等余下约 0.9 ms；如果醒来又输给别人，就再付一个完整 timeout。

`SleepFor` 不是用户态毫秒 timer wheel：Celer 把 duration 拆成秒和纳秒并提交 timeout（`celer/src/io/storage.cpp:218-245`），io_uring backend 调用 `io_uring_prep_timeout(..., 0, 0)`（`celer/src/io/io_uring_backend.cpp:299-308`）。所以截图所说的“约 1 ms 阶梯”方向正确，但“只能落在精确的 1 ms 桶”并不是源码保证；kernel 唤醒、run queue 和 coroutine 调度都可使其超时。

仓库固定的 Celer revision 是 `2d2bf8088813f8c013b879bb46525676ef01781b`。本次工作树预先 checkout 在另一个 clean revision `1e07787469966a41239521fced02e939c097d7f1`，两者上述 timeout 实现相同；本文没有修改 submodule。

## 2. 实际触发范围比截图更大

普通 command path 先取得 DB admission（`src/redis/command.cpp:7198-7215`），随后在以下条件同时满足时取得 global order 和 snapshot transaction 两个不同的 gate（`src/redis/command.cpp:7221-7244`）：

- 不是 replication-origin replay；
- replication manager、storage 都存在且当前 worker 的 replication log active；
- command metadata 同时含 `kCmdWrite | kCmdMultiShard`；
- 不含 `kCmdMayBlock`。

`kCmdMultiShard` 的定义是“key set **可能**跨 shard owner”，不是这次请求经动态解析后确实跨 shard（`include/keylane/command_table.h:16-26`）。例如 `DEL` 的最小调用只有一个 key，但表项固定带 `kCmdMultiShard`；MSET 也是同一静态分类（`src/redis/command_table.cpp:232-246`）。因此：

- `DEL one-missing-key` 会拿门，尽管没有多 key、没有写入、没有 transaction envelope；
- 多 key 全部映射到一个 owner 时仍会拿门；
- “多 key 写”也不是完整范围，SORT/集合与 zset store、RENAME/COPY、DEL/UNLINK 等多个静态 multi-shard write 都在范围内（`src/redis/command_table.cpp:30-31,51-52,110-196,232-246`）。

RAII guard 一直活到 `ExecuteCommandBody` 返回（定义见 `src/redis/command.cpp:1398-1418`），所以序列化的是**整个逻辑 command body**，而不只是最后给复制日志分配一个编号或放置 publication marker。

还有三类额外路径：

- `EXEC` 只要队列中有 source write 且 log active，就获取全局门，然后才获取 snapshot gate（`src/redis/command.cpp:5843-5891`）；之后才添加涉及的 DB guard（`src/redis/command.cpp:6025-6033`）。即使最终只有一个 owner，也会获取全局门。
- blocking list/zset 被中央条件排除，但每次具体 attempt 自己按 `global -> snapshot` 获取，并在 attempt 结束释放；两处同样是 1 ms 轮询（`src/redis/list_command.cpp:580-617`、`src/redis/zset_command.cpp:603-636`）。这说明截图只指向 `command.cpp:7222` 并不完整。
- FLUSHDB/FLUSHALL 也使用同一全局门来排序跨 flow control barrier，并非 multi-key transaction 专用（`src/redis/command.cpp:1765-1835`）。

`ReplicationLogActive()` 读取当前 worker log state（`src/storage/engine/replication_log.cpp:175-177`）。native replication handshake 会在每个 worker 上 enable log（`src/replication/replication.cpp:5798-5813`）；enable 把状态设为 active 并初始化 flow-local LSN（`src/storage/engine/replication_log.cpp:65-93`）。但 replica 断开时 session removal 只 cancel session（`src/replication/replication.cpp:6000-6009`），不会立刻 disable log；只有 invalid-history reset 等路径才 disable（`src/replication/replication.cpp:6019-6064`）。因此截图里的“只有挂着 replica 才存在”也过窄：第一次 handshake 激活后，断线重连窗口内 backlog/log 可以继续 active。

## 3. ONLINE 复制为什么要求 participant 的一致顺序

### Source publication

一笔跨 shard 写在 source 上创建 shared `ReplicationTransaction`。同一个 immutable envelope 在持有 shard locks 时被每个 participant worker 放进自己的 publish FIFO，commit/abort 再把 shared resolution 从 Pending 改为 Publish/Discard；这个对象不是 wire-level commit state（`include/keylane/storage/engine.h:407-424`）。

replication tx id 由进程级 atomic `fetch_add` 分配，当前是在 guard 初始化时而非命令完成时取得（`src/redis/command.cpp:6591-6640`）。transaction 的 shard-entry hook 把 marker 分别放进 participant worker 队列（`src/redis/command.cpp:6716-6742`）。每个 worker publisher 在 FIFO 头等待 shared resolution，随后物化 transaction envelope 并分配本 flow 的 LSN（`src/storage/engine/replication_log.cpp:415-458,488-590`）。

对于 MSET，真实执行仍走 `tx::Transaction`：预先收集 keys、schedule、跨 shard 执行/回滚并最终 `replication.Commit()`（`src/redis/command.cpp:4414-4517`）。全局门包在这个完整过程的外面。

### Target rendezvous 与 ACK

每个 source worker 有独立 flow LSN；flow LSN、partition mutation sequence 和 DB epoch 是不同序号域，不能互换（`docs/design-docs/replication-design.md:69-87`）。Target 收到 transaction envelope 后以 txid 聚合 arrivals，只有所有 participant 都到齐才 apply；成功后所有 participant cursor 一起前进，其余 flow 在 `CoroutineBarrier` 上等待（`src/replication/replication.cpp:4058-4188`）。对应 flow 只有在 apply 返回以后才 ACK（`src/replication/replication.cpp:4541-4577`）。

Source 现在会成批发送，不再严格“一条命令发完 ACK 才发下一条”：一批可含多个 frame/command；但每批发送后仍逐个等所有 completed LSN 的 ACK，之后才能继续该 flow（`src/replication/replication.cpp:5640-5719`）。所以头文件注释“ACK before next command”（`include/keylane/command.h:435-439`）应理解为 ACK-gated 的有界批，而不是严格 stop-and-wait；形成环的条件仍存在。

最小三 flow 环如下。三笔事务分别覆盖 `{A,B}`、`{B,C}`、`{C,A}`，各 flow 的 FIFO 头如果不一致：

```text
flow A: T1 -> ... -> T3       target 在 T1 等 B
flow B: T2 -> ... -> T1       target 在 T2 等 C
flow C: T3 -> ... -> T2       target 在 T3 等 A
```

三个 flow 都无法越过当前 rendezvous，也就不会 ACK；source 对应 flow 的发送窗口不能继续推进。这正是源码给全局门写下的动机：防止 overlapping flow subsets 形成 arrival/ACK cycle（`include/keylane/command.h:435-439`）。全局执行串行是一个充分条件，因为所有 participant FIFO 都会观察同一个 source 总序；但它不是唯一实现方式。

## 4. ONLINE order 与 FULLSYNC_CUT 是两件事

截图第二张把两种 gate 的职责混在了一起：

| 机制 | 形态 | 实际职责 |
|---|---|---|
| `snapshot_transaction_state_` | 每 worker 的 closed bit + active count | FULLSYNC_CUT 关闭新 transaction admission 并 drain 已进入事务；允许平时并发 |
| `g_replication_transaction_order` | 进程全局单 owner bool | 让 ONLINE 多 flow transaction/control publication 获得一致 source order |

Full-sync 期间，transaction write 不立即把 participant after-image 投影给 session，而是收集到 `tx->fullsync_effects_`；确定 commit 后一次发布（`src/storage/engine/write.cpp:1003-1043,1233-1248`）。最终 cut 的设计步骤明确是“关闭并 drain transaction admission，再关闭普通 DB gates”（`docs/design-docs/replication-design.md:300-320`）。ONLINE envelope 的一致 commit order 在文档另一节单独陈述（`docs/design-docs/replication-design.md:322-336`）。

因此，“为了避免 full-sync 看到半个事务，所以必须全局串行所有多 key 命令”不是当前源码给出的因果链。半事务问题由 participant after-image + snapshot gate/cut 解决；全局 bool 的直接理由是 ONLINE rendezvous order。

## 5. 1 ms 阶梯的实测

为隔离存储 IO，本次使用两个不存在的随机 key 执行 `DEL`；它仍带 `kCmdWrite | kCmdMultiShard`，但没有实际 mutation 或 transaction envelope。每组 20,000 请求，单位为 ms：

| 状态 | 并发 | p50 | p75 | p87.5 | p95 | avg |
|---|---:|---:|---:|---:|---:|---:|
| log inactive | 4 | 0.167 | 0.207 | 0.231 | 0.263 | 0.179 |
| replica online / log active | 1 | 0.223 | 0.303 | 0.439 | 0.615 | 0.283 |
| replica online / log active | 4 | 0.343 | 1.407 | 2.583 | 4.327 | 1.125 |

这组数据同时说明两点：

- active/c1 的增加包含 replication-log 路径自身开销，不能全部算到等待；首个 owner CAS 成功不会 sleep。
- active/c4 的 p75、p87.5、p95 出现约 1 ms 级台阶，与“一次或多次 CAS 失败各付一次固定 timeout”一致。更关键的是，这是一条没有实际 mutation、没有跨 flow envelope 的 DEL，证明损失主要来自过宽静态 admission，而不是业务上必需的原子复制。

截图给出的 `0.2-0.3 / 1.2 / 2.2 ms` 可以作为某次观测，但不能从源码直接推出；基线命令成本和调度 overshoot 都依赖环境。可由源码保证的只有固定的 **requested 1 ms per failed attempt**。

## 6. 当前 gate 顺序存在可达互等

这是本次核对发现的独立问题。普通 MSET 等中央路径先增加 DB active count，再等待 global 和 snapshot（`src/redis/command.cpp:7204-7244`）。但：

- FLUSH 先持有 global，再 close DB gate 并等待 active count 清零（`src/redis/command.cpp:1765-1801`）；
- FULLSYNC_CUT 先 close snapshot gate、等待 snapshot active 清零，再 close DB gates、等待 DB active 清零（`src/replication/replication.cpp:5143-5163,5452-5472`）。

可达 interleaving 一：

```text
MSET: 取得 DB count -------- 等 global（FLUSH 持有）
FLUSH:取得 global --------- 等 DB count（MSET 持有）
```

可达 interleaving 二：

```text
MSET:         取得 DB count -------- 等 snapshot（CUT 已关闭）
FULLSYNC_CUT: 关闭/drain snapshot --- 关闭 DB 并等 MSET 的 count
```

第二种情况下，CUT 在关闭 snapshot 时可以观察到 active count 为 0 并继续；随后 MSET 既不能进入 snapshot，也不会释放已持有的 DB count。

Git 历史显示 `e2183ce32fa93aca48e88920a8a5dccce80a8c93`（2026-08-21，`Replace replication backlog with shared memory log`）在同一大改动中加入了普通 command 的 DB-before-transaction-gates 顺序，也加入了 FLUSH 的 global-before-DB 顺序。当前 FLUSH 注释仍声称“transaction takes this order gate before entering its DB operations”（`src/redis/command.cpp:1769-1772`），而中央路径注释明确说先拿 DB（`src/redis/command.cpp:7217-7220`）；实现和注释互相冲突。

最小安全方向是统一 partial order，例如让需要三个 gate 的普通写也遵循 `global -> snapshot -> DB`。这与 EXEC 当前顺序一致，并分别与 FLUSH 的 `global -> DB`、CUT 的 `snapshot -> DB` 相容。最终修改前仍需审计 COPY、blocking attempt、所有 MultiDb guard 和 publisher admission；本文只记录结论，不在调查中改运行时代码。

## 7. 能不能去掉全局执行串行

### 可以缩小，而且应该先缩小

当前门把“metadata 可能跨 shard”当成“这次执行一定产生跨 flow rendezvous”，是直接可证的过度保守。低风险的第一层收缩应基于动态 participant 集合：

- 0/1 participant 不需要 ONLINE cross-flow order；
- 没有产生 transaction/control envelope 的 no-op 或单 key DEL 不需要门；
- 同 shard MSET 的单 participant envelope不需要跨 flow 门；
- 真正 disjoint participant 集合彼此也不会构成等待环，但要安全利用这一点需要 participant-aware admission，而不是一个 bool。

即便暂不改复制协议，也可以先把 1 ms polling 换成 unlock notification/FIFO async mutex，消除固定台阶和无意义 wakeup；这只改善等待实现，不消除全局 serialization。

### 对 tx-backed envelope，现有 scheduler 已经给出可复用的顺序

可以证明一个边界明确的结论：若一条 multi-participant envelope 的 participants 恰好是 `tx::Transaction::shard_ids()`，且每个 marker 只由该 transaction 的 entry hook 入队，那么共享 participant flow 上的 marker 会按该 transaction **最终成功的 scheduler txid** 单向入队，不需要 execution-wide global bool。

理由是：真正 multi-shard transaction 已取得全局 txid，并在每个 shard 使用按 txid 升序的队列（`src/tx/transaction.cpp:227-247`、`include/keylane/tx/tx_queue.h:35-54`）。`Schedule()` 必须等所有 participant 的 schedule round 都成功后才允许 Arm。若较大 txid 已在某 shard 启动，`Poll()` 已先推进 `committed_txid_`，晚到的较小 txid 会让整轮失败、在产生任何 entry hook/marker 之前取消，再用更大 txid 重试；若较大 txid 尚未启动，sorted queue 会把较小 txid 插到前面，未 armed 的 head 也会阻止后者越过（`src/tx/transaction.cpp:147-169`、`src/tx/tx_shard.cpp:16-45`）。disjoint key 不破坏这个性质，因为 TxQueue 是 worker-global；multi-hop node 又留在 queue head 直到 release。

于是任意共享 flow 的两个 transaction edge 都沿全局 txid 递增，`{A,B}/{B,C}/{C,A}` 这样的 participant 组合也不可能形成有向环。代码路径审计还表明：multi-owner EXEC、multi-shard list/zset attempt 都使用同一 `Transaction + entry hook` 形态；single-owner 手工 marker 只有一个 flow，不参与 rendezvous 环。普通 single-flow write 也不会生成跨 flow 等待边；同 key 时，它与 transaction marker 都在 key lock 内入队，lock 顺序提供本地先后。

这仍不是“直接删掉所有 global gate”的完整证明，因为 FLUSH control barrier 完全绕过 TxQueue。若普通 transaction 不再参与 global bool，而 FLUSH 只是继续独占这个 bool，会出现新的最小反例：

```text
长单 key 写 P 暂占 db1/W1 上 T 所需的 key；T=MSET(db1, W0/W1)
T arm 后先在 W0 entry hook 入队，W1 尚被 P 挡住
并发 F=FLUSHDB(db0)；db0 与 db1 的 DB gate 不相交，F 向所有 worker 发布
W0 FIFO: T -> F；W1 FIFO: F -> T（P 释放后 T 才能在 W1 入队）
target flow W0 在 T 的 two-flow barrier 等 W1，W1 在 F 的 all-flow barrier 等 W0
```

两个 DB gate 不相交，所以这个 interleaving 可达。结论是：normal tx-backed envelope 可以优先复用 scheduler order 移除 execution-wide gate；FLUSH 必须另行 close/drain transaction publication，或进入同一个可比较的 publication order domain。FULLSYNC snapshot gate、abort/discard、publisher backpressure 也要保留并新增确定性回归测试。当前测试只覆盖 TxQueue/`committed_txid_` 基元，没有覆盖反向跨 shard schedule、retry/cancel 到 marker 的端到端不变量。

### “完成时分配一个单调序号”缺的是什么

仅有数字不会改变 FIFO。假设 T1 完成后拿到 CSN=1，T2 拿到 CSN=2，但 T2 先在某个 participant flow enqueue，target 仍会先看到 T2。当前 marker 特意在持有 shard locks、进入 transaction 时 enqueue（`include/keylane/storage/engine.h:413-416`、`src/redis/command.cpp:6716-6742`），正是为了不让后续本地 mutation 越过线性化点。

要让 completion sequence 生效，至少需要二选一：

1. **Source publication sequencer**：命令并发执行；完成后取得/提交 ticket，但各 participant publisher 只按 ticket 顺序原子放置或释放 marker。临界区只覆盖 publication，不覆盖整条命令。还必须给 control barrier 定义同一序号域。
2. **Target reorder**：各 flow reader 不在第一个未完成 rendezvous 上阻塞，而是持续接收并按 global CSN 缓冲，等一个事务的所有 participant 到齐且前序依赖满足后 apply/ACK。这需要新的 bounded buffer、backpressure、cursor/replay 与断线恢复规则。

若采用第一种，正确的 completion-time contract 应是：事务的数据结果已经确定、但相关 key locks 尚未释放时，把 immutable commit descriptor 交给一个有界 sequencer；sequencer 原子地分配 CSN 并按该次序向所有 participant FIFO 安装 marker。只有 sequencer 接受成功后，命令才可释放 locks 并回复客户端；不必等待各 flow 的物理 append/ACK。FLUSH/control barrier 必须提交到同一 sequencer，FULLSYNC_CUT 也必须 fence 并 drain 这个队列。ticket 分配后若 producer 取消或失败，必须由 sequencer 发布 Discard/跳过槽位，不能留下阻塞所有后继的永久 gap。

这里的 CSN 是跨 flow publication order token，不是现有 per-flow LSN 的替代品；后者仍负责每条 flow 的 reconnect cursor 和 ACK。

现有 replication tx id 虽单调，但在执行开始时分配且 target 只拿它做 rendezvous map key（`src/redis/command.cpp:6620-6639`、`src/replication/replication.cpp:4073-4127`）；现有 flow LSN 又是每 worker 独立域。因此用户提出的方向是可行的协议设计候选，但关键不是 `fetch_add` 本身，而是让 publisher/target **遵守**这个顺序。

## 8. Git 历史与测试覆盖

`g_replication_transaction_order`、1 ms begin loop、transaction envelope/rendezvous 以及 list/zset attempt gate 首次出现在：

- `6b90a369be90ed8615cf7ac80bd8833716ed0543`，2026-08-19，`Replicate multi-key transactions atomically`。

该 commit message 只有 subject，没有设计说明。之后：

- `e2183ce32fa93aca48e88920a8a5dccce80a8c93`，2026-08-21，`Replace replication backlog with shared memory log`，保留固定 1 ms 轮询，增加 FLUSH/control barrier 共用 order gate，并引入上文的 gate 顺序冲突。

现有 `keylane_replication_log_e2e` 只直接断言 global bool 第一个 owner 成功、第二个失败、释放后可重入（`tests/replication_log_e2e_test.cpp:1104-1134`），并没有让第二个 coroutine 进入 `BeginReplicationTransactionOrder()` 的实际 sleep path。它在本次调查中运行通过：

```text
ctest --test-dir build_debug -R '^keylane_replication_log_e2e$' --output-on-failure
1/1 passed, 3.08 s
```

native replication E2E 已覆盖三 flow transaction 在不同 source/target worker 数下复制（`tests/list_e2e_test.cpp:1939-1966`），以及 transaction apply 后断流/重连和普通 MSET 到两个 replicas（`tests/list_e2e_test.cpp:3275-3343`）。但仓库中没有构造 `{A,B}/{B,C}/{C,A}` 乱序环的测试，也没有针对 1 ms contention latency 或上述 gate-order 互等的回归测试。

`TxLockTest.*`（3 项）、`keylane_multikey_e2e` 和 `keylane_multi_exec_e2e` 也在本次调查中通过（分别 0.03 s、14.74 s、1.65 s）；它们验证现有 queue、multi-key 和 EXEC 行为，但同样没有覆盖上述 gate-order 互等或反向跨 shard marker 顺序。

## 9. 已验证的 1 ms 分支复现

本次还做了一个不修改源码的 Debug 诊断：使用源码已有的 `KEYLANE_TX_ACTIVE_BLOCK_PAUSE_MS` hook，让第一笔 foreground transaction 在首次 block allocation 暂停 3 秒（hook 见 `src/storage/engine/write.cpp:649-670`）；启动 2-thread source 和 replica，等待 replica ONLINE 后并发发两笔 MSET。GDB 在 `src/redis/command.cpp:1642` 命中第二笔命令的真实 `SleepFor(1ms)`，backtrace 为：

```text
BeginReplicationTransactionOrder
ExecuteCommandBody
ExecuteAdmittedCommand
ExecuteCommand
Dispatch / Serve
```

可重复步骤：

1. Debug build：`cmake --build build_debug --target keylane -j2`。
2. 给 source/replica 各创建一个 256 MiB 临时 data file；source 用 `-t 2` 启动。
3. 在 GDB 中对 source 执行 `set environment KEYLANE_TX_ACTIVE_BLOCK_PAUSE_MS 3000`，在 `src/redis/command.cpp:1642` 设 breakpoint 后 `run`。
4. 启动 replica，执行 `REPLICAOF 127.0.0.1 <source-port>`，轮询 `INFO replication` 至 `keylane_replication_state:online`。
5. 两个连接近同时执行不同 MSET；第一笔命中 3 秒 debug pause，第二笔必然在全局 CAS 失败后进入 1 ms timeout。

这属于确定性的手工 diagnostic，不是现有自动测试。下一步应把 waiter notification/ordering seam 做成可观测测试点，并新增：实际 contender wakeup、静态 false-positive（单 key DEL）、三 flow cycle 防护、FLUSH/MSET 和 FULLSYNC_CUT/MSET gate-order 四类回归。

## 建议实施顺序

1. **先修 correctness**：统一 gate acquisition order，并加两条 deterministic deadlock regression。
2. **立即消除 1 ms polling**：改为事件驱动且最好 FIFO 的 coroutine waiter；同时加入等待次数/时长指标。
3. **动态收窄**：按实际 participant/envelope 决定是否需要 ONLINE order，先去掉单 key DEL、single-owner MSET 等确定的 false positive。
4. **再移除 execution-wide serialization**：把已审计的 tx scheduler order 变成显式 replication invariant（携带/断言最终 scheduler txid，并补反向 schedule、retry/cancel、三 flow 环测试），然后只对 tx-backed multi-participant envelope 删除全局门。FLUSH 通过 close/drain transaction publication 或短 publication sequencer 单独纳入同一无环顺序。
5. 若选择 completion CSN/target reorder，则把它视为 replication protocol 变更，完整定义 bounded buffering、ACK/cursor、reconnect 和 full-sync cut，而不是只增加一个 atomic counter。
