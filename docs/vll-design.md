# Plan: keylane VLL 事务框架(参考 the reference engine 设计,原创实现)

## Context

keylane 是 thread-per-core、io_uring、C++20 协程的磁盘型 Redis 服务(celer submodule 提供运行时)。目前只有引擎内部的逐 key 阻塞锁(`storage::IntentLockTable`),多 key 命令仅 DEL/EXISTS 且逐 key 串行、无原子性,无 MULTI/EXEC。目标:参考 the reference engine 的 VLL(Very Lightweight Locking, Abadi et al.)设计一套**原创**的事务调度框架,支持跨 shard 原子多 key 命令(MSET/MGET、原子 DEL/EXISTS)和 MULTI/EXEC。

**硬性约束(用户要求):单 shard 命令在无竞争路径上绝不触碰全局 atomic txid 计数器。** 做法:txid 懒分配 + 调度期乐观执行(the reference engine 同款思路,但机制适配 celer 的 SPSC lane / 协程模型)。

**不抄 the reference engine 代码**——只借用概念:意向计数锁(Acquire 永不阻塞,只记录 intent 并返回"是否全部授予")、每 shard 按 txid 排序的 TxQueue 作仲裁者、全授予⇒可乱序执行、调度可失败但执行永不回滚、锁保持到 concluding hop。

**keylane 特有分歧(核心设计创新):** the reference engine 的 shard 回调不抢占、队头 run-to-completion;keylane 引擎操作持锁挂起在磁盘 I/O 上。解法:`IntentLock` 扩展为 **intent + hold 双层计数**——intent 是调度仲裁(the reference engine 语义),hold 标记"回调正在执行(可能挂起)"。挂起的持有者不阻塞不冲突的后续事务(intent 计数使授予检查在持有者睡眠时依然有效);队头只等真正冲突的 hold 排空。这保住了磁盘型 shard 的 I/O 重叠能力。

WATCH 纳入本里程碑(用户追加):做成**基于版本号的乐观校验**,零写路径钩子——keylane 每条 `RecordLocation` 已带 `mutation_sequence`(engine.cpp:3964 每次 AppendLocked 递增;EXPIRE 也是整记录重写会递增;defrag 搬迁保留不变 @5052),FLUSHDB 有 `DbEpoch(db_id)` @2837。快照 + EXEC 时锁下重读比对即可。

## 一、新模块:`tx/` 调度核心

新文件(注册进 CMakeLists.txt 的 `keylane_module`):
```
include/keylane/tx/fingerprint.h   LockFp = Digest 前 8 字节(SHA-1 已为索引计算,零额外哈希)
include/keylane/tx/intent_lock.h   IntentLock 计数器 + LockTable
include/keylane/tx/tx_queue.h      每 shard TxQueue
include/keylane/tx/transaction.h   Transaction、ShardData、hop awaiter
include/keylane/tx/tx_shard.h      TxShard(每 worker)+ TxRuntime(全局)
src/tx/tx_shard.cpp, src/tx/transaction.cpp
```
独立模块而非塞进 WorkerStore:调度发生在命令层(引擎之上),且引擎后台路径也要用;`TxRuntime`(`vector<unique_ptr<TxShard>>` + `atomic<uint64_t> next_txid`)在 RunServer 于 worker 启动前创建。

### LockTable(每 worker × 16 逻辑 DB 一张,单线程,无 atomic)
```cpp
struct IntentLock {   // absl::flat_hash_map<LockFp, IntentLock, IdentityHash>;四计数全零时删除
  uint32_t shared_intent, exclusive_intent;   // 调度意向(含排队/运行中)
  uint32_t shared_held,  exclusive_held;      // 正在执行回调(可能挂起于 I/O)
};
// Acquire(fp, mode): 永远记录 intent,返回 granted:
//   shared: exclusive_intent==0;  exclusive: shared_intent==0 && exclusive_intent==1(仅自己)
// CanHold(fp, mode): shared: exclusive_held==0;  exclusive: 两个 held 均 0
// 另有 ReleaseIntent / AcquireHold(断言 CanHold) / ReleaseHold
```
无等待队列——旧表的 FIFO waiters 删除,唤醒变为 TxQueue 的 Poll。锁粒度 = 整 key(路由仍按 hashtag slot);fp 碰撞只造成假竞争,正确性由队列排序 + 引擎索引的全 digest 比较兜底。

**为什么要拆 intent/held(传统 VLL 与 the reference engine 都只有 Cs/Cx):** 它们的执行前提是 run-to-completion——事务开跑后在分区线程上不挂起地跑完,所以任何调度决策时刻"正在执行"这个状态观察不到(之前启动的要么已完成计数已减,要么还排在队列里),两个计数器 + 队列位置即完备。keylane 的回调持锁睡磁盘,出现第三种状态:"不在队列、没结束、正挂着"(快速路径事务从不入队)。此时只看 Cs/Cx 有不可消解的歧义——队头看到冲突计数 1,分不清那是**睡着的前序运行者**(必须等)还是**排在自己后面的 intent**(绝不能等,等了死锁);不等则与睡着者并发读写、撕裂。held 计数恰好补上这一位信息:`*_intent` 保持传统 VLL 语义(排队+运行都计,授予/乱序判定全用它),`*_held` 只计"此刻回调在执行(可能挂着)",不变量 held ⊆ intent;队头门槛 = 轮到我 **且** CanHold 全过。否决的替代:快速路径也入队(需要 txid 排序位 → 碰全局 atomic 或引入无序的 txid=0 条目)、维护运行中事务指针集合(与 held 等价但更重,CanHold 不再 O(1))。

**intent/held 运转示例(逐拍,k1 计数记作 (Cs_i,Cx_i|Cs_h,Cx_h)):**
```
t0 GET1 快速路径:记intent授予+记held,挂起读盘         (1,0|1,0)
t1 SET 到达:不授予→txid入队为头;CanHold(X)见Cs_h=1→等  (1,1|1,0)   ← 等的是 held(运行者)
t2 GET2 到达:不授予→排 SET 后                          (2,1|1,0)
t3 GET1 完成:放held+intent→Poll;头CanHold:held全0→跑  (1,1|0,0)   ← 剩的 Cs_i 是 GET2 的 intent,不看
t4 SET 执行中,GET3 想快速路径:Cx_i=1→不授予入队        (1,1|0,1)   ← 反插队由 intent 层完成
t5 SET release→出队→Poll→GET2 起跑;完后全零删条目
```
队头两次看到非零 shared 计数(t1/t3),held 层一眼分清"运行者(等)"与"排后者(不等)"——单层 Cs/Cx 恰好丢失的就是这一位。快速路径记 held 必成功:全授予⇒唯一意向者⇒(held⊆intent)无人有 held。

**the reference engine 面对 SSD offload(tiered storage)的做法及为何不适用(已查证源码):** 它绝不让事务回调持锁睡磁盘——①兜底:`PollExecution` 见 `running_tx_` 直接 return(engine_shard.cc:620),回调真挂起时整个 shard 队列停摆(冲突不冲突都等);②主路径:offload 值的 GET 在 shard 回调里只注册读请求并返回 Future(string_family.cc:75),事务照常 conclude、锁照常释放,真正等磁盘的 `fut.Get()` 在连接 fiber 的 Send 里(:716),已在事务外;一致性靠 OpManager 记账(blob 读完成前不回收、`HasModificationPending` 拦截脏段)。keylane 不能照抄②:the reference engine 内存为主、事务本体只碰 RAM,磁盘读是可挪到锁外的纯数据取回;keylane 磁盘为本,写路径(AppendLocked)与读改写(INCR/EXPIRE 载入→计算→重写)的磁盘 I/O 就在临界区正中间,挪出去原子性即失。故必须支持持锁挂起 → held。潜在未来优化:纯 GET 可学②(锁内取 RecordLocation 快照、放锁后读块,配块租约防 defrag 回收),不影响 held 的必要性。

### Transaction(栈上、嵌入协调者协程 frame,无堆分配、无引用计数)
```cpp
class Transaction {
  uint8_t db_id;  const CommandContext* ctx;
  struct KeyRef { Digest digest; LockFp fp; uint32_t arg_index; Mode mode; };
  absl::InlinedVector<KeyRef, 2> keys_;            // 按 shard 连续分组
  struct ShardData { celer::RemoteWork msg;        // 内嵌 arm/schedule/cancel 消息
                     uint16_t shard_id, flags;     // kActive|kGranted|kQueued|kHoldsAcquired|kRanFirstHop|kArmed|kScheduleFailed
                     uint16_t key_begin, key_count; };
  absl::InlinedVector<ShardData, 1> shards_;       // 单 shard 内联 1 元素
  uint64_t txid_ = 0;                              // 0 = 从未分配(快速路径永远 0)
  ShardCallback cb_; bool releasing_; uint8_t phase_;   // 回调=函数指针+void* ctx,避免 std::function 堆分配
  std::atomic<uint32_t> barrier_;                  // 唯一跨线程热字
  std::coroutine_handle<> coord_handle_;  celer::WorkerId coord_worker_;
};
```
生命周期安全规则:**协调者每发起一轮(schedule/hop/cancel)必等 barrier;shard 对 barrier 的 fetch_sub 是它对 tx 的最后一次访问**——协调者不可能在最后一次递减前恢复,故 frame 不会悬垂(含回调挂起于 io_uring 期间:该 hop 的 barrier 尚未递减)。

**Hop 协议(celer 原生,替代 the reference engine 的 is_armed 原子交换):** `Execute(cb, release)` 的 awaiter 先写 `coord_handle_`,再 `barrier_.store(n, release)`,然后逐 shard:本地直接调 `ArmOnShard`,远端 `PostRequest(&sd.msg)`(SPSC lane 的 release/acquire 即所有权转移)。`ArmOnShard` 在 shard 线程置 `msg.reply_deferred = true`(抑制 celer 自动回复,已核实 cross_core.h/RunRemoteWork 契约)、标 kArmed、调 `Poll()`。完成侧:`barrier_.fetch_sub(1, acq_rel) == 1` 的 shard 若是协调者 worker 直接 `Enqueue(coord_handle_)`,否则 `PostNotification` 到协调者 worker,由其 drain 循环 Enqueue。**不变量:协调者 handle 只由它自己的 worker 线程 Enqueue。** 回调把结果写进调用者 frame 的槽位;跨 shard 数据只经协调者在 hop 间流动,回调之间绝不通信。

### TxShard / TxQueue / 调度算法
```cpp
struct TxShard { std::array<LockTable, 16> locks; TxQueue queue;   // deque + 惰性 tombstone(v1 从简)
                 uint64_t committed_txid = 0; bool polling = false; celer::Worker* worker; /* stats */ };
```
**ScheduleInShard(shard 线程,非挂起段):** (1) `txid_ != 0 && txid_ <= committed_txid` → 失败(过期);(2) 无条件记录全部 intent,记 granted;(3) **重排规则:队列非空且我的 txid < 队尾 txid 且 !granted → 释放 intent、调度失败**(队尾可能已乱序执行,不能插到它前面);(4) 按 txid 有序插入。

**txid 懒分配:** 多 shard 事务由协调者在每轮调度前 `next_txid.fetch_add(1)`(所有 shard 必须同一 txid);任一 shard 失败 → cancel 轮(成功的 shard 出队+释放+Poll)→ 取更大新 txid 重试(无界重试 + retries 统计)。**单 shard 事务 txid 保持 0 走快速路径;仅当授予检查失败才由 shard 线程 fetch_add 并入队——此时新 txid 必大于队内一切(fetch_add 全局单调 + 实时序),故必然队尾插入、调度永不失败、永不重试。**

**Poll(替代 the reference engine 的 PollExecution + running_tx_ 门):** 从 arm、每次事务完成(含快速路径完成)、cancel、release 触发。取队头(跳 tombstone);头在运行中/未 armed/`!HoldsCompatible(head)` 则 break(对应事件必再触发 Poll);否则 `committed_txid = max(committed_txid, head->txid_)`(**在回调首次可能挂起之前发布**,同一非挂起段,worker 协作调度天然原子)→ AcquireHolds → `SpawnOnCurrentWorker(RunTx)`。RunTx:`co_await cb(...)`(可挂起)→ 非挂起尾声:concluding 则释放 holds+intents、出队、Poll();最后 barrier 递减。多 hop:非 concluding 的 hop 后保留 holds 与队位,下次 arm 见 kRanFirstHop 直接继续(continuation 是 per-tx 标志而非 shard 字段——多个不冲突的挂起事务可同时在飞)。

**三类执行体并发模型:** ① 快速路径事务(全授予,txid 0,不入队)——全授予⇒唯一 intent 持有者⇒无冲突 hold,睡眠期间 intent 留在计数器里挡住后来者;② 乱序队内事务(多 shard 全授予)同理;③ 队头——只等先它而行的挂起持有者排空(I12:头的 intent 记录后无新冲突者能获授予,冲突集有限必排空)。

### 单 shard 快速路径阶梯(免全局 atomic 的证明)
- **梯 1(连接 worker == owner):** 协调者协程内直接 AcquireAllIntents;全授予 → AcquireHolds → 直接 `co_await GetLocked(...)` → 释放 → Poll。零 txid、零队列、零跨核、零 spawn。
- **梯 2(远端乐观):** 单条内嵌 msg(kScheduleAndRun),shard drain 循环内授予检查;通过 → 立即 Spawn 运行,**永不入队,txid 终身为 0**。相对现在 SubmitTaskTo 的增量成本:N 次计数增减 + 一次空队检查。
- **梯 3(竞争回退):** 授予失败(已在 shard 线程)→ shard 线程 fetch_add 取 txid、队尾插入、等 Poll。
`next_txid` 只在梯 3 与多 shard 调度中被访问;梯 1–2 只碰 per-shard map、per-tx barrier 与 SPSC lane——全局计数器缓存行从不加载。∎

## 二、命令层

### 命令表(新 `include/keylane/command_table.h` + `src/redis/command_table.cpp`,行为中性可先合)
`CommandSpec{name, kind, arity(Redis 约定,负数=最小), first_key, last_key(负=倒数), key_step, flags}`;flags: kWrite/kReadOnly/kNoKeys/kMultiShard/kGlobal/kNoTx/kNotQueueable。constexpr 数组 ~20 项,按长度分桶线性查找(保持零分配)。`DetermineKeys(spec, argc) -> KeyIndexView` 集中 arity 报错(消灭 ExecuteStorageCommand 里 ~10 处重复检查)。替换 `MatchCommandKind` 和硬编码 mutating/uses_db 链;dispatch 本里程碑仍用 switch。
新表项:MSET `{-3,1,-1,2,W|MS}`(另验 argc 奇偶)、MGET `{-2,1,-1,1,RO|MS}`、DEL/EXISTS 改 MS、MULTI/EXEC/DISCARD `{1,0,0,0,NoTx|NoKeys}`、WATCH `{-2,1,-1,1,RO|MS|NoTx}`(MULTI 内报错)、UNWATCH `{1,0,0,0,NoKeys}`(可入队)。

### 引擎 API 改造(禁止两套锁并存于同一 key——切换是原子里程碑)
- **步 A(行为中性):** 8 个客户端操作拆为 `XxxCore`(现有获锁行之后的全部逻辑,writer_mutex 仍内部持有并在返回前释放,key lock→writer_mutex 顺序不变)+ 旧锁包装。公开 `GetLocked/SetLocked/DeleteLocked/ExistsLocked/IncrementLocked/StringLengthLocked/GetExpirationLocked/UpdateExpirationLocked`,**接受预计算 Digest**(SHA-1 移到协调者 worker 计算,shard 不再哈希),debug 断言 `TxShard::HoldsKey`。
- **步 B(VLL 切换):** 删除引擎内 `key_locks` 获取与 `storage/intent_lock.h`;所有客户端 keyed 命令走事务。
- **后台路径**(均单 key、已在 owner worker):SnapshotPartition@~1917(S 逐 key)、ApplyReplicaRecords@~2166(X 逐条)、ExpireCandidate@~4511(X)、defrag RelocateIfCurrent@~5036(X)→ 统一改用 `TxShard::RunLocal(db, keys, cb)`(梯 1 阶梯的库化:try-grant 内联执行,否则栈上内部事务入队)。从此与客户端事务同一仲裁体系,公平排序、无第二锁系统。
- 回调作者规则:writer_mutex 绝不跨 hop 边界持有(现有引擎操作已天然满足)。

### 多 key 命令(单 hop 多 shard;删除 RouteMultiKey)
ShardView 暴露该 shard 分片的**原始参数下标**。MSET:shard 内按参数序 SetLocked(重复 key 锁去重、写按序 → last-wins);MGET:协调者预分配 `vector<optional<string>>(n)`,各 shard 填自己的槽位(不相交 + barrier acq_rel ⇒ 无竞争),按请求序编码回复;DEL 各 shard 计数求和;EXISTS 按出现次数计数(`EXISTS k k`→2)而锁集去重。单 shard 情形(hashtag)自动走快速路径。

### MULTI/EXEC(LOCK_AHEAD)+ WATCH
- 新 `include/keylane/session.h`:`ConnectionContext{selected_db, in_multi, multi_dirty, multi_db, vector<CommandRequest> queued, vector<WatchedKey> watched}`;`WatchedKey{uint8 db; string key; Digest digest; WatchStamp stamp}`,`WatchStamp{uint64 db_epoch; uint64 seq; bool live}`。Serve(server.cpp:405)的 `selected_db` 替换为 ctx,加 `DispatchCommand(ctx, request)` 包装层。
- 排队语义(Redis 兼容):未知命令/arity/READONLY/kNotQueueable → 报错+置 dirty;嵌套 MULTI、WATCH-inside-MULTI(`-ERR WATCH inside MULTI is not allowed`)→ 报错不置 dirty;SELECT 更新 multi_db 并入队;UNWATCH 可入队(EXEC 内为 no-op);其余 `+QUEUED`。DISCARD/EXEC-without-MULTI 标准错;dirty EXEC → `-EXECABORT`;空队 → `*0`;运行期错误内联在数组里继续执行。
- **WATCH 机制(用户选定:push 式 shard 本地标记表,零跨线程共享内存)**:每 (worker, db) 一张 `flat_hash_map<LockFp, WatchItems>` 表,表项按 (conn_id, key) 登记,conn_id 只作键、永不解引用。四要素:
  1. **登记**:WATCH(仅 MULTI 外可用)逐 shard `SubmitTo` 到 owner 建表项(`try_emplace`——重复 WATCH 保留原项原标记,粘性,匹配 Redis);表项记 `live` 位(当时是否存在且未过期);
  2. **标记**:挂在**真实修改的收口**而非命令分类——`AppendLocked`/索引更新点(覆盖 SET/DEL/INCR/EXPIRE/主动过期)、副本应用写入点、FLUSHDB 清库点(标本 worker 该 db 全部表项,含不存在 key 的 watch);`SET NX` 未生效等"没改"的不标;defrag 搬迁不标。写路径成本 = 空表分支(≈零);
  3. **检查(EXEC)**:先调度加锁,再看标记。队内命令触及的 shard 在 hop 0 顺路查本 shard 表项;仅被 watch、事务不去的 shard 用普通 `SubmitTo` 读(无锁安全:EXEC 不读该 key,写与 EXEC 任意排序皆合法序列化)。**补被动过期洞**(Redis 亦用 isWatchedKeyExpired 补):比对表项 live 位与当前 IsExpired,变了也中止。任一标记/live 变 → `Release()` 回 `*-1`;竞态论证:标记与检查同 shard 线程,写要么在锁前完成(标记必被看到)要么被 intent 排在事务后,无第三种;
  4. **清理**:UNWATCH/DISCARD/EXEC(无论成败)与连接断开(M3 唯一清理点)向登记过的 shard 发 `PostNotification` 按 conn_id 擦除(急切发送、异步生效、mailbox 不丢)。
- EXEC = 一个事务:先取 DbOperationGuard(整个 EXEC 持有,避免 hop 中途 TRYAGAIN);锁集 = 排队命令 key 并集(per-key mode:任一写者触及则 X)→ `InitKeys` 显式 (db,key,mode) 列表 → `Schedule()`(单 shard 全授予时直接取 intent+hold、不入队、txid 0——EXEC 也享受快速路径)→ watch 检查(上述 3)→ 通过则逐命令串行执行:每条内部命令一个非 concluding hop(barrier 分隔),handler 重构为 `RunXxx(Transaction&, request)` 共用于独立执行与 EXEC;无 key 命令(PING/SELECT)在协调者 hop 间执行;末尾 `Release()` 保证锁恰好释放一次。
- push 式下 Redis 各边界天然对齐:SET 后 DEL 再回收 tombstone(标记在 SET 时已打、粘性,无 pull 式的 ABA);FLUSHDB 使"不存在的 watched key"也失效(清库点标全表);值改回原值仍失效(标记看修改事件不看值)。**备选记录(已评估未采用):pull 式版本快照**——WATCH 时记 `{db_epoch, mutation_sequence, live}` 三元组(复用现成 seq/epoch,零新增状态),EXEC 校验 hop 锁下重比;缺点:watched 需进锁集、多一个校验 hop、tombstone 回收有 ABA 窗口(需最小回收年龄加固)。push 落地遇阻时退回此案。
- Transaction 公开:`InitKeys` / `Schedule()` / `Execute(cb, release)` / `Release()`(原名 Conclude,2026-08-08 更名)。(RENAME 双 hop 是该 API 的验证命令,作可选扩展项。)

### 复制与 FLUSHDB(本里程碑决策)
- 复制:保持逐 key delta 不变(在 key lock 之下写入,多 key 事务自然逐 key 发)。**已记录缺口:副本可见撕裂的 MSET/EXEC**(与今日 DEL 行为一致);原子 journaling 留待专门里程碑。副本只读检查同时在 MULTI 排队期执行。
- FLUSHDB/DBSIZE/SCAN:留在 g_db_gates;每个 VLL 事务全生命周期持 DbOperationGuard;三者 kNotQueueable(与 Redis 的偏差,文档注明);后续里程碑再迁 shard 级全局事务。

## 三、实现顺序——小里程碑,每步独立编译、测试全绿、可单独合入

| # | 目标 | 大小 | 行为变化 |
|---|------|------|---------|
| M1 | **命令表**:CommandSpec + FindCommand + DetermineKeys,替换 MatchCommandKind 与硬编码 mutating/uses_db;`tests/command_table_test.cpp` | 小 | 无 |
| M2 | **引擎拆分**:8 个客户端操作拆出 `*Locked` 变体 + Digest 参数化,旧锁包装保留 | 中 | 无 |
| M3 | **ConnectionContext** 穿线 Serve/Dispatch(MULTI 还不接);同时把现有循环抽成内层 `ServeLoop(stream, ctx)`,外层 `Serve` 在 `co_await ServeLoop` 后设唯一清理点 `CleanupConnection(ctx)`(所有断开路径必经、可 await——为 WATCH 注销等连接级清理立好结构) | 小 | 无 |
| M4 | **tx/ 模块**:LockTable(intent+hold)+ TxQueue + TxShard + RunLocal + TxRuntime 接线,只有单测(`tests/tx_lock_test.cpp`,用会挂起的假回调测 grant/hold/queue/poll),不接任何命令 | 中 | 无 |
| M5 | **VLL 切换**(唯一切换点,此时只剩机械替换):单 key 命令走快速路径阶梯;4 个后台路径迁 RunLocal;删 `WorkerStore::key_locks` 与旧 `storage/intent_lock.h`;事务持 db gate 全生命周期。全 CTest + e2e + ASan | 中 | 无(语义等价) |
| M6 | **多 shard 单 hop**:调度轮/cancel/重试、hop awaiter + barrier + notification 恢复;MSET/MGET + 原子 DEL/EXISTS(删 RouteMultiKey);`tests/multikey_e2e_test.cpp` | 中 | 新功能 |
| M7 | **MULTI/EXEC/DISCARD** + Release(时名 Conclude) + 多 hop(continuation);`tests/multi_exec_e2e_test.cpp`。EXEC 执行模型 = **逐命令串行 hop**(用户定,理由:要支持全部命令,串行 hop 对所有命令形态——单 key、多 key、RENAME/EVAL 类跨 shard 数据流、将来阻塞类——统一成立,无需切段判断):每条命令一个非 concluding hop,barrier 分隔,后令可见前令效果、回复按序;内部命令不单独入队——事务间顺序归 TxQueue,事务内顺序归 hop 序。优化挂账(命令面稳定后):分段打包/squashing——正确性三条件已论证(切片预先可定、shard 内保排队序即同 key 必同 shard、回复槽位组装),在跨 shard 数据流命令处切段,段内一 hop | 中 | 新功能 |
| M8 | **WATCH/UNWATCH**(push 式 shard 本地标记表):登记/修改收口标记/EXEC 锁后检查(含被动过期比对)/急切注销四件套;WATCH e2e 用例并入 multi_exec 测试 | 中 | 新功能 |
| M9 | **压测 + 观测**:`tests/atomicity_stress_e2e_test.cpp`;fastpath/OOO/queued/retries/head_wait 统计;复制缺口文档。(TxQueue 换 vector-ring + pq_pos 为可选优化) | 小 | 无 |

M1–M4 相互独立;M5 依赖 M2+M4;M6 起顺序依赖。

### 单 key 命令的成本说明(用户关切)
切到 VLL 后单 key 命令**运行期成本不升反降**:今天已是 `try_emplace` 20 字节 Digest 键 + waiters 队列检查;之后是 `try_emplace` 8 字节 fp 键(恒等哈希)+ 两个计数器加减 + 一次空队检查。无竞争时不入队、不取 txid、不过 barrier,路由结构(inline 或 SubmitTaskTo)不变。框架复杂度集中在多 key/多 hop 分支,单 key 命令只穿过最平凡路径。

## 四、验证

- 单测:command_table(arity/负下标/flags)、tx_lock(S/S 授予、S/X 冲突意向、零计数驱逐、RunLocal 对队列的公平性、挂起持有者不挡不冲突事务)。
- E2E(扩展 RespClient 递归解析数组回复;server `--threads 4` 保证真跨 shard,另留 `--threads 1` 一节):
  - multikey:乱序跨 shard MGET 按请求序返回、缺失 key 为 nil、MSET 重复 key last-wins、hashtag 快速路径回归。
  - multi_exec:完整 RESP 语义矩阵(QUEUED/嵌套/EXECABORT/DISCARD/空 EXEC/内联运行错/SELECT-in-MULTI/状态复位)。
  - WATCH(双连接):他客户端改 watched key → EXEC `*-1` 且队内命令未执行;自己在 MULTI 前改 → 同样失效;无修改 → 成功;值改回原值(SET k v; SET k v)仍失效(版本语义,匹配 Redis);DEL watched → 失效;EXPIRE watched → 失效;watched key 从无到有 → 失效;FLUSHDB → 失效;UNWATCH 后 EXEC 成功;EXEC 后 watch 已清空(再 EXEC 不受旧 watch 影响);WATCH-inside-MULTI 报错但事务可继续;跨 shard watched keys。
  - **原子性压测**(抓乱序/锁模式/barrier 错误的形状):W1 循环 `MSET a v b v`、W2 循环 `MSET b u c u`(值带写者标签,key 经 CRC16 验证跨 shard);读者(MGET 与 MULTI-GET×3-EXEC 两种)断言:b 是 W1 值 ⇒ a==b,b 是 W2 值 ⇒ c==b;另加同 key 对 (a,b) 重叠锤击断言恒 a==b。跑 5–10s,失败时转储三元组+服务器日志。
- 每步后:`scripts/build_debug.sh && ctest --test-dir build_debug`;步 5 起加 ASan 构建跑 e2e(build_asan/ 已存在)。

## 五、已决风险项

- 阻塞命令(未来 BLPOP)会破坏 frame 内嵌生命周期(挂起阻塞事务活过 hop barrier)→ 现在保持 frame 内嵌;记录升级路径:仅 BLOCKING 命令走侵入式引用计数+堆分配。
- 多 shard 重试理论上可饿死 → 无界重试 + 统计 + 重试间 `celer::Yield`,基准显示问题再处理。
- 队头被慢速挂起持有者拖延 → 磁盘型原子性的固有代价,per-fp 粒度已限制在真冲突;加 head_wait 统计。
- `reply_deferred` 契约依赖(已对照 cross_core.h 核实)→ celer 侧加注释 + keylane 加跨 worker arm 压测。

## 关键文件

改动:`src/redis/command.cpp`(路由重写)、`src/redis/server.cpp`(ctx 穿线)、`include/keylane/command.h`、`src/storage/engine.cpp`(*Locked 拆分、后台路径迁移、删 key_locks)、`include/keylane/storage/engine.h`、`CMakeLists.txt`;删除:`include/keylane/storage/intent_lock.h`;新增:`include/keylane/tx/*`、`src/tx/*`、`include/keylane/command_table.h`、`src/redis/command_table.cpp`、`include/keylane/session.h`、5 个新测试。只读依赖:`celer/include/celer/runtime/cross_core.h`(RemoteWork/PostRequest/PostNotification/reply_deferred)。
