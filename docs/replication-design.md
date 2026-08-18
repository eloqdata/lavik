# Keylane 主从复制设计

> 状态：设计提案
> 范围：Keylane 到 Keylane 的原生异步主从复制
> 目标：支持启动时配置从节点、运行时动态添加、一个主节点挂多个从节点，并为断线增量续传和未来大 Value 拆分保留正确边界。

## 1. 结论

Keylane 原生复制采用以下模型：

- replica 主动连接 master，master 不保存静态从节点地址；
- 启动时使用 `replicaof host port`，运行时使用 Redis 兼容的
  `REPLICAOF host port`；
- master 可以同时服务多个 replica，每个 replica 拥有独立 session 和发送游标；
- 全量复制使用“partition snapshot + snapshot 期间的 mutation delta”；
- 稳态复制发送提交后规范化的确定性命令，不转发客户端原始请求，也不复制本机物理块引用；
- 每个 source worker 维护独立 replication LSN 和有界 backlog，replica 保存一个 LSN 向量；
- backlog 同时按记录数和逻辑字节数限制，慢 replica 不得无限钉住内存或磁盘；
- backlog 已覆盖时只让对应 replica 重新全量，不影响其他 replica；
- 第一版不做自动选主、quorum、split-brain fencing 和级联复制；
- 第一版 master 或 replica 进程重启后允许退化为全量同步，不依赖周期 checkpoint。

现有 `src/replication/replication.cpp` 和
`src/storage/engine/replication.cpp` 中的 partition snapshot、mutation sequence、
replication epoch、DB epoch、幂等 apply 和大 Value 网络分帧可以保留，但连接方向、
backlog 所有权和 session 模型需要重构。

## 2. 目标与非目标

### 2.1 第一阶段目标

- 启动时成为 replica；
- 运行时执行 `REPLICAOF` 动态成为 replica 或更换 master；
- `REPLICAOF NO ONE` 手工解除复制关系；
- 一个 master 支持多个 replica；
- master 写入不中断地完成全量复制；
- 网络短暂断开后，在 backlog 仍存在时增量续传；
- replica 默认拒绝客户端写命令；
- master 和 replica 的 worker 数可以不同；
- SET、DEL、TTL、FLUSHDB、所有集合类型和大 Value 使用同一复制框架；
- 复制资源有明确内存、磁盘 pin、网络队列和并发上限；
- 提供足够的状态、指标和故障测试。

### 2.2 暂不包含

- 自动 failover 和选主；
- Sentinel/Raft/Cluster bus；
- 多 master 写入；
- synchronous replication；
- replica 链式转发；
- Redis RDB/PSYNC 输入适配；
- 一个 Keylane 聚合多个 Redis Cluster master。

这些能力必须构建在本文的 apply、逻辑 mutation 和 role state machine 之上，不能
反过来污染第一版原生协议。

## 3. 为什么复制规范化命令

复制单元选择“提交后的确定性命令”：

```text
SET key value [PXAT absolute_ms]
DEL key
RESTORE key absolute_ttl logical_value
DB_EPOCH(db, new_epoch, barrier_id)
```

这些不是原始客户端请求：条件写只在成功后发布，TTL 转成绝对时间，随机操作发布其
确定结果，集合可转换成确定性的修改命令或 RESTORE。原因是：

- 命令可能包含当前时间、随机数、条件写和阻塞语义；
- master 与 replica 执行时的数据前置状态可能不同；
- 同一命令在未来版本中的内部编码可能不同；
- 事务必须复制最终提交结果，而不是把中间命令提前暴露给 replica。

不复制物理 storage delta，原因是 block ID、allocation epoch、record offset、extent
和 worker ownership 都是单机状态。网络协议只传稳定逻辑编码。

当前集合类型是单记录编码，因此集合修改可以暂时发布包含完整逻辑 Value 的 RESTORE。
未来大 Value 拆分后，命令参数由对象迭代器流式提供；底层 frame 可以分片，但业务层
仍然只有一条命令和一个 flow LSN。

## 4. 角色和状态机

进程级角色由 `ReplicationManager` 独占管理：

```text
MASTER
  | REPLICAOF host port
  v
CONNECTING -> FULL_SYNC -> CATCHING_UP -> ONLINE_REPLICA
     ^            |              |              |
     +------------+--------------+--------------+
                  断线后重连

ONLINE_REPLICA -- REPLICAOF NO ONE --> PROMOTING --> MASTER
```

约束：

- 任意时刻最多有一个 upstream；
- MASTER 可以有多个 downstream session；
- 第一版 ONLINE_REPLICA 不允许 downstream，避免 A -> B -> C 静默丢 delta；
- 所有角色切换由一个 control generation 串行化；
- 每次更换 upstream 都增加 local apply generation，旧连接和旧 coroutine 即使稍后
  恢复，也不能继续写入新 generation；
- `REPLICAOF` 先完成连接、认证和能力检查，再破坏当前复制关系；
- full sync 期间默认对数据命令返回 `LOADING`；
- partial sync 期间 replica 可以继续提供只读服务。

`REPLICAOF NO ONE` 的第一版语义是手工提升：停止 upstream、等待在途 apply
结束、启用本地过期权、生成新的 master replid，并保留当前数据。它不提供 fencing，
操作者必须先保证旧 master 不再接受写入。

## 5. 配置和命令

Keylane 使用 Redis 风格主配置文件。复制角色不提供 `--replica*`、
`--replicate-to` 或 `--replication-port` CLI 参数，避免启动参数和运行时角色形成两套
状态来源。启动方式与 Redis 一致：第一个位置参数是配置文件，CLI 只用于覆盖普通
server 配置，不用于建立复制关系：

```bash
keylane /etc/keylane/keylane.conf
```

配置文件按行解析，支持空行、`#` 注释、大小写不敏感的 directive、单/双引号和
反斜线转义；未知 directive、错误参数个数和非法值必须使启动失败并报告文件名和
行号。第一版不静默接受尚未实现的 Redis directive。

Replica 配置示例：

```text
port 6379
replicaof 10.0.0.10 6379
replica-read-only yes
replica-serve-stale-data yes
masteruser replication-user
masterauth secret

repl-backlog-size 1gb
repl-backlog-records 1000000
repl-output-buffer-limit 256mb
repl-timeout 30s
repl-fullsync-parallelism auto
repl-fullsync-rate-limit 0
repl-max-replicas 16
```

命令：

```text
REPLICAOF <host> <port>
REPLICAOF NO ONE
ROLE
INFO REPLICATION
CONFIG REWRITE
```

运行时 `REPLICAOF` 只修改内存配置；`CONFIG REWRITE` 才将它写回配置文件。CLI
参数可以覆盖端口、线程数和存储路径等普通配置，但不存在复制角色相关 CLI。
`CONFIG REWRITE` 使用临时文件、`fdatasync` 和原子 rename，保留原文件中的注释与
未知非复制配置，并只重写受 Keylane 管理的 effective directives。未从配置文件启动时
返回错误，不猜测写入路径。

master 不需要 `ADDREPLICA` 命令。添加从节点是在新节点执行 `REPLICAOF`，因此
多个 replica 天然成立。

## 6. 连接和原生协议

所有连接由 replica 发起。为了让用户只填写普通服务地址，握手复用 Redis 端口：

```text
replica -> master: AUTH（如果配置）
replica -> master: REPLCONF capa keylane-repl-v1
replica -> master: KLPSYNC <replid|?> <flow-offset-vector>
master  -> replica: FULLSYNC 或 CONTINUE
```

能力协商完成后，该连接切换为 Keylane replication framing。额外 flow 连接仍连接
同一服务端口，并使用 session token 绑定：

```text
KLFLOW <session-id> <flow-id> <next-lsn>
```

稳态连接数固定为 `1 + source_worker_count`：一条 control/session 连接，加上每个 source
worker 一条 data-flow 连接。每条 data socket 只由所属 worker 访问，不使用 MPMC，也不把
大 Value 跨核搬到统一 sender；target worker 数量可以不同，接收后再按 `partition_id`
路由。

当前 connection-control slice 已实现协议版本 1 的连接骨架：replica 在普通 Redis 端口
发送 `KLPSYNC 1 ?`，master 返回 session id、40 字节 replid 和 source worker 数；随后
replica 发送 `KLFLOW 1 <session-id> <flow-id> <next-lsn>` 建立所有 data flow。master 将
接错 worker 的已握手 socket adopt 到对应 source worker，target worker 较少时允许一个
target worker 持有多个 flow。所有 flow 建立后状态机进入 ONLINE。snapshot/backlog frame
尚未接入这些 socket，因此此时 ONLINE 只表示连接组完整，不表示数据已经同步。

升级后的 socket 不计入 `connected_clients`，另由
`keylane_replication_control_connections` 和
`keylane_replication_flow_connections` 两个 gauge 统计；`keylane_connections` 仍表示进程
当前所有 TCP 连接。

协议帧至少包含：

```text
magic/version
frame type/flags
session id
flow id
frame sequence
payload length
payload CRC32C
```

需要的帧类型：

- HELLO / FULLSYNC / CONTINUE / ERROR；
- FLOW_OPEN / FLOW_ACK / HEARTBEAT；
- PARTITION_RESET；
- SNAPSHOT_RECORD / SNAPSHOT_END；
- COMMAND_DATA（可以使用 first/last 和 fragment index 分成多个传输 frame）；
- TX_BEGIN / TX_FRAGMENT / TX_COMMIT；
- DB_EPOCH；
- SESSION_END。

协议必须带显式版本和 feature bits。Keylane 原生协议与未来 Redis PSYNC adapter 分离，
不能让内部协议受 Redis 单一 byte offset 限制。

## 7. replid、flow LSN 和 partition sequence

保留三种不同身份：

### 7.1 master replid

`master_replid` 标识一段可续传的 master 历史。第一版每次进程启动随机生成，master
重启后 replica 退化为 full sync。手工提升也生成新 replid。

这避免为了保存 replid/offset 周期性 checkpoint，也避免错误地从一个已经失去
backlog 的新进程续传。

### 7.2 flow LSN

每个 source worker 有一个单调递增的 replication LSN。一次已经提交的逻辑 mutation
被发布到所属 worker 的 backlog 时获得 flow LSN。

Replica 的稳定进度是：

```text
master_replid + [flow0_next_lsn, flow1_next_lsn, ...]
```

它的大小只与 source worker 数相关，不需要传 16,384 个 offset。master 重启或 worker
拓扑变化时 replid 改变，自动 full sync。

### 7.3 partition mutation sequence

现有 partition `mutation_sequence` 继续作为数据版本，用于：

- snapshot/delta 幂等 apply；
- 丢弃重复记录；
- 防止旧 snapshot 覆盖新 delta；
- partition full-copy overflow 检测。

flow LSN 用于传输续传；partition sequence 用于数据正确性。两者不能合并。

## 8. Master backlog

现有 `partition.deltas_` 会被一个 ACK 从队首删除，只能服务一个 target。新设计改为：

```text
WorkerReplicationLog
  next_flow_lsn
  bounded deque<ReplicationLogBlock>
  block LSN ranges + sparse frame offsets
  one active 8 MiB staging block
```

关键规则：

- event 在一个 worker backlog 中只保存一份，所有 replica 共享；
- 每个 session 单独保存 next LSN 和 ACK；
- ACK 不直接删除 backlog；
- backlog 达到记录数或字节数上限时覆盖最老 event；
- 落后于 floor 的 session full sync，不能让最慢 replica 阻止覆盖；
- 同时限制 backlog block bytes、frame count、logical bytes 和 age；
- 没有 replica 时可以不保留 backlog，首个 replica 直接 full sync；
- 写路径只向有界后台发布器提交 committed event，不等待 backlog IO 或任何网络发送；
- 稳态复制先写入每个 worker 预留的有界内存 queue，由后台 publisher 异步 spill 到 disk backlog；当前预留上限为 8 MiB/worker；
- backlog 分配或 IO 失败只将该 flow 标记为 invalid，主写继续，replica 改走 full sync；
- master 重启更换 replid，恢复只识别并回收旧 replication blocks。

当前 command-journal slice 的每 worker 发布队列上限是
`min(backlog capacity, 64 MiB)`，并且把正在写 backlog 的 event 计入上限。队列满或单条
event 超限时不反压客户端，而是立即 invalid 当前 flow；后续以 full sync 恢复。未来主数据
支持外部大 Value 后，发布项改为带生命周期保护的 record/extent source，避免为了排队长期
持有整条 Value，同时保留同样的有界和不反压语义。

每个 mutation 记录 `partition_id`，per-partition index 用于 full sync 期间按
`partition_sequence` 查找 delta。稳定阶段按 flow LSN 顺序发送。

### 8.1 Value 如何进入 backlog

backlog block 保存自包含的逻辑 event，不引用可能被 defrag/覆盖的主数据物理位置：

- 小 event 编成单 frame；
- 大命令由 payload source 流式写成共享同一 flow LSN 的多个 frame；
- frame 可以跨 block，使用 first/last flag 和 fragment index；
- 一个 event 要么完整保留，要么整体位于 floor 之前，不能只留下中间 fragment；
- receiver 收到 last frame 并完成命令校验后才发布结果；
- 内存只保存 active block、block LSN 范围和每 64 frame 一个稀疏 offset；
- sealed block 只要求运行期 IO 完成，不进入主写 fdatasync durability boundary；后续 TODO 是增加可选的 durable replication ACK；
- output window 仍然独立有界，连接不能无限预取 backlog frame。

这会增加一次顺序写放大，但消除了 replication pin 对 defrag 和主数据回收的长期阻塞，
也让断线续传不依赖主数据 block 是否已经被重写。

## 9. 全量同步

全量同步仍以 16,384 个逻辑 partition 为恢复和重试单位：

```text
BEGIN partition
  -> source 建立 partition sequence fence
  -> target 安装新的 local replication epoch
  -> target 清空该 partition 的旧索引代际
  -> source 扫描 16 个 DB 的 baseline
  -> source 发送 fence 之后的 partition delta
  -> target ACK 到 watermark
  -> partition SYNCED
```

Master 正常处理写入。`BeginPartitionReplication` 必须先建立 fence 和 delta 捕获，再
开始扫描。现有 `ScanHashMap` 保证持续存在的 entry 在并发删除/rehash 时不被漏掉；
发生变化的 key 由 fence 后 delta 修正。Snapshot 发送前仍需在 key shared lock 下
重新验证 location。

已完成 partition 的 delta 在复制其他 partition 时继续转发。某 partition 的 delta
已被 backlog 覆盖时，只 reset 并重抄这个 partition。

### 9.1 Full-sync delta 容量与溢出

全量同步不能只依赖会循环覆盖的稳态 backlog。对每个正在扫描的 partition，
source 必须从 fence 开始为该 session 保留 delta，直到 target 完成 baseline apply
并 ACK 到 watermark。这些 delta 可以与稳态 backlog 共用 frame block，但必须拥有
独立的 session/partition 引用和容量计费，不能因稳态 floor 前进而被回收。

保留量估算至少要考虑：

```text
required_delta_bytes ~= snapshot_generation_and_transfer_time * peak_write_bytes_per_second
```

硬性规则：

- full-sync delta 落盘，内存只保留有界的 active block、索引和网络 window；
- 同时限制 per-session、per-partition 和全局 pinned delta bytes；
- 一个 partition 完成 catch-up 后立即释放它的 full-sync pin；
- 任一容量上限溢出时，终止该 session/generation，或只 reset 受影响的
  partition 并重新建立 fence；绝不允许覆盖后继续声称同步成功；
- 不因慢 replica 反压客户端写入；持续超限会导致该 replica 重试后仍失败，
  并通过 metrics 和错误状态暴露，由运维增加配额、降低写入速率或先离线预热。

这个限制同样适用于未来拆分的大 Value：frame 可以分块，但整个 logical event
在 last frame 收到前都必须可重放，并且所有 fragment 都计入同一个有界配额。

### 9.2 Target reset 重构

当前 `ResetReplicaPartition` 为旧 key 逐个写 tombstone，并在持有
`store_state_mutex` 时可能等待空间回收，存在死锁风险。新实现应：

1. 串行进入该 partition 的 apply lane；
2. 批量持久化新的 partition replication epoch；
3. O(1) detach 该 partition 的 DB indexes；
4. 发布新空 index generation；
5. 后台遍历 detached indexes，更新 live-byte accounting 并回收旧 blocks/extents；
6. full sync 可立即向新 generation 写数据。

恢复只接受 metadata 中最新 replication epoch 的记录，因此不需要为每个旧 key 写
tombstone。旧 generation 的回收必须受 pin 和 index generation 保护。

### 9.3 全量同步结束屏障

所有 partition 标记 SYNCED 后不能立即声明 ONLINE。每个 source flow 需要：

1. 记录 stable barrier LSN；
2. 将所有 partition catch up 到 barrier 对应的 watermark；
3. target 等待所有 apply 完成；
4. 从 barrier LSN 切换为顺序 stable stream；
5. 所有 flow 均完成后才清除 LOADING。

## 10. 稳态复制和 partial sync

稳态阶段 master 按每个 flow 的 LSN 顺序发送 mutation。Replica 可以把不同 partition
路由到不同 target worker 并行 apply，但 ACK 必须是连续完成的 flow LSN，不能越过尚未
完成的 frame。

Replica 定期发送：

```text
FLOW_ACK(flow_id, applied_lsn, durable_lsn)
```

当前 native flow 已先实现有序的 frame ACK：每个 RESET/RECORDS frame 在 target apply 成功后回传
partition 和最后的 mutation sequence。这个 ACK 是 applied ACK，不是 durable ACK；它为后续
partial sync cursor 和 durable fence 提供了连续进度。

- `applied_lsn`：已经写入 replica index；
- `durable_lsn`：对应 storage durability fence 已完成。

第一版异步复制以 applied ACK 为进度，指标同时暴露 durable lag。未来 `WAIT` 可以使用
applied ACK，持久复制策略或 `WAITAOF` 使用 durable ACK。

断线重连：

- replid 相同且每个 flow 的 next LSN 都仍在 backlog：CONTINUE；
- 任意 flow 已落后于 floor：整 session full sync；
- 正在进行 full sync 时断线：第一版重新开始 full sync；
- master replid 变化：full sync；
- 混合 partial/full flow 不允许对外进入 ONLINE。

## 11. Replica apply 正确性

每个 partition 增加一个串行 `PartitionApplyLane`：

```text
partition_id
local replication_epoch
apply_generation
current source partition sequence
large-value staging state
```

Reset、snapshot apply、delta apply 和 session cancellation 都经过该 lane。每个可能
suspend 的步骤恢复后必须重新验证：

```text
session generation
partition replication epoch
DB epoch
index generation
```

这样旧 session 即使在连接关闭后恢复，也不能越过新 reset 写入数据。

命令层现有 file-static DB gate 应抽成共享 `DbGateManager`。客户端命令和 replica
apply 都必须取得相同的 shared DB guard；DB_EPOCH/FLUSHDB 使用 exclusive guard。
这解决 KEYS/FLUSHDB 与 replica apply 并发导致的 RESP 数量和索引一致性问题。

Apply 的幂等规则：

- source DB epoch 小于本地：忽略；
- source DB epoch 大于本地：必须先收到 DB_EPOCH；
- key 当前 sequence 大于等于 event sequence：忽略重复或旧记录；
- replication epoch/generation 不匹配：取消整个旧请求；
- protocol frame、Value checksum 或类型元数据错误：断开 session，不接受部分 Value。

## 12. 大 Value

网络层对一条逻辑命令统一使用：

```text
COMMAND_DATA(lsn, fragment_index, FIRST/LAST, bytes, frame_crc)
```

frame 分片不是 `SET_CHUNK` 一类业务命令。接收端命令解码器把大参数直接写入 staged
extents，不能长期把全部 frame 拼回一个 `std::string`：

1. 解码到大参数头时创建不可见 staging object；
2. 后续 COMMAND_DATA 顺序写 extents，保持 bounded buffer；
3. 收到 LAST 后校验命令长度和 digest；
4. 最后写并发布顶层逻辑 record/root；
5. 失败、断线、reset 和进程恢复时回收未发布 extents。

未来集合拆分后，snapshot 和 delta 仍传逻辑对象流；所有 child 先不可见写入，最后由
root commit 原子发布。因此该复制方案不依赖“一个 key 等于一个物理 record”。

## 13. 事务

现有 mutation 在每个 key 写成功后立即进入 delta，multi-key transaction 可能在
replica 上短暂撕裂，失败回滚还会强制 partition 重抄。完整实现必须改为 commit-aware
发布：

- 非事务写：逻辑提交后立即发布 mutation；
- 事务写：先在 `TxShardWrites` 中收集 replication fragments；
- transaction commit record 成功后才把 fragments 发布到 backlog；
- rollback 丢弃 fragments，不产生需要补偿的外发 mutation。

跨 flow transaction 使用：

```text
TX_BEGIN(txid, participant flows, fragment counts)
TX_FRAGMENT(txid, logical mutation ...)
TX_COMMIT(txid, descriptor digest)
```

Replica 将 fragment 暂存到有界 staging，收到完整 TX_COMMIT 后通过现有 transaction
engine 一次提交并发布。各 flow 的 ACK 不能越过尚未完成的 transaction；断线重连从
transaction 开始位置重发。

Full sync snapshot 在 key lock 下等待正在提交的 transaction，因此只读取提交前或
提交后的完整 key 状态。Replica 在 LOADING 期间不对外暴露跨 partition 中间状态。

## 14. TTL、FLUSHDB 和过期权

- 网络传绝对 `expire_at_ms`，不传相对 TTL；
- master 是过期 mutation 的唯一 authority；
- replica 读路径可以隐藏已经到期的值，但 ONLINE_REPLICA 不生成自己的 tombstone；
- 提升为 master 后启用主动过期和 tomb raider；
- FLUSHDB 复制为广播到每个 source flow 的 DB epoch barrier，不发送所有 key 的 DEL；
- FLUSHALL 在一个 barrier 中携带 16 个新 DB epoch；target 收齐所有 flow 后才原子
  detach 旧索引并放行 barrier 后的命令；
- DB epoch 必须在该 DB 后续 mutation 之前安装并持久化；
- full sync 的 target reset 和 DB epoch advance 使用统一 gate/generation 机制。

当前 command-journal slice 已实现 FLUSHDB source 广播和 replica epoch-detach apply
primitive：每个 worker 产生一个 `kControl` frame，payload 为内部命令
`FLUSHDB <new_db_epoch>`。网络 receiver 必须先按 `(db, epoch)` 收齐所有 source flow，暂停
这些 flow 的 barrier 后命令，然后只调用一次 apply primitive；不能在收到第一条 frame 时就
清库。FLUSHALL 的 16-epoch 原子 barrier 尚未实现，因此不会降级成 16 条 FLUSHDB 传播。

## 15. 多 replica 和慢 replica

每个 downstream session 独立维护：

```text
session id
replica id/address
state
flow next/ack/applied/durable LSNs
output bytes
last heartbeat
full-sync partition progress
```

共享的 master backlog 不按最小 ACK 裁剪。慢 replica 的处理顺序是：

1. 达到 per-session output limit 后停止为该 session 预取；
2. 超过 timeout 后断开该 session；
3. backlog 尚在则重连 partial；
4. backlog 已覆盖则该 replica full sync；
5. 其他 replica 和 master 写路径不受它长期反压。

全量同步还需限制并发数和带宽，避免同时加入多个 replica 将 storage read 和网络打满。

## 16. 重启和进度持久化

### 16.1 第一版

- master replid、flow backlog 和 session progress 都在内存；
- master 重启生成新 replid，所有 replica full sync；
- replica 重启可以恢复本地数据，但重新连接时保守地 full sync；
- 不做周期性 16,384 offset checkpoint，因此没有 checkpoint 尖峰。

这是明确的能力边界，不影响运行中断线续传正确性。

### 16.2 后续优化

若要支持 replica 重启续传，不写全量 checkpoint，而是在普通 storage append stream
中追加小型 progress records：

- 每个 target worker 批量记录已 durable 的 source flow LSN；
- progress 只能落后于真实 apply，不能领先；
- crash 后落后的 cursor 会导致重复发送，由 partition sequence 幂等过滤；
- upstream replid/config generation 通过低频 A/B metadata 持久化；
- 只有在 master backlog 也跨重启持久化后，master 才能保留相同 replid。

## 17. 安全和资源控制

- replication handshake 必须先通过 AUTH/ACL；
- TLS 复用普通服务端配置；
- session token 随机、短期有效并绑定 replid；
- 限制 replica 数、flow 数、frame 大小、chunk 大小、full-sync 并发和带宽；
- 所有长度计算防溢出；
- frame 和 Value 分别校验 CRC/digest；
- malformed frame 关闭 session，但不得使 storage 进入部分发布状态；
- 连接取消必须传播到 snapshot read、disk IO、apply staging 和 session queues。

## 18. 可观测性

`INFO REPLICATION` 至少提供：

```text
role
master_host/master_port/master_link_status
master_replid
replica_state
fullsync_partitions_done/total
flow_applied_lsn/flow_durable_lsn
master_backlog_first_lsn/master_backlog_last_lsn
master_backlog_records/master_backlog_bytes/master_backlog_pinned_bytes
connected_replicas
replica_lag_records/bytes/ms
last_io_seconds_ago
fullsync_count/partial_sync_count/partial_sync_miss_count
```

Prometheus 还应包含 snapshot read bytes、delta bytes、apply latency、ACK latency、
output throttle、session timeout、partition recopy、generation cancellation、大 Value staged
bytes 和 replication pin 导致的不可回收 bytes。

## 19. 故障语义

| 故障 | 行为 |
|---|---|
| replica 网络短断 | backlog 内 partial sync |
| replica 落后超过 backlog | 仅该 replica full sync |
| full sync 中断 | 取消 generation，重新 full sync |
| master 重启 | replid 变化，full sync |
| replica 重启 | 第一版 full sync |
| master 写入失败 | 不发布 mutation |
| transaction rollback | 不发布 transaction fragments |
| target apply 磁盘满 | 不 ACK，断开并保留只读旧状态或进入 ERROR |
| target 收到旧 session frame | generation/epoch 拒绝 |
| Value chunk 中断 | 不发布 staging object并回收 |
| 手工提升 | 保留已 apply 数据，生成新 replid；无自动 fencing |

复制保证是异步的：master 对客户端返回成功不代表 replica 已收到。第一版不承诺
零数据损失 failover。

## 20. 实现阶段

### M0：删除旧静态复制入口

- 删除 `--replication-port`、`--replicate-to` 和 `--replica-read-only` CLI；
- 删除 source 静态 target 和 receiver 静态端口的配置入口；
- 将现有静态复制 E2E 改为配置文件加 `REPLICAOF`；
- 给旧 apply handler 增加 session cancellation/generation 检查；
- 将已知限制暴露在 INFO 和启动日志中；
- 保持现有 E2E 全绿。

### M1：角色和配置控制面

- `ReplicationManager` role state machine；
- Redis 风格配置文件解析、启动 `replicaof` 和原子 `CONFIG REWRITE`；
- `REPLICAOF`、`REPLICAOF NO ONE`、`ROLE`、`INFO REPLICATION`；
- read-only 和 expiration authority 随角色切换；
- 暂时允许断线后重新全量。

### M2：反转连接方向和原生 session

- replica 主动连接 master；
- 普通端口 capability handshake；
- master session registry；
- replica pull/initiated full sync；
- 一个 master 多个 full-sync replica；
- 删除 source 静态 target 假设。

当前已完成 M1 中启动/动态 `REPLICAOF`、`NO ONE`、generation、read-only 和 INFO 状态，
以及 M2 中 replica 主动连接、普通端口握手、master session registry 和
`1 + source_worker_count` socket ownership。`ROLE`、`CONFIG REWRITE`、动态 expiration
authority、full sync 数据面和多 replica full-sync 调度仍未完成。

### M3：安全的 target apply

- PartitionApplyLane 和 apply generation；
- DB gate 抽成共享组件；
- reset 改为持久 epoch + index detach + 后台 reclaim；
- 所有 suspend 点重新校验 generation/epoch；
- full-sync LOADING 和结束 barrier。

### M4：有界 multi-replica backlog 和 partial sync

- per-worker flow LSN；
- 自包含 replication block、LSN range 和稀疏 frame index；
- block/frame/logical-byte 有界共享 backlog；
- per-session ACK，不再按单 replica ACK 删除；
- reconnect CONTINUE；
- overflow full-sync fallback；
- 慢 replica output limit 和 timeout 隔离。

### M5：大 Value 流式化

- master backlog 使用一条命令 LSN 下的 self-contained transport frames；
- 主数据 extent reader 直接流入 active replication block，不复制完整 Value；
- snapshot/delta chunk reader；
- target staged extent writer；
- begin/chunk/commit checksum 和取消回收；
- 峰值内存资源断言。

### M6：事务一致性

- commit 后发布 replication fragments；
- TX_BEGIN/FRAGMENT/COMMIT；
- replica 原子 apply 和跨 flow ACK barrier；
- rollback 不外发；
- MSET、RENAME、MULTI/EXEC 原子可见性测试。

### M7：持久进度和同步策略（可选）

- applied/durable 双 ACK；
- progress records；
- replica restart partial sync；
- WAIT/持久 ACK 策略；
- 只有持久 backlog 完成后才考虑 master restart partial sync。

### M8：Redis adapter 和 HA（独立项目）

- RDB import；
- Redis PSYNC/FULLRESYNC 和 command stream；
- 多 Redis master 的非重叠 slot-range upstream；
- Sentinel/Cluster/Raft/fencing 评估。

## 21. 必须测试的矩阵

功能：

- 启动 replica、动态 REPLICAOF、更换 master、NO ONE；
- 一个 master 同时 2/3/N 个 replica；
- source/target worker 数不同；
- DB 0--15、TTL、DEL、FLUSHDB、全部 Value 类型；
- baseline 期间持续写入，最终逐 key/DB 校验；
- 断线 backlog hit 和 miss；
- 只让慢 replica full sync，快 replica 持续在线。

一致性和竞态：

- 旧 apply coroutine 在新 reset 后恢复；
- reset 与 key read、KEYS、SCAN、FLUSHDB、defrag、expiry 并发；
- transaction 跨 worker 且在每个阶段断线；
- snapshot key 在扫描前、扫描中、扫描后被覆盖/删除/过期；
- flow apply 乱序完成但 ACK 只能连续前进；
- master/replica worker-count 改变后恢复。

资源和故障：

- backlog record/byte/pin 三种上限；
- 单 Value 大于 backlog 上限；
- 大 Value 每个 chunk 边界断线和 checksum 错误；
- target ENOSPC、source read error、timeout、半包、坏 frame；
- full-sync 限速和多 replica 并发；
- 峰值 RSS、pinned storage bytes、写放大和 snapshot IO 断言；
- ASan/UBSan、长时间 chaos 和重复 crash/restart。

## 22. 首个可交付版本的明确边界

首个对用户开放的版本至少完成 M1--M4，并明确声明：

- 异步、只读 replica；
- 启动和动态添加均支持；
- master 支持多个 replica；
- 运行中网络断线支持 partial sync；
- 进程重启退化为 full sync；
- 不自动提升、不提供 split-brain 防护；
- transaction 原子复制若 M6 尚未完成，则必须标记为 experimental，不能称为完整 HA。

大 Value staging 仍使用当前内存拼接时也必须有硬上限；超过上限应拒绝建立该版本不
能安全承载的复制关系，而不是冒 OOM 风险。生产推荐版本应完成 M5 和 M6。
