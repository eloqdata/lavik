<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Lavik 主从复制设计

> 状态：正式功能规范，与当前实现同步
>
> 范围：Lavik 到 Lavik 的原生主从复制；storage format version 保持 1

## 1. 核心结论

Lavik 使用 replica 主动连接 master 的模型。一个 master 可以同时服务多个 replica，
每个 replica 有独立 session 和发送游标，但所有 replica 共享每个 source worker 上的一份
内存 backlog。

Replica 只有一个数据 root，不维护 `serving_root`、`loading_root` 或来源 identity：

- 执行 `REPLICAOF host port` 后立即进入 `CONNECTING/LOADING`，普通数据命令返回
  `LOADING`；
- full sync 开始 reset partition 时，旧索引被直接 detach 并后台回收；
- baseline、replacement 和 handoff 后的 tail command 都写同一个 root；
- 同步失败时丢弃本轮半成品并保持 `LOADING`，随后从新的 full sync 重试；
- 所有 flow 到达 `FULLSYNC_CUT` 且 target 完成 epoch 发布后，节点才进入 `ONLINE`；
- `REPLICAOF NO ONE` 若发生在 `LOADING`，先中止复制、清空所有半成品和尚未 reset 的
  旧数据，再以空 master 提供服务；若节点已经 `ONLINE`，则保留完整数据并提升为 master。

这个选择减少一套索引、计数和 root 路由，不允许同步期间 stale read，
也不承诺失败后回退到旧数据。

稳态复制发送提交后的确定性逻辑命令，不复制物理 block、extent 或 record offset。Full sync
由每个 session/worker 的独立内存 full-sync publish queue 承载，不从共享 backlog 重放扫描期间的
增量。每条主写仍同时进入共享 backlog，以服务已经 ONLINE 的其他 replica，并为最终 cut 建立
连续 suffix。每个 partition 内按 DB 依次扫描，逐 key 从完整 after-image 切到有序命令；最终
cut 才让本 session 取得 ONLINE backlog 的连续 flow cursor。

## 2. 角色和访问语义

状态机：

```text
MASTER
  | REPLICAOF host port
  v
CONNECTING/LOADING -- full sync + cut --> ONLINE_REPLICA
        ^                                  |
        |------- 失败后清空并重试 ----------|

ONLINE_REPLICA -- backlog CONTINUE --> ONLINE_REPLICA
ONLINE_REPLICA -- backlog miss ------> CONNECTING/LOADING

LOADING -- REPLICAOF NO ONE --> empty MASTER
ONLINE_REPLICA -- REPLICAOF NO ONE --> MASTER（保留完整数据）
```

`CONNECTING` 和 `SYNCING` 都是对外的 `LOADING` 状态。此时除连接、认证和管理命令外，
所有普通数据命令都返回：

```text
LOADING Lavik is loading the dataset from the primary
```

允许的管理命令包括 `PING`、`ECHO`、`AUTH`、`SELECT`、`CLIENT`、`REPLICAOF`、
`CONFIG`、`INFO`、`CLUSTER`、`COMMAND`、`READONLY` 和 `READWRITE`。复制来源执行的内部
命令不经过客户端 LOADING 门禁。

Replica 默认拒绝客户端写。`READONLY` 只控制 Redis Cluster 兼容的读路由；它不能绕过
LOADING，也不能让客户端读取半成品。

角色切换在 command DB gates 下完成。`REPLICAOF NO ONE` 必须先等待 flow 停止并清理存储，
再重新开放 gates，不能先宣布 master 后暴露部分数据。

## 3. 身份和三个序号域

设计只使用含义明确的身份，不泛化使用 generation：

- `role_epoch`：角色/write admission 切换序号；
- `master_replid`/history id：标识一段可以连续 `CONTINUE` 的 master backlog 历史；
- `session_id`：标识一次网络复制及 target apply attempt，用于拒绝迟到 frame；
- flow LSN：每个 source worker backlog 内的逻辑 event 序号；
- partition mutation sequence：一个 partition 内逻辑记录的版本；
- DB epoch：`FLUSHDB/FLUSHALL` 后隔离旧数据库记录。

这些域不能互相替代。Master 重启后更换 replid；新的 full sync 为 target partition 保留新的
replication epoch，并从 source 本次 `start_sequence` 建立版本域。不得把磁盘记录中来自旧
进程的 sequence 当成本次 baseline 协议版本。

Baseline frame 使用 partition 开始扫描时的 `baseline_version=S`。写路径登记的 replacement
使用真实的本次 `mutation_sequence=V`。Tail command 携带 source partition sequence；target
在 apply 上下文中使用这个 sequence，且若目标 key 已有不小于它的版本则跳过旧命令。这是
防止 `INCR`、`APPEND`、`LPUSH` 等非幂等 mutation 被 replacement 覆盖后再次执行的关键。

## 4. 连接、TLS 和 CLIENT 管理

复制使用普通 Redis listener 上的 Lavik 原生握手：control connection 建立 session，随后
每个 source worker 建立一个 data flow。Source 和 target worker 数可以不同，partition 仍按
逻辑 slot/owner 路由到 target worker。

当 `tls-replication yes` 时，control connection 和每一条 data flow 都必须使用 TLS，并使用
配置的 CA、证书验证和 master authentication；不得只给 control connection 加 TLS。

Master 把复制连接登记为 `CLIENT TYPE REPLICA`。支持：

```text
CLIENT LIST TYPE REPLICA
CLIENT KILL TYPE REPLICA
CLIENT KILL ID <id>
CLIENT KILL ADDR <ip:port>
```

`CLIENT KILL` 只断开 socket，不解除 replica 的 upstream 配置。Replica 会重连，backlog 仍
覆盖 cursor 时走 CONTINUE，否则重新 full sync。

## 5. Master backlog 和 publisher 反压

每个 source worker 有独立 flow LSN、publisher queue 和共享内存 backlog。一个 event 只编码、
保存一份，多个 replica 只各自保存 ACK cursor。Backlog frame 自包含逻辑 payload，不引用可能
被覆盖或 defrag 的主数据物理位置；大命令可以在一个 LSN 下拆为多个 frame。Backlog 不占用
storage block、不使用 registered storage buffer、不执行磁盘 IO，也不参与恢复；master 重启更换
replid，所有旧 cursor 自然失效。

写提交前取得 publisher admission。Queue 达到每 worker 高水位时，前台写等待空间，而不是
让主写成功后静默丢 publication。单个 event 大于普通 queue 高水位时允许独占 heap staging，
因此大 key 不会仅因固定 queue slot 放不下而产生 history gap。

ONLINE session 注册“第一个尚未 ACK 的 LSN”。任何 live cursor 需要的 chunk 都不可淘汰。
Backlog 到达高水位时 publisher 保留当前 admission 并挂起；内存 publisher queue 随后达到上限，
把反压传递到提交前的客户端写。它使用高低水位迟滞：达到上限后，必须由 ACK 推进到 75% 以下
才重新放行，避免每收到一个 ACK 就放一条写、导致 replica 永远贴着 backlog 上限而不能靠近 tail。

这里仍存在不可消除的吞吐约束：

- 已连接且持续消费的慢 replica 会通过反压把 master 的有效写入带宽压到它能够推进的范围；
- 完全无进展的连接由 session stall/网络超时关闭，否则“保证不覆盖”必然可以永久阻塞 master；
- 连接一旦断开，立即移除该 replica 的 pin 并唤醒 writer。断线期间内存 backlog 按容量滚动，
  不为失联 replica 阻塞 master；此时退出 ACK 反压的低水位模式，只按新 chunk 所需逐块淘汰，
  不能主动把 reconnect window 从上限裁到 75%。重连 cursor 仍低于 floor 时才重新 full sync。

`repl-backlog-size` 是唯一 backlog 容量配置，默认 1 GiB，为所有 worker 的全局总额并按
worker 分配。它按 8 MiB memory chunk 懒分配，可以运行时动态调整：

```text
CONFIG GET repl-backlog-size
CONFIG SET repl-backlog-size <bytes|kb|mb|gb|tb>
```

扩容先更新 worker-local 上限并唤醒被反压的 publisher，不能排在 publisher 持有的 append mutex
之后。缩容按完整 event/chunk 推进 floor；若 live cursor 仍需要超出新上限的历史，新值先作为
目标配额，绝不强删，后续写等待 ACK 使占用收敛。单个 event 大于配置上限时允许独占一组临时
chunk，后续 publication 等它 ACK/可回收后再继续。容量估算：

```text
backlog_bytes >= peak_replication_bytes_per_second * desired_disconnect_window
```

任意 backlog allocation/encode failure 都使当前 history 失效：取消所有受影响 ONLINE 和
full-sync session，旧 replid/cursor 永远不能继续 CONTINUE。新 full sync 只有建立新的无空洞
capture suffix 后才能开始。

## 6. Registered buffer 分区

每个 worker 的 registered memory 在启动时一次注册，运行期只借还 token。它只服务 storage：

1. storage write pool；
2. 剩余预算用于 read pool。

配置项：

```text
registered-buffer-mb-per-worker
storage-write-buffers-per-worker
replication-publish-queue-mb-per-worker
```

storage write buffer 和 read buffer 从注册预算中按需借用并复用。Replication backlog 是普通
进程内存，不再消耗注册预算。若 `RLIMIT_MEMLOCK`、设备注册限制或配置预算不足，启动诊断必须
给出所需 bytes、当前 limit 和调整建议；不能按某一台开发机写死。

SPDK backend 只适配 Lavik 自己的 buffer 注册和 token 使用，不修改第三方 SPDK 源码。

## 7. Full sync：DB-by-DB 扫描和逐 key coverage

每个 replica session、每个 source worker 只有一个 full-sync publish queue。这个 queue 同时
承载当前扫描 DB 中已经有 base 的 key 命令，以及该 worker 上已经完成 DB/partition 的后续命令；
不为 16384 个 partition 预分配独立 payload buffer。多个 replica session 各有独立 ACK/credit，
不可变 canonical command payload 可以共享所有权。

每个正在扫描的 `(partition, DB)` 有一份独立 capture state；它不放进普通 HashMap Entry，也不
修改业务 Entry 布局。同一 partition 内 DB 依次经历：

```text
UNSTARTED -> SCANNING -> TAILING
```

只有当前 `SCANNING` DB 保留 `map<key, state>`，所以 coverage key 不需要包含 DB id；
replacement/transaction 元数据的逻辑身份仍是 `(db_id,key)`。状态只保存 key identity、
mutation sequence 和 ACK 状态，不在内存保存 Value。Value 在发送时从当前内存/磁盘 record
materialize。Snapshot/replacement batch 同时受 key count 和 2 MiB 总字节预算限制；单条超过
预算的 external Value 独占当前 flow，锁内固定 immutable extent manifest 和 block pin，随后按
2 MiB 网络 chunk 读取，完整 Value 不进入 batch vector。固定 8 MiB storage extent 的读取和 CRC
校验也始终只保留当前一个 extent scratch。

Source 启动 partition：

1. 在 owner worker 上登记 full-sync subscriber；
2. 读取当前 `start_sequence=S` 和 DB epoch vector；
3. target 为 partition 持久保留新的 replication epoch；
4. target detach 该 partition 的旧索引，计数归零，并在唯一 root 中创建空 map；
5. 对 DB 0 到 DB 15 依次把 phase 切为 `SCANNING`，scanner 通过 owner 串行的
   `Scan(cursor)` 遍历对应 live HashMap；空 DB 也必须完成 phase 切换。

`Scan`、rehash 和 compact 都由 partition owner 串行执行。修改可以发生在两次 `Scan()` 调用
之间，但不能与一次 bucket-chain 遍历并发，也不能在 scan callback 中修改 map。该契约保证
fence 时存在且之后未修改的 key 至少被枚举一次。

逐 key 状态至少是：

```text
ABSENT -> BASELINE_INFLIGHT -> TAILING
   \---- latest replacement ----/
```

scanner 和写提交都在同一 key lock 排序边界内检查 map：

- scanner 看到 map 不存在，在锁内捕获当时的 immutable record/value（大 external Value 捕获
  extent pin），以 `baseline_version=S` 登记 `BASELINE_INFLIGHT` 后才释放 key lock；frame ACK
  后该 key 进入 `TAILING`；baseline 不会在发送时重读“当前值”；
- 写先看到 map 不存在，说明 target 没有可靠 base；commit 后登记 metadata-only 最新
  after-image/tombstone，scanner 以后看到该 key 直接跳过；
- map 已存在时，完整 base 已经可靠占有发送顺序，后续普通写将 canonical command 放进同一个
  session/worker FIFO；base frame 总在对应命令之前发送；
- 只有 replacement sender 根据 identity 从存储读取最新 Value，携带实际 mutation sequence `V`；
- target 只应用版本更高的 record/command，旧 ACK 不能删除更新的 replacement。

因此 scanner 不会把一个已经在它之前提交的新值误标为 S：写若先取得 key lock，就一定先登记
replacement，scanner 会跳过；scanner 若先取得 key lock，后续非幂等 mutation 一定排在它登记
的 base 后面。没有异步 consumer。

DB phase 的写规则：

- `UNSTARTED`：不捕获、不发送；稍后的 live scan 会读到提交后状态；
- `SCANNING`：未覆盖 key 发 after-image，已覆盖 key 发 command；
- `TAILING`：所有普通写直接发 command，新 key 也可由命令创建；已提交事务 participant 的
  after-image 作为 record item 进入同一个 FIFO，不能被后续普通 command 越过。

Full-sync reservation 是逻辑空间/内存 admission，不预先把所有 Value 复制进 RAM。无法取得
所需 reservation 时拒绝开始同步；每个 DB handoff 后立即释放 scan batch、record pin、临时
block 和 coverage map 的实际分配。

## 8. DB handoff 和 full-sync publish queue

当前 DB baseline 完成后执行 owner-local、无挂起 handoff：

```text
SCANNING: baseline/replacement + covered-key commands
    |
    | replacement empty，所有已发送 frame 已 ACK
    v
TAILING: session publish queue commands
```

写路径先取得 queue credit，再进入 key/transaction lock 和提交；credit 不足时 coroutine
挂起，不阻塞 worker 线程，也不持有 key lock。Queue item 只有在 target ACK 后出队并释放
credit。单个大于 queue limit 的 command 可以独占一个 heap staging item；它会阻止其他 item，
直到 ACK 或 session 取消。

首次出现的 pending replacement 也持有 `(metadata + 两份 key identity)` 的 queue credit；同 key
覆盖复用这份 credit，replacement ACK、partition 结束或 session 取消时释放。因此持续创建并删除
不同 key 会触发与 command FIFO 相同的反压，不能靠稳定的 live-key 数绕过容量上限。
主动过期在删除前只做非阻塞的 full-sync credit 预留：空间不足或已有前台 admission 排队时，保留
过期候选并结束本轮，稍后重试。它不会持有 key/store lock 等待慢 replica，也不能绕过 queue 上限。

单 key 命令只向实际 owner worker 申请 credit，并把 `(partition,DB)` 传给 admission：尚未为该
session 启动的 partition/DB 不预留 full-sync queue 空间，也不会被另一个正在扫描或已经 tailing
的 partition 反压。跨 key/事务在执行前一次性、按 worker 顺序为全部 participant 保守预留，
避免持有部分 key/事务锁后等待 credit。

UNSTARTED 的精确 admission 虽不占 queue bytes，仍登记一个短生命周期 phase guard。DB 从
`UNSTARTED` 切到 `SCANNING` 前必须等这些已放行写执行完：若写先登记 guard，它按 UNSTARTED
语义提交并由随后扫描读取；若 phase 先切换，写必须先取得该 session 的 queue credit。这样既
不会让未开始 partition 被慢 replica 误反压，也不会在 phase 切换竞态中产生未预留的入队项。

DB handoff 在 owner worker 上检查该 DB 没有 pending replacement，然后原子切为 `TAILING` 并
释放 coverage map。若检查前有写提交，它仍在 replacement 集合中，handoff 返回重试；检查后
的写直接进入 FIFO，所以不存在空洞。16 个 DB 都完成后，partition 发送完成标记；target 仍在
LOADING，允许 record 和 command 按版本交错写入当前唯一 root；该 root 在 ONLINE 前不可读。

未开始的 DB/partition 不进入 full-sync queue。Full sync wire frame 使用 session-local、从 1
连续增长的 frame sequence ACK；command item id 只标识该 worker FIFO 的重发/ACK 顺序，不是
ONLINE flow LSN。增量 command 与 ONLINE backlog 使用相同的批量发送窗口：每批最多 2 MiB、
128 frames，以一次 `WriteAllV` 发出。编码器直接从 immutable command arguments 按 fragment
读取，不创建完整 encoded command 副本。一个 command 的中间 fragment 只推进接收端 sequence，
不产生 stop-and-wait ACK；完整 command apply 后由最后一个 fragment ACK，source 此时才按 FIFO
释放 item 和对应 credit。每次 `WriteAllV` 成功都更新 session progress，所以持续传输的大命令
不会因最终 ACK 尚未到达而被 stall monitor 误杀。断线时未完成 session 整体取消，因此不会把
只有部分 fragment 的命令带入下一次 full sync。

Prometheus 按 worker 暴露 `lavik_fullsync_publish_queue_bytes`、
`lavik_fullsync_publish_queue_admitted_bytes`、
`lavik_fullsync_publish_queue_capacity_bytes`、`lavik_fullsync_sessions` 和
`lavik_fullsync_publish_queue_backpressure_waits_total`，与普通 ONLINE backlog 指标分开，避免把
full-sync credit 等待误判为 backlog 没有反压。

## 9. 最终 cut

所有 partition 的全部 DB handoff 后：

1. 先关闭 transaction admission 并 drain 已进入事务，再关闭普通
   command/DB control gates；gate 关闭期间新命令以 coroutine 等待，不向客户端返回
   `TRYAGAIN`；这个顺序保证等待 DB gate 的命令不会占着 cut 所需的事务 admission；
2. 把已提交事务的 participant after-image 登记完；
3. drain 所有 replacement 和 full-sync publish queue，等待 target ACK；
4. 每个 flow 向共享 backlog publisher 插入 fence，得到 `stable_next_lsn`；
5. 在重新开放写 admission 前，为本 session pin 每个 flow 的 `stable_next_lsn`；
6. 所有 flow 停止该 session 的 full-sync capture 并经过 owner-local barrier；
7. barrier 完成后立即重新开放 master admission，不等待任何网络 ACK；此后的写只进入已 pin 的
   ONLINE backlog，不再填充已排空的 full-sync queue；
8. 发送 `FULLSYNC_CUT(stable_next_lsn)`；
9. target 等所有 flow cut 到齐，drain storage writes，持久发布 DB epochs，删除临时 sync state；
10. control connection 收到所有 flow ready 后发布 `ONLINE`，随后从 stable cursor 消费 backlog。

因此慢 replica 不会在最终网络追平阶段长时间阻塞 master 事务。Fence 后的新写位于稳定 cursor
之后，等 cut 完成后通过普通 ONLINE backlog 发送。Full sync 不保留从扫描开始的 backlog
cursor；共享 backlog 中 stable cursor 以前的重复命令不会再发给这个 session。

## 10. 事务

ONLINE 稳态事务使用事务 envelope。共同 participant 上必须有一致的 source commit order；
所有 participant frame 到齐后 target 原子执行命令，所有 participant cursor 一起前进，任意
后续 mutation 不得越过未完成事务。

Full sync 隐藏期间不发送 pre-cut transaction envelope。事务只在 commit decision 为
`Publish` 后，才把每个 participant 的最终 after-image/tombstone 登记到对应 session；abort
participant 不发送。对于已经有 baseline 或进入 `TAILING` 的 key，这个 after-image 是与普通
command 同 FIFO 的 record item；对于尚无 base 的 key，它仍是 coalesced replacement。Target
可以逐 participant apply，因为 LOADING root 对客户端不可见。

事务身份仍用于 final cut 完整性：cut 必须等待 cut 前所有 committed participant 全部发送并
ACK，不能让一笔事务一半进入 full-sync queue、一半落到 stable cursor 以后。Gates 重新开放后
的新事务只通过 ONLINE backlog envelope 原子 apply。

如果未来要在 full sync 期间提供读服务，就必须重新引入真正的双 root/MVCC 和 cut 前事务
原子可见性，不能复用当前单 root 方案。

## 11. FLUSHDB、FLUSHALL、TTL

DB epoch advance 是跨 key/flow control barrier，不能依赖逐 key hook。当前安全规则是：

- full-sync session 从开始就在 worker-local registry 中登记；
- `FLUSHDB/FLUSHALL` commit 同步标记所有活动 full sync invalid；
- 当前 attempt 立即取消，target 清空半成品并从新的 DB epoch vector 重新 full sync；
- 最终 cut 同时 drain transaction、普通 command 和 control barrier，不能切开 FLUSHALL。

ONLINE 的 `FLUSHDB/FLUSHALL` 通过所有 source flow 的同一 barrier id 到达；target 收齐后原子
推进 DB epoch 并一起发布 cursor。

Replica 不拥有主动过期权。Master 发布绝对过期时间和确定性删除；节点成为 replica 时暂停
本地主动 expiry，`REPLICAOF NO ONE` 完成后恢复。绝对 TTL 的跨机可见时刻仍依赖时钟同步，
这是外部部署契约。

## 12. 大 Value

Baseline/replacement 的大 Value 使用 `VALUE_BEGIN`、多个 `VALUE_CHUNK` 和 `VALUE_COMMIT`，
chunk 默认 2 MiB。Target 允许在内存暂存当前单个 Value，commit 前不发布 record；断线或 session
取消时释放 staging。

ONLINE backlog 的一个大命令共享一个 LSN，可以跨多个 8 MiB block/frame。Publisher queue
放不下普通 slot 时使用独占 heap string/payload source；不能因为 Value 大于普通 queue limit
就使复制失败。单 event 仍受协议和可用 backlog 容量约束。

## 13. 多 replica、续传和故障

每个 replica session 独立选择：

- replid 相同且所有 flow cursor 都在 backlog floor 以后：CONTINUE；
- 任一 flow cursor 丢失、history id 不同或 target 没有完整 ONLINE root：full sync；
- 不允许一部分 flow CONTINUE、另一部分 flow full sync。

主要故障语义：

| 故障 | 行为 |
|---|---|
| 网络短断且 cursor retained | CONTINUE |
| 已连接 replica 消费慢 | pin 未 ACK cursor；高水位反压，降到低水位再恢复写入 |
| replica 断线 | 立即释放 pin；有限内存窗口继续滚动，重连 cursor miss 时 full sync |
| full sync flow 10 分钟无任何进展 | 取消 attempt，清空半成品并重试 |
| full sync 中 FLUSHDB/FLUSHALL | 取消 attempt，使用新 epoch 重试 |
| publisher encode/allocation failure | history 失效，取消所有相关 session |
| target storage IO failure | 保持 LOADING/ERROR，不发布 ONLINE |
| LOADING 中 REPLICAOF NO ONE | 清空全部数据，成为空 master |
| ONLINE 中 REPLICAOF NO ONE | 保留完整数据并成为 master |
| 进程重启 | 更换 replid；允许退化为 full sync |

Publication queue/backlog 达到配置水位属于可恢复反压，不是 history failure。只有 event 已获
admission 后仍无法编码或从 process maxmemory 分配 chunk，才使 history 失效。

同一 worker 的 admission 使用 FIFO ticket。尤其当单条命令大于 publish queue 水位时，它在
到达队首后会阻止后来的小命令继续插队，等待既有队列排空后作为唯一 item 获得 credit；ACK
释放该 item 后再唤醒后继写入。因此持续小写流量不能让大 key 永久饥饿。

## 14. 配置和可观测性

配置文件和 CLI 覆盖支持：

```text
replicaof <host> <port>
replica-read-only yes|no
repl-backlog-size 1gb
tls-replication yes|no
registered-buffer-mb-per-worker <MiB>
storage-write-buffers-per-worker <count>
replication-publish-queue-mb-per-worker <MiB>
```

运行时至少报告 role、upstream、master link status、session id、connected flows、每 flow phase、
cursor/floor/tail、ACK pin 数、publisher queue bytes、backpressure 状态/次数、backlog bytes/chunks、full-sync partition、snapshot
和 replacement 进度、registered pool wait、history invalidation reason。

`FULL_SYNC` 不是进程角色；状态对外只使用 `LOADING/SYNCING` 和 `ONLINE`，full sync 是 session
内部 phase。

## 15. 必须覆盖的测试

- 空库和多 DB baseline；
- scan 中 SET/DEL/TTL/非幂等 INCR、APPEND、LPUSH；
- mutation 发生在 key base 前、base inflight、DB handoff 临界点和 handoff 后；
- 同名 key 位于多个 DB；已完成 DB/partition 持续写、未开始 DB/partition 持续写；
- 跨 partition 事务与普通 mutation 交错；
- full sync 中 FLUSHDB、FLUSHALL；
- 同步期间所有数据命令返回 LOADING；
- LOADING 中 NO ONE 清空半成品，ONLINE 中 NO ONE 保留完整数据；
- upstream A 切到 B，不暴露 A 的旧数据；
- 大于普通 publisher queue 的 Value baseline、replacement 和 ONLINE tail；
- publisher 反压等待后恢复且 LSN 无空洞；
- backlog 动态扩缩、cursor hit/miss；
- CLIENT KILL ID/ADDR/TYPE 后 CONTINUE 或 full sync；
- control 和所有 data flow 的 TLS；
- 不同 source/target worker 数；
- backlog 不消耗 storage registered buffer，storage pool 耗尽不影响已分配的复制历史；
- reset、大 Value 分片重组、DB handoff、publish queue、cut、epoch persist 各故障点断线；
- master 满载持续写时 full sync 最终追平，或在 target 长期吞吐不足时明确 backpressure/
  明确反压，不能静默少写。

上述正确性功能均是正式功能，不存在 experimental 模式或降级标签。
