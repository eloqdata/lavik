# 大 Key 重新设计注意事项

## 当前状态

大 Key 拆分实现已经移除。当前所有集合类型都只有单记录表示：

- List 使用一个完整的 `KLL1` 编码；
- Hash 和 Set 使用一个完整的 Hash 编码；
- Sorted Set、Geo 和 Stream 使用各自的完整编码；
- 每条命令完整读取、完整解码，修改后完整编码并重写；
- 不存在分段 marker、匿名 collection object、目录树、radix 树、owner
  side table 或分段对象 defrag；
- TTL、事务、恢复和复制只处理普通的顶层 value record。

这个阶段不承诺大 Key 的内存、读放大或写放大上界。存储格式版本继续保持
`1`；开发期布局变化后直接清空旧数据文件，不做原地兼容或升级。

本文不是现有实现说明，而是下一次设计大 Key 时必须重新满足的约束清单。

## 先确定边界，再选择树形

新设计开始前必须明确以下目标，不能从某个容器结构反推需求：

- 哪些类型需要拆分，以及开始拆分和退回单记录的阈值；
- 点查、顺序遍历、随机采样、范围删除和跨 Key 运算各自允许的 IO 上界；
- 单条命令允许使用的内存、临时磁盘空间和回复缓冲上界；
- 每次写允许重写多少数据，如何限制写放大和节点数量；
- 是否允许一个逻辑 Key 的物理对象跨 worker、设备和复制分区；
- 事务、TTL、恢复、复制、defrag、FLUSHDB 各自看到的原子发布单位。

拆分表示应当是类型实现之上的统一存储能力。List、Hash、Set、ZSet、
Stream 不能各自复制一套负数随机采样、游标、退休、恢复和复制逻辑。

## 原子发布与对象生命周期

如果采用不可变 COW 子对象，顶层 root 必须是唯一可见性提交点：

1. 新子对象先写入并达到要求的持久性边界；
2. 新 root 最后发布；
3. 只有新 root 持久后，旧 root 独占的对象才可退休；
4. 失败或事务回滚必须退休所有未发布的新对象；
5. 共享子树必须有明确的所有权模型，不能靠“扫描时大概还能找到”判断存活。

特别要覆盖以下失败窗口：

- 子对象写成功，root append 失败；
- root 已进入内存索引，但对应 block header 尚未持久；
- 事务把退休记录加入 journal 后，后续 append 失败；
- defrag 写出新副本，但父链接或 root 更新失败；
- FLUSHDB、复制 reset 或普通写在对象 IO 解锁期间替换了整个索引代际。

每个写入口必须使用相同的 publication/rollback primitive。不能在 List、Hash、
compact API 中分别手写退休回滚，否则新类型会再次漏接。

## TTL、遮蔽和恢复

过期值仍可能遮蔽磁盘上的旧版本。不能先回收分段子对象，再等待顶层 tombstone
落盘；时钟回拨或崩溃恢复会让旧 root 再次成为 winner，而它的孩子已经被复用。

安全顺序是：

1. 重新校验 key、mutation sequence、DB epoch 和 index generation；
2. 发布普通、可恢复的持久 tombstone；
3. tombstone 的持久性 fence 完成后再退休旧图；
4. 恢复时先完成事务裁决和顶层版本选择，再遍历 winning root；
5. 未发布、已放弃或非 winning root 的匿名对象不能被计为 live。

恢复必须验证完整图不变量：引用的 allocation epoch、record offset、长度、
checksum、节点类型、层级、聚合计数、排序边界、无环和共享规则。格式不支持时要
明确报“不支持的版本”，不能让 block header 通过后把每条数据都报告为损坏。

## 事务

顶层记录使用 txid 和 commit record 时，匿名子对象的处理必须单独定义：

- 子对象不能因为没有 txid 就在 abort 后泄漏；
- 子对象也不能错误地进入“带 txid 出生 block”的 commit-GC 集合；
- `TxShardWrites` 要区分“提交前必须持久的 fence”和“提交后才能退休的对象”；
- 任一 append 失败都要把本次命令加入的 retirement/journal 回滚到入口位置；
- EXEC 的命令级错误语义和事务崩溃原子性要分别测试。

跨 Key 命令还要保证锁顺序稳定，并避免在持有全局 store mutex 时做整棵树 IO。

## 复制

物理引用只能在本机当前分配代际内使用，不能直接复制到另一台机器。复制协议应
传输逻辑编码或稳定的逻辑操作，并满足：

- 所有无大小上限的类型都进入同一套 begin/chunk/commit 分帧；
- 类型白名单不能遗漏 ZSet、Stream 或未来新增类型；
- snapshot 和 delta 使用相同的可移植表示；
- replica apply 不能接受只含 root marker、却没有本地 side state 的记录；
- reset/FLUSHDB 与正在进行的 apply 必须用 replication epoch 和 DB gate 隔离；
- 多级复制要么明确支持并转发 delta，要么配置时拒绝。

大 Key 拆分与网络回复流式化是两个问题。即使存储已经分段，HGETALL、SMEMBERS、
ZRANDMEMBER 等仍可能把完整回复物化到内存；回复层必须独立提供有界批次和背压。
EXEC 的嵌套回复也必须支持分块；不能把顶层流式化的批次大小误用成事务内命令的
总量上限。事务内随机读取可在命令的串行位置保存不可变视图，释放锁后再按批次
生成回复，避免慢客户端长期钉住事务锁。

## 分裂、合并与写放大

分裂规则必须有迟滞，并对头尾操作对称。曾经出现的失败模式是 LPUSH 每次重写
约 512 KiB 后又裂出一个单元素段，导致一百万次小写产生约一百万个段和数百 GB
写放大，而 RPUSH 不受影响。

重新设计时至少要定义并测试：

- split 上限、merge 下限和两者之间的迟滞区；
- 超大单元素/字段/成员的独立表示；
- 连续 LPUSH、RPUSH、头删、尾删和中间插入的对称性；
- 空节点消除、根收缩和树高上限；
- 分裂后最小填充率，避免长期 singleton 节点；
- 每次命令重写字节数、产生对象数和读取节点数的可观测指标。

不要只测最终 Redis 语义；还要断言物理写入量、节点数量和树高。

## 游标、随机采样和遍历

SCAN 类游标不能使用数组位置。并发删除会使位置左移并漏元素，极大 COUNT 还可能
产生不前进的游标。应统一使用稳定 digest 前缀或另一种可验证的 continuation
token，并对 Hash、Set、ZSet 复用同一实现。

随机采样也应统一：

- `INT64_MIN` 必须在求绝对值前拒绝；
- 负 count 的有放回采样要分批生成并直接写回复，不能先 reserve count；
- 小 k 可用 Floyd，以 O(k) 内存换取 O(k) 次定位；
- 大 k 应顺序选择采样，以 O(1) 工作内存扫描一次；
- `k >= n` 时直接顺序遍历，不要先生成 n 个 rank；
- 对磁盘集合，算法分派依据应是“随机访问是否便宜”，不能只看表示名称；
- 有序 rank 应按叶节点分组，使每个叶子最多加载一次。

浮点解析、随机采样、glob 和游标必须做成公共模块，避免新类型再次复制已经修过的
边界错误。

## 锁与调度

禁止在以下操作期间长期持有 key lock、`store_state_mutex_` 或 DB gate：

- 等待客户端解除阻塞；
- 向慢客户端发送巨大回复；
- 遍历整棵树做 reset、FLUSHDB 或退休；
- 跨 worker 逐对象往返；
- 等待可能依赖同一 mutex 才能推进的空间回收。

会阻塞的命令应由命令表的性质标志统一驱动，而不是在分派处枚举 BLPOP、XREAD
等具体命令。只读路径在释放 mutex 做 IO 后，必须检查 DB epoch/index generation；
副本 reset 后应返回 nil/空结果，而不是把旧 location 的读失败暴露为 ERR。

阻塞等待也应保持类型无关：每个 shard 只维护自己拥有 key 的等待 lane，修改提交后
只发布一次 touched-key 事件；命令自己的就绪条件在重新取得 DB gate 和 key lock 后
复查。List 的“非空”、XREAD 的“存在更大 ID”和 XREADGROUP 的“组内有可投递条目”
不应复制三套注册、超时和取消代码。非消费型读取可广播唤醒，消费型读取必须按
`(key, group)` 保持 FIFO。跨 worker 只传递唤醒事件，waiter 状态由发起命令的 worker
独占，不能为此引入进程级 waiter map 或 mutex。

## Defrag 与 side table

如果未来重新引入 runtime side table，必须列出每个生命周期出口并有统一清理：

- 普通覆盖和类型转换；
- TTL 过期及过期后新建；
- DEL、FLUSHDB、复制 reset；
- 事务 rollback/discard；
- 恢复 winner 替换；
- defrag relocation 成功、失败和 stale-source abort。

defrag 更新子对象时必须通过当前 winning root 重新验证父边，不能信任可能过期的
反向 owner/parent 表。新记录写成但父链接提交失败时，也必须立即退休新记录及其
extent，不能只处理另外两个错误出口。

## 必须具备的测试

实现重新进入主分支前，至少需要：

- 对真实 Redis 的命令级差分测试，覆盖语法、错误、RESP 形状和浮点格式；
- 缩小节点阈值后的模型测试，强制产生 4--5 层树并运行多 seed 随机操作；
- 每个 publication 阶段的 crash-point 矩阵；
- append、extent、父链接、tx commit 和 retirement 的故障注入；
- TTL + 时钟回拨 + shielding + block reuse 恢复测试；
- 复制 snapshot/delta 超过 RPC chunk 阈值的全类型测试；
- defrag 与 4 worker 并发读写、FLUSHDB、replica reset 竞态；
- ASan/UBSan 运行，并为大模型测试设置现实的独立超时；
- 物理写放大、峰值 RSS、节点数量和回复流式化的资源断言。

在这些基础设施齐全之前，保持当前单记录实现比再次加入一套不完整的类型专用树更
安全，也更容易验证 Redis 兼容性。
