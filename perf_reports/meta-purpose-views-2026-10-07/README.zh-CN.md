<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->

# 按用途捕获 Meta 只读视图

[English](README.md)

本报告用于 [#256](https://github.com/eloqdata/lavik/issues/256) 的最终验收，
覆盖 [#144](https://github.com/eloqdata/lavik/issues/144) 的消费者迁移序列。
对照对象是迁移前源码与完成迁移后的源码；本批删除已无普通调用方的完整
aggregate API，本身不代表普通读路径又获得了一次独立优化。

## 版本和环境

- 基线：`25e159418603a95c1b2cca68173b329067e3ef48`，即批次 1 的基线源码。
  [#257](https://github.com/eloqdata/lavik/pull/257) 已说明原临时测量包被清理。
  本次在同机重新编译该固定版本并成对测量，不把重建数据冒充历史原始样本。
- 候选：`920f879b05636d066ef0485f1ee1121813ef79ac` 加本次完整接口清理。
  原始元数据保留生产源码 diff、可执行文件 SHA-256、插桩源码哈希与编译命令。
  没有回退两个 revision 之间的其他提交；组件插桩直接证明复制量变化，但进程延迟
  比较的是完整 Meta revision，不能把每一项时间差都单独归因于视图迁移。
- 同一台 x86-64 VM：Intel Xeon Platinum 8573C，8 个逻辑 CPU、4 个物理核、
  单 NUMA，两侧 CPU affinity 都是 `0-7`。未锁定频率，不宣称独占宿主机或
  生产集群容量。测量期间没有其他本任务构建或功能测试并行运行。
- GCC 13.3.0、C++23、Release `-O3 -DNDEBUG`、native CPU 优化、LTO，静态
  C++ runtime。Meta 使用 glibc 2.39 支撑的系统 C++ allocator，**不是 mimalloc**。
  内嵌 Raft bridge 的实际 Go toolchain 为 1.26.8。
- 两侧 Bycorf 均为 `62509c93d40c2480f5046b71454db6cf95801b04`，mimalloc 均为
  `acf2fdd329f9dc2a7ffe3f12a133fe7175e39378`。进程级对照固定使用同一份候选
  Debug Data/CLI 二进制，Data 保留其正常 mimalloc allocator。因此这是 Meta
  的受控对照，不是两套全 Release 服务栈的比较。
- 构建、缓存、临时文件、socket、WAL、snapshot 和 Data 文件均在
  `/mnt/local_nvme/issue256`；短路径 `/mnt/local_nvme/i256d` 指向其中的 data 目录。
- 每个场景三轮独立交替成对运行，顺序为 **AB、BA、AB**。保留各轮结果，不挑
  最佳轮；汇总是各轮统计量的中位数，不是混合全部样本后计算的分位数。

## 测量边界

### 捕获与析构

组件程序向两侧状态机安装相同且通过校验的序列化 fixture。每种读取先等时预热
25 ms，再预热三次，随后测量 100 对捕获/析构；持久化 snapshot 序列化控制组
测量十对。等时预热用于减轻完整复制耗时更长、准备时间不同造成的冷启动偏差。
捕获计时包含把返回值转移到 benchmark 的 optional owner，析构阶段 reset 该 owner；
因此包含内嵌返回值/移动成本，与单独插桩的状态锁区间不同。

候选覆盖 proposal、observation facts、创建/membership/failover discovery、
自动 failover detection、Data publication、Admin Group、operation status、
committed cursor 和两个按域导出入口。旧版组件行统一使用 `StoresSnapshot()`
作为**完整 aggregate 复制参考**，不是每个历史端点的实际实现。例如旧版 operation
status 已有定点读取，不能拿表中该行推导这个端点的历史提速。
discovery fixture 只有 maintenance operation，没有匹配的活跃创建/membership/
failover 工作项；空 worklist 测的是筛选和无关载荷排除，零分配不代表活跃规划零成本。

全局 C++ new/delete 包装保留 `malloc/free`，分别记录申请字节、分配次数、释放
次数和 allocator-usable 释放字节。申请大小与 usable 大小不是同一指标，不计直接
C/Go 分配。这些字节量只表示动态分配，不涵盖所有读写/复制流量，例如 aggregate
内嵌 slot array。计数器为线程局部，fixture 准备与输出不在计时范围。状态锁仅在
任务生成的翻译单元中插桩，没有给生产代码增加锁或测量 hook。hold/wait 为均值，延迟
列为分位数。

矩阵分别扩大以下维度：

- 真实当前记录：8/2、128/32、4,096/512 个 Data 节点/Group，包含活跃 transition
  facts；当前 directives 从 3 增至 96。
- 固定所选记录：1/16/128 个无关 256 KiB operation，1/128 个归档 64 KiB result，
  每个相关 Policy 的 32 个版本，以及 128 个未引用 manifest，每个含 128 个 entry。
- 所选完整 operation 正文：1 KiB、64 KiB、256 KiB。持有该完整记录的路径随其
  正文增长属于明确允许的记录级成本。
- 静态 audit 精确保留 0、1,024、65,536 条，并另设组合 fixture。

每轮针对十个普通捕获入口自动检查无关增长前后的申请字节和分配次数。按域导出
不属于这一不变量：archive 导出必须保留其域，audit 导出保留既有共享 page handle。
全局筛选、事实扫描和当前 topology 的必要成本仍然存在，不宣称全部读取为 O(1)。

### 读写争用

独立的两秒场景连续 apply 成功的幂等 `RegisterNode`，同时按 100 Hz 发起 publication
读取。两侧 offered 读取节奏相同，实际样本数保留在原始 CSV 中。每次 apply 都解码
并检查 accepted；吞吐包含结果检查和采样管理，不是 Raft 吞吐或生产极限。
争用场景的 publication 计时包含视图的捕获和析构，不同于独立的 capture-only 行。
写入会增加并轮转 audit，因此这是从指定初始状态出发的读写负载，不是固定窗口实验。

### 真实组合进程

每次新建三 Meta、两 Data 的 managed-Single 集群，并确认 owner 实际可读写。
一个顺序流按 **20 Hz 目标节奏、最多一个在途请求**重复提交同一 maintenance operation，避免扩大
live operation 集合；
Admin status、Sentinel MASTER、Redis SET 各按 100 Hz offered 节奏运行。连续
50 次 lease Policy revision 变更驱动向两个 Data session 的真实发布，每次发布
确认后等待 600 ms，给 1,100/1,200 ms 策略的因果续租留出时间。更快的策略变更
属于不同的租约饥饿压力负载。每轮 publication p99 是 50 个样本中的最大值，
属于本机观察到的尾延迟，不是高置信度的生产 p99 估计。

每次发布要求两个 Data 的 `full_states_applied_total` 各精确增加一；同时检查 session
持续 connected、reconnect/protocol-error 计数不变、Meta term/leadership 不变。
发布延迟是带 HTTP metrics 轮询检测开销的**可观察完成上界**，不是纯 wire 延迟。
保留从提交开始和从 proposal 回复开始计算的两种样本。

填充 setup 数据后，先在十秒内等待 Sentinel 发布预期 owner，并记录耗时；
随后有界等待 owner 可写，只对 `MASTERDOWN`/`LOADING` 重试，
最长十秒，并记录重试次数。计时阶段的 Redis SET 不重试，必须返回 `OK`。
Sentinel MASTER 必须指向预期 owner；Policy/ACK 新鲜度间隙出现的
`master,disconnected` 与 `master` 分别计数，不将前者当作健康可用性样本。
Sentinel flags 计数覆盖整个测量阶段。

延迟汇总和 proposal 吞吐仅使用 publication 并发区间。为满足至少 500 次请求，
proposal 流可能在发布结束后继续；原始数据保留这些尾部样本，但并发汇总不包含它们。
这是有节奏的顺序请求实验，不是饱和容量测试；延迟从发送前开始计时，不含调度
等待。慢回复会延误目标 tick，随后可能追赶发送。吞吐仅表示并发区间内完成的
请求速率，不代表容量。snapshot distance 设置为
1,000,000，计时区间内 snapshot index 必须不变。

进程场景的 audit 是**初始且持续变化的窗口**。先填充 audit 并为随后建立 operation
预留准确条数，最终必须恰好为 1,024 或 65,536。所有场景都保留同一填充身份，避免
当前 identity 集合不同。target 0 表示必要 bootstrap/fixture 审计，而不是实际空窗口。
保留开始和结束时的真实计数；成功写入仍追加 audit，bounded rotation 未修改。
只有静态组件矩阵证明精确的 0/1,024/65,536 维度。

### 编码、Raft I/O、Snapshot 和 Strict

- 捕获/析构不包含命令编码、Raft、网络、磁盘和持久化 snapshot。
- `command_encode` 单独测最小 `RegisterNode` 编码；`snapshot` 单独测既有
  `Capture(index)` 的完整状态序列化，完整持久化路径并未裁剪。
- 真实 proposal 包含 admission、编码、Raft/quorum/WAL、dispatch 和 Admin
  transport，不标为 raw Raft I/O，也不通过相减不同微基准的中位数虚构 Raft 成本。
  较早的独立 raw-Raft 对照可见[批次 2 原始证据](https://gist.github.com/liunyl/a24279afc090f89ecd6ad13aafc58a32)。
- 每轮进程负载后另测 manual snapshot，包含 Raft 协调、序列化与持久化。
  两侧完成负载导致最终 audit 数不同，因此该值是系统背景成本，不是同一 image
  的纯序列化对照。
- strict-export 另用单 Meta 场景：先测临近满窗口的 256 次成功写入，再测容量
  65,536 时 256 次精确 `ERR resource-exhausted` 拒绝；拒绝前后稳定的 committed
  cursor 必须不变。拒绝与成功路径绝不混合计算分布。

## 结果

原始三轮数据和按场景生成的中位数汇总与本报告一起保留。小规模、写路径及未改善
的场景与大状态场景使用同一呈现规则，不预设统一提速比例。

### 复制量与捕获成本

**420 项**固定所选记录的不变量检查全部通过，即三轮、十个普通入口、七种无关
增长场景，分别检查申请字节和分配次数。组合 65,536-audit fixture 包含 128 个
Data 节点、32 个 Group、128 个无关 64 KiB operation、32 个归档 64 KiB result、
Policy 历史、未引用 manifest 和 12 个当前 directive。下面是与旧版**完整 aggregate
复制参考**的对照，不是历史端点延迟。

| 捕获 | 申请字节 前 / 后 | 分配次数 前 / 后 | 捕获 p50 us 前 / 后 | 析构 p50 us 前 / 后 | 锁 hold us 前 / 后 |
|---|---:|---:|---:|---:|---:|
| Proposal audit gate | 约 10,861,500 / 8,233 | 2,844 / 2 | 842.673 / 3.552 | 69.116 / 2.940 | 793.339 / 3.475 |
| Observation facts | 约 10,861,500 / 37,408 | 2,844 / 322 | 842.619 / 9.917 | 69.177 / 3.820 | 792.892 / 9.833 |
| Data publication | 约 10,861,500 / 76,391 | 2,844 / 797 | 842.614 / 39.271 | 69.343 / 9.222 | 792.927 / 38.832 |
| Admin Group | 约 10,861,500 / 470 | 2,844 / 7 | 843.054 / 0.233 | 69.977 / 0.069 | 793.464 / 0.147 |

消除的是无关深拷贝，必要的当前状态成本仍存在：

- 当前节点/Group 从 128/32 扩至 4,096/512，facts 从 37,408 字节、10.037 us p50
  增至 930,304 字节、255.187 us。再包含 512 个 active transition，增至 980,480
  字节、335.349 us。
- 在 128/32 拓扑下，当前 directive 从 3 增至 96，publication 从 69,947 字节、
  37.177 us 增至 136,535 字节、50.155 us。
- **所选** operation 正文从 1/64/256 KiB 增长，publication 分别申请
  6,535/71,047/267,655 字节，operation-status 分别为 1,741/66,253/262,861 字节；
  facts 保持 2,338 字节。这是允许的所选完整记录成本。
- 65,536 条 audit 导出仍为 page-handle vector 申请 8,192 字节；archive 导出保留
  约 2.10 MB 的完整目标域，不属于普通视图不变量。

完整持久化 snapshot 在组合 fixture 下两侧都累计申请约 160.95 MB、84 次分配，
p50 为 13.436/13.512 ms。最小命令编码两侧均为 375 字节、六次分配，p50 为
0.168/0.166 us。这些控制组没有预期的实质优化。CSV 使用六位有效数字，大字节量
为近似值。

### 相同 Offered 读取下的成功 Apply

| 初始状态 | 成功 apply/s 前 / 后 | Apply p99 us 前 / 后 | Publication 捕获 + 析构 p99 us 前 / 后 |
|---|---:|---:|---:|
| Small | 543,749 / 571,569 | 3.698 / 2.090 | 172.777 / 123.786 |
| Combined, audit 0 | 151,309 / 164,373 | 10.967 / 7.580 | 1,481.720 / 710.563 |
| Combined, audit 1,024 | 150,667 / 165,436 | 10.975 / 7.724 | 1,531.440 / 942.787 |
| Combined, audit 65,536 | 150,588 / 166,361 | 11.439 / 8.150 | 1,440.250 / 595.982 |

组合场景 writer-lock 平均 wait 从约 0.58 降至 0.067 us。这是 100 Hz offered 读取
下的直接状态机 apply，不是 Raft 或线上写容量。

### 真实联合进程延迟

36 轮均完成：计时 Redis 写全部成功，leader 和 Data session 稳定，每次 Policy
发布恰好应用一次，计时期间 snapshot index 不变。实际 proposal 速率为
19.996-20.031/s，符合给定节奏，不代表容量上限。

| 初始 audit 目标 | 无关 operation | Proposal p50/p95/p99 ms 前 -> 后 | Publication p99 ms 前 / 后 |
|---:|---:|---|---:|
| 0 | 0 | 2.073/2.654/5.673 -> 1.233/1.473/1.676 | 20.238 / 12.087 |
| 0 | 128 | 4.098/6.522/12.143 -> 1.258/1.454/1.669 | 35.885 / 11.935 |
| 1,024 | 0 | 2.137/2.759/5.378 -> 1.238/1.425/1.673 | 21.116 / 12.160 |
| 1,024 | 128 | 4.156/6.725/11.101 -> 1.250/1.467/1.689 | 36.680 / 11.967 |
| 65,536 | 0 | 2.241/3.356/6.933 -> 1.282/1.471/1.672 | 21.795 / 12.456 |
| 65,536 | 128 | 4.278/8.184/12.841 -> 1.298/1.485/1.692 | 50.353 / 12.269 |

目标 0 的实际初始 audit：无历史时为 36 条，128 个无关 operation 时为 164 条。
最大场景 Admin p99 为 8.289/0.205 ms，Sentinel MASTER p99 为 6.933/0.481 ms，
Redis SET p99 为 0.375/0.275 ms。三轮完整阶段仍有 304/9,433 个旧基线样本、
122/9,198 个候选样本返回 `master,disconnected`，不能把成功请求延迟解释为
Sentinel 持续健康可用。所有场景和 flag 计数见[自动汇总](summary.md)。

最大场景在负载结束后单独测得手动 snapshot 中位数 90.107/90.690 ms，不包含在
proposal/publication 计时内。结合组件控制组，最终 small/写路径成对结果在本负载
下没有可重复的实质回归；不宣称所有完整状态操作或压力协议都一致加速。

### Strict Audit 准入

每侧三轮，每轮先执行 256 次接近满容量的成功命令，再执行 256 次准确的容量拒绝。
六轮均通过成功状态、拒绝类型及拒绝期间 committed cursor 不变的检查，分别汇总：

| 路径 | 版本 | p50 us | p95 us | p99 us |
|---|---|---:|---:|---:|
| 接近满容量的成功写 | Before | 481.534 | 698.676 | 809.463 |
| 接近满容量的成功写 | After | 215.236 | 287.241 | 336.663 |
| 满窗口拒绝 | Before | 255.382 | 284.197 | 320.875 |
| 满窗口拒绝 | After | 45.634 | 57.212 | 74.657 |

这是单 Meta 准入实验，快速拒绝不代表成功写吞吐，也不与成功写延迟混合。

### 诊断和中止实验

所有中止实验单独保留，不混入最终成对结果：

- 初次组件测量只预热三次，small snapshot p50 呈现 18.904 -> 21.345 us，
  但 Capture/Serialize 实现没有变化。给两侧增加同等 25 ms 预热并重跑全部六轮，
  结果为 21.338/20.772 us，说明结果对初始条件敏感，不能据此断言特定 CPU/cache
  原因。初次 combined-65,536 publication tail 的范围也重叠；正文呈现完整等时
  预热后的结果。
- 早期进程准备阶段在填充 audit 后出现 `MASTERDOWN`、`s_down,master`，因此增加
  有界且不计时的就绪等待；计时阶段仍严格拒绝错误。先填充 audit 再添加大 blob，
  避免准备阶段反复深拷贝，并准确预留后续 audit 条数。
- 每次发布后仅等 10 ms 的实验完成 15 轮后，旧基线 audit-1,024/无历史场景的
  Redis 连接关闭。Owner 记录 lease expired，Meta 记录 pending/stale causal lease
  anchor。策略变更会作废待完成 grant，因此证据符合并发提交/变更下的租约饥饿，
  不是进程崩溃。这是观察到的基线负载失败，不是成功延迟证据，也不证明候选
  不会遇到同类压力问题。
- 仅增加发布间隔至 600 ms、保留无节流 proposal 时，首轮旧基线仍在约 9.4 秒、
  1,929 次 proposal、15 次发布后失败，说明仅放慢策略变更不够。旧基线 heartbeat
  要求 publisher 的 validated high-water 追上 committed cursor 才授予授权，持续
  proposal 可能使证明长期落后。因此最终成功延迟实验也把两侧 proposal 固定为
  相同的 20 Hz 目标节奏；不宣称饱和运行成功，也不复用失败轮次的成功前缀。

组件与最终进程实验分别保留元数据；只调整进程准备和节奏，可执行文件哈希一致。

## 原始证据

- [`summary.md`](summary.md) 包含自动生成的中位数、捕获/析构全部计数、状态锁、
  apply、进程状态和 strict 分布。生成器拒绝不完整的轮次矩阵。
- [`evidence.tar.gz`](evidence.tar.gz)、[归档校验和](evidence.tar.gz.sha256) 与
  [逐文件校验和](raw-SHA256SUMS) 保留原始 CSV/JSON 样本、进程日志/配置、编译/
  二进制/源码来源、任务环境、测试 XML/日志和前置批次 CI 证据。二进制、WAL 和
  Data image 不提交到归档，继续保留在本任务的 NVMe 目录。
- `measurements/component-*` 是最终六轮等时预热组件数据；
  `measurements/combined/results.json`、`measurements/strict/results.json` 是
  最终进程数据。原组件 metadata 与 `process-metadata.json` 分别保留执行时快照。
- `measurements-initial`、`combined-aggressive`、`combined-unpaced`、保存的旧
  harness、process/strict smoke 和 preflight 目录属于诊断证据，**不是额外的最终轮次**。

先执行 `sha256sum -c evidence.tar.gz.sha256`，解压到独立 `/mnt/local_nvme` 目录，
再从解压根目录执行 `sha256sum -c /path/to/raw-SHA256SUMS`。运行
`bench/summarize.py <extracted-root>/measurements` 可重新生成汇总。

## 复现

[`bench/`](bench) 保留全部测量源码，[英文报告](README.md#reproduction) 提供完整
NVMe 路径、环境变量、构建和执行命令。先构建完所有二进制并结束测试，再运行
`run.py --root /mnt/local_nvme/issue256 --rounds 3`；`summarize.py` 检查不变量并
生成逐场景汇总。输出目录必须是新目录，防止混入旧样本。

本次最终进程运行使用 `--process-only`，此前中止实验已移入独立目录。该模式要求
二进制哈希与原组件运行一致，并保留组件数据及 metadata。全新复现使用默认命令，
按最终协议运行全部阶段；不会从失败轮次中间恢复或拼接样本。

本次两侧构建共享只读 Bycorf 源码 `/home/ubuntu/workspace/keylane/bycorf`，其构建
产物仍在 NVMe。复现时可使用隔离 checkout 内相同 revision 的已初始化依赖。

完整生产调用点允许清单和功能验收命令、revision、结果保留在 issue/PR，当前架构
模型见 [`08-meta-control-plane.md`](../../docs/architecture/08-meta-control-plane.md)。
