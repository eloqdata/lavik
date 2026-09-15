# 80 连接、2 KiB 只读：Keylane 与 Aerospike

## 结果

补测的 **Keylane 默认构建（内核网络 + io_uring，RAID0 裸块设备）为 218,223 GET/s，平均延迟 366.23 µs，合并 p99 为 503 µs**。
三轮共成功读取 196,402,902 次，实际统计窗口合计 900.009 秒；零错误、零未命中。三轮最高与最低吞吐相差 1.35%。

| 配置 | 数据条数 | GET/s | 平均延迟 µs | 合并 p99 µs |
| --- | ---: | ---: | ---: | ---: |
| Keylane：内核网络 + io_uring / RAID0 裸分区 | 500,000,000 | 218,223 | 366.23 | 503 |
| Keylane：内核网络 + SPDK / 六个 NVMe namespace | 500,000,000 | 237,099 | 337.04 | 463 |
| Keylane：DPDK + SPDK / 六个 NVMe namespace | 500,000,000 | 303,841 | 262.93 | 327 |
| Aerospike CE：内核网络 + RAID0 裸分区 | 10,000,000 | 212,075 | 376.80 | 546 |

所有 value 都是 **2048 字节**。Keylane 为 **5 亿条、1.024 TB 纯 value**，Aerospike 按要求采用 **1000 万条、20.48 GB 纯 value**。
Keylane 使用 memtier/RESP，Aerospike 使用 asbench/原生协议；索引规模、记录格式和存储组织也不同。表格比较本机这些配置的实测结果，不是同规模、同协议的产品 A/B。

此前两组 SPDK 测量使用同一可执行文件、数据集和六盘配置：DPDK 相对内核网络的吞吐高 28.15%，平均延迟低 21.99%。新增 io_uring 一组按要求改用了 Linux RAID0，且使用合并生命周期修复后的默认构建；与 SPDK 的差距不能全部归因于存储 API。

## 共同条件与存储配置

- 同一台服务端、同一 boot ID，AMD EPYC 9V74，16 个逻辑 CPU、约 125 GiB RAM；服务进程均限制在 CPU 0–11，即 12 个逻辑 CPU、6 个物理核心的 SMT。Keylane 为 12 个固定 worker。
- 一台客户端 `.5`，AMD EPYC 9V45、16 个物理 CPU；全部读取访问服务端加速网卡 `.6`。第二台客户端 `.7` 未使用，`.4` SSH 管理网卡保持内核驱动。
- 正式测量均为 80 个数据连接，每个连接最多一个未完成请求，均匀随机只读，每轮 300 秒、三轮。Keylane 的 memtier 为 8 线程 × 10 连接、pipeline=1；Aerospike 的 asbench 为 80 个同步线程/连接，batch=1、max-retries=0，另有一条维护连接。
- 六块 Microsoft NVMe Direct Disk v2。SPDK 两组直接使用六个 namespace；io_uring 与 Aerospike 使用六盘 RAID0，512 KiB chunk，直接打开裸分区，**没有文件系统**。
- Keylane io_uring 使用新分区 `/dev/md127p2`，2 TiB，起始按完整 RAID 条带对齐；使用前整段清零。Aerospike 的 128 GiB `/dev/md127p1` 保留，补测期间停止 Aerospike，避免共享 CPU/磁盘竞争。设备身份及边界见 [分区记录](evidence/keylane-uring/storage-after.json)。
- io_uring 一组使用 `KEYLANE_KERNEL_BYPASS=OFF`，启动参数 `--network kernel --storage uring --busy-poll-us 20 --shutdown-checkpoint`。每个 worker 只有一个 io_uring，网络、存储和定时器共用；内核 fdinfo 的固定文件表均指向该裸分区，见 [实际 ring 检查](evidence/keylane-uring/uring-r1.before.audit.json)。测量版本的存储打开路径设置 `O_DIRECT`，固定文件的 flags 不显示在普通进程 fd 表中。
- Aerospike CE 8.1.2.4（安装包 8.1.2.4-4），单节点、复制因子 1、无 TTL；48 个服务线程限制在同样的 12 个逻辑 CPU 内。配置为 `storage-engine device`、`post-write-cache=0`、`read-page-cache=false`。实际设备 fd 均带 `O_DIRECT`，三轮缓存读比例均为 0，见 [配置](evidence/aerospike/aerospike.conf) 与 [运行检查](evidence/aerospike/final.audit.json)。

## io_uring 补测过程与逐轮结果

使用同一 memtier 2.5.1 二进制，先以 640 个连接顺序写入 `kv_1` 到 `kv_500000000`，value 为随机 2048 字节。**灌数的连接数不用于正式读取。**
写入计数、640 份客户端 CSV 与服务端 SET 增量均为 500,000,000；然后干净关闭并恢复。每一轮重新启动后核对总条数及 8 个分散 key 的长度和内容 SHA256，预热 20 秒，再开始正式读取；每轮结束再核对数据并干净关闭。

| 轮次 | GET/s | 平均 µs | p99 µs | 客户端 CPU 核数 | 服务进程 CPU 核数 | 六盘物理读 IOPS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 219,731 | 363.71 | 503 | 3.27 | 10.51 | 217,650 |
| 2 | 218,128 | 366.39 | 503 | 3.24 | 10.52 | 215,963 |
| 3 | 216,811 | 368.62 | 511 | 3.23 | 10.49 | 214,764 |

三轮物理盘读增量合计约为每次成功 GET **1.00383 次 I/O、2640.0 字节**，读取实际到达了磁盘。只累加六块物理盘，不把 MD 与分区层重复相加。
CPU 与磁盘速率使用包住读取的服务端快照窗口，包含少量采样和客户端收尾时间；QPS 使用客户端正式窗口。进程 CPU 不包含独立的内核中断线程。

Aerospike 的较小数据集在写入后也经过进程重启和内容核对；索引在内存、value 直接从设备读取。其原始 HDR、设备计数及逐连接采样保留在 [三轮证据](evidence/aerospike/)。

## 统计定义与复核

- QPS = 三轮成功请求总数 / 三轮实际统计时长之和，不平均日志中的瞬时速率，也不直接平均四舍五入后的 QPS。
- Keylane 平均延迟 = 累计延迟 / 成功请求数。逐轮与全部 80 份连接 CSV 的请求数、命中数、接收字节和累计延迟核对，再与服务端 GET 增量核对；正式窗口 SET 增量为零。
- Aerospike 平均延迟与 p99 来自完整 HDR；该版本文件头为 `StartTimestamp,EndTimestamp`，时长是结束减开始。HDR 成功数与服务端读成功数完全一致。平均延迟受 HDR 分桶精度影响。
- 所有合并 p99 都由合并直方图计算，不平均各轮 p99。完整数值见 [results.json](results.json)。
- [verify_report.py](verify_report.py) 离线重新读取压缩归档中的原始 JSON、CSV、HDR 和服务端计数，复算统计并检查 [SHA256SUMS](SHA256SUMS)。在本目录运行：

```sh
python3 -m venv /tmp/keylane-report-review
/tmp/keylane-report-review/bin/pip install hdrhistogram==0.10.7
/tmp/keylane-report-review/bin/python verify_report.py
```

## 版本与代码验证

io_uring 测量使用 Keylane `e1fbccb4adaa01a7ff221775d3b4b66c6e62ce5a`、Celer `0d27a6d586958714451d7d94d0281b7753fd7712`，Release / Clang 18 / `-march=native`；可执行文件 SHA256 为 `d82031f774c7125c0acf293075984bdb681fc1f05cc0563ebcc21cd29ed510f1`。
完整启动、客户端参数见 [服务端命令](evidence/keylane-uring/uring-r1.server-command.json) 和 [客户端命令](evidence/keylane-uring/uring-r1.get.command.json)。

此前 SPDK 两组使用当时的功能分支加未提交修复；提交号、二进制 SHA256、补丁和新增源文件一起保存在 [SPDK 源码记录](evidence/keylane-spdk/source-revisions.json) 及同目录。它们不是合并 main 后重新测量的结果。
Aerospike 与 asbench 的二进制及版本记录见 [服务端记录](evidence/aerospike/server-command.json) 和 [客户端环境](evidence/aerospike/client-environment.json)。

发布代码验证包含 613 项 Keylane 单元测试（另有 3 项原有禁用用例）、3 项 Celer 默认后端检查、内核/DPDK 切换与数据恢复、128 worker 初始化/关闭与 129 worker 超限拒绝、3 GiB TCP 序号回归，以及带丢包的虚拟 TAP 流测试。详见 [验证记录](validation/summary.json)。物理 NVMe 的 SPDK 压测沿用上述已完成测量，合并后未重新绑定 RAID0 成员去运行 SPDK。

补测结束状态见 [final-state.json](final-state.json)：两个裸分区的数据均保留，Keylane 已干净关闭，Aerospike 已恢复服务；管理网卡和加速网卡保持原有内核驱动。
