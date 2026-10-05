# Native FULL 大对象窗口：无抓包持续写补测

2026-10-05 · [English](README.md) · [原报告](../README.zh-CN.md) · [CI测试修复](../validation/CI-FIX.md)

无抓包补测确认：在5/20ms附加RTT下，大对象窗口降低了FULL中位耗时，但前台写延迟并非全面正向。40次运行全部通过正确性和清理校验。本报告仅比较main与大对象窗口。

| 附加RTT | main FULL中位数 | chunks FULL中位数 | 耗时下降 | 更快的配对数 |
| --- | --- | --- | --- | --- |
| 5 ms | 5.4924 s | 4.9901 s | 9.14% | 9/10 |
| 20 ms | 13.6226 s | 10.4858 s | 23.03% | 10/10 |

5ms仍有一对candidate比baseline慢。[完整十对数据](full-table.md)保留全部分布及配对变化，不筛选有利样本。

## 前台写入

| 附加RTT | FULL期间开始的样本数，main/chunks | 每轮最大值的中位数，main/chunks | 全部样本最大值，main/chunks | candidate最大值更高的配对数 |
| --- | --- | --- | --- | --- |
| 5 ms | 50 /45 | 125.24 /129.63 ms | 177.07 /225.22 ms | 6/10 |
| 20 ms | 134 /105 | 166.76 /212.63 ms | 311.10 /335.42 ms | 7/10 |

20ms下，每轮最大延迟的中位数上升约27.51%；5ms下约3.50%。这些是样本较少、观察时长不同的最大值，不是稳定p99估计。FULL变短也缩短了前台写观察窗口，并改变并发复制工作量。[每轮延迟、样本数及完成QPS](latency-table.md)和原始样本保留了这些口径。

因此，排除抓包后，前台延迟上的取舍仍然存在。原抓包批次出现过更大的延迟峰值，但两个时间不同的批次无法证明差异由抓包造成。这批证据支持两种持续写负载下的FULL加速，同时保留前台延迟顾虑，尤其是20ms场景；不能据此宣称所有指标都改善。

两版都使用预先固定的每worker32MiB发布队列。本批次没有重测或修复原16MiB队列的baseline admission/gate卡住问题，原诊断和失败记录继续保留。

## 方法与版本

这是预先声明的独立无抓包批次，每个场景固定10对，按A/B、B/A交替顺序运行，保留全部结果，不替换原来的三对样本。全批次共60次，其中大对象40次、普通baseline20次。测量期间没有并行运行本机构建、测试或重型离线分析。

沿用原报告冻结的Release二进制：main `25e159418603a95c1b2cca68173b329067e3ef48`、chunks `51d03f97fede341fb42c14bd46a9f1aaf416eeb1`、records `9da9c797eefaea45dfde3ccc521baa440de559a8`；Bycorf为 `62509c93d40c2480f5046b71454db6cf95801b04`。三份二进制的SHA-256仍分别为 `a056e41003b0c833157fe89737448b6a7715d2c5ba63aa32ad1675b4d643fabc`、`c9dae66f6d752823acc72f7d399016d2a14201c6b848ea1aa610e910aaada751`、`ae24694323ff25448658bf56797f0fb36694fea310ce37224b93ab00d1f57e6a`。后续CI修复仅改变测试编排，生产源代码与这两个优化核心提交逐字节一致。这是固定基线的分项对比，没有重新构建最新main，也没有测量main到两项优化叠加后的总收益。

大对象场景为8个已有的16MiB字符串，源/目标各1worker，增加5/20ms RTT，发布队列每worker32MiB，每秒1次同步 `MULTI; SET; EXEC`，预热5秒。源CPU0、目标CPU1、客户端CPU6–7互不重叠。普通baseline场景为32,768个4KiB值，源4worker、目标2worker，20ms RTT，每worker16MiB发布队列，无并发写入；源CPU0–3、目标4–5、客户端6–7互不重叠。共同配置为每进程2GiB数据文件及内存上限、每进程64MiB backlog、每worker64MiB注册buffer、100ms flush和900秒超时。输入与原匹配场景相同，两版均关闭tcpdump。网络两端分别增加半个目标RTT，实际PING结果保留在原始数据中。源端刚完成灌数，不代表冷缓存或磁盘受限的大规模测试。

FULL计时从发送 `LAVIK.REPLICAOF` 前开始，到轮询观察到目标ONLINE结束，包含reset、传输、handoff、final cut及轮询，不含启动、灌数及最终摘要校验。每次都必须满足：每个预期flow只发生一次FULL、所有flow ONLINE、源/目标最终摘要相同、每flow哨兵写入可见、前台写全部成功、进程及网络资源正常退出。不会用重跑后的成功替换失败或慢样本。

前台延迟统计纳入FULL期间开始的操作，包括ONLINE后才结束的操作；完成QPS则只计算FULL期间完成的操作。FULL时长不同会改变并发写入次数，复制工作量也可能不同。由于没有抓包，不推算frame字节数或不同record来源各自的收益。样本数、每轮最大值仅用于描述这批观测，短跑不足以证明稳定p99/p99.9或罕见停顿行为。`per-run.json`中的内存/CPU是采样峰值与有效采样区间统计，不是精确峰值或全程CPU成本。

较晚运行的无抓包批次无法把抓包开销与时间上的环境波动隔离，原有抓包回退仍然保留。结论仅适用于这些负载、拓扑、队列配置和机器，不能证明普遍提升。

## 数据与复现

- [冻结配方](recipe.json)、[配对结果](comparisons.json)、[每轮摘要](per-run.json)、[全部FULL耗时](full-table.md)。
- [原始结果、验收记录与执行命令](raw-results.jsonl)，包含精确二进制哈希、计时边界、摘要和清理结果。
- [原始写入样本](writer-samples.jsonl)，包含预热和跨越FULL结束边界的操作；静态普通baseline场景该文件为空。
- [保留的原始路径及哈希](retained-artifacts.json)。完整资源采样和进程日志保留于原NVMe机器的 `/mnt/local_nvme/i131/measurements-followup`；结果和写入样本已收录到本报告。

复现使用大对象报告的共享portable工具、新输出目录，以及将 `main`、`chunks`、`records` 映射到本地二进制和上述精确版本的variants JSON。本目录配方仅含本报告场景，原始声明仍记录全部60次。原机器命令及跨机器路径参数见[英文版复现说明](README.md#evidence-and-reproduction)，完整构建依据、环境准备和计时口径见父报告的METHODS/REPRODUCE文档。
