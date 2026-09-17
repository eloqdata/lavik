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

# Keylane SPDK：16 个 worker 下，十亿条记录、128–1024 字节 value 的 GET 吞吐超过百万 QPS

[English](README.md) | **简体中文** | [报告目录](../README.md)

> 本报告记录项目更名为 Lavik 之前的 Keylane 测试。产品名、版本、命令和数据均对应当时的实验，
> 不代表当前 Lavik 版本的实测结果。
> 恢复来源： [2026-09-15 历史版本](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports)。

日期：2026-08-31 UTC

## 技术总结

Keylane 在 **1,000,000,000** 个已有 key 上执行随机 GET，固定 value 大小从 **128 字节到 1024 字节**时均超过百万 GET/s。128 字节的最佳结果为 **1,058,414 GET/s**，全量 256 字节为 **1,043,567 GET/s**，全量 512 字节为 **1,031,819 GET/s**，全量 1024 字节为 **1,007,197 GET/s**。下一个测试大小 2048 字节得到 843,047 GET/s。因此，在本配置已验证的大小中，1024 字节是超过百万 GET/s 的最大值。所有测试均使用 16 个固定 CPU 的 Keylane worker、六个裸 SPDK NVMe namespace，以及 `dfly_bench` 的 16 个客户端线程、640 个连接、pipeline=1 和不限速配置。每次测量的 64,000,000 个请求全部命中，未报告错误。

这是闭环吞吐结果，不是开放环延迟 SLA 承诺。读取开始前持久化 keyspace 恰好有十亿个 key，随机读取均匀覆盖完整的 `0` 到 `999999999` 范围。

## 主要测量结果

| 数据集与服务端设置 | 均匀采样 key 范围 | 连接数 | 测量 GET 数 | QPS | 平均延迟 | p99 | 命中率 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 十亿 key， 128 B value，SPDK completion cap 8 | 1B | 640 | 64M | 973,095 | 655.210 us | 4.932 ms | 100% |
| 十亿 key， 128 B value，SPDK completion cap 16 | 1B | 640 | 64M | **1,058,414** | 602.071 us | **894.884 us** | 100% |
| 十亿 key， 256 B value，SPDK completion cap 16 | 1B | 640 | 64M | **1,043,567** | 610.576 us | 905.363 us | 100% |
| 十亿 key， 512 B value，SPDK completion cap 16 | 1B | 640 | 64M | **1,031,819** | 617.648 us | 937.584 us | 100% |
| 十亿 key， 1024 B value，SPDK completion cap 16 | 1B | 640 | 64M | **1,007,197** | 632.995 us | 964.339 us | 100% |
| 十亿 key， 2048 B value，SPDK completion cap 16 | 1B | 640 | 64M | 843,047 | 755.227 us | 1.175 ms | 100% |
| 相同数据，completion cap 8 | 500M | 640 | 64M | 972,010 | 656.067 us | 4.908 ms | 100% |
| 相同数据，completion cap 8 | 20M | 640 | 64M | 980,248 | 650.307 us | 4.916 ms | 100% |
| 相同数据，completion cap 8 | 1B | 704 | 64,000,640 | 930,681 | 753.769 us | 6.961 ms | 100% |

500M 和 20M 读取是在干净装载完整数据集后进行的敏感性检查，均未达到百万 QPS。因此，上述成功结果不能归因于缩小随机 key 范围。

completion cap 从 8 到 16 的对比具有运维参考价值，但不是严格的单变量因果 A/B 实验：cap=16 的进程经历了重启和索引重建。重启前，`perf` 在 GET 请求中发现了增量 `RehashStep` 工作。因此，可以支持的运维结论是，在恢复和索引维护稳定后使用 cap=16，而不是认定仅调整 cap 就能解释全部差异。

## 硬件和软件

### 服务端

- Azure 机型：`Standard_L16aos_v4`。
- CPU：AMD EPYC 9V74，16 个逻辑 CPU；单插槽、八个核心，每核心两个硬件线程。
- 内存：135,066,603,520 字节（125.79 GiB）。
- 存储：六个 Microsoft NVMe Direct Disk v2 namespace，每个 1,919,850,381,312 字节；裸数据总容量 10.48 TiB。SPDK BDF 为 `5361:00:00.0`、`6d30:00:00.0`、`32a1:00:00.0`、`78f5:00:00.0`、`9093:00:00.0` 和 `c7c0:00:00.0`。
- Keylane 可执行文件 SHA-256：`be1f71c6c8c11e6130685ca0c0e83ad8478bb30b27cff1b61742376959ce32bf`。
- 测试时源码：Keylane `07d4115ab6e8a66e8b2dc81a5a7abf42f6eb78a6`，Celer `6437653f87887924c5e7ea1e83defb85d8e1f9f1`。

### 客户端

- Azure 机型：`Standard_F16als_v7`。
- CPU：AMD EPYC 9V45，16 个逻辑 CPU；单插槽、16 个核心，每核心一个硬件线程。
- 客户端：Dragonfly `dfly_bench` v1.40.1 x86_64 release 二进制，SHA-256 为 `68fbf912ddd469e621025e35b5b42cb658ed0daef476bf725a9d1e37cf0a538f`。

## 数据集与资源使用

128 字节数据集先清空数据库，再执行十亿次顺序 SET，以便复现。Key 是**没有前缀**的十进制字符串。`DBSIZE` 返回 `1000000000`；`STRLEN 0`、`STRLEN 500000000`、`STRLEN 999999999` 均返回 `128`。

首次 128 字节装载耗时 18m17.511s，整体速率为 957,870 SET/s。256 字节结果通过覆盖完整的相同 key 范围产生，key 数量保持不变；耗时 18m14.970s，整体速率 965,823 SET/s。256 字节测试前，`DBSIZE` 仍为 `1000000000`，相同三个 `STRLEN` 检查均返回 `256`。整体装载速率低于中间稳定阶段，是因为 `dfly_bench` 在结束时需要排空进度不均的顺序连接区间，服务端也出现过若干短暂写入减速。512 字节结果再次覆盖同一十亿 key 范围，耗时 19m11.976s，整体速率 910,384 SET/s；GET 测试前 `DBSIZE` 仍为 `1000000000`，相同三个长度检查均返回 `512`。

1024 字节覆盖耗时 20m2.139s（877,563 SET/s），2048 字节覆盖耗时 26m7.184s（685,017 SET/s）。每次均保留恰好十亿个 key；GET 前，`0`、`500000000`、`999999999` 三个 key 的长度都符合目标大小。1024 字节读取仅比百万门槛高 0.72%；2048 字节没有达到门槛是实测结果，而非外推。

用于 cap=16 测试的重启之前，Keylane 报告：

| 指标 | 值 |
|---|---:|
| `used_memory` | 49.06 GiB |
| RSS | 50.62 GiB |
| 峰值 RSS | 51.89 GiB |
| 配置的最大内存 | 100.63 GiB |

128 字节数据集的逻辑 key 和 value 总量约 127.5 GiB。计入 104 字节 record header、内联 key 和八字节 record 对齐后，存活记录分配量约 230 GiB，尚未计入 block header 和 direct-I/O flush padding。256 字节 value 的同类估算约 349 GiB。Keylane 的内存索引大小未随 value 大小发生显著变化：256 字节测试后 `used_memory=49.06 GiB`，RSS 为 `50.25 GiB`。

裸设备没有文件系统 `df` 计数；以上是存活数据估算，不代表安全擦除。`FLUSHDB` 是逻辑删除，之前实验的记录可能仍保留在物理介质上，在回收发生前增加恢复扫描时间。

## 复现步骤

将六个专用 namespace 绑定到 `vfio-pci`，并固定全部 16 个 worker 的 CPU。读取测试使用 `--defrag-paused` 排除回收工作。

```bash
taskset -c 0-15 /mnt/dev/keylane-spdk-main \
  --bind=172.16.0.4 --port=6379 --metrics-port=9100 \
  --threads=16 --pin-workers --busy-poll-us=20 \
  --foreground-budget-us=1000 --background-budget-us=10 \
  --background-warrant-percent=1 --defrag-paused \
  --spdk-max-completions-per-poll=16 --spdk-foreground-pre-poll-us=5 \
  --data-file=spdk://5361:00:00.0/1 \
  --data-file=spdk://6d30:00:00.0/1 \
  --data-file=spdk://32a1:00:00.0/1 \
  --data-file=spdk://78f5:00:00.0/1 \
  --data-file=spdk://9093:00:00.0/1 \
  --data-file=spdk://c7c0:00:00.0/1 \
  --logtostderr
```

`spdk-max-completions-per-poll` 支持运行时热修改。以下命令无需重启即可应用到所有 worker；若需要重启后保持设置，启动配置中也必须指定。

```bash
valkey-cli -h 172.16.0.4 -p 6379 \
  CONFIG SET spdk-max-completions-per-poll 16
```

在客户端创建干净数据集。对于该 key 生成器，`dfly_bench` 将 maximum 视为不包含的上界，因此 `1000000000` 恰好创建预期的闭区间数字 key 范围。

```bash
valkey-cli -h 172.16.0.4 -p 6379 FLUSHDB

ulimit -n 8192
taskset -c 0-15 /tmp/dfly_bench-x86_64 \
  --h=172.16.0.4 --p=6379 \
  --proactor_threads=16 --c=40 --n=1562500 \
  --ratio=1:0 --pipeline=1 --qps=0 \
  --key_prefix= --key_minimum=0 --key_maximum=1000000000 \
  --key_dist=S --d=128
```

读取前验证记录数量和固定 value 长度：

```bash
valkey-cli -h 172.16.0.4 -p 6379 DBSIZE
valkey-cli -h 172.16.0.4 -p 6379 STRLEN 0
valkey-cli -h 172.16.0.4 -p 6379 STRLEN 500000000
valkey-cli -h 172.16.0.4 -p 6379 STRLEN 999999999
```

执行正式 GET 负载：

```bash
ulimit -n 8192
taskset -c 0-15 /tmp/dfly_bench-x86_64 \
  --h=172.16.0.4 --p=6379 \
  --proactor_threads=16 --c=40 --n=100000 \
  --ratio=0:1 --pipeline=1 --qps=0 \
  --key_prefix= --key_minimum=0 --key_maximum=1000000000 \
  --key_dist=U --tcp_nodelay=true
```

该命令建立 640 个客户端连接，执行 64,000,000 次 GET。先提高客户端文件描述符上限：默认的 1024 不足以支撑 1,024 个连接，会产生客户端 `EMFILE`，并非 Keylane 服务端错误。

复现 256、512、1024 或 2048 字节版本时，再次执行相同顺序 SET 命令，将 `--d=` 设置为对应大小；验证 `DBSIZE` 和三个 `STRLEN` 后执行相同 GET 命令。覆盖 value 大小时无需 `FLUSHDB`。

## CPU 热点与限制

在 cap=8 的 60 秒 GET 采样中，`perf` 将 14.46% 的用户态样本归于 `ScanHashMap::FindWithoutStep`，2.24% 归于 `RehashStep`。增量扩容未完成时，`RehashStep` 会在可变 GET 查找路径中执行。这类维护可能降低装载后 benchmark 的性能，而本次 cap=8/cap=16 对比没有独立排除该影响。

正式代码优化的方向是通过 owner worker 后台维护排空增量 rehash，并在宣布恢复就绪之前完成，同时在修改路径保留有界 fallback 以保证进展。GET 不应承担 hash bucket 迁移。本报告不声称该改动已经实现或测量。

## 建议的运行配置

对于这台专用 benchmark 主机，使用 16 个固定 CPU 的 worker、640 个客户端连接、pipeline=1 和 `spdk-max-completions-per-poll=16`。不要把结果解释为必须缩小到 20M 或 500M 工作集：成功结果均匀读取了全部十亿个 key。

若将其作为发布门槛，应在同等稳定的恢复后状态下重复 cap=8 和 cap=16 测试，交替执行顺序，并保持同样的 64M 请求窗口，以区分 completion budget、增量 rehash 和恢复状态的影响。
