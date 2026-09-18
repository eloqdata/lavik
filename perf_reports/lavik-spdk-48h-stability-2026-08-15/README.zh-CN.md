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

# Lavik SPDK 48 小时在线稳定性实验（2026-08-15）

[English](README.md) | **简体中文** | [报告目录](../README.md)

> 恢复来源： [2026-09-15 历史版本](https://github.com/eloqdata/lavik/tree/eedb3080d808769519d93e971195b53b38b09c1e/perf_reports)。

## 结论

Lavik 在 4 亿条 1–4 KB 数据、10 万 QPS、95% 读 / 5% 覆盖写的持续负载下，使用最终 defrag 参数连续运行了严格的 48 小时。窗口内完成约 172.80 亿次 GET/SET，服务没有重启、报错、内存拒绝或数据条目丢失。

服务端延迟保持稳定：

- p99.9 平均 `0.944 ms`，5 分钟点位最高 `2.184 ms`。
- p99.99 平均 `2.383 ms`，5 分钟点位最高 `2.810 ms`。
- 全部 577 个有效 5 分钟点中，p99.9 和 p99.99 均低于 `3 ms`。

磁盘空间随着旧版本 block 达到 defrag 条件后呈锯齿式回收，而不是平滑不变。48 小时内两块 SPDK 设备合计占用范围为 1,268.1–1,493.1 GiB，结束时为 1,364.7 GiB，已经从本轮峰值回落 128.4 GiB；最后 24 小时净下降 52.6 GiB。该窗口没有出现磁盘空间单调耗尽。

因此，本实验支持 Lavik 在该 workload 和最终 defrag 参数下连续 48 小时稳定承载 10 万 QPS，并将服务端 p99.99 控制在 3 ms 以内。该结论只适用于本报告的配置与时间窗口，不等价于任意 workload 下的无限期稳态证明。

## Grafana 截图

<img width="1512" height="949" alt="image" src="https://github.com/user-attachments/assets/4783c5a6-80ec-48ec-b048-bb1a7764054d" />


## 48 小时结果

延迟使用 Lavik Prometheus 直方图计算，表示服务端命令执行时间，不包含网络传输和 socket response write。平均值与最高值来自严格 48 小时内的所有 5 分钟窗口；只保留 QPS 位于 90,000–110,000 的点，排除窗口边界处的非完整采样。

| 指标 | 结果 |
| --- | ---: |
| 时间范围 | 2026-08-13 07:06:30–2026-08-15 07:06:30 UTC |
| 有效 5 分钟窗口 | 577 |
| 平均 QPS | 99,999.9 |
| 5 分钟 QPS 最低值 | 99,993.2 |
| 5 分钟 QPS 最高值 | 100,002.7 |
| p99.9 平均值 | 0.944 ms |
| p99.9 最低值 | 0.487 ms |
| p99.9 最高值 | 2.184 ms |
| p99.99 平均值 | 2.383 ms |
| p99.99 最低值 | 1.515 ms |
| p99.99 最高值 | 2.810 ms |

10 万 QPS 是客户端 rate limit，而不是峰值吞吐测试。这里的稳定性含义是 Lavik 在持续后台回收期间始终跟上设定负载，不能据此推导系统的最大 QPS。

## 数据完整性与服务状态

| 检查项 | 48 小时结果 |
| --- | ---: |
| GET 增量 | 16,415,992,616 |
| SET 增量 | 863,999,609 |
| GET/SET 合计增量 | 17,279,992,225 |
| 成功 defrag block | 525,153 |
| Defrag `error` | 0 |
| Defrag `resource_exhausted` | 0 |
| 内存拒绝命令 | 0 |
| 服务重启次数 | 0 |
| 测试结束后的 `DBSIZE` | 400,000,000 |

GET 与 SET 的实际计数比例为 95:5。测试没有使用 DEL 或 TTL，Tomb Raider 动态关闭；覆盖写产生的旧物理版本由 defrag 回收。因此本实验验证的是覆盖写回收路径，不覆盖大量 tombstone 或 TTL 过期数据的清理稳定性。

压测停止后 Lavik 仍为 `active (running)`，systemd 的 `NRestarts=0`，测试窗口内没有 warning/error journal。

## 内存稳定性

| 指标 | 开始 | 结束 | 48 小时最高值 |
| --- | ---: | ---: | ---: |
| RSS | 37.388 GiB | 37.392 GiB | 37.399 GiB |
| Lavik 计费内存 | 37.352 GiB | 37.354 GiB | 37.354 GiB |

RSS 的最大波动约 11 MiB，没有随覆盖写次数增长。测试结束后的 `INFO memory` 显示 `oom_rejected_commands=0`；进程使用 mimalloc。

## 磁盘空间与 Defrag

两块 SPDK namespace 各提供 1,920,370,475,008 bytes 可用数据容量，合计约 3.49 TiB。Lavik 的 `storage_available` 已扣除 defrag reserve；本报告用 `capacity - available` 表示已占用容量，该值不是 Linux `df`。

| 指标 | 48 小时结果 |
| --- | ---: |
| 开始占用 | 1,268.1 GiB |
| 结束占用 | 1,364.7 GiB |
| 最低占用 | 1,268.1 GiB |
| 最高占用 | 1,493.1 GiB |
| 从峰值到结束 | -128.4 GiB |
| 窗口净变化 | +96.6 GiB |
| 最后 24 小时净变化 | -52.6 GiB |
| 平均 defrag 速率 | 3.04 blocks/s |
| 最高 5 分钟 defrag 速率 | 14.12 blocks/s |

空间曲线不能按短窗口线性外推。SET 采用追加新版本的方式；旧版本所在 block 的有效数据比例降到 50% 以下后才会成为 defrag 候选，因此曲线自然表现为先增长、再集中回收。

48 小时窗口从一次回收后的低位开始，所以结束占用比开始高 96.6 GiB；这本身不能证明严格的长期空间平衡。更关键的是，窗口内占用在 1,493.1 GiB 形成峰值后持续回落，最后 24 小时仍净下降，并未在测试结束时继续逼近容量上限。该结果支持“本次两天窗口内空间受控”，但更长期验证仍应覆盖多个完整锯齿周期。

本次没有记录 `defrag_reclaimed_bytes_total` 和 `defrag_relocated_bytes_total`，只能用 block 完成次数与容量变化交叉判断。若要证明数周级稳态，建议增加这两个字节计数器。

## 最终 Defrag 参数

严格 48 小时窗口开始前，参数已经动态调整为以下值，并在整个窗口内保持不变：

```bash
redis-cli -h 10.0.0.4 -p 6379 DEFRAG MAX-ACTIVE 1
redis-cli -h 10.0.0.4 -p 6379 DEFRAG BLOCK-SLEEP-MS 100
redis-cli -h 10.0.0.4 -p 6379 DEFRAG RECORD-SLEEP-US 0
redis-cli -h 10.0.0.4 -p 6379 TOMBRAIDER OFF
```

`MAX-ACTIVE` 是每个设备的上限；两块盘最多同时执行两个 defrag。Block cooldown 只在完成一个 block 后异步等待，不会 sleep 阻塞整个 worker。Record sleep 保持为 0，记录之间仍执行协作式 yield。

## 测试环境

| 角色 | Azure 机型 | CPU | 地址 |
| --- | --- | --- | --- |
| Server | `Standard_L16s_v3` | 16 vCPU，Intel Xeon Platinum 8370C | `10.0.0.4:6379` |
| Client | `Standard_L16s_v3` | 16 vCPU，Intel Xeon Platinum 8370C | `10.0.0.5` |

Server 使用 12 个 Lavik workers，systemd `AllowedCPUs=0-11`。IRQ 58–74 分散到逻辑 CPU 12–15，避免和 Lavik workers 直接争抢。Client 的 memtier 使用 CPU 0–15。

存储为两个独立 SPDK NVMe namespace，不使用 RAID：

```text
spdk://021d:00:00.0/1
spdk://69f9:00:00.0/1
```

版本：

```text
Lavik: 13dab14e786fa3e9465d5e82780d53b83ba93268
Celer:   e394652ddacffd18854b927367911ba8d283f0ca
Binary SHA-256: 66d3a451f337e7f4852dc3f397757d4415ab680e7ebe892c4ea1c74e4b1cd8a4
```

## 测试命令

测试前已经灌入 4 亿条 `kv_` key，value 为 1,000–4,000 bytes 随机数据。长期 workload 使用 8 个 memtier threads、每线程 10 个连接，总计 80 个连接；每个连接限速 1,250 ops/s，总限速为 100,000 ops/s。

```bash
taskset -c 0-15 memtier_benchmark \
  -t 8 -c 10 \
  -s 10.0.0.4 -p 6379 \
  --test-time 259200 \
  --distinct-client-seed \
  --ratio=1:19 \
  --key-prefix="kv_" \
  --key-minimum=1 \
  --key-maximum=400000000 \
  --random-data \
  --data-size-range=1000-4000 \
  --data-size-pattern=R \
  --hide-histogram \
  --print-percentiles="99.9,99.99" \
  --rate-limiting=1250 \
  --randomize
```

memtier 实际运行超过 48 小时；本报告固定截取最终参数已经稳定生效后的最后 48 小时，不混入之前的调参窗口。

## 监控口径与复核查询

Prometheus 位于 client，保留 30 天数据，每 5 秒抓取一次 Lavik metrics。报告使用以下 PromQL，并以 5 分钟 rate 作为稳定性统计粒度：

```promql
# GET + SET QPS
sum(rate(lavik_command_calls_total{command=~"get|set"}[5m]))

# p99.9 / p99.99，分别把 P 替换为 0.999 和 0.9999
histogram_quantile(
  P,
  sum by (le) (
    rate(lavik_command_duration_seconds_bucket{command=~"get|set"}[5m])
  )
)

# 两块设备合计已占用容量
sum(lavik_storage_capacity_bytes - lavik_storage_available_bytes)

# 成功 defrag block 速率
sum(rate(lavik_storage_defrag_runs_total{result="success"}[5m]))
```

严格 48 小时窗口为 `2026-08-13 07:06:30` 至 `2026-08-15 07:06:30 UTC`。

## 适用边界

- 本报告验证的是 400M keys、1–4 KB value、均匀随机 key、95:5 覆盖写、100k QPS 和双 SPDK NVMe 的组合。
- Tomb Raider 关闭，因此没有验证大量 DEL、TTL 或 tombstone 扫描。
- 结果覆盖一次明显的空间回收波次，但不是数周级 soak test；“稳定”指严格 48 小时内无故障、p99.9/p99.99 受控且空间没有单调耗尽。
- Prometheus 分位数是直方图插值，且不包含客户端网络。
- 两台 VM 位于同一测试网络。不同 CPU、NVMe、key 分布或写比例需要重新选择 defrag 参数。
