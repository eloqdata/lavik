# Keylane MSET 提交协调器优化（2026-08-25）

## 结论

在 16 worker、单个 XFS/O_DIRECT 大文件、约 2.73 亿现存 key 的同机 A/B 中，最终有界提交协调器把 8-key MSET 从 12,627.29 提高到 52,830.89 ops/s（4.18x，+318.39%）。memtier 墙钟从 68.948 秒降到 60.022 秒，不再在 60 秒停止发流后等待多秒未完成请求。

最终版本没有改变事务磁盘格式或原子性规则：每个事务仍写自己的 tagged records 和一条 commit record；优化只把每个事务一个 detached coroutine 改为每 worker 一个 FIFO runner，每批最多处理 256 个事务，并提前合并/触发共享 durability fence。单事务、空队列路径不增加批等待；只有 worker 本地队列达到 4,096 时，现有连接命令协程才等待容量。

## 测试口径

- Server：`10.0.0.4`，16 workers，固定 CPU 0-15。
- Client：`10.0.0.5`，memtier 8 threads × 10 connections，pipeline=1，60 秒。
- Storage：`/mnt/data/keylane.data`，1,600 GiB XFS regular file，O_DIRECT；defrag 和 tx cleaner 保持开启。
- 命令：一次 MSET 8 个 key，每个 value 随机 1-4 KiB。
- key 空间：8 个独立前缀，各 1-50,000,000，共 4 亿个逻辑 key。
- 旧版开始前 `DBSIZE=273,221,776`；最终版开始前 `DBSIZE=273,644,250`。两组使用同一文件顺序执行，最终版面对更老化的数据，因此不是快照级严格同态 A/B。

```bash
taskset -c 0-15 memtier_benchmark \
  --server=10.0.0.4 --port=6379 --protocol=redis \
  --threads=8 --clients=10 --test-time=60 --pipeline=1 \
  --key-minimum=1 --key-maximum=50000000 \
  --data-size-range=1024-4096 --data-size-pattern=R --random-data \
  --command='MSET mset:0:__key__ __data__ mset:1:__key__ __data__ mset:2:__key__ __data__ mset:3:__key__ __data__ mset:4:__key__ __data__ mset:5:__key__ __data__ mset:6:__key__ __data__ mset:7:__key__ __data__' \
  --command-ratio=1 --command-key-pattern=R \
  --command-miss-tracking=off --command-stats-breakdown=line \
  --print-percentiles=50,90,99,99.9 --hide-histogram
```

## 结果

| 版本 | MSET/s | Avg ms | p50 ms | p90 ms | p99 ms | p99.9 ms | memtier wall s | commit queue peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 旧版：每 Tx 一个 detached coroutine | 12,627.29 | 6.335 | 0.879 | 1.823 | 4.287 | 794.623 | 68.948 | 未提供指标 |
| 诊断版：共享 runner、无队列上限 | 61,042.42 | 1.310 | 0.823 | 2.175 | 4.799 | 101.375 | 60.006 | 1,186,358 |
| 最终版：共享 runner + 每 worker 4,096 高水位 | 52,830.89 | 1.514 | 0.599 | 1.575 | 41.471 | 52.479 | 60.022 | 65,474 |

最终版相对旧版：

- QPS 提高 318.39%，平均延迟降低 76.11%。
- p50 降低 31.85%，p90 降低 13.60%，p99.9 降低 93.40%。
- p99 从 4.287 ms 增加到 41.471 ms：有界版本把旧版少量多秒级全局停顿变成约 1.8% 请求可见的容量背压。尾部上界明显改善，但 p99 是当前明确的后续优化点。
- 无界诊断版虽然高 15.54% QPS，但队列峰值 1,186,358，停止发流后仍需后台排空，不能作为正式实现。最终版队列峰值降低 94.48%，并保持无界吞吐的 86.55%。

## 批处理有效性

最终 60 秒内：

- 3,170,254 个事务由 12,572 个批次处理，平均 252.17 Tx/batch，接近 256 上限。
- 输入 durability fences 为 20,453,535，合并后为 329,306，减少 98.39%（62.11x）。
- `tx_commit_queue_peak=65,474`，低于 16 × 4,096 = 65,536 的配置总水位。
- `tx_commit_backpressure_waits=57,651`，`storage_tx_commits_pending=0`，测试后队列完整排空。
- 测试跨过一次 tx cleaner 周期，defrag 峰值 7 个 active task，没有隐藏关闭后台维护。

## 验证

- `keylane_multikey_e2e`、`keylane_multi_exec_e2e`、`keylane_atomicity_stress_e2e` 均通过；关键三项曾连续运行 3 轮通过，最终 4,096 水位版本再运行一轮通过。
- 多键压力测试验证提交事务数不丢失、批次数少于事务数、合并 fence 数少于输入 fence 数。
- 修复了跨 shard EVAL 在同一参数列表中读取 `tx_writes.front()` 并 move `tx_writes` 的求值次序问题：现在先保存 txid，再转移 receipts。

原始 memtier JSON 保存在 client：`/tmp/keylane-old-mset60.json`、`/tmp/keylane-new-batch-mset60.json`、`/tmp/keylane-final-bounded-mset60.json`。
