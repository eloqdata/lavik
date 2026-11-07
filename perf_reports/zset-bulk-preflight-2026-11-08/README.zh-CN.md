# ZADD 大批次预判优化

[English](README.md) | **简体中文**

在新 main `a9756dca` 上，三组配对的成员吞吐变化为 **+27.90% / +30.72% / +29.69%**。只代表有序端点外的批量装载，不是 ZINCRBY 或 Gaming 混合负载的收益。

六轮均从空库开始，顺序 main／候选／候选／main／main／候选。每轮 100 万个 32 B member，score=序号×1024；8192 成员/批、1 连接、1 线程，共 123 条 ZADD。每轮后核验全量 member 与 score。计时覆盖命令完成，不包含随后等待持久化排空。

| Visit | Binary | Elapsed s | Members/s |
| --- | --- | ---: | ---: |
| bulk-1-main-load | main | 65.156270 | 15347.7 |
| bulk-2-bulk-load | bulk | 50.944261 | 19629.3 |
| bulk-3-bulk-load | bulk | 50.509178 | 19798.4 |
| bulk-4-main-load | main | 66.025405 | 15145.7 |
| bulk-5-main-load | main | 65.936463 | 15166.1 |
| bulk-6-bulk-load | bulk | 50.840372 | 19669.4 |


服务端 16 vCPU、128 GiB，独立压测机。此次诊断暂停 defrag 与 tomb raider，SPDK completion poll=16，DPDK 内存 8 GiB；不等同于正常后台维护的 Gaming 测试。双方配置相同，计时内不运行 perf 或连续监控。系统维护定时器按阶段暂缓并恢复。

故障注入 14 项、Release 9 项均通过且无跳过，检查覆盖条件标志、重复成员、既有成员改分、双索引、TTL、故障原子性与恢复。

[结果及构建身份](results.json)；复现命令及限制见 [English](README.md)。
