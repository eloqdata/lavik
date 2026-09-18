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

# Meta / Cluster Data 管控精简记录

记录日期：2026-09-16。审计基线：`bf35af4`。本文件保存原始 9 点、字段与 SHA
清单以及讨论确认的决策；当前架构见 [Meta](../architecture/08-meta-control-plane.md)
与 [Cluster Data](../architecture/06-cluster-data-plane.md)。

## 实施约束

- 只要求从零启动的新集群正常工作，不承担旧数据目录、旧 wire layout 或混合版本兼容。
- 原地更新协议和持久化布局，沿用主干 `23095f2` 统一后的 v1 标记（包括 control、
  command、Topology、aggregate / Identity / Operation）。不为本改动添加升级、迁移或旧版本拒绝机制。
- 临时构建、日志、测试数据、脚本和缓存全部位于挂载的 NVMe `/mnt/local_nvme`。
- 幂等性、有限租约、boot / assignment / term / action / attempt 边界、fencing 与 drain 保留。
  删除 SHA 不删除业务合法性检查，也不把 TLS 当作状态机一致性或持久化完整性的替代品。

## 原始 9 点

| 编号 | 原始问题 | 实现状态 |
|---|---|---|
| 1 | 同一 FDS 至少六次语义 SHA256 | 已删除 FDS hash 字段、计算和比较 |
| 2 | 大对象传输额外整包 SHA256 | 已删除发送 / 接收摘要与传输字段 |
| 3 | Meta / control 两份手写 SHA256 | 已删除，必要摘要统一使用已有 OpenSSL |
| 4 | Data 保存全部 Group 的 FDS 执行细节 | 已改为启动输入选取；仅保留全局路由、本组控制、Meta 目录和本节点任务 |
| 5 | FDS 与独立 Directive 重复投递完整 payload | 已删除独立任务发送线程 / dispatch tracker；bootstrap 或任务 upsert 直接进入执行 |
| 6 | 通用 evidence 和多阶段回执链 | 已删除 wire、缓存、预算、TTL、命令 / record 字段、序列化及强制阶段状态机 |
| 7 | Topology / Grant 两份 Group 持久化事实 | 已删除 Grant store / snapshot 段；Topology Group 唯一拥有 term、owner 与 authority |
| 8 | Raft apply 整库复制并构造完整 FDS | 已改为 command delta 与受影响记录回滚；apply 不构造 FDS，不分配全 store 编码缓冲 |
| 9 | 两份 Data Group control identity | 已删除 control_groups_；本组 DesiredClusterControl 为唯一控制身份来源 |

原始 9 点均已落地。实现已对齐主干 `23095f2` 的 v1 格式约束；验证与审查记录见文末。

## Data 状态、更新与版本

FDS 只作为启动、重连、重同步输入。Data 选取需要的信息后释放完整输入；不维护
Meta FDS 的内存镜像，也不追踪统一的 Meta applied index。Meta 可以继续用一个一致的
committed view 生成 bootstrap / 更新输入，这不改变 Data 状态的所有权。

| 状态 | Data 保留内容 | 更新范围 / 版本 |
|---|---|---|
| RoutingState | 全局 endpoint、Group members、slots、term、owner / owner assignment、可路由状态 | 只在这些路由事实改变时推进自己的 revision |
| LocalGroupState | 本组 assignment、owner / authority、term、manifest / partition epoch、failover action、复制控制、lease duration | 本组独立 control revision；本组成员 endpoint 改变也触发复制调和 |
| MetaDirectoryState | Meta 地址与身份目录 | 独立 revision |
| Task state | 本节点当前任务与 operation / directive / attempt / directive revision | base_revision → revision 的 upserts / removals |
| ServingState | 从前述状态派生的不可变请求视图 | 保留 boot-local readiness / lease，不作为另一份可修改控制真相 |

Routing / local / directory 以完整对象更新，旧 revision 忽略，同 revision 不同内容拒绝。
任务增量校验 base，精确重试幂等；缺失中间增量则重连并 bootstrap。版本在当前会话
独立推进，bootstrap 的初值不意味着后续追踪该来源 Raft index。
`request_id` 只关联一次更新与 `FullStateApplied` 响应，不承担状态版本作用。
租约 challenge 和 fence 引用本组 control revision；业务 term、assignment、manifest
revision、partition epoch、transition revision 与 attempt 仍各自保留。

其他 Group 的 rebuild 或 candidate 准备不改变本节点所需路由时，不发送更新。
远端 fencing / owner activation / term / membership / slot 变化按路由事实发布；单靠 term
不能表达同 term 下的首次激活，因此路由对象还有自己的 revision。
路由更新不取消本组任务、不刷新 source capabilities、不推进本组租约依赖版本。
本组控制或任务变化在安装前停止旧 completion observer，调和本地执行后再确认。

### 实际任务与控制操作

- `initialize-empty-population`：无源初始化目标 population。
- `authorize-source`：授权源提供指定 population / lineage。
- `rebuild`：目标从已授权源执行 rebuild。
- `revoke-sources`：撤销源授权。
- Task removals：移除已结束 / 取消 / 被替代的期望任务并调和。
- LocalGroupState 更新：本组 failover selection / authorization / activation、owner following、
  membership / population / lease policy 的必要状态；failover 不另外包装通用 evidence。
- RoutingState、MetaDirectoryState 更新：对应的发现状态。
- Fence / FenceAck 与 heartbeat lease challenge / decision：保留现有有限授权与 drain 语义。

任务正文只在 bootstrap 或新增 upsert 中传一次，不再重发另一份 payload 与 FDS 比较。
相同任务身份复用已有本地执行；新 boot 必须重新证明 ready，历史成功不能重新开启服务。

## 第 6 点：幂等请求、响应与最终结果

请求经本地 admission 后返回 `DirectiveResponse.started`，完成后发送 `DirectiveResult`。
开始前拒绝与开始后的执行失败分别表达。Meta 不要求先收到 Started / Completed 等过程
回执才接受最终结果；经 Raft 提交 exact terminal receipt 后返回 `ResultCommitted`。
同 attempt 的相同结果重试返回原确认，冲突结果拒绝；已撤销任务可返回
`ResultNoLongerTracked`，不能伪装成功。

彻底删除：OperationEvidence、ObservationEvidence transfer、MetaOperationEvidenceObs、
EvidenceForOperation / SummarizeOperationEvidence、evidence Admin 注入、MetaEvidenceSummary、
phase command 与 operation record 的 evidence_、对应持久化编解码，以及
Accepted → Started → Completed 强制状态机。任务投递本身由 bootstrap / delta 的确认覆盖。

保留实际业务消费者需要的 heartbeat readiness、history、typed LSN、SourcePaused、
CandidatePrepared、ActionFailed。Meta operation 可能编排多个 directive；单个 directive
完成不等于整个 operation 完成。RPC 指长连接上的带身份请求 / 响应，不引入额外 RPC 框架。

## 第 7 点：单一持久化 Group

```text
Topology
  cluster_lifecycle
  slot_map
  groups[group_id]
    members / assignments / membership_revision
    term / owner
    authority_active / optional activation_action_id
    population_manifest_reference
    partition_replication_epoch
    optional failover_transition
```

term 与 owner 各存一份。fenced Group 可以保留最后 owner；owner 存在不等于有服务授权。
BeginGroupTerm 在 Group 内推进 term 并撤销 authority；ActivateAuthority / failover cutover
原子安装 owner 和 authority。同 term 最多一次授权，撤销旧授权必须推进 term。
AuthorityFor 返回只读派生视图，没有第二份持久化 term / owner。

删除独立 Grant 表、snapshot 段、跨 store Group 集合 / term / owner 相等检查和双写。
manifest 内容仍由独立 immutable content store 持有；session / heartbeat / lease 运行状态
不写入 Topology。持久化 aggregate 现在有 Identity、Topology、Policy、Operation、
PopulationManifest、Audit 六个 store。

## 第 8 点：只 apply delta

普通命令验证后原地修改；复合命令在状态机写锁内保留受影响 Group、Operation 及相关标量，
用于拒绝时回滚。包括自动 failover 抢占的 operation 和被清除的 stale directives。
拒绝不留下部分修改，audit 在拒绝确定后记录。没有 MetaStores 或单个 store 的 apply 副本。

可执行 kind、任务身份、对象数量和容量在领域边界检查；网络投影与编码属于 publisher。
manifest 插入通过计数式 snapshot writer 核算当前字节量及新增文档增量，不创建完整编码串。
这仍可能遍历现有记录计数，未宣称常数时间，也未做性能基准。
只读 proposer preflight / committed-view capture 与 snapshot 自身不属于 Raft apply。

## 第 9 点：Data identity 合并

已删除 PreparedFullState.control_groups_ 与 NodeControlInstaller.control_groups_，也删除了
两份 identity 相等校验、双写与重复查找。控制使用者统一读取 DesiredClusterControl.identity_。
当前 Data 只支持一个本地 Group，输入边界对此作限制；其他 Group 只有路由资料。
ServingState 仍是请求路径的派生视图；复制模块实际 ready / history / LSN 与期望控制分别拥有。

## 字段清单

| 字段 | 冗余依据 | 已完成的处理 |
|---|---|---|
| `MetaDirectiveSpec.preconditions_` 及 wire/local 副本 | 当前可执行类型要求为空 | 已删除字段、编解码和对应分支 |
| directive `force_` / `force` | 当前可执行指令要求 false | 已删除 |
| directive `storage_mutating_` / `storage_mutating` | 可由 kind 唯一确定 | 已改为按 kind 判断；保留并发 mutation 排斥规则 |
| FDS 内 `WireProjectedDirective.basis` | 必须等于外层 FDS 的来源 index | 已删除内嵌 basis；本地执行只绑定本组 control revision |
| `MetaClusterLifecycleState.terminal_outcome_` | 由 `state_` 唯一确定 | 已删除持久化副本，由 state 表达 |
| `MetaClusterLifecycleState.revision_` | 当前状态机严格固定为 0、1、2 | 已删除存储字段，通过 `Revision()` 派生并保留输出值 |
| `MetaNodeRecord.capability_mask_` 及注册/更新命令中的副本 | 当前只有保存、编解码、内容相等/效果验证，没有能力协商决策 | 已从命令、节点记录及编解码删除 |
| candidate `applied_flow_vector_` | typed LSN vector 的文本副本 | 已删除字符串副本，直接使用 typed LSN vector |
| candidate `backlog_coverage_` / `readiness_` | 生产心跳路径写固定 `complete` / `ready` | 已删除固定字符串；诊断输出实际 storage/population readiness；旧 Admin 注入参数同步删除 |


额外删除：独立 Grant 持久化字段、control_groups_、evidence_ 及通用过程回执字段。
projection_hash 和 source_meta_applied_index 已移除；租约 / fence 的依赖字段现在是本组
control_revision，更新确认额外使用 request_id。

## SHA 用途与结论

| 用途 | 状态 | 边界 |
|---|---|---|
| 同一 FDS 反复 SHA、`projection_hash` 本身 | **已删除** | 由会话内对象版本和业务 term / assignment / attempt 拒绝过期操作 |
| Transfer 整包 SHA | **已删除** | 保留逐帧 CRC32C、顺序、长度、对象身份与尺寸检查 |
| Meta / control 两份手写 SHA 算法 | **已删除** | 剩余用途统一使用 OpenSSL 实现 |
| 每次投影重新计算已入库 manifest 的 SHA | **已删除** | 投影解析不可变文档引用；保留入库/恢复边界验证 |
| `manifest_digest` 本身 | **保留** | 内容寻址及复制身份的一部分；删除重复计算不等于删身份字段 |
| operation `intent_hash` | **保留** | archive 不保留完整 intent，仍需重放指纹；各层重复计算可继续收敛 |
| 创建流程的确定性子任务 ID 派生 | **保留现状** | 当前也使用 `MetaSha256()`，属于身份派生，不是 FDS/传输校验 |
| WAL / snapshot 完整性校验 | **保留** | 保护持久化恢复；原始清单涉及这些校验，但它们并非本次删除的 SHA |


## 验证与审查记录

所有命令使用 NVMe 构建与临时目录：`/mnt/local_nvme/lavik-f2b2`。
`LAVIK_TEST_DATA_DIR=/mnt/local_nvme/f2t` 指向同一测试目录；成员恢复的长场景名
额外使用显式短目录 `test-data/mr` / `test-data/mr1`，使 Unix socket 路径小于系统上限。
Debug / Clang 18，kernel bypass 关闭；未运行 ASan、TSan 或性能基准。

- 初始基线上的 `ctest --test-dir build -j 4 --output-on-failure` 完整运行 1,400 个
  注册项，耗时 3,141.94 秒。首次结果有 6 项失败；不是一次全绿运行。
- 其中 3 项是旧模型断言：两份 term 的预期与大诊断文本缓存预算的预期。更新后相关
  25 项测试通过；保留拒绝时字节级状态不变、authority 和 typed health 的检查。
- 旧 evidence 进程 gate 已替换为 `gate_observation_lifecycle`，覆盖 term 推进后的
  缓存清除、旧 term 拒绝、当前 term 重新汇报、leader 切换和 committed history 收敛；通过。
- 另外两项分别是成员恢复的 Unix socket 路径过长，以及 tomb-raider 重启连接超时。
  前者用 NVMe 短目录复测通过；后者单项复测通过，没有修改存储代码或放宽超时。
- 全量运行中原有 3 个大集合测试禁用，`cluster_hardware_safety_gate` 按环境跳过。
  已启用的其他测试通过，包括 14 个 failover 场景和完整数据端到端套件。
- 整合主干 v1 变更后，`cmake --build build -j 10` 通过；
  `ctest --test-dir build --output-on-failure -j 4 -R '^(meta\.|cluster_status\.|cluster_model\.|[A-Z])'`
  的 1,328 项测试通过，3 项原有禁用测试未运行。
- 整合后复测 `gate_recovery`、`gate_observation_lifecycle`、`gate_data_control`、
  `gate_cluster_create`、`gate_failover_prepared_leader_resume`，5 项全部通过。
- 整合后的 `gate_membership_recovery` 在新的 NVMe 短目录 `test-data/mr1` 通过。
- `git diff --check`、clang-format 23.1.0 修改行检查与修改的 Python 脚本语法检查通过。
- Standards / Spec 两轴审查完成：修复 bootstrap 传输期间的旧任务取消检查、未知
  directive kind 的领域边界拒绝，清理残留 API / 注释。主干整合复审确认 v1 标记、
  六个 store、Group 唯一 term / owner 和 evidence 删除一致；陈旧版本说明已删除。
