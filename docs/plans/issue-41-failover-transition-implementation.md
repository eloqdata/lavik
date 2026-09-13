# Issue #41 Failover Transition 实施计划

> 状态：本地实施计划 v2，2026-09-13；已纳入实现前设计复核结论。
>
> 实现分支：`codex/issue-41-failover-transition`，基线
> `a968d002c839828102a44316d43eb7dd63f03f09`（最新 `origin/main`，已包含 #40
> 和原子 Cluster Create）。
>
> 关联 issue：[#41](https://github.com/thweetkomputer/keylane/issues/41)、
> [#42](https://github.com/thweetkomputer/keylane/issues/42)、
> [#45](https://github.com/thweetkomputer/keylane/issues/45)、
> [#46](https://github.com/thweetkomputer/keylane/issues/46)。

本文是实现顺序和验证契约，不是当前架构说明。实现完成后，以源码、测试和
`docs/architecture/` 为当前事实源；本文保留为变更历史。

## 1. 目标和范围

本期把 failover 实现为一个随 Group topology 一起复制的、可由新 Meta Leader
恢复执行的 `FailoverTransition`，并完成以下闭环：

1. 人工发起的 Controlled Failover；
2. Controlled Source 丢失后的原地降级；
3. transition 已存在后的完整 Uncontrolled Executor；
4. Cutover 后所有非 Owner 由 Full Desired State 自动 Follow Owner；
5. stale action、stale observation、Meta/Data restart 和 leader change 下的安全收敛；
6. operator result、committed audit、结构化日志及真实多进程验证。

本期不实现：

- #42 的自动 SUSPECT detector、policy 和自动 Begin driver；
- replid2/secondary history（#46）；
- staged population replacement（#45）；
- parent-to-child history bridge；
- Meta rebuild queue、FULL admission controller 或串行 rebuild；
- durable candidate failure list、fallback cursor、frontier 或 observation；
- standalone cleanup/rebuild operation；
- mixed-version schema negotiation。

## 2. 已验证基线和分支策略

- 当前分支从最新 `origin/main` 新建；旧分支
  `codex/issue-41-controlled-failover` 原样保留，不能整体合并或 rebase 到实现分支。
- Debug 构建目录位于
  `/mnt/local_nvme/keylane-issue41-redesign/build-debug`。
- `keylane_unit_tests`、`keylane_meta_tests`、`keylane-meta`、`keylane-ctl`、`keylane`
  已完整编译。
- 最新基线 `a968d00` 上 582 个 unit tests（86 suites，约 23.6 秒）和 468 个
  Meta tests（44 suites，约 48.0 秒）通过。W0 的 process-support 5 个 tests 及
  population、ReplicationManager、serving-generation、rebuild-failure、
  rebuild-protocol 5 个独立 CTest cases 也全部通过（后五项约 130 秒）。
- `keylane_cluster_replication_manager_integration_test` 已在 NVMe build 中完整编译；
  它的现有 fixture 已验证会忽略 `TMPDIR` 并硬编码 `/tmp`，因此未将中断的
  运行计为 baseline。W0 先对 test-support temp-root 做无行为变更的机械性修正，
  再在 NVMe 上运行并记录 ReplicationManager/population/process-support baseline，
  然后才改 failover 行为。
- 后续 build、test workdir、日志和进程数据全部放在
  `/mnt/local_nvme/keylane-issue41-redesign/`；所有测试命令显式设置
  `TMPDIR=/mnt/local_nvme/keylane-issue41-redesign/tmp` 和
  `KEYLANE_TEST_TMPDIR=/mnt/local_nvme/keylane-issue41-redesign/tmp`。W0 的六个
  helper-based targets 未新建 `/tmp/keylane-*` 产物。

旧分支只按函数级 diff 选择性移植：

- Cluster prepared promotion 的严格 activation 校验；
- finite lease 对 active-expiration/frontier-mutating storage authority 的约束；
- stable replication export scope；
- 同一 source history 的 Cluster CONTINUE/reconnect；
- FDS replacement 中旧 action admission/join barrier；
- 相应的 race/fault 测试场景。

以下旧设计不得移植：

- `MetaFailoverRecoveryStore` 和第八个 snapshot store；
- committed phase、frontier、proof、recovery generation；
- source-history hold、frozen source/proof、recovery handoff；
- failover one-shot directive、terminal receipt/evidence 驱动；
- candidate attempt history、failed set、fallback cursor；
- serving/rebuild/cleanup phase；
- 给通用 `BeginGroupTerm` 或 `ActivateAuthority` 增加 failover-operation coupling。

## 3. 实现红线

以下约束在所有波次中同时成立：

1. **唯一 durable workflow state**：每个 Group 最多一个 optional transition；
   `MetaStores` 仍只有七个 Store。
2. **Owner 语义**：Controlled Cutover 前 committed Owner 和 grant 不变；
   Uncontrolled Begin 立即推进 term、撤 grant并 fencing，但 topology Owner 暂时仍是旧值。
3. **进度不持久化**：paused frontier、candidate applied vector、prepared result、
   liveness 和 action failure 都是 leader-local Observation。
4. **一个 action 一个身份**：每次选择都生成新随机 128-bit `action_id`；换 action
   必须同时清掉旧 authorization。
5. **不跨 domain 比 LSN**：先按 `source_group_term` 和 canonical domain 选择 domain，
   再在 exact domain 内复用现有 `StrictlyDominates` + maximal-set/vector-deficit
   selector。
6. **Data desired-state 驱动**：action replacement/removal 就是 cleanup request；不依赖
   one-shot abort/cleanup 消息。
7. **FullStateApplied 是 capability barrier**：ack 前旧 action 已不能继续 admission、
   发布结果或 activation；replication catch-up 本身可异步继续。
8. **authority 与 preparation 绑定**：只有 grant 的 `activation_action_id` 与本机
   boot-local prepared context 完全一致，Data 才能激活。
9. **pause frontier 必须稳定**：客户端写、`PUBLISH`、active expiry 和其他会
   推进 dataset/replication frontier 的路径全部进入同一个可 drain 的 pause
   边界；读和 finite lease heartbeat 不停。Tomb Raider 是不推进 frontier 的
   物理 cleanup，本期不把它纳入 stable-watermark barrier。
10. **不主动毁掉恢复数据**：Uncontrolled Cutover 前非 Candidate 都是 Preserved
    Replica；不发起 destructive FULL，也不故意掐断旧 Owner 已建立的 compatible flow。
11. **确定性 apply**：apply 不读时钟、不读 Observation；compound mutation 在 bounded
    copy 上全部验证后一次发布。
12. **schema 原地替换**：当前格式尚未发布，直接修改 schema 和 fixtures，不 bump
    version，不保留 legacy decoder。
13. **Controlled Operation 有界终止**：Operation 在 Begin 前后都有 typed terminal
    path；无 Candidate、Group 已被其他 transition 占用、无合法 Owner/grant 或
    deadline 到达都不能留下永久 `Submitted` 记录。
14. **Cluster lifecycle 隔离**：failover Operation 提交、八个 typed command 和
    snapshot restore 都要求拓扑 lifecycle 为 `Created`；`Uninitialized`、`Creating`
    和 `ProvisioningFailed` 不得与 failover 并行。

## 4. TDD seam

用户已确认以下四个 seam。每个纵向切片先写一个最小失败测试，确认失败原因正确，
再写实现；不提前批量写后续测试。

| seam | 测试入口 | 证明内容 |
|---|---|---|
| Meta command/apply/snapshot | `meta_model_test`、`meta_stores_test`、`meta_state_machine_test` | durable model、codec、原子 apply、replay、snapshot/restart |
| FDS + heartbeat wire roundtrip | `control_protocol_test`、`meta_control_projector_test`、`meta_observation_test` | committed projection 和 boot-local observation 不串层 |
| Data full desired-state reconcile | `node_control_test`、`meta_client_test`、`replication_manager_integration_test` | pause/action/activation/follow 的 level-triggered cleanup barrier |
| 真实多进程 failover | 新 `tests/meta_integration/gate_failover.py --case=<name>`，每个 case 独立 CTest | operator→Meta→Data→Redis 的完整安全和可用性闭环 |

日常只运行当前切片的测试和必要的编译目标；完整单测、Meta integration 和全量 ctest
只在最终验证波次统一运行一次。

## 5. Committed model

### 5.1 Durable 类型

在 `include/keylane/meta/commands.h` 定义命令 codec 和 topology snapshot 共享的 durable
failover 值类型；Store 层只依赖这些纯值类型，不引入另一个 Store 的 record。
`successor_grant` 的类型明确是已定义在 `commands.h` 的 `MetaGrantSpec`，不是
带 owner/revision 的 `MetaGroupGrant`：

```text
MetaFailoverMode = controlled | uncontrolled
MetaFailoverLoss = none | unknown

MetaFailoverCandidate {
  node_id
  assignment_id
  boot_id
}

MetaFailoverCompatibilityDomain {
  source_group_term
  source_node_id
  source_assignment_id
  source_boot_id
  source_history_id
  flow_count
}

MetaFailoverAuthorization {
  authorized_revision       // Authorize command 的 Raft apply index
  loss_if_cutover
}

MetaFailoverCandidateAction {
  action_id
  candidate
  domain
  optional authorization
}

MetaControlledFailover {
  operation_id
  absolute_deadline_unix_ms
}

MetaFailoverTransition {
  transition_id
  revision                  // 最近一次 transition mutation 的 Raft apply index
  mode
  target_term
  successor_grant: MetaGrantSpec
  optional candidate_action
  optional controlled
}
```

所有 identity、enum、字符串、flow count、deadline 和 grant 字段都在 command codec、
topology-store primitive 和 aggregate restore 三层验证。`controlled` 只允许出现在
Controlled mode；Uncontrolled 可以没有 action；authorization 只能属于当前 action。

transition 明确不保存：

- manifest revision/digest；
- partition replication epoch；
- config epoch、membership revision；
- candidate local history/readiness/frontier；
- paused frontier、prepared hash；
- failed candidate/action history；
- fallback cursor 或 outcome history。

### 5.2 存储位置

- `MetaTopologyStore::GroupState` 增加
  `std::optional<MetaFailoverTransition> failover_transition_`。
- `MetaTopologyGroupView` 暴露相同只读字段。
- topology store 提供 narrow primitive：查询、install/replace、clear transition；primitive
  只负责本 Store 的结构和 revision 规则，cross-store 规则由 `state_apply.cpp` 负责。
- `MetaTopologyStore::Serialize/Deserialize` 在原 schema 中编码 optional transition；
  Group 排序和 deterministic bytes 规则不变。
- `MetaStores::Serialize/Deserialize` 仍编码原七个 store blob；不增加 snapshot 模块。
- 在最新 main 的 command v2、topology-store v2 和 grant-store v1 布局上原地修改；
  同步替换 `MakeTopologyBlob` 等 raw-layout fixtures，不升级任何现有版本。

aggregate restore 在发布任何 Store 前验证以下跨 Store invariant：

- cluster lifecycle 必须为 `Created`，其他 lifecycle 不允许任何 active transition；
- Controlled transition 引用 exact、nonterminal、`kind=failover` Operation，且
  typed intent 的 group/deadline 与 transition.controlled 一致；
- Controlled 状态为 `current_term + 1 == target_term`、current active grant 存在；
  Uncontrolled 状态为 `current_term == target_term`、fenced/grantless；所有加法先检查溢出；
- successor `MetaGrantSpec` 合法且引用 active policy；
- Candidate 为 current member，node/assignment 精确匹配，且 domain 结构合法；
- transition revision 非零，authorization 只属于 current action，
  `authorized_revision` 和 mode/loss 结构合法。

### 5.3 Authority activation identity

`MetaGroupGrant` 增加 optional `activation_action_id`：

- 普通非 failover grant 为 null；
- failover Cutover 必须写入 winning action id；
- grant snapshot、FDS projection、hash 和 equality 都包含它；
- Data 只有在该 id 与 boot-local prepared context 匹配时才能 activation；
- 后续普通 grant renewal 保留当前 activation id，不能静默清除或替换；
- 新 term、fence 或显式非-failover authority installation 才按各自语义清除。

### 5.4 Command-only preconditions

为防止 observation 检查和 Raft apply 之间的 stale proposal，Begin/Cutover 命令携带
一次性 expected anchors；这些字段用于 CAS，但不复制进 transition：

- current owner node/assignment；
- group membership revision；
- group term、authority version、grant revision；
- manifest revision/digest；
- partition replication epoch；
- group config epoch；
- operation revision（Controlled 命令）；
- transition id/revision 和 action id（后续 mutation）。

不使用全局 topology epoch 作为 Begin 的无关 CAS；其他 Group 的合法提交不能使本
Group failover 饥饿。Cutover 自己携带绝对的新 topology/config epoch，并复用当前
`ActivateAuthority` 的 gap-free 规则。

### 5.5 Successor grant 唯一来源

本期不允许 operator 或 reconciler 为 failover 发明一个新 grant spec。两种 Begin
都精确快照 Begin 前 current active grant 的完整 `MetaGrantSpec`，并把它写入
transition：

- Begin command 携带 current grant revision 和 exact spec CAS；
- aggregate apply 复用现有 grant validator，校验 lease duration 可编码、非零，
  且 spec 引用的 policy version 仍为 active；
- Group 没有 active current grant 时，Controlled request 走 pre-Begin Abort，
  Uncontrolled Begin 拒绝；
- active transition 的 successor policy 计入 `RetirePolicy` 的 in-use 检查；
- Controlled Degrade 保留 Begin 时已验证的 exact successor grant。

这一规则同时定义 Controlled 和 Uncontrolled 的 lease/policy 来源；后续若需要
在 failover 中更换 policy，作为独立的已验证命令扩展。

最新 main 尚无可直接复用的完整 grant-spec validator：grant-local 规则位于
`MetaGrantStore::ValidateActivate`，member/policy/epoch 规则位于 aggregate
`ActivateAuthority` dispatch。实现时提取共享纯 `ValidateMetaGrantSpec`，并把完整 authority
activation 抽为支持 optional `activation_action_id` 的 aggregate kernel；普通 activation
显式清除该 id，failover Commit 写入 winning action。`RetirePolicy` 同时扫描 active grant
和 transition 中冻结的 successor policy。

## 6. 八个 typed commands 及原子效果

`MetaCommandTag` 在 29 后追加 30–37；variant 顺序、codec switch、apply-result tag 上限
和 compile-time variant-size 断言同步更新。

`AbortControlledFailover` 用一个 command schema 表达 Begin 前后两种 form：

```text
MetaAbortControlledFailover {
  operation_id
  expected_operation_revision
  group_id
  optional expected_transition { transition_id, revision }
  bounded_reason
}
```

`expected_transition=null` 是 pre-Begin form，非空是 post-Begin form。两者都验证
Operation `kind=failover`、typed intent group/deadline 和 exact revision；pre-Begin replay 不触碰
后来出现的 unrelated transition。

| Command | 关键 proposal gate 与 deterministic apply CAS | 接受后的原子效果 |
|---|---|---|
| `BeginControlledFailover` | Proposal：Source 是 current Owner exact incarnation，Candidate 是同组不同 member 且 current boot/domain 合格。Apply：Group 无 transition；Operation id/revision 非终态且 group 匹配；Owner、membership、term/authority/grant、manifest/partition/config anchors 精确；current grant spec/policy 合法；`target_term=current+1` 且不溢出 | 安装 Controlled transition，写入 target term、current grant 的 exact successor spec 和首个 unauthorized Candidate Action，revision=apply index；Operation 保持 Submitted；不改 current term、Owner、grant |
| `BeginUncontrolledFailover` | Proposal：可选的首个 Candidate Action 来自 fresh exact-domain observation。Apply：Group 无 transition；Owner、membership、term/authority/grant、manifest/partition/config anchors 精确；current grant spec/policy 合法；`target_term=current+1` 且不溢出 | 快照 exact successor spec；在 aggregate copy 中 begin target term、撤 grant/fence、同步 topology term，安装可含 unauthorized action 的 Uncontrolled transition；Owner 字段不改 |
| `SetUncontrolledCandidate` | exact transition id/revision、Uncontrolled mode；proposer 用 CSPRNG 生成 fresh 128-bit action id；apply 只验证非零且不等于 current action id，不声称检测已清除的历史 id | 原子替换或清空 action/domain；新 action authorization 为空；term 不变 |
| `AuthorizeFailoverPrepare` | Proposal：fresh observations 证明 progress/domain 可授权。Apply：exact transition id/revision/action id；authorization 尚无或是 exact replay | 安装单向 latch，`authorized_revision`=apply index；Controlled 正常路径只允许 `none`，普通 Uncontrolled 新 action 只允许 `unknown` |
| `AbortControlledFailover` | Apply 共通：Operation id/revision 精确、非终态且 group 匹配。Pre-Begin form：本 Operation 没有 Controlled transition，当前另一 transition 若存在也不得触碰。Post-Begin form：exact Controlled transition id/revision | Pre-Begin 只将 Operation 标记 Aborted；Post-Begin 同时清 transition。两者都记录具体原因，不改 Owner/term/grant；Data 由新 FDS 解除 pause |
| `DegradeControlledFailover` | Proposal：Source 确定替换或 disconnect/absence grace 已过，且在 absolute deadline 前提案。只有 exact action 已授权 `none`、CandidateProgress/CandidatePrepared 仍 fresh+eligible，且无 matching ActionFailed/session/population replacement 时可声明 retain。Apply：exact Operation id/revision、transition id/revision 及 action/authorization 快照；apply 不读时钟或 Observation | Operation Aborted；原子 begin/fence stored target term；同一 transition 改 Uncontrolled 并移除 `controlled`。只有 command 声明 retain 且 committed action/authorization 快照精确匹配时保留，否则清 action |
| `CommitControlledFailover` | Proposal：fresh matching CandidatePrepared。Apply：exact Operation id/revision、transition id/revision、`action_id + authorization.authorized_revision`、candidate 和全部 Cutover anchors；current term 尚未推进 | 在 aggregate copy 中 begin stored target term 并 activate：切 Owner、安装 successor grant+action id、推进 authority version 和 topology/config epoch、清 transition、Complete Operation；loss 必须 `none` |
| `CommitUncontrolledFailover` | Proposal：fresh matching CandidatePrepared。Apply：exact transition id/revision、`action_id + authorization.authorized_revision`、candidate 和全部 Cutover anchors；Group 已在 target fenced term | activate winning Candidate、安装 successor grant+action id、推进 authority version 和 topology/config epoch、清 transition；不创建 Operation；loss 取 authorization 中的 `none/unknown` |

实现要求：

- 每个 `Dispatch` 的 summary 是 command + log index 的纯函数；replay 产生完全相同的
  verdict 和 audit record。
- 八个 typed command 统一使用“post-state 先行”replay：先按 command identity 和
  apply index 检查该命令的完整 exact post-effect；全部匹配则 accepted no-op，
  否则才检查 pre-state CAS 并施加 mutation。部分匹配、已被后续合法命令
  推进的 stale retry 或不同 payload 都拒绝，不尝试“补齐”。
- compound command 先复制必要的 bounded stores/aggregate，完成全部 validation 和
  mutation，再移动发布；任一失败不留下半个 term、grant、Owner、Operation 或 transition。
- 两个 Commit 进入 `BeginGroupTerm`/`ActivateAuthority` 内核前，先检查整个
  compound post-state：Owner/term/grant+activation action/authority version/topology+config
  epoch/transition absence，以及 Controlled Commit 的 Operation terminal result 全部精确匹配
  才视为 already-applied no-op；
  任何半应用组合都拒绝。这一整体短路先于内核自身的 replay 谓词。
- Cutover 抽出并复用现有 `ActivateAuthority` 的 validation/apply kernel，不复制第二套
  authority invariant。
- Begin/Degrade 抽出并复用现有 `BeginGroupTerm` kernel。
- generic `completeop`/`abortop` 对 `kind=failover` 拒绝；typed Abort/Degrade/Commit
  内部复用 operation terminal kernel。
- Coordinator durability fail-safe 显式允许 typed `AbortControlledFailover`，但只允许
  exact Operation terminalization 和最多删除 matching transition 的 bounded/shrinking effect；
  它复用现有 candidate stores + `ApplyCommitted` 模拟并比较完整 before/after，确认 term、
  Owner、grant 和 topology epoch 均未改变；Commit、Degrade 和其他会扩大/转移 authority
  的 failover command 仍拒绝。
- deadline、disconnect/grace、CandidatePrepared 和 action readiness 都只是 leader-local
  proposal gate；apply 只验证 command payload 中的 committed anchors/CAS。同时到来的
  Abort/Degrade/Commit 由 Raft apply 顺序决定。
- failover Operation 不使用 `kind_phase_blob`、current directives、terminal receipts 或
  evidence；通用 Operation Store 仍保留这些字段供 cluster-create/membership 使用。
- active transition 期间，任何会改变 Owner、member assignment、term/grant、manifest、
  partition epoch、slot/config anchor 或 transition 引用 policy 的普通命令都必须拒绝。
- `SubmitOperation(kind=failover)` 和八个 typed command 都在 deterministic apply 中
  再检查 lifecycle=`Created`。每条会安装或改变 transition 的命令还要复用/泛化
  `ValidateAffectedFullStateProjections`，在发布 committed state 前证明所有受影响 Data
  projection 都能编码进协议上限。

## 7. Operation 请求模型和 operator 入口

替换当前 phase-oriented `FailoverIntent/FailoverPhase`：

- `FailoverOperationIntent` 只编码 operator 请求：group、absolute deadline 和必要的
  idempotency inputs；successor grant 由 Begin 按 §5.5 快照，不从 operator intent 传入；
- dedicated failover operator handler 继续用底层 `SubmitOperation(kind=failover)` 提供
  永久 operation-id 幂等；通用 `submitop`、`completeop`、`abortop` 在 Admin 和 apply
  两层都拒绝 failover kind；
- operation 可以在没有 phase blob 的情况下保持 `Submitted`，执行中状态由 Group
  transition 派生；Operation 在 Commit/Abort/Degrade 前始终保持 Submitted，status API
  看到 matching Controlled transition 时投影为 Running；现有 Store 允许从 Submitted
  直接 Completed/Aborted；
- `ValidateFailoverProposal` 只校验 request 和八个 typed command 所需的 volatile
  observation，不再校验 phase graph/directive receipt；
- `keylane-ctl failover` 生成稳定 operation id、绝对 deadline 并提交请求；重复请求
  返回同一 operation，不新建 transition。客户端复用最新 `ClusterOperator::Create`
  已建立的 leader discovery、单次发送和 definitely-not-sent/uncertain-outcome 区分，
  不在 CLI 重写重试逻辑；Raft-free request codec 进入 `keylane_meta_admin`。
- executor 在 observation warmup 后发现无合格 Candidate、Group 已被其他
  transition 占用、无合法 current Owner/grant，或 request 在 Begin 前已过
  deadline 时，提交 pre-Begin `AbortControlledFailover`；每个已接受 request 都有
  terminal result。

因此 Begin 不修改 Operation revision，后续 typed command 始终 CAS Submit 产生的
exact revision；本期不新增 Running lifecycle primitive。`getop` 必须从一次 state-machine
快照同时取得 Operation 和 matching Group transition 后派生 Running，不能连续读取两个
Store。`cluster-status` 本期不新增 active-transition wire payload；它仍展示当前 committed
状态，但 member role 必须从 Group 的 current Owner 动态派生，Cutover 后不能沿用初始角色。

## 8. Observation 和选主

### 8.1 Wire/leader-local observation

`CandidateProgress` 增加 boot-local `source_group_term`。Heartbeat 另增 optional
`failover_observation`，与现有 role variant 并列，因为：

- Controlled Source 同时需要 LeaseRequest 和 SourcePaused；
- Candidate 同时需要普通 CandidateProgress 和 CandidatePrepared/ActionFailed。

failover observation 的 typed alternatives：

```text
SourcePaused {
  transition_id
  source node/assignment/boot/history
  source_group_term
  stable_next_lsns[]
}

CandidatePrepared {
  transition_id
  action_id
  candidate node/assignment/boot
  prepared context identity/hash
}

ActionFailed {
  transition_id
  action_id
  candidate node/assignment/boot
  population identity
  bounded failure class/detail
}
```

Observation Store：

- latest-by-node，受 authenticated session generation、TTL、boot 和 committed assignment
  约束；heartbeat replace-or-clear 必须原子；
- leader loss 清空全部 Observation；transition replacement/removal 清理该 transition
  的 Source 和 Candidate facts；action replacement 只清理旧 action 的
  CandidatePrepared/ActionFailed，不得清理同 transition 的 SourcePaused；
- Candidate session disconnect/replacement 立即过滤其 progress/action facts。Source disconnect
  保留 exact-incarnation SourcePaused 到 source grace 结束，但不再刷新；Source boot/
  assignment/history replacement 立即使它失效；
- Meta leader change 后先投影最新 FDS，当前 Data boot 再 heartbeat 重报；
- 新 Leader 因 observation store 刚清空时，单纯“尚未重报”不是 Candidate/Source 已失败。
- exact Candidate 的显式 disconnect、boot/assignment/population mismatch 可立即结束
  当前 attempt；Candidate 失败不推导 Source proof 失效。
- Source 只有 disconnect/absence 时从首次缺失开始等待 bounded source grace；在 grace
  内以同 boot/assignment/history 重报则继续 exact Controlled 路径。Source 以新
  boot/assignment/history 出现是确定的 incarnation replacement，可立即触发
  Degrade proposal。deadline 仍按 committed absolute value 由 reconciler 判断。

Heartbeat 仍必须装进 single-frame 上限。若完整 CandidateProgress 和 failover observation
不能同时容纳，优先保留当前 executor 必需的 failover observation，并省略可在下一次
heartbeat 重报的普通 candidate payload；不得截断 frontier 或构造半个 proof。

### 8.2 Domain selection

重构 `CandidatePlanFor` 为两层：

1. exact compatibility domain 内沿用当前 `StrictlyDominates` + maximal-set/
   vector-deficit/node-id tie-break；
2. Uncontrolled planner 先按 `source_group_term` 降序分组，再按 canonical domain identity
   逐个尝试第一层 selector。

规则：

- 不跨 domain 比较 LSN；
- Controlled Begin 只从当前 Source exact domain 选；
- fallback 到旧 domain 后，较新 domain 恢复不抢占健康 action；
- action 终止或变得不合格时重新扫描全部 live domains，仍从最新开始；
- `ActionFailed` 后该 Data boot/population 停止提供 eligible CandidateProgress，直到 boot
  或 population identity 改变；Meta 不保存 blacklist；
- target-term fence 后恢复的旧 Owner可以作为普通 Candidate，但必须走新 action、
  authorization、prepare、Cutover，绝不恢复旧 grant。

former Owner 是“`source_group_term` 只来自 authenticated live upstream”的唯一显式
adapter：

- 它在 current boot 作为 Owner 时保留 boot-local self-origin provenance；不从任意
  磁盘 population 反推该身份；
- 仅在 matching Uncontrolled transition 的 target-term fence 已应用、旧 grant 已撤销且
  current-boot native population Ready 时可上报 CandidateProgress；
- canonical domain 使用它任 Owner 时的 self node/assignment/boot/history，
  `source_group_term=target_term-1`，frontier 来自稳定 native watermark；绝不声明
  target term 的 source data；
- Data restart 使这个 boot-local self-origin proof 失效；新 boot 只能依靠当前可证明的
  population/upstream 关系重新进入候选集；
- 它被选中后，prepare adapter 复用该 native master population，但仍执行新
  action authorization、history rotation、durability 和 activation 校验。

## 9. FDS 和 Data reconcile

### 9.1 Full Desired State

`WireDesiredGroup` 增加：

- optional `failover_transition`；
- optional current grant `activation_action_id`。

`MetaControlProjector` 只从同一 `MetaCommittedView` 投影，不读取 Observation，也不生成
failover directives。现有 projection hash/object hash 自动覆盖新字段。

projection content 变化与 authority lease 失效必须分开判断。transition-only 变化会改变
projection hash，但只要 Owner、term、authority version、grant revision/spec 和 assignment
anchor 都没变，就保留当前 finite lease；通过独立的 pause/admission generation 让旧写请求
在 final recheck 失败。否则 Source 收到 Controlled transition 时会先变成
`CLUSTERDOWN`，无法满足“读和 lease 继续、写返回 TRYAGAIN”。

`PrepareMetaFullState` 把 wire 值严格校验并归一化为 wire-independent：

- `PreparedFailoverTransition` / `PreparedFailoverAction`；
- 挂入与稳定 replication/export identity 分开的 `PreparedDesiredClusterControl`；
- source/candidate/member/Owner/grant 交叉一致性在 publish 前验证。

`PreparedGroupControlIdentity` 的 whole-object equality 不得因 transition revision、action、
authorization、pause 或纯 authority-control 字段改变 established export scope。实现一个
显式 per-group `EstablishedExportScope` 和 `SameEstablishedExportScope()`，只包含
group/source node+assignment/current-boot history/manifest/partition identity 以及该 flow 的
downstream member assignment。group term、authority version、grant revision/spec/`granted_`、
config epoch、transition/action/authorization/pause 都不参与 established-flow preservation；它们
只分别关闭新 write/source admission。删除或替换现有 comparator 外层的
`new_group->granted_ == old_group.granted_` 等 authority 条件，并把现有 process-wide
scalar verdict 改为 per-group/export-scope decision，避免多 Group 时 last-wins。用 comparator tests
锁定 Controlled Begin、Authorize、Uncontrolled fence 和 action replacement 都不会单因
control metadata 提前撤销已认证 flow。

### 9.2 唯一 Data mutation seam

给 `NodeControlActions` 增加一个 level-triggered 入口，暂定：

```text
ReconcileClusterControl(optional<DesiredClusterControl>)
```

`NodeControlInstaller::InstallFullStateTransition()` 在其既有 replacement barrier 内调用。
ReplicationManager 根据完整 desired group 派生本机角色，而不是接收多个 failover
one-shot action：

- Controlled Source；
- current Candidate Action；
- Preserved Replica；
- Cutover winner pending activation intent；
- steady-state Owner/source exporter；
- steady-state Follow Owner。

相同 desired state replay 必须 no-op；transition/action/Owner/anchor 改变时，先关闭旧
capability、join 旧 admission，再安装新 capability。FDS ack 不等待异步 catch-up 完成。

当前 FDS replacement 无条件清 source authorization 的行为需要拆分：

- 关闭新的 source admission；
- 是否保留已 ONLINE、已认证、same-domain export；
- 是否 drain client/frontier-mutating work；
- 是否 cancel/join population ingress。

Controlled Pause 或 Uncontrolled fence 不得因为一个通用 cleanup helper 而主动切断旧
Owner 仍有价值的 compatible downstream flow。

## 10. Data 端最小新增能力

### 10.1 Controlled Pause

新增 per-group、transition-scoped、boot-local pause context：

1. 在 `ServingState::GroupView` 增加 write/dataset pause 标志，并纳入写 admission token；
   authority lease anchor 不因 transition-only projection 变化而失效；
2. `NodeControlInstaller` 先发布 paused `ServingState`，使新 mutation 无法 admission；
   pause false→true 时显式对旧 state 执行 `RememberDrain`，将它放入独立
   pause-drain 集合，不依赖 authority anchor 变化或伪装成 retired authority；
3. NodeControl 等待该集合的已 admitted mutation 完成后，才让
   ReplicationManager 获取并持续持有 active-expiry pause，capture/cache stable
   watermark，最后才将 SourcePaused 暴露给 heartbeat。`ReconcileClusterControl`
   不得在 NodeControl drain 完成前 capture；
4. 新增 `Decision::kTryAgain`，同步修改 `EmitClusterDecision`、blocking-wait 的
   exhaustive mapping 和单测；Redis mutation admission 在 paused 时映射为 `TRYAGAIN`，
   读继续；
5. 将普通 `PUBLISH` 视为 channel-hash-slot scoped cluster mutation：在异步 dispatch 前绑定
   slot/group，经过 authority/pause admission，并跨 source-worker publisher admission 和
   replication-log append 全程持有该 Group 的 `GroupInFlight`；
6. `MULTI/EXEC` 的 queue-time slot union 同样纳入每个 PUBLISH channel slot，不再
   跳过 `kCmdNoKeys` PUBLISH；多 channel 和键命令按同一 exact-slot 规则拒绝
   cross-slot。以 `has_cluster_mutation = has_write || scoped kCmdMayReplicate` 获取
   aggregate admission/`GroupInFlight`，一直持有到 `PublishLateAdmittedReplicationCommand`
   完成；
7. 新 PUBLISH 在 paused 后返回 `TRYAGAIN`，pause 前已注册的普通或 EXEC
   PUBLISH 完成后 drain 才能结束；用单条和 MULTI/PUBLISH 用例证明
   stable watermark 不漏复制事件；
8. active expiry 使用 nestable `QuiesceExpiration/ResumeExpiration`；drain 后复用
   `CaptureNativeReplicationWatermark()` 并缓存稳定 vector；
9. 同 transition/FDS replay 不重复 pause count；Meta session replacement 不解除；
10. Abort/transition removal 精确平衡恢复原来的 expiration 配置；Source
    boot、assignment、history 变化使 boot-local context 失效并停止上报
    `SourcePaused`，由 Meta 基于 authenticated session facts/grace 推导 Degrade，不冒充
    Candidate `ActionFailed`。

pause 不撤 grant、不停止 lease heartbeat、不清 source backlog、不杀已有 downstream flow。
Cluster 启动时 Tomb Raider 默认 OFF，获得 authority 后可由运维配置启动，但它只
做不推进 replication frontier 的物理 cleanup。#41 不新增 Cluster Tomb Raider 的
enable/resume 机制，也不为 Controlled Pause 新增 pause API；失去 authority、转 follower
或 destructive FULL 时继续复用现有 `QuiesceTombRaiderForReplica()` 安全边界。

### 10.2 Candidate catch-up 和 preparation

- catch-up、FULL/CONTINUE、per-flow frontier 复用现有 replication coordinator；
- authorization 前 Candidate 保持 fenced，不能 prepare/activate；
- matching authorization 到达后，把 action/domain 转成现有
  `StartClusterPromotionPrepareDirective()` 所需的内部 input；
- #40 的 durability、frontier、history rotation 和 `PreparePromotion()` kernel 继续复用，
  但两种 mode 都以 action-bound Cluster admission 取代旧
  `old_authority_exclusion_hash` contract：committed FDS 必须提供
  transition/action/authorization revision/target term 和 exact domain；
- Controlled 中，Meta 只在 fresh SourcePaused + CandidateProgress 已证明 Candidate
  covers stable frontier 后提交 `Authorize(loss=none)`；committed authorization 就是这次
  volatile 检查的 certificate。FDS 和 Candidate Data 不读取、不携带、不重新验证
  SourcePaused/frontier；
- Candidate Data 对两种 mode 都只验证 matching committed
  transition/action/authorization revision、exact local population/domain，并在 prepare 开始时
  冻结自己单调前进的 applied vector。Uncontrolled 再额外验证 committed target
  term 已 fenced、旧 grant 已撤销。因 schema 尚未发布，直接替换 Data-side
  opaque old hash，不为两种 mode 发明新 hash；
- boot-local prepared context 增加 `transition_id + action_id`；
- action replacement/removal 在 FullStateApplied 前使旧 task不能再发布 prepared/failure、
  不能 activation；
- transient transport/resource failure 在同 action 内按现有固定 1 秒间隔重试；不可恢复错误或
  bounded watchdog 到期才报告 `ActionFailed`。
- watchdog只从本地 action已经具备执行条件时计时；等待 Meta authorization或新的 FDS不算
  Candidate失败。
- prepare创建 child history后，当前 control session 的 ClientHello history已经过时；完成
  prepare时主动关闭并重建 Meta session，以当前 child history重新认证，同时保留同 boot的
  action-local prepared context。Leader change测试必须证明重连后仍能重报 CandidatePrepared，
  且 pre-Cutover child history不得提前把 `source_group_term` 声明为 target term。

### 10.3 Activation

从旧分支选择性改写一个薄的 `ActivateClusterPreparedPromotion` adapter：

- FDS reconcile 只校验并 pin pending activation intent（Data boot、assignment、population、
  child history、durability token、grant `activation_action_id` 与 prepared action）；它不直接
  activation；
- 从现有 `ApplyAuthority` 抽出无 mutation 的 lease validation kernel。
  `ApplyLeaseGrantTransition` 先完整验证 exact finite-lease message、deadline、session、
  FDS anchor 和 pending action，但只将它保留在 NodeControl transition 内的不可服务
  provisional slot；此时不调用 `AuthorityGuard::RenewLease`，因此 FDS 即使将
  本机投影为 granted Owner，客户端写仍因无 effective lease 而 fenced；
- 持有 control-transition serialization 期间，使用 provisional lease 授权 matching pending
  intent 调用 activation adapter。activation 成功后重新验证 exact FDS/action/session、
  lease deadline 和 control generation，然后才原子调用 `RenewLease` 打开 write
  admission；失败或过期则丢弃 provisional lease、fence+drain，不得出现已续租
  但未 promotion 的窗口；
- 旧 action、普通 Owner lease 或 lease-before-FDS 都不能误激活。lease-before-FDS
  拒绝后，matching FDS 加下一个 fresh lease 必须能成功激活；
- 把现有 `ActivatePreparedPromotion` 拆为可共享的纯 promotion-role kernel 和调用方的
  expiration 配对责任；standalone `SetUpstream` 路径才恢复它自己持有的 pause，
  Cluster activation 绝不调用未持有 pause 的 `ResumeExpiration()`；
- activation await 后再次检查 provisional lease identity/deadline 和 control generation；
  只有完全匹配才把 lease commit 进 `AuthorityGuard`，失败则 fence 并 drain；
- 只在上述 final recheck 成功后开启 active-expiration/frontier-mutating storage
  authority，复用/移植 deadline-aware `EnableExpirationAuthorityUntil`/
  `SetExpirationAuthorityUntil` 语义，将其绑定到 exact lease generation/deadline；不退化成
  裸 `SetExpirationAuthority(true)` + 独立 timer。

普通 owner lease 不能误激活一个遗留 prepared context；同 action/FDS replay必须幂等。
不可服务 provisional 分支只用于本 boot 存在 matching、尚未 activation 的
prepared context 的首个 lease；同 boot 已成功 activation 后的 exact renewal 直接续租，
不重跑 promotion。Data restart 使旧 prepared/activated runtime context 失效；新 boot 只能
在既有 committed-Owner startup 路径完成 storage/population recovery 后获得 lease，绝不复用
旧 boot 的 prepared context。

### 10.4 Follow Owner

不直接调用会拒绝 Cluster 且过早清 population 的 standalone `SetUpstream()`；抽出
cluster-safe adapter，暂定：

```text
ReconcileClusterFollowOwner(optional<DesiredClusterUpstream>)
```

行为：

- Cutover 后每个非 Owner由 FDS得到同一 level-triggered desired upstream；
- 相同 owner/scope replay不重启健康 flow；Owner或 replication anchors 改变时 cancel/join
  旧 ingress后切换；
- 先 fence旧本地角色并保留 usable population；新 source 已认证且 export-ready 后，才
  进入可能 destructive 的 FULL；
- history兼容则 CONTINUE，否则走现有安全 FULL；
- 连接和 source resource失败由现有 Coordinator 固定 1 秒 retry delay 循环重试，不创建 Meta
  directive/operation；
- former Owner 开始 Follow Owner时在 Data-local role transition 中 retire旧 source
  history/backlog；Meta 不发送 cleanup phase/message；
- source authorization 从 steady FDS relationship + authenticated handshake 派生，不再
  要求一次性 rebuild directive；
- source scope保持稳定且最小，允许同 desired relationship reconnect，禁止跨
  group/assignment/history/manifest/partition identity复用。
- follower从 FDS得到 Owner node/assignment/endpoint/term和 population anchors；source端
  从 FDS得到允许连接的 member node/assignment，target boot由认证握手提供，source
  boot/history由握手响应学习。普通 follower 的 `source_group_term` 只在当前 boot
  建立这条已认证 live 关系后产生；former Owner 只能使用 §8.2 的 current-boot
  self-origin adapter，两者都不能从任意磁盘 population 猜测。

本期允许所有 follower 并发 FULL，也接受新 Owner再次失败时暂时没有 Candidate 的已知
availability gap；用测试明确记录，不在本期暗中实现 staged replacement。

## 11. Meta Failover Executor

新增一个共享 `MetaFailoverReconciler`，复用现有 reconciler 的生命周期骨架：

- 只在 leader运行；BecomeFollower cancel并 join；
- 从 `SubscribeCommitted()` 的 fresh view启动；每次只提出一个 typed command；
- leader change后不保留内存 phase，从 Group transition + fresh observations重新推导；
- uncertain proposal outcome先重读 Committed State，以 transition id/revision幂等恢复；
- Controlled request扫描和现有 transition执行共用一条有界循环；
- 自动 failure detection 不进入本 reconciler。

`app/keylane_meta.cpp` 按 membership、cluster-create、Data-control、failover 顺序注册；
demotion/shutdown 逆序先停 failover，保证新 Leader 的 Data publisher 已能重新收集 observation
后 executor 才开始。warmup/source grace 复用现有 `observation_ttl_ms`，不新增配置旋钮。

leadership observation warmup 复用 Observation Store 已配置的 heartbeat/session grace
上限，不再发明无界等待：Controlled 等待上限是
`min(configured grace, operation deadline - now)`，deadline 先到则 Abort；Uncontrolled
在新 Leader 刚就任时先等同一 configured grace，之后仍未重报才将 action 判为
unavailable。当前 Leader 亲眼看到 exact Candidate session disconnect 则不再等 warmup。

### 11.1 Controlled 推导

```text
submitted request + no transition
  -> wait only for leadership observation warmup
  -> invalid/missing current Owner or grant, no eligible exact-domain Candidate,
     another transition already owns the Group, or deadline reached
       -> pre-Begin AbortControlled
  -> otherwise select current-source exact-domain candidate -> BeginControlled

submitted request + matching Controlled transition
  -> derive Running from the transition and continue below

transition, no SourcePaused
  -> wait/reacquire observation

SourcePaused + Candidate below frontier
  -> wait for existing replication catch-up

SourcePaused + Candidate covers frontier + no authorization
  -> Authorize(loss=none)

authorized + no CandidatePrepared
  -> wait/retry locally; terminal action failure follows failure policy

CandidatePrepared
  -> CommitControlled
```

Failure policy：

每一个 fresh observation cut 按以下固定顺序决策，不依赖代码分支排列：

1. deadline 已到且 committed mode 仍是 Controlled：Abort，不在 deadline 分支临时
   Degrade；
2. deadline 前 Source 已确定替换，或 disconnect/absence source grace 已过：
   Degrade。Candidate 同时失败时在 Degrade 中清 action；仅当它仍 fresh/eligible、
   已授权 `none` 且无 failure/replacement 证据时才声明 retain；
3. Source 仍健康时，Candidate disconnect/restart/population change/ActionFailed/terminal
   promotion failure：Abort；不等 Candidate 重启，不降级。

Source 不发 Candidate `ActionFailed`；它的 disconnect/grace/incarnation facts 由 Meta
从 authenticated session 推导。
- Begin 前的 invalid request state/no Candidate/group conflict/deadline 使用 pre-Begin Abort；
  Begin 后的 Abort 使用 exact transition form；
- Abort、Degrade、Authorize、Commit 竞争由 Raft顺序和 transition revision裁决；
- 每个失败原因写入 Controlled Operation terminal result并对 operator可见。

### 11.2 Uncontrolled 推导

```text
transition without action
  -> scan newest live domain -> SetUncontrolledCandidate

action without authorization
  -> validate current progress -> Authorize(loss=unknown)

action with retained loss=none authorization after Controlled Degrade
  -> keep the exact action/authorization and continue preparation

authorized action without prepared
  -> wait for Data retries/watchdog

ActionFailed/unavailable/stale
  -> current Leader observed exact disconnect/failure, or post-leadership warmup still absent
  -> rescan all live domains -> replace action or clear to candidate-null

matching CandidatePrepared
  -> CommitUncontrolled
```

Uncontrolled 没有总 deadline；没有 Candidate时保持 fenced transition无限等待。健康 action
不因更新 domain恢复而 preempt。替换 action 总是清掉旧 authorization 并以
`loss=unknown` 重新授权；只有 Degrade command 明确保留的 exact action 才能继续
使用 `loss=none`。Cutover提交后 Candidate再失败属于下一次新 term failover。

## 12. 实施波次

### W0：测试工作区和独立 baseline

在任何 failover 行为改动前，先让相关 test-support helper 支持
`KEYLANE_TEST_TMPDIR`（未设置时保留现有默认），并用 helper unit test 证明所有子目录
位于指定 root。这个机械性切片不改产品行为。

然后在 `/mnt/local_nvme/keylane-issue41-redesign/` 下完整运行并记录：

- `keylane_cluster_replication_manager_integration_test`；
- `keylane_cluster_population_integration_test`；
- `keylane_process_support_tests`；
- serving-generation/rebuild integration 中本期会受影响的现有 cases。

出口：上述 baseline 全绿，运行时无新建 `/tmp/keylane-*` 产物；结果写回 §2。

### W1：Committed foundation——candidate-null Uncontrolled tracer bullet

先写 RED：

- 八个 command type/tag 的最小 codec compile/roundtrip test；
- `BeginUncontrolledFailover(candidate=null)` 原子 apply test；
- current grant exact-spec CAS、lease/policy validation 和 policy-retirement reference test；
- exact replay、stale precondition、任一半边失败均不改 state；
- topology store 和 aggregate snapshot roundtrip；
- MetaStateMachine snapshot/restart 保留同一 transition；
- 四种 `MetaClusterLifecycle` matrix 证明只有 `Created` 可 submit/begin/restore failover；
- command v2、topology v2、grant v1 的 raw-layout fixtures 原地替换。

再实现：durable types、topology optional transition、grant activation id、codec、
`BeginUncontrolled` dispatch、successor grant snapshot/validator、snapshot、aggregate invariant 和
active-transition mutation guard。

出口：无 Candidate 也能在一个 commit 中持久推进 target term并 fencing；重启/重放不重复
推进 term，仍无第八个 Store。

### W2：八命令完整 apply 与 Operation terminal coupling

逐条先 RED 再 GREEN：Controlled Begin、candidate replacement、Authorize、pre/post-Begin
Abort、Degrade、两种 Commit。抽取共享 BeginTerm/Activate kernels；阻止 generic
completeop/abortop。八命令 replay matrix 覆盖每条 exact post-state no-op、stale
transition/action/operation revision 和 partial post-state 拒绝；两种 Commit 额外覆盖
内核前 aggregate shortcut 和每种 half-applied state。

出口：mode matrix、loss规则、authorization latch、action replacement cleanup、Operation
从 Submitted 在 Begin 前后都可 typed terminal、successor grant 不可非法封存，且所有
compound atomicity/replay tests 通过。

### W3：FDS、heartbeat 和 Observation

先写 wire roundtrip/hash/size/stale-session RED；再实现 transition projection、activation id、
`source_group_term`、former-Owner current-boot self-origin adapter、三种 failover observation、
Observation Store replace/query/filter。测试分开 Candidate 显式 disconnect、Source 显式
disconnect、Source pure absence/grace 和 Source incarnation replacement。

出口：leader change清空后可由最新 FDS+当前 heartbeat重新取得状态；任何 observation不进
snapshot；stale boot/action/FDS均不能推进。

### W4：Data full desired-state seam 和 Controlled Pause

先写 `NodeControlInstaller` recording adapter tests，固定相同 FDS no-op、
per-group `SameEstablishedExportScope`、grant true→false/term/control-only 变化仍 preserve、
multi-Group verdict 不 last-wins、replacement cleanup 和 FullStateApplied barrier；再接
`ReconcileClusterControl`。随后逐项覆盖 write、普通 PUBLISH 的 channel-slot/admission/
in-flight、MULTI/EXEC slot union + late publish、pause-only `RememberDrain`、
“publish paused state → wait old GroupInFlight → quiesce expiry → capture → expose
SourcePaused”顺序、stable frontier、session replacement、`TRYAGAIN` 两处映射和 Abort
恢复。Tomb Raider 只回归已有 OFF/configure/role-transition 行为，不新建 pause
API。

出口：pause期间 GET/heartbeat/lease继续，所有 mutation返回/表现为 TRYAGAIN或停止；两次
watermark一致；transition removal无 leaked pause count。

### W5：Candidate prepare、watchdog 和 action-bound activation

在独立 `keylane_cluster_replication_manager_integration_test` target 复用 #40 fixture，
先写 Controlled SourcePaused 和 Uncontrolled fenced-term 两种 action-bound authorization gate、
stale action cleanup、prepare replay、former-Owner native population prepare、activation identity、
Meta 授权作为 volatile SourcePaused/frontier coverage 的 committed certificate且 Candidate FDS
不携带该 Observation、FDS-before-lease 只 pin pending intent、lease-before-FDS 不误激活、
exact lease 先只进入 NodeControl 不可服务 provisional slot、activation + final recheck 后才
`AuthorityGuard::RenewLease`、lease-before-FDS 后 matching FDS+fresh lease 的 liveness、
Cluster activation 不会 unmatched
`ResumeExpiration`、deadline-aware expiration authority 和 lease-expiry race RED；再改写旧分支
可复用内核。

出口：未授权永不 prepare，旧 action永不 activate，matching Cutover仅激活一次，failure
在 bounded watchdog后报告且同 population不热循环。

### W6：Controlled Executor

以 fake committed stream + fake observations 写 reconciler决策测试；覆盖 normal、Begin 前
无 Candidate/group conflict/invalid grant/deadline 的 terminal Abort、每个等待点的
Candidate 失败、Source disconnect grace/recovery/incarnation replacement、deadline/commit Raft-order
race、leader cancel/resume、proposal reply 丢失后重读 committed state。然后接 operator
CLI/status，构建 `keylane-ctl`、`keylane_cluster_status_tests` 和新的 failover
CLI/operator request/terminal-status tests。operator 路径复用 `ClusterOperator` 的 leader
routing 与 uncertain-outcome seam；`getop` 用一次原子 committed cut 派生 Running，status
在 Cutover 后按 topology Owner 渲染新角色。

出口：正常路径 `loss=none`；Candidate failure 必 Abort；Source 仅断连先等 grace，
确定失效只在 deadline 前 Degrade；无论是否 Begin，operator 总能读到 Controlled
terminal cause。

### W7：Uncontrolled Executor 和 domain fallback

先扩展 candidate planner tests，再写 candidate-null、replacement、fallback、newer-domain
non-preemption、action failure eligibility、former Owner self-origin provenance/re-entry/restart loss、
retained-none authorization 和无限等待测试。

出口：transition一旦 Begin就始终 fenced到成功 Cutover；失败 Candidate不阻塞RTO；不跨
domain比 LSN；replacement/fallback loss为 unknown，保留 exact authorized action才可保持
none。

### W8：Post-Cutover Follow Owner

先写 ReplicationManager integration RED：相同 desired replay、owner change、source未ready、
same-history reconnect CONTINUE、history mismatch FULL、former-owner cleanup、并发 follower。
再抽取 cluster-safe follow adapter和 steady source authorization。

出口：Cutover删除 transition后所有非 Owner自动订阅新 Owner；无需 Meta rebuild op/directive；
已有 compatible旧 flow不因 fence被主动切断。

### W9：真实进程、fault matrix、日志和文档

新增共享 `gate_failover.py --case=<name>` driver，每个 case 独立 `add_test`，显式
传入 `keylane-meta`、Data binary、`keylane-ctl` 和 `redis-cli`；不放入只传
`keylane-meta` 的 `KEYLANE_META_INTEGRATION_GATES` 列表。每个 case 至少启动 3 个
Meta、一个 Group 的 3 个真实 Data Node 和 Redis client；先按 controlled、
recovery、partition 三类拆分，测量后设置每个 CTest timeout。driver 复用
`gate_cluster_create.py`/`harness.py` 的 `DataProcess`、leader discovery 和
`wait_cluster_ready`；Genesis 接受后必须等待 lifecycle=`Created` 且 READY 才开始
failover。覆盖：

- Controlled pause 时旧 Owner GET成功、SET返回 TRYAGAIN；
- Cutover 后新 Owner SET/GET成功；
- former Owner和第三 replica自动 Follow Owner并收到 post-cutover write；
- 任意时刻不存在两个不同节点同时成功写；其中 partition case
  保持旧 Owner 进程和 Redis 端口存活，仅 blackhole Meta-control 路径，验证 finite
  lease 到期后旧 Owner 失写权，新 Owner 才获权，heal 后旧 Owner 收敛；
- 分别覆盖 RST/显式 disconnect 和 half-open/pure absence 的 grace 行为；
- Meta Leader 在 pause/auth/prepared/Cutover append 前后更换，以及 Cutover reply
  丢失后的 replay；
- Candidate 在 prepare 前/中、Cutover commit 前，以及 Cutover 已 committed 但新
  FDS/lease activation 尚未完成时失败；Controlled 前两者 Abort，后者属于新 term
  recovery。因 #42 的自动 detector/Begin driver 不在本期，process test 由
  harness/operator 显式提交下一次 `BeginUncontrolledFailover`，不假设自动触发；
- Source失败触发 Degrade并由另一 Candidate完成 Uncontrolled；
- candidate-null等待后恢复；
- 延迟/乱序的旧 lease、FDS、SourcePaused、CandidatePrepared、heartbeat 和
  ActionFailed 都不会推进或激活 stale action；
- Cutover后新 Owner再次失败时，已知 availability gap得到明确、非误导性断言。

结构化日志至少包含 transition id、action id、group、mode、event、loss、commit index；
candidate replacement、domain fallback、degradation、Cutover都记录。日志at-least-once，
committed audit为权威；metrics不使用 node/action/transition等高基数 label。

同步更新：

- `CONTEXT.md` 和已接受 ADR（术语/决策）；已接受的 ADR 统一采用
  现有 `## Status` heading，其值为 `Accepted`；
- `docs/architecture/01-overview.md`（leader 可恢复 durable workflow）；
- `docs/architecture/02-request-serving.md`（pause/`TRYAGAIN` admission）；
- `docs/architecture/04-storage-and-recovery.md` 和 `docs/operations/tomb-raider.md` 做
  claim audit；本计划保持 Cluster 默认 OFF/不纳入 frontier barrier，只在实现使
  当前 claim 失真或不完整时改文档；
- `docs/architecture/05-replication.md`；
- `docs/architecture/06-cluster-data-plane.md`；
- `docs/architecture/08-meta-control-plane.md`；
- `docs/architecture/README.md`；
- `docs/operations/meta-control-plane.md`；
- 必要时 `docs/operations/README.md`。

W9 建立 issue #41 acceptance criterion → unit/integration/process test 的逐条证据表。提交
前显式 `git add` 本分支预期交付的新文档：`CONTEXT.md`、`docs/adr/`、
Redis research 和本 implementation plan；不将未跟踪状态当成“已在提交中”。

## 13. 文件级改动地图

| 模块 | 主要文件 |
|---|---|
| Durable schema/codec | `include/keylane/meta/commands.h`, `src/meta/commands.cpp`, `include/keylane/meta/failover.h`, `src/meta/failover.cpp` |
| Topology/grant/snapshot | `include/keylane/meta/topology_store.h`, `src/meta/topology_store.cpp`, `include/keylane/meta/grant_store.h`, `src/meta/grant_store.cpp`, `src/meta/state_apply.cpp` |
| Operation/proposal | `include/keylane/meta/operation_store.h`, `src/meta/operation_store.cpp`, `src/meta/coordinator.cpp` |
| Executor/admin/status | 新 `include/keylane/meta/failover_reconciler.h`, 新 `src/meta/failover_reconciler.cpp`, `app/keylane_meta.cpp`, `app/keylane_ctl.cpp`, `src/meta/ctl_server.cpp`, `include/keylane/meta/state_machine.h`, `src/meta/state_machine.cpp`, cluster status相关文件；typed submit/poll client 必须进入 Raft-free `include/keylane/meta/admin_client.h`, `src/meta/admin_client.cpp`/`keylane_meta_admin` |
| Observation/planner | `include/keylane/meta/observation_store.h`, `src/meta/observation_store.cpp`, `include/keylane/meta/candidate_plan.h`, `src/meta/candidate_plan.cpp` |
| FDS/wire/projector | `include/keylane/cluster/control_protocol.h`, `src/cluster/control_protocol.cpp`, `src/meta/control_projector.cpp`, `src/cluster/meta_control.cpp` |
| NodeControl/client | `include/keylane/cluster/topology.h`, `include/keylane/cluster/node_control.h`, `src/cluster/node_control.cpp`, `include/keylane/cluster/meta_client.h`, `src/cluster/meta_client.cpp` |
| Authority/Redis admission | `include/keylane/cluster/authority.h`, `src/cluster/authority.cpp`, `src/redis/command.cpp`, `src/redis/blocking_wait.cpp` |
| Replication/storage | `include/keylane/replication.h`, `src/replication/replication.cpp`, `include/keylane/storage/engine.h`, `src/storage/engine/replication_log.cpp`, expiration authority实现文件；Tomb Raider 仅做回归/claim audit，不预设修改 |
| Build/tests/docs | `CMakeLists.txt`,上述四个 seam对应 tests、新 `gate_failover.py --case`、architecture/operations/domain/ADR 文档 |

具体函数名可在 RED test固定 seam时小幅调整，但模块 ownership 不变：Meta不接管 Data
replication细节，ReplicationManager不解释 Raft/Operation，NodeControl仍是唯一 Data
control mutation入口。

## 14. Targeted 验证顺序

每个切片采用：

1. 写一个最小 RED；
2. 只构建相关 target；
3. 运行单个 test/filter，确认因缺失行为失败而非 fixture错误；
4. 写最小实现转 GREEN；
5. 运行同 seam相邻现有测试；
6. 必要时简化刚改的代码，但不改变行为；
7. 再进入下一条行为。

统一环境：

```bash
mkdir -p /mnt/local_nvme/keylane-issue41-redesign/tmp
export TMPDIR=/mnt/local_nvme/keylane-issue41-redesign/tmp
export KEYLANE_TEST_TMPDIR=/mnt/local_nvme/keylane-issue41-redesign/tmp
export KEYLANE_ISSUE41_BUILD=/mnt/local_nvme/keylane-issue41-redesign/build-debug
```

Targeted 目标：

```bash
cmake --build "$KEYLANE_ISSUE41_BUILD" --target keylane_meta_tests -j 8
"$KEYLANE_ISSUE41_BUILD/keylane_meta_tests" --gtest_filter='MetaFailover*:*MetaStateApply*:*MetaTopologyStore*:*MetaGrantStore*:*MetaOperationStore*:*MetaStateMachineTest*:*MetaControlProjector*:*MetaObservationStore*:*MetaCandidatePlanTest*'

cmake --build "$KEYLANE_ISSUE41_BUILD" --target keylane_unit_tests -j 8
"$KEYLANE_ISSUE41_BUILD/keylane_unit_tests" --gtest_filter='Meta*:*ControlProtocol*:*NodeControl*:*NodeDirectiveCompletionTest.*:*AuthorityGuardTest.*:*ClusterAuthorityTest.*:*GroupInFlightTest.*:*ClusterRequestAuthorityTest.*:*ClusterCommandTest.*'

cmake --build "$KEYLANE_ISSUE41_BUILD" --target keylane_cluster_replication_manager_integration_test -j 8
"$KEYLANE_ISSUE41_BUILD/keylane_cluster_replication_manager_integration_test"
```

执行 targeted filter 前先用 `--gtest_list_tests` 确认每个预期 suite 确实命中；
`ReplicationManager` 不属于 `keylane_unit_tests`，始终运行独立 binary。受影响时
同样单独构建/运行 `keylane_cluster_population_integration_test` 和
`keylane_process_support_tests`。W6 另外构建 `keylane_cluster_status_tests`、`keylane-ctl`，
并运行 failover CLI/operator 精确测试。

真实进程 gate 显式传入
`/mnt/local_nvme/keylane-issue41-redesign/runs/<case>`，不依赖根盘默认临时目录。
实现新测试前先审计它调用的 helper：仅设置 `TMPDIR` 不能覆盖硬编码 `/tmp`；若复用
`tests/support` 中的硬编码临时目录，先让 helper 支持 `KEYLANE_TEST_TMPDIR`（保留默认
行为）并增加 helper test。新增测试不得直接写 `/tmp`。

## 15. 最终验证和 PR

实现全部切片后才跑完整矩阵一次：

1. fresh default-target Debug build，并确认以下独立目标均已物化：
   `keylane_unit_tests`、`keylane_meta_tests`、
   `keylane_cluster_status_tests`、`keylane_cluster_replication_manager_integration_test`、
   `keylane_cluster_population_integration_test`、`keylane_cluster_serving_generation_integration_test`、
   `keylane_cluster_rebuild_failure_integration_test`、
   `keylane_cluster_rebuild_protocol_integration_test`、`keylane_process_support_tests`、
   `keylane-meta`、`keylane-ctl`、`keylane`；
2. 完整 unit/meta tests；
3. `ctest --output-on-failure` 的 Meta、cluster 和分开注册的新 failover gate；
4. 必要的 sanitizer/fault targets（按仓库现有可用配置）；
5. 检查所有运行产物都在 NVMe；
6. diff audit：无 recovery store/phase/proof/one-shot failover directive残留；
7. 用 code-review workflow 并行做 Standards 与 #41 Spec 双轴审查；
8. 修复所有 P0/P1，以及确认成立的 P2；复跑受影响 targeted tests；
9. 最终一次完整验证保持全绿；
10. 检查 architecture claims、public API comments、stale comments 和 issue acceptance
    criterion → test evidence checklist；
11. 将预期交付的 untracked `CONTEXT.md`、`docs/adr/`、research/plan 文档纳入
    diff，再按逻辑切片整理 commit，push `codex/issue-41-failover-transition`；
12. 创建 PR，正文包含 `Closes #41`、明确 #42/#45/#46 out-of-scope、测试命令、loss
    语义、首版 availability gap和旧分支未整体复用的说明。

## 16. 主要风险及停止条件

| 风险 | 约束/验证 |
|---|---|
| Begin 前 Controlled request 没有 terminal path，泄漏 active-operation 配额 | pre-Begin typed Abort；无 Candidate/group conflict/invalid owner-grant/deadline 决策测试；Submitted→Aborted replay |
| 非法 successor grant 被封存后使 Uncontrolled 永久 fenced | Begin 只快照 current active exact `MetaGrantSpec`；grant/policy/lease validation 和 retirement reference tests |
| pause 漏掉 PUBLISH/expiry/frontier-mutating work，产生伪稳定 frontier | channel-slot 在 dispatch 前绑定；普通和 EXEC late-publish 跨 publisher admission/append 持有 `GroupInFlight`；单独 PUBLISH/MULTI/expiry admission-drain tests |
| pause-only FDS 没有触发既有 authority drain | 独立 pause-drain 集合和 `RememberDrain(before, anchor)`；SourcePaused 前显式 await |
| #40 prepare 的 old-authority hash 使新两种路径不闭合 | 只复用 prepare 内核；Meta 用 fresh SourcePaused 签发 Controlled committed authorization certificate，Candidate 不读 volatile Source proof；Uncontrolled 用 action authorization + committed target fence；保留 #40 回归测试 |
| transition-only FDS使当前lease失效，Source先CLUSTERDOWN | 把projection freshness、authority anchor和write-pause generation分离；读/lease/TRYAGAIN测试 |
| FDS replay反复重启 FULL | desired scope equality和 no-op测试；runtime-local retry，不由FDS重发触发 |
| transition/grant/control 变化导致 export 被错误撤销 | transition/control 与 stable export identity 分离；移除外层 `granted_` 相等限制；per-group `SameEstablishedExportScope` comparator matrix |
| generic source cleanup误杀旧 Owner有价值的下游 flow | 拆 source admission、established export、mutation drain、ingress cancel effects；partition fault test观察旧flow |
| prepared context在action replacement后迟到激活 | action id进入prepared context、grant、FDS和activation四处精确匹配；FullStateApplied barrier test |
| FDS/lease 在 promotion 完成前打开 write admission，或 Cluster 调用 standalone expiration 配对 | FDS 只 pin intent；lease 只进 NodeControl 不可服务 provisional slot；activation final recheck 后才 `AuthorityGuard::RenewLease`；抽取纯 promotion kernel，Cluster 不 `ResumeExpiration`；counter=0、FDS/lease 乱序、失败/过期始终 fenced 及 deadline-aware authority regression tests |
| prepare旋转history后旧Meta session无法上报current proof | prepare完成后以child history重建ClientHello；跨Meta Leader切换重报测试 |
| 误把 Tomb Raider 当成 frontier mutation，引入不必要 Data 机制 | 本期不新增 TR pause/enable；保留默认 OFF/configure 和既有 role-transition quiesce 回归 |
| command replay 先检查已失效 precondition，或 compound apply 半写 | 八命令统一 exact post-state-first；Commit 内核前 aggregate shortcut；bounded copy 上注入 topology/grant/operation failure并比较完整 pre/post state |
| leader change后空 Observation被误判成节点失败 | leadership warmup；Candidate explicit disconnect、Source explicit disconnect/incarnation replacement、pure absence/grace 分开测试 |
| Candidate失败导致等待重启、RTO无界 | Controlled立即Abort；Uncontrolled立即replace/clear；process gate kill -9 candidate |
| domain间错误比较 offset | 类型化 domain grouping；测试用不可比较vector和不同history/source term |
| former Owner 无 provenance 被永久排除，或直接恢复旧 authority | current-boot self-origin adapter + target fence + new action/prepare/Cutover 强制链；restart/provenance/旧 grant identity tests |
| kill-only 故障无法证明无双写 | 旧 Owner 存活的 Meta-control blackhole test；等 lease/grace，再授予新 Owner，heal 后收敛 |
| follower切新source过早清唯一Ready population | 鉴权/export-ready前保留；明确覆盖现有 destructive FULL gap；#45后续解决 |
| schema改动与旧fixture混用 | 当前未发布前提下统一替换codec/fixtures；不实现compat路径 |
|实现被迫新增 durable store/phase/frontier或 Data failover engine| 停止当前切片，回到 #41 讨论；不得用局部补丁绕过已确认架构边界 |

## 17. 完成定义

只有同时满足以下条件才创建 PR：

- #41 acceptance criteria逐项有实现和测试证据；
- Controlled/Uncontrolled/Cutover/Follow Owner真实进程闭环通过；
- Controlled Operation 在 Begin 前后都可终止，不留下永久 Submitted 记录；
- 旧 Owner 存活但与 Meta-control 分区时，finite lease 边界仍证明全程无双写；
- Meta Leader切换后仅依靠 Committed State + 重报 Observation继续；
- Data新增面保持为 pause、action-bound activation、desired Follow Owner和必要的
  heartbeat/status adapter，没有第二套复制/failover引擎；
- 所有临时产物位于 NVMe；
- full validation、Standards review、Spec review均通过；
- architecture/operations文档与源码一致；
- commit已推送且PR已创建。
