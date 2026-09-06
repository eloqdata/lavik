# Issue #19 正式实现计划:Metadata Raft、身份安全与 Operation Store

> 状态:v5 修订稿(2026-09-04),已吸收四轮外部 AI 评审;第四轮 4 个 P1 + 1 个 P2 经核实全部成立,修订对照见附录 D。
> 前置:spike 已完成并验证(主仓 `d259c0c`,celer `0618b1e`;六项 gate 全绿,结论已回写 issue #19 评论);partition gate 验收口径拆分已回写 issue(#19 评论 5543402487,第三轮评审确认关闭)。
> 本期交付 issue #19 交付物;`#20` 只做 seam,不实现 control session 本身。

## 0. 本期固化决策(对应 issue「开工前再决定」逐项)

| 决策项 | 结论 |
|---|---|
| Raft library | NuRaft pin `0b01b18`,`DISABLE_ASIO` + Celer adapter(spike 已证) |
| Controller deployment | 独立 `keylane_meta` 进程,典型 3 节点 Meta 组;coordinator(#21–#23 的 failover/migration/placement 逻辑)以**同进程 C++ seam** 形式运行在 Meta leader 内(用户确认) |
| Snapshot/WAL layout | Keeper 式模型:**最新快照 + 其后的 WAL 共同构成持久事实源**;恢复/commit-watermark 语义以 NuRaft 实际行为为准(§3,已对源码核实)。WAL v2:按快照边界+大小上限分段,compact 删段 |
| 身份/bootstrap/RBAC | **预置证书身份 + 最小 RBAC**(用户确认):operator 自有 CA 预签;注册时绑定 canonical SAN principal(全局一对一,见 §6);RBAC 两档;join-token/在线签发/轮换留后续 |
| 升级策略 | committed 编码带 `schema_version`;SM 读 N 与 N−1;committed `active_write_schema` 决定混合版本窗口内的写格式(§3 升级契约);fixture + 滚动重启 + 混合版本测试矩阵 |
| Observation retention | 纯内存、boot-scoped、leader-local(Meta 重启/leader 切换即清空,由节点经新 session 重新上报);统一 envelope + session_generation 生命周期管理(§4) |
| Operator audit schema | **所有 privileged 命令**携带可信入口注入的 `ActorContext`(apply 只复制,不可自报);audit record 以 raft log index 为键,排序即 log index;audit store 有界窗口 + 滚动 hash 链 + ctl 导出外部归档,满时 fail-safe(§2) |

## 1. 范围与边界

**做**(issue #19 交付物):
1. versioned Metadata Raft state model;
2. identity/enrollment、topology、term/grant、policy、operation 五个 committed store + 通用 audit record;
3. committed snapshot/replay/upgrade contract;
4. soft observation ingestion、freshness validation 与生命周期管理;
5. 提供给 #20/#21/#22/#23 的 coordinator seam(同进程 C++ API,隐藏 NuRaft)。

**不做**:#20 的 control session/typed directive 协议本身(含 session_generation 的线产生,见 §4);#21/#22/#23 的协调器业务逻辑(含 kind 专属 phase 机);data node 侧任何改动;join-token 自动 enrollment;per-group Raft(#24)。

**分层红线**:NuRaft 仅 `keylane_meta` 链接;Celer 不承载 topology/grant/term/operation 语义;configure 期负向检查不得移除。

## 2. 版本化状态模型(src/meta/model 层)

编码:手写 versioned 小端二进制;每条命令与快照文件头带 `u16 schema_version`。

**硬上限(状态规模全面有界,超限一律 fail-safe,不静默截断)**:单命令/单 payload(policy、evidence、intent)字节上限;节点数/group 数上限;**非终结 operation 数上限 `max_active_operations`**;终结 operation 摘要(见下)数量上限,达到后 `ArchiveOperations` 拒绝、operator 须先 ctl 导出;policy 版本数与总字节上限;**audit 窗口满时 privileged `Propose` 返回 RESOURCE_EXHAUSTED,直到 operator 导出归档——绝不静默丢弃未导出记录**;**snapshot 总字节硬上限**,超出即 `create_snapshot` 失败并告警(与 §3 的 fail-safe 联动)。

**失败分两类**:未知 schema/损坏编码 = **fail-stop**(解码失败即 `system_exit`;同一字节序列在所有节点同一结果);成功解码后的 CAS/领域校验拒绝 = **消费 index + 写 audit record**,状态不变。

**核心语义规则**:
- **绝对值命令**:所有改变量由 proposer 显式写入命令,apply 只做校验与赋值,无"+1"式相对变更。
- **apply 的 replay 幂等定义**:同一 log index 的重放产生**相同状态、相同判定、相同 audit record**(audit 以 log index 为键)。为此领域判定分两种:**效果已存在且内容一致 → 幂等接受(no-op);内容冲突 → 拒绝**。例:replay 经过已生效的 `SetSlotMap` 得到幂等接受;不同内容撞同一 revision 才是冲突拒绝。这是恢复正确性(§3)与 audit 唯一性的共同前提。
- **expected_revision CAS**:可变记录的修改命令携带期望 revision;冲突即拒绝;超窗重试得到确定性冲突结果。

**Leader-local 校验与 apply 校验的拆分**:
- `ValidateProposal`(仅 leader,提议前):可访问易失上下文(observation freshness、coordinator 注册的 kind 专属规则);通过后把**规范化、不可变的 evidence 摘要**编进命令。
- `ApplyCommitted`(所有节点 commit 线程):**确定性纯函数**,只依赖命令内容与 committed state,永远不查 observation、**不读本地时钟**。
- operation journal 与 audit record 持久保存 evidence 摘要而非 observation 引用。

**审计模型**:`ActorContext` 由可信入口经不可伪造 API 注入——`Propose(command, AuthenticatedPrincipal)`;**外部 codec 不接受 actor 字段**;每条 privileged 命令 apply 时原子写入 audit record:**以 raft log index 为键**(replay 重写同一记录,天然唯一),含 actor、命令摘要、判定结果、propose 时由可信入口写入的可读时间(apply 只复制);排序即 log index;外部审计归档按 `(cluster_id, raft_log_index, record_hash)` 去重;NuRaft membership 变更前先 commit 绑定/审计命令(§6)。

**Operation 标识与归档(防重用、防卡死、防分叉)**:
- `operation_id`:**客户端提供的稳定 UUID**(外部唯一),是 operation 的永久幂等键;`SubmitOperation` 按 id 查记录(含归档摘要):同 id + 同 intent_hash → 返回已有/已归档结果(幂等接受);同 id + 不同 intent_hash → 拒绝(payload 复用)。
- `operation_seq`:**= SubmitOperation 命令所在的 raft log index**(天然唯一、单调、各节点一致,apply 时直接获得,**不需要任何计数器**),仅作排序/归档引用字段。
- `ArchiveOperations{seq 集合}`:**非连续归档**终结 operation(长期 Running 的旧 operation 不再卡住归档);归档摘要保留 `(operation_id, operation_seq, intent_hash, actor, terminal result, data_loss_possible)`,在保留期内充当墓碑索引——迟到的重复提交确定性解析为"已完成";引用不存在(未归档且无记录)的旧 seq/id 一律拒绝。摘要存储有上限,超出须先导出。
- 非终结 operation 不可归档。

Committed 命令集 v1(每条携带 `request_id` + 适用时的 `expected_revision`;`ActorContext` 由入口注入):
- **identity/enrollment**:`RegisterNode`(node_id、证书 principal 绑定、endpoints、capabilities、角色)、`UpdateNode`(**不得修改 principal 绑定**)、`RetireNode`。
- **topology**:`CreateGroup`、`AssignNodeToGroup`(one-node-one-group 强制)、`RemoveNodeFromGroup`、`SetSlotMap`(slot→group)。**所有 topology 可见变更(membership、owner、endpoint、slot)都显式携带新 `topology_epoch`**;owner 可见变更联动 `config_epoch`。
- **GroupRecord**(per-group committed 记录):`owner`、`group_term`、`authority_version`、`population_manifest_id`、`partition_replication_epoch`。递增规则:`authority_version` 在影响 grant 安全性的 group 配置变更时提升;`partition_replication_epoch` 在 partition 移出/移回时提升。**`replication_history_id` 是 data-plane boot-scoped 身份(#14),不进入 GroupRecord**;observation 的 history 校验锚定 operation 已 committed 的 history 绑定(§4)。
- **term/grant(term 只升一次,激活不再动 term)**:
  - `BeginGroupTerm(expected=T−1, new=T)`:提升 group_term 并进入**无 grant/fenced 状态**;此后该 group 的候选 observation/evidence 全部绑定 T;
  - `GrantAuthority`:同 owner 续租(lease 参数 + policy 版本引用),不改 owner/term;
  - `ActivateAuthority(expected_term=T, ...)`:failover/迁移的原子提交点——**不再改变 term**,原子设置 owner + 新 grant + authority_version + topology_epoch/config_epoch;`expected_term != current` 即拒绝;
  - `RevokeGrant`/`FenceGroup`。
- **policy(PolicyStore)**:`PutPolicy`(versioned 文档本体,content-hash 寻址)、`RetirePolicy`(**拒绝退休仍被 active grant 或非终结 operation 引用的版本**);grant 引用的 policy 版本必须已 committed。
- **operation journal(通用生命周期,kind 专属 phase 机不在本期)**:`Submitted → Running(kind_phase_blob, revision) → Completed | Aborted`。SM 只校验:幂等键/intent_hash、revision CAS、kind/version 不可变、终态不可逆、evidence 摘要与 committed state 一致、归档/引用规则;**kind 专属 phase 图合法性由 coordinator 注册的 ValidateProposal 插件校验(leader-local),不进 apply**。命令:`SubmitOperation`、`TransitionOperationPhase`、`CompleteOperation`、`AbortOperation`、`ArchiveOperations`。
- **升级**:`SetSchemaVersion`(自身以最旧可读格式编码;门控见 §3)。

Apply 层 enforcement:term/epoch 单调不回退、one-node-one-group、grant 与 topology/policy 一致、通用生命周期约束、principal 全局唯一、规模上限。

## 3. MetaStateMachine 与持久化契约(替换 spike_state_machine)

- 新 `MetaStateMachine` 持有五个 committed store + 归档摘要 + audit store;`commit()` 即 `ApplyCommitted`。
- **恢复与 commit-watermark 契约(按 NuRaft 实际行为,已对源码核实)**:
  - 重启后 `quick_commit_index_`/`sm_commit_index_` 初始化为 `last_commit_index()`(快照 index),**不会立即 replay WAL 尾部**;
  - 尾部重新确认走正常 leader 路径:新 leader 追加当前 term 的 config entry(`index_at_becoming_leader_`),其达成 quorum 后 committed 前缀才被重新确认;
  - **未 committed 尾部:最终要么被合法 commit、要么被截断**。尾部持有节点当选时,该 entry 可在当前 term entry 达成 quorum 后作为前缀合法 commit;不含尾部的节点当选时被覆盖/截断。**quorum 重新确认前任何节点不暴露尾部状态;无 quorum 时 Meta unavailable**;
  - **apply 可能重复**:commit watermark 只随快照持久化,快照后已 apply 的 committed entry 在再次崩溃恢复后会被重新 `commit()`。正确性不依赖"至多一次",而依赖 §2 的 replay 幂等定义(同 index 重放 → 同状态、同判定、同 audit);崩溃前未 ack 的 propose 对客户端是 uncertain outcome(与 #17 `ClientOutcome::kNotReturned` 一致);
  - `wait_for_sm_catchup_on_becoming_leader_` 使 BecomeLeader 延迟到 SM 追平 `index_at_becoming_leader_`,coordinator 只在 BecomeLeader 后启动(§5);
  - gate:§7.1(e) 双支。
- **快照切点(按 NuRaft 实际行为)**:自动快照在 commit 线程 commit boundary 调用,持锁深拷贝此刻状态即精确切点;**手动快照必须 `serialize_commit_=true` 或 `schedule_snapshot_creation()`**;落盘线程只做序列化/IO。follower 收快照在调用线程同步 apply,对象有界;**snapshot 总字节硬上限**,超出即失败并告警。
- **replay 可观测性与 fail-safe**:`snapshot_distance` 调小;`create_snapshot` 连续失败计 metrics + 告警;**`max_uncompacted_wal_bytes` 硬上限**:超过且快照未完成时 `Propose` 返回 RESOURCE_EXHAUSTED;运维文档写明处置。
- **升级契约**:committed `active_write_schema`;新二进制在 `SetSchemaVersion` commit 前继续写旧格式;升级后旧二进制不得重新加入(握手交换 schema 区间,leader 拒绝过旧 join/add_srv;旧二进制重启读到新编码响亮失败);测试矩阵 §7.1(d)。
- WAL v2 分段;不兼容 spike 目录;state_mgr/log_store 沿用 spike 并演进;`system_exit` 策略不变。

## 4. Soft observation ingestion、freshness 与生命周期(src/meta/obs 层)

- `MetaObservationStore`:纯内存、**leader-local**——Meta 重启或 leader 切换即清空,由节点经新 session 重新上报;**永不进 Raft log**。
- **统一 `ObservationEnvelope` + 认证会话代**:ingestion 只接受可信三元组 **`{node_id, boot_incarnation, session_generation}`**;`boot_incarnation` 为 opaque 值(**不参与大小比较**);`session_generation` 是 controller-local 单调序号,由 #20 认证 session 层颁发(本期由 ctl 注入路径模拟可信 generation);**只摄入当前 generation;新 generation 接受时原子 purge 该节点全部旧 observation**;envelope 按类型携带 group_term、population_manifest_id、replication_history_id、operation identity,缺字段即拒。类型:`NodeBoot`、`NodeHealth`、`CandidateProgress`、`OperationEvidence`。
- **失效三管**:入库校验;committed term/manifest/owner/operation 变化时主动 purge;查询时按当前 committed state 再过滤。
- **Freshness 规则**:node 已注册、generation 为当前;term-bound observation 的 `group_term` **== committed 当前值**;`population_manifest_id` == GroupRecord committed 值;`replication_history_id` 锚定 operation journal 已 committed 的 history 绑定;operation identity 引用存在(含归档摘要)的 operation。
- 保留:per-(node,kind,generation) 最新覆盖;per-group 有界集;TTL 可配;淘汰/拒绝事件进有界环形缓冲。
- 摄入入口:`Ingest(envelope, session_generation)` 同进程 API(#20 调用),本期经 ctl 注入驱动。
- 测试:乱序/迟到 generation、commit 后 stale 化、leader 切换后清空重建、归档/不存在引用拒绝。

## 5. Coordinator seam(src/meta/coordinator 层,给 #20–#23)

同进程 C++ API,**不暴露 NuRaft 类型**:
- `Propose(command, AuthenticatedPrincipal) -> Task<StatusOr<ApplyResult>>`:仅 leader;内部先 `ValidateProposal`(含 kind 插件);`ActorContext` 由此注入;非 leader 返回 NOT_LEADER + 已知 leader 提示;`ApplyResult` 携带命令的 raft log index(即 operation_seq 等排序字段的来源)。
- `CommittedView`:**单一快照对象,一次原子读取包含全部五个 store + 同一 applied index**。
- `SubscribeCommitted() -> {CommittedView, cursor, subscription}`:**原子三元组**;按 commit 严格顺序回调;每订阅者有界队列,溢出即取消并强制从最新视图重同步;句柄析构退订;committed 流跨 leader change 存活。**回调可因 replay 重复覆盖同一 index,订阅者按 index 去重**。
- `Observations()`:obs 层只读访问(已过查询时过滤)。
- `RunAsLeader(reconciler)`:BecomeLeader(已被 `wait_for_sm_catchup` 门到 SM 追平)后启动;BecomeFollower 取消并等待收尾。
- 幂等:reconciler 只经 `Propose` 推进;operation 幂等键 + CAS + 语义级幂等判定使重放/重试安全;leader 切换后从 `CommittedView` 重建。
- 单元测试:mock reconciler 切换后幂等续跑;订阅原子交付/顺序/背压/取消/按 index 去重;uncertain outcome 专测。

## 6. 身份与安全落地

- **Canonical principal**:规范化 SAN principal(如 `keylane://node/<id>`);**全局一对一**:apply 拒绝同一 principal 绑定第二个 node_id;`UpdateNode` 不得修改 principal(rotation 未实现)。
- **meta 成员 bootstrap**:初始配置携带每成员 principal 绑定;listener 的 `IdentityVerifier` 校验对端证书 principal 与成员身份一致。
- **Meta membership 动态变更两阶段**:加入 = 先 committed 证书绑定(+audit)再 add_srv;移除 = 先 remove_srv 再退休绑定;apply 交叉校验,单边状态不产生权限。
- **data node 注册/通道**:注册绑定 principal;#20 控制通道必须以绑定证书建立;(a) 自报告通道只能上报自身 node_id 的 observation、只能响应发给自身的 directive;(b) **operator/admin 通道:默认 UDS + SO_PEERCRED(内核 uid 注入 ActorContext);TCP(含 localhost)必须 mTLS;明文 TCP admin 启动 fail-fast**。
- 证书过期/吊销:依赖有效期 + 运维重签;CRL/OCSP 不做(注释写明)。
- 测试:四类证书拒绝 + 绑定绕过 + principal 二次绑定 + `UpdateNode` 改 principal + admin 面(明文 TCP 拒启动、UDS 非授权 uid 拒)。

## 7. 验证矩阵

测试基建:`tests/meta_spike/harness.py` + `keylane_meta_tests` gtest + `tests/cluster` fault harness 不变量命名。

1. **leader crash / log replay / snapshot install / rolling upgrade 不回退 epoch/term/grant**:既有 gate 切真实命令保持;新增 `gate_meta_upgrade.py`:(a) 滚动重启单调性;(b) v(N−1) fixture 加载;(c) `SetSchemaVersion` 门控;(d) 混合版本矩阵;**(e) 未 committed 尾部双支 gate**:截断支(tail 持有节点被隔离,不含 tail 者当选 → 截断,永不 apply)与合法 commit 支(tail 持有节点当选 → quorum 重新确认前不可见,确认后作为前缀合法 commit);**断言最终状态与 audit 按 log index 唯一,不断言 apply 调用次数**;未 ack 的 propose 对调用方呈现 uncertain outcome(超时/错误而非假成功)。
2. **stale incarnation/evidence 不能完成新 operation**:单元 freshness 矩阵(旧 boot/旧 term/未来 term/旧 history/跨 operation 引用/归档与不存在引用)+ 进程 gate(伪造旧 generation/未来 term evidence,operation 不推进、拒绝入审计);乱序 generation 与 commit 后 stale 化用例。
3. **Meta partition**:Meta 侧子 gate(少数派不 commit、让位、愈合无 fork、grant 不续发)+ 现有 data-plane 保留证据。边界声明:data-node lease 自失效端到端子项划入 #21,已回写 issue #19(评论 5543402487)。
4. **与 #17 对齐的正确性测试**:fork/write_at/截断/flush 失败注入直接打 NuRaft + `NuraftLogStore`;**语义 reference model**(tests 内五个 store 命令语义的轻量模型;同一命令流含合法/非法/**重放**/快照往返,比较完整状态、判定与 audit 唯一性);observation 永不入 committed 流的构造性保证(`ApplyCommitted` 不访问 obs 层、不读时钟)+ 断言。
5. **不回归**:既有 meta 单测 + 7 个 spike gate 切真实命令后全绿;全量 ctest 除既有 pre-existing 环境失败外全绿。

## 8. 文档(AGENTS.md claim-driven)

- 新增 `docs/architecture/08-meta-control-plane.md`:stores 与边界、observation 易失性/生命周期、恢复与 commit-watermark 契约(含 apply 可重复与 replay 幂等定义)、快照切点、升级契约、身份模型、审计模型、coordinator seam、与 data plane 的接缝(#20)。
- 更新 `docs/architecture/README.md`、`01-overview.md`。
- `docs/operations/`:部署与证书预置、滚动升级顺序、snapshot_distance/replay 可观测性与 `max_uncompacted_wal_bytes` 处置、audit/归档导出。

## 9. 执行波次

| 波 | 内容 | 出口标准 |
|---|---|---|
| W1 | model 层:编码+硬上限+失败分类、命令 schema(绝对值+CAS+ActorContext+幂等判定)、identity/topology/policy store、audit store(log-index 键+窗口+hash 链)、不变量 apply 校验 | 单测覆盖命令矩阵、拒绝路径、principal 唯一、幂等接受/冲突拒绝二分 |
| W2 | term/grant store(BeginGroupTerm/ActivateAuthority 语义)+ operation journal(客户端稳定 id、seq=log index、非连续归档+摘要墓碑)+ ValidateProposal/ApplyCommitted 拆分 | 单测:term 单次提升、跨 term 激活拒绝、重复提交幂等、归档后引用解析、终态不可逆 |
| W3 | MetaStateMachine 整合;快照精确切点+异步落盘+大小上限;active_write_schema;WAL v2;max_uncompacted_wal_bytes fail-safe | 单测+fixture;既有单测/gate 切真实命令全绿 |
| W4 | obs 层(envelope+session_generation、==term、commit 驱动 purge、查询时过滤)+ ctl 注入/查询 | freshness/生命周期矩阵单测 + stale-evidence gate |
| W5 | coordinator seam(CommittedView、原子订阅+index 去重、RunAsLeader) | mock reconciler 单测 + 订阅语义 + uncertain outcome 专测 |
| W6 | 身份安全接线(IdentityVerifier、principal 规范化/唯一、membership 两阶段、UDS+SO_PEERCRED、RBAC) | §6 全部拒绝用例 |
| W7 | 验证矩阵收尾(upgrade gate 含混合版本与尾部双支、partition gate、log-store 故障注入、语义 differential)+ 文档(§8) | §7 全绿;文档合入 |

并行性:W1/W2 可拆两个 agent;W4/W5 可并行;W6 依赖 W1;W7 依赖全部。agent 共享 build_debug 用 flock;禁 git mutation;主线统一全量测试、code-review、提交。

## 10. 主要风险

| 风险 | 缓解 |
|---|---|
| 通用生命周期对 #21–#23 表达力不足/过度 | kind 规则全部留在 ValidateProposal 插件;W5 mock 验证;schema 版本化留扩展路 |
| commit-watermark/尾部/apply 重复语义理解偏差 | 已对 NuRaft 源码核实并写入 §3;§7.1(e) 双支 gate + §7.4 replay differential 实证 |
| 快照切点错误(非串行手动快照) | 手动快照强制串行化/调度;gate 覆盖手动快照路径 |
| 状态规模失控 | 全量硬上限(含非终结 operation/policy/audit/snapshot 字节)+ 非连续归档 + fail-safe 溢出行为 |
| obs/committed 交叉校验读锁竞争 | obs leader-local 单点化;commit 驱动 purge + 查询时过滤;压测覆盖 |
| admin 面误暴露 | UDS peercred / 全 mTLS;明文 TCP fail-fast |
| 范围蔓延 | seam 头文件评审卡边界;kind phase 机与 session 线协议明确属 #20/#21 |

## 附录 A:v2 相对 v1 的修订(对应第一轮评审 9+1 条)

1. observation 校验拆分 leader-local `ValidateProposal` / `ApplyCommitted` 确定性;命令内嵌 evidence 摘要;journal 持久化摘要。
2. 补 PolicyStore、GroupRecord 承载 authority_version/population_manifest_id/partition_replication_epoch 及递增规则。
3. 统一 `ObservationEnvelope`;term-bound observation 改 `==` committed term。
4. 升级:committed `active_write_schema`、bump 前写旧格式、`SetSchemaVersion` 最旧可读格式、旧二进制拒入、混合版本矩阵。
5. 恢复措辞修正;snapshot 失败告警;异步快照改先冻结再落盘。
6. partition gate 拆分:本期 Meta 侧子 gate,data-node lease 子项划 #21。
7. admin 通道改 UDS+SO_PEERCRED / 全 mTLS;审计身份取自认证上下文。
8. 幂等:绝对值命令 + expected_revision CAS;去重表降级为带水位的快速路径。
9. 订阅:`SubscribeCommitted` 快照+cursor 原子交付,补背压/取消/leader-loss。
10. Meta membership 与证书绑定两阶段顺序。

## 附录 B:v3 相对 v2 的修订(对应第二轮评审 5+4 条)

1. commit-index 论证按 NuRaft 实际行为重写(重启 watermark=快照 index、当前-term config entry 经 quorum 重确认、`wait_for_sm_catchup` 关门)。
2. 快照切点:自动=commit boundary;手动强制 `serialize_commit_=true`/`schedule_snapshot_creation()`。
3. boot/history 生命周期:history 锚定 operation committed 绑定;boot 单调确立+原子失效+commit purge+查询过滤。
4. operation 通用生命周期;kind phase 合法性归 ValidateProposal 插件。
5. 原子 `ActivateAuthority`;`topology_epoch` 覆盖全部 topology 可见变更。
6. 全 privileged 命令 ActorContext + 通用 audit record;membership 变更关联审计。
7. 订阅契约改原子"当前完整 CommittedView + cursor + 订阅";getter 合并。
8. #17 differential 强化:log-store 故障注入 + 语义 reference model 全状态比对。
9. canonical principal 一对一 + `UpdateNode` 禁改 principal;硬上限 + `ArchiveOperations`。
10. issue #19 验收口径回写(已发,评论 5543402487)。

## 附录 C:v4 相对 v3 的修订(对应第三轮评审 5 P1 + 3 次要)

1. 未 committed 尾部契约修正为"最终 commit 或截断";§7.1(e) 拆双支。
2. term 单次提升:`BeginGroupTerm(T)` 提升并 fenced;`ActivateAuthority(expected_term=T)` 不再动 term。
3. boot 排序废除,改认证三元组 `{node_id, boot_incarnation, session_generation}`,只摄入当前 generation。
4. operation 身份改 Meta 颁发单调 seq + archive watermark;摘要保留 actor/intent hash/终态/data_loss_possible。
5. audit:排序按 log index;时间 propose 前写入;ActorContext 经 `Propose(command, AuthenticatedPrincipal)` 注入;audit 窗口+hash 链+导出。
6. replay 可观测性 + `max_uncompacted_wal_bytes` fail-safe。
7. 失败分类:fail-stop vs 消费+审计。
8. `RetirePolicy` 引用检查。

## 附录 D:v5 相对 v4 的修订(对应第四轮评审 4 P1 + 1 P2,均已核实)

1. **SubmitOperation 永久幂等**:`operation_id` 改回客户端提供的稳定 UUID 作永久幂等键(同 id+同 intent_hash 幂等接受、不同 payload 拒绝);归档摘要永久保留 `(operation_id, seq, intent_hash, actor, 终态, data_loss_possible)` 作墓碑索引,保留期内迟到重复提交确定性解析;保留期外的重试窗口为运维文档化的已知边界。
2. **归档去卡死**:改**非连续归档**(终结 operation 按集合归档,长期 Running 不再阻塞);删除单一 prefix watermark;新增 `max_active_operations`。
3. **seq 颁发合规**:`operation_seq` = SubmitOperation 所在 **raft log index**(天然唯一/单调/各节点一致,apply 直接获得),废除隐式计数器,与绝对值规则一致。
4. **apply 次数保证修正**:删除"至多 apply 一次";正确保证 = apply 可重复,同 log index 重放产生同状态/同判定/同 audit(audit 以 log index 为键);领域判定分"幂等接受(效果已存在且内容一致)"与"冲突拒绝";gate 断言最终状态与 audit 唯一性;外部审计按 `(cluster_id, raft_log_index, record_hash)` 去重;订阅回调按 index 去重。
5. **规模有界补全**:新增 `max_active_operations`、policy 数量/总字节上限、snapshot 总字节硬上限;audit 窗口满时 privileged `Propose` fail-safe(RESOURCE_EXHAUSTED 直到导出),禁止静默丢弃未导出记录。
