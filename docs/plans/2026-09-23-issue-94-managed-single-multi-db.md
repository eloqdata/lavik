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

# #94：受管 Single 多 DB 执行计划

日期：2026-09-23。状态：实现、审查与软件验收完成。
关联：[业务票 #94](https://github.com/eloqdata/lavik/issues/94)、
[父票 #77](https://github.com/eloqdata/lavik/issues/77)。
编制基线：`4adaffb66d95320e579bbb674fd6570d05d7d057`。
依赖 #92 已关闭；执行前确认工作分支包含其实现。

本文件记录本次设计讨论确认的执行顺序和验收要求，不描述已经实现的能力。
业务跟踪继续使用 #94；当前行为以源码、测试和 `docs/architecture/` 为准。
本次讨论补充的只读命令范围与验收矩阵也属于交付范围。

## 已确认的合同

- Single 开放既有 DB0–15；各 DB 的 key 命名空间隔离，整个数据集共用一个
  Group authority。Cluster 保持 DB0、CROSSSLOT 和现有 COPY DB 限制。
- SELECT、跨 DB COPY 沿用已有 standalone 语义。纳入 DBSIZE、SCAN、RANDOMKEY、
  KEYS，并接入受管 Single 的读取准入、population 完整性和 Serving Generation。
- COPY 保留源内容、REPLACE、返回值、错误和绝对过期时间语义；同名 key 在不同
  DB 是不同对象。已有类型包括 Stream 的消息、消费组、消费者和 PEL。
- 所有 DB 使用既有 native FULL、增量复制、Candidate Recovery、Promotion 和
  partial reparent；完整性与恢复资格按整个 population 判断。
- KEYS 保留现有两遍扫描、固定存活时间戳、流式回复和 DB gate 生命周期。
  回复开始后需要排空的恢复步骤等待它完成；沿用现有停滞 watchdog，不增加
  生命周期主动取消或总时长上限。持续慢读可能拖延恢复是保留的限制。
- DBSIZE 保留已有计数口径，可能包括已到期但尚未清理的记录；SCAN 保留无状态
  cursor 和普通遍历语义，不新增跨 DB、跨 population 的连续遍历保证。
- 副本沿用既有陈旧读政策；不完整 population 不可读。Owner 失权不转化为
  副本读权限。COPY 保留既有撤权、回滚及不确定结果合同，不承诺异步复制零丢失。
- FLUSH 的受管授权与不可逆变更归 #98，事务与脚本归 #96，catalog 归 #97；
  迁移工具验收继续使用原有业务票。本票的内部 FULL 清理和 epoch 验证必须完成，
  不依赖提前开放客户端 FLUSH。不得通过放开整体拒绝表顺带开放其他未完成能力。

## 执行顺序

### 1. 建立可复用的多 DB 验证数据与基线

主要位置：`tests/meta_integration/gate_managed_single.py`、
`gate_native_replication.py`、`gate_candidate_recovery.py`、
`gate_population_recovery.py`、`gate_failover.py`、`CMakeLists.txt`。

- [x] 在既有 harness 上整理最小共享数据准备/核验 helper，显式选择每条连接的 DB，
  避免 SELECT 状态污染后续断言；Cluster 场景保持 DB0。
- [x] DB0–15 放同名不同值的永久 key；DB0、DB1、DB15 额外包含已有类型、TTL、
  COPY 源/目标及仅在旧目标存在的数据。使用可证明分布到不同 worker 的 key。
- [x] TTL 核验使用绝对截止时间或允许执行耗时的区间；不以精确相等的剩余毫秒数
  断言正确性。复制追平使用有界轮询/已有进度屏障，不用固定 sleep 推定完成。
- [x] 记录原有非 Meta Single 与 Cluster 的对应语义断言；新增受管场景明确暴露
  SELECT/命令准入的当前缺口，不能把它当成环境失败跳过。

完成标准：同一套业务数据可用于普通同步、重启、FULL 与 HA 场景；每个断言有
明确 DB、角色和预期值，全部 DB 的同名 key 能区分错 DB 回放。

### 2. 接通 SELECT、非零 DB 和数据库只读命令

主要位置：`src/redis/command.cpp`、`src/redis/command_table.cpp`、
`src/cluster/authority.cpp`，及现有 serving-generation/命令准入测试。

- [x] 精确移除受管 Single 的非零 DB 拒绝，保留 Cluster SELECT 0 成功及非零 DB
  拒绝、非法 DB/参数报错和连接的 selected DB 行为。
- [x] 将 DBSIZE、SCAN、RANDOMKEY、KEYS 从受管 Single 的 deferred 边界中单独
  接通，复用既有处理器；其余 keyless/global、事务、脚本拒绝保持。
- [x] 这些没有显式 key 的数据读取绑定唯一 Group；验证 Owner authority、完整
  副本读取与陈旧读政策，避免把它们当成只检查 readiness 的进程诊断命令。
- [x] 检查跨 worker 调度、DB admission 等待以及 KEYS exclusive cut 前后的
  Serving Generation 验证，防止旧请求读到替换 population；沿用已有读 authority
  合同，不扩展为回复发送期间持续撤销全部读取。
- [x] 验证 DB0–15 隔离、连接之间 SELECT 隔离、SCAN MATCH/TYPE/COUNT、空 DB、
  RANDOMKEY 的所选 DB/过期过滤、DBSIZE 原有口径和 KEYS 结果。
- [x] 对 KEYS 慢读采用可控屏障：证明恢复等待；显式完成读取或关闭连接后，证明
  gate 与 expiration 暂停释放、恢复继续。保留停滞超时，不要求持续慢读被主动取消。
- [x] 覆盖完整副本正常读、断流陈旧读、禁止陈旧读、LOADING 以及失权旧 Owner。

完成标准：受管 Single 的以上命令在非零 DB 可用；读取无法越过 population
替换边界；Cluster 与尚未接入的命令边界保持，KEYS 生命周期与原行为一致。

### 3. 核验并补齐跨 DB COPY 的 authority 与失败边界

主要位置：`src/redis/command.cpp` 的 COPY、MultiDbOperationGuard、两阶段执行
及 replication guard；`src/storage/engine/transfer_api.cpp`；
`tests/multikey_e2e_test.cpp`、`tests/grouped_transfer_e2e_test.cpp`、
`tests/grouped_stream_e2e_test.cpp` 及受管集成测试。

- [x] 核验源 DB shared key lock、目标 DB exclusive key lock、两个 DB gate 和
  worker 参与者都沿用同一请求的 Group admission；等待后及实际发布边界复查。
- [x] 保留 shared-source flow 的复制依赖，不能把 COPY 简化成无依赖的目标写。
- [x] 覆盖 DB0→15、15→0、1→15；同 worker 与跨 worker；同名跨 DB；源缺失或
  过期；目标存在/不存在；有无 REPLACE；同 DB 同对象错误和非法 DB。
- [x] 核验 COPY 不重启 TTL，源不改变，目标替换保持完整类型内容；Stream 校验
  消费组、消费者和 PEL，并在副本与重启后再次验证。
- [x] 用已有故障注入设施验证 DB gate 冲突、锁等待、Controlled Pause、撤权、
  ingestion 失败及 publication/settlement 边界。缺少屏障时仅加必要的测试钩子。
- [x] 未发生不可逆效果的失败不能留下目标半成品；需回滚时旧目标完整恢复。
  一旦进入既有不确定结果边界，沿用断连合同，不将其伪装成安全重试错误；
  rollback/settlement 不能因撤权失去执行清理的机会。

完成标准：成功、无操作、拒绝、回滚和不确定结果均有可观测断言；源/目标最终
数据、TTL 与复制结果一致。只有发现缺口才修改既有 COPY 实现。

### 4. 完成 FULL、epoch、重启与统一 HA 验收

主要位置：`src/replication/replication.cpp`、
`src/replication/population_recovery.h`、`src/storage/engine/replication.cpp`、
`system_state.cpp`、`recovery.cpp`；既有 replication log、population 和 Meta gates。

- [x] 逐项确认 export、import/reset、canonical command DB、DB epoch 向量、恢复
  proof 和有效性检查覆盖启用 DB。已有通用路径通过测试即保留，不另建恢复模型。
- [x] 强制 FULL 前让目标的非零 DB 含源端没有的 key，以及源端为空的 DB；验证
  旧数据全部清除，DB0 与其他 DB 不串写，FULL 后的增量仍到达正确 DB。
- [x] 在 FULL 中断/重启边界验证整个 population 不可读、不具备自动候选资格；
  完成合法替换后才开放全部 DB。验证旧 epoch 数据不会复活。
- [x] 扩展既有 clean Owner、clean replica 与 crash/operator recovery 场景，
  验证多 DB 的完整性与 Clean Shutdown Proof 规则；重启不自动恢复旧 authority。
- [x] 在 Single 的 Controlled Failover、Owner crash、租约 fencing 和 Candidate
  Recovery 场景中加入非零 DB、COPY 和 TTL；复用多 donor、历史缺口与预算路径。
- [x] 对有 retained history 的 direct-parent partial reparent，既核验数据，也
  核验协商/日志证据没有额外 FULL；对必须 FULL 的场景验证清理、收敛和后续增量。
- [x] 故障测试只对已由指定复制/持久化屏障保证的数据断言保留；不得把请求成功
  当成所有副本已持久化的证明。必要的底层 epoch/clear 测试不开放受管 FLUSH。
- [x] 需要为 population recovery 增加 Single 参数时，扩展同一 fixture 和 CTest
  注册，保留 Cluster 用例；不复制一套恢复实现或 harness。

完成标准：下面矩阵的每行都有自动化测试与实际运行结果；不会只凭 DB15 FULL
单测或最后一个 GET 正确就宣称整个恢复链通过。

| 验收面 | 最低数据覆盖 | 必须证明 |
|---|---|---|
| 普通读写与增量 | DB0–15 同名不同值 | 命名空间、回放 DB、Owner/副本读取合同 |
| COPY | DB0/1/15，双向及跨 worker | 类型、绝对 TTL、REPLACE、失败边界、源依赖 |
| FULL/reset | DB0/1/15，旧目标残留、空源 DB | 清理完整、epoch 隔离、后续增量正确 |
| FULL 中断及重启 | 非零 DB 已部分导入 | population 整体不可读、不可误判完整 |
| 正常重启与 crash recovery | DB0/1/15 与 Stream 元数据 | 数据恢复、proof 与自动资格符合既有合同 |
| 受控及故障切主 | DB0/1/15、TTL、COPY | 权限边界、可保证数据保留、旧 Owner fencing |
| Candidate Recovery/reparent | 非零 DB 分布到多个 flow | 同域向量、donor 补齐、partial 证据或安全 FULL |
| KEYS 慢读 | DB15，大回复 | 保持等待语义，释放连接/回复后恢复推进 |
| Cluster/角色控制回归 | DB0 和非零 DB 反例 | DB0、CROSSSLOT、COPY 限制及拒绝 REPLICAOF 旁路 |

### 5. 同步文档并完成集成验证

- [x] 更新 `docs/architecture/06-cluster-data-plane.md` 的 DB0-only/deferred
  命令主张，说明实际支持范围与统一 Group 准入；保留其他未交付限制。
- [x] 按实际改动更新 `02-request-serving.md` 的等待/KEYS 生命周期说明，以及
  `05-replication.md` 中受管 Single 的 DB 范围描述。通用 FULL 模型未变时不重写。
- [x] 更新 `docs/operations/cluster-deployment.md` 的支持矩阵与操作示例，包含
  SELECT 15、COPY DB、读取与副本核验，以及 KEYS 慢读限制。不把 SCAN 可用等同于
  迁移工具端到端验收已完成；不改动仍未交付的迁入/迁出承诺。
- [x] 保留本次 `CONTEXT.md` 对 Single 数据集与 DB 命名空间的澄清。本次继承既有
  设计，无需新 ADR；只有实现发现需改变已确认设计时才重新讨论。
- [x] 运行聚焦验证，再运行完整软件 CI runner；检查 CTest 注册情况，不能把缺少
  Redis/Python/标准客户端依赖导致的未注册测试当作通过。
- [x] 交付说明包含功能范围、KEYS 保留限制、测试命令/结果及任何未运行项。

## 验证命令与执行约束

先按 `CONTRIBUTING.md` 和 `docs/operations/building-and-packaging.md` 准备依赖、
io_uring、memlock、临时测试目录及容量。以下从仓库根目录执行，Debug 同时启用
Meta fault gates 与 Data test faults；不要在 fixture 使用二进制时重建它。

```bash
./scripts/configure_debug.sh -DLAVIK_ENABLE_TEST_FAULTS=ON
cmake --build build_debug --parallel

# 先确认新增与既有测试确实注册；新增用例同步补入后续选择范围。
ctest --test-dir build_debug -N

# 请求准入、多 DB/COPY 与 serving-generation 聚焦回归。
ctest --test-dir build_debug --output-on-failure \
  -R 'meta_integration[.]gate_managed_single|lavik_multikey_e2e|lavik_grouped_ordered_write_e2e|cluster_integration[.](serving_generation_fence|replication_manager_control_api)'

# HA/恢复：覆盖现有 Single 与 Cluster，用例名以 CTest 注册列表核实。
ctest --test-dir build_debug --output-on-failure \
  -R 'meta_integration[.].*(native_replication|candidate_recovery|population_recovery|failover|cluster_client)'

# 合并前完整软件验收，包括 Valkey 与大型对象场景。
./scripts/run_ci_tests.sh build_debug
git diff --check
```

格式检查使用 `pre-commit run --files <本次改动的源码文件>`。
按受影响测试选择底层 replication-log/epoch 用例，具体名称从 `ctest -N` 获取。
聚焦测试失败先修复；同一版本完成完整验收后，只有新增修改、失败或未解决疑点
才重复扩大测试。记录测试日志与构建版本。本次执行使用 `/mnt/local_nvme/issue94/source`
构建镜像和 `/mnt/local_nvme/issue94/build-debug`，缓存、测试数据及日志也全部
位于 `/mnt/local_nvme/issue94/`；不在工作区所在网盘生成临时构建产物。

步骤依赖为 1 → 2 → 3 → 4 → 5；每步随业务改动补测试与必要文档，最后一步做
完整性检查，不把测试、文档或恢复正确性留给其他票。复用 worker ownership、
message passing 和现有 gates，不引入跨 worker 阻塞 mutex。

## 执行结果

- 已开放受管 Single DB0–15、跨 DB COPY 与四个数据库检查命令；KEYS 生命周期
  保持原行为。规范与需求两项审查均无遗留发现。
- 聚焦验证覆盖多 DB 复制、COPY 撤权与收尾、FULL 清理、部分导入重启、全部
  Single HA/恢复场景，以及 Cluster 反例。
- 完整 CI runner 已执行：1,530 项 CTest 中，硬件安全门禁按配置跳过；native
  复制测试的一处副本就绪等待竞态已修复，整项复测通过，其余软件用例通过。
  大型 native List/Hash、超过 1 GiB 的 RDB 和 Valkey 套件均通过。
- 格式检查、Python 语法检查及 `git diff --check` 通过。构建、依赖缓存、测试
  数据和完整日志均保存在 `/mnt/local_nvme/issue94/`。
