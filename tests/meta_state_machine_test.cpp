/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// MetaStateMachine component tests exercise direct apply, exact-cut owned
// snapshots, atomic installation, replay/audit uniqueness, and fail-stop on
// invalid committed commands. Component snapshot files are test fixtures;
// crash-durable WAL and publication are tested by raft/engine.
//
// Integration tests run the production C ABI and Go etcd/raft runtime with a
// single voter, covering committed replay, snapshot compaction, and shutdown.
//
// ACTOR ON THE WIRE: the command codec encodes the trusted-entry-injected
// ActorContext (actor_principal, readable_time) as ordinary bounded fields of
// every command body (commands.h), so a committed command decodes with
// the same actor the entry injected and the audit/journal records below
// carry it verbatim. Unforgeability is enforced at the ctl/coordinator entry
// server, not by this internal encoding.

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/encoding.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_apply.h"
#include "lavik/meta/state_machine.h"
#include "spdlog/sinks/ostream_sink.h"
#include "spdlog/spdlog.h"
#include "support/meta_raft.h"
#include "support/test_data_path.h"

namespace {

using lavik::meta::CreateGroup;
using lavik::meta::MetaAuditVerdict;
using lavik::meta::MetaCommand;
using lavik::meta::MetaRequestId;
using lavik::meta::MetaStateMachine;
using lavik::meta::MetaStores;
using lavik::meta::RegisterNode;
using lavik::meta::SubmitOperation;

std::filesystem::path MakeTestDir(const char* suite, const char* name) {
  const ::testing::TestInfo* info =
      ::testing::UnitTest::GetInstance()->current_test_info();
  std::string test_name =
      std::string(info->test_suite_name()) + "_" + info->name();
  std::replace(test_name.begin(), test_name.end(), '/', '_');
  std::filesystem::path dir =
      lavik::test::TestDataDirectory() /
      ("lavik_meta_test_" + std::string(suite) + "_" + name + "_" + test_name +
       "_" + std::to_string(::getpid()));
  std::filesystem::remove_all(dir);
  return dir;
}

void RemoveTestDir(const std::filesystem::path& dir) {
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

bool WaitFor(const std::function<bool()>& predicate,
             std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return predicate();
}

class ScopedLogCapture {
 public:
  ScopedLogCapture()
      : original_(spdlog::default_logger()),
        sink_(std::make_shared<spdlog::sinks::ostream_sink_mt>(stream_)),
        logger_(std::make_shared<spdlog::logger>("meta-sm-test", sink_)) {
    logger_->set_level(spdlog::level::trace);
    logger_->set_pattern("%v");
    spdlog::set_default_logger(logger_);
  }

  ~ScopedLogCapture() {
    logger_->flush();
    spdlog::set_default_logger(std::move(original_));
  }

  std::string Take() {
    logger_->flush();
    std::string result = stream_.str();
    stream_.str("");
    stream_.clear();
    return result;
  }

 private:
  std::ostringstream stream_;
  std::shared_ptr<spdlog::logger> original_;
  std::shared_ptr<spdlog::sinks::ostream_sink_mt> sink_;
  std::shared_ptr<spdlog::logger> logger_;
};

// ---------------------------------------------------------------------------
// Command builders (same field conventions as meta_stores_test.cpp)
// ---------------------------------------------------------------------------

MetaRequestId MakeRequestId(std::uint8_t seed) {
  MetaRequestId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = static_cast<std::uint8_t>(seed + i);
  }
  return id;
}

// 40 lowercase hex chars, matching the data-plane node_id convention.
std::string MakeNodeId(std::uint8_t seed) {
  std::string id(40, '0');
  for (std::size_t i = 0; i < id.size(); ++i) {
    id[i] = "0123456789abcdef"[(seed + i) & 0xF];
  }
  return id;
}

std::string MakePrincipal(std::uint8_t seed) {
  return "lavik://node/" + MakeNodeId(seed);
}

// Stands in for the trusted entry: every built command carries the
// injected ActorContext that the raft-log codec must carry through to apply.
constexpr std::string_view kEntryPrincipal = "lavik://operator/test-entry";
constexpr std::string_view kEntryReadableTime = "2026-09-04T01:02:03Z";

RegisterNode MakeRegister(std::uint8_t seed) {
  RegisterNode cmd;
  cmd.request_id_ = MakeRequestId(seed);
  cmd.actor_.principal_ = std::string(kEntryPrincipal);
  cmd.actor_.readable_time_ = std::string(kEntryReadableTime);
  cmd.node_id_ = MakeNodeId(seed);
  cmd.principal_ = MakePrincipal(seed);
  cmd.endpoints_ = {"10.0.0.1:7000", "10.0.0.1:17000"};

  cmd.role_ = lavik::meta::MetaNodeRole::kReplica;
  return cmd;
}

CreateGroup MakeCreateGroup(const std::string& group_id,
                            std::uint64_t new_topology_epoch) {
  CreateGroup cmd;
  cmd.request_id_ = MakeRequestId(0x21);
  cmd.group_id_ = group_id;
  cmd.new_topology_epoch_ = new_topology_epoch;
  return cmd;
}

// Committed wire encoding wrapped for the raft log. nullptr on encode failure;
// callers ASSERT_NE.
std::shared_ptr<lavik::meta::MetaRaftBuffer> EncodeOrDie(
    const MetaCommand& cmd) {
  auto encoded = MetaStateMachine::EncodeCommand(cmd);
  EXPECT_TRUE(encoded.ok()) << encoded.status();
  if (!encoded.ok()) return nullptr;
  return *encoded;
}

TEST(MetaApplyResultTest, StartCandidateRecoveryRoundTrips) {
  for (auto verdict :
       {MetaAuditVerdict::kAccepted, MetaAuditVerdict::kRejected}) {
    const lavik::meta::MetaApplyResult result{
        verdict, verdict == MetaAuditVerdict::kAccepted ? "" : "rejected", 42,
        lavik::meta::MetaCommandTag::kStartCandidateRecovery};
    const auto decoded = lavik::meta::DecodeMetaApplyResult(
        lavik::meta::EncodeMetaApplyResult(result));
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(*decoded, result);
  }
}

TEST(MetaApplyResultTest, RejectsTagsAboveStartCandidateRecovery) {
  for (std::uint16_t tag : {39, 65535}) {
    const lavik::meta::MetaApplyResult result{
        MetaAuditVerdict::kAccepted, "", 42,
        static_cast<lavik::meta::MetaCommandTag>(tag)};
    const auto decoded = lavik::meta::DecodeMetaApplyResult(
        lavik::meta::EncodeMetaApplyResult(result));
    ASSERT_FALSE(decoded.ok()) << tag;
    EXPECT_EQ(decoded.status().message(), "unknown apply-result command tag");
  }
}

// ---------------------------------------------------------------------------
// MetaStateMachine component tests
// ---------------------------------------------------------------------------

class MetaStateMachineTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = MakeTestDir("w3a", "sm"); }
  void TearDown() override { RemoveTestDir(dir_); }

  // Persistence belongs to the Go engine. These unit tests retain an owned
  // image and explicitly restore through the same Install seam used by replay.
  absl::StatusOr<std::unique_ptr<MetaStateMachine>> Open() {
    auto machine = MetaStateMachine::Open(dir_);
    if (machine.ok() && captured_) {
      auto status = (*machine)->Install(captured_->index, captured_->bytes);
      if (!status.ok()) return status;
    }
    return machine;
  }
  struct Image {
    uint64_t index;
    uint64_t term;
    std::string bytes;
  };
  std::optional<Image> captured_;
  // Commits one encoded command at `log_idx` and returns the stores copy.
  void Commit(MetaStateMachine& machine, uint64_t log_idx,
              const MetaCommand& cmd) {
    std::shared_ptr<lavik::meta::MetaRaftBuffer> buf = EncodeOrDie(cmd);
    ASSERT_NE(buf, nullptr);
    std::shared_ptr<lavik::meta::MetaRaftBuffer> result =
        machine.commit(log_idx, *buf);
    ASSERT_NE(result, nullptr);
  }

  void CreateSnapshot(MetaStateMachine& machine, uint64_t index,
                      uint64_t term) {
    auto bytes = machine.Capture(index);
    ASSERT_TRUE(bytes.ok()) << bytes.status();
    captured_ = Image{index, term, std::move(*bytes)};
  }

  std::filesystem::path dir_;
};

TEST_F(MetaStateMachineTest, CommitAppliesRealCommands) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*opened);

  Commit(*machine, 1, MakeRegister(0x11));
  {
    const MetaStores stores = machine->StoresSnapshot();
    const auto node = stores.identity_.FindNode(MakeNodeId(0x11));
    ASSERT_TRUE(node.has_value());
    EXPECT_EQ(node->principal_, MakePrincipal(0x11));
    EXPECT_EQ(node->revision_, 1u);
    EXPECT_TRUE(stores.identity_.IsActiveNode(MakeNodeId(0x11)));

    // Every privileged command writes exactly one audit record keyed by its
    // raft log index, carrying the trusted entry's actor
    // fields verbatim off the wire (see the file header).
    const auto audit = stores.audit_.Find(1);
    ASSERT_TRUE(audit.has_value());
    EXPECT_EQ(audit->verdict_, MetaAuditVerdict::kAccepted);
    EXPECT_NE(audit->command_summary_.find("RegisterNode"), std::string::npos);
    EXPECT_EQ(audit->actor_principal_, kEntryPrincipal);
    EXPECT_EQ(audit->readable_time_, kEntryReadableTime);
  }
  EXPECT_EQ(machine->last_commit_index(), 1u);

  Commit(*machine, 2, MakeCreateGroup("g1", /*new_topology_epoch=*/1));
  {
    const MetaStores stores = machine->StoresSnapshot();
    EXPECT_TRUE(stores.topology_.GroupExists("g1"));
    EXPECT_EQ(stores.topology_.TopologyEpoch(), 1u);
    EXPECT_EQ(stores.audit_.size(), 2u);
  }
  EXPECT_EQ(machine->last_commit_index(), 2u);
}

TEST_F(MetaStateMachineTest, LateConfigurationCallbackCannotRegressCursor) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*opened);

  Commit(*machine, 2, MakeRegister(0x11));
  // The new executor delivers all entry kinds in one ordered stream.
  // A regressing completion is a protocol bug, never a cursor rollback.
  EXPECT_DEATH(machine->Advance(1), "");
  EXPECT_EQ(machine->last_commit_index(), 2u);
  machine->Advance(3);
  EXPECT_EQ(machine->last_commit_index(), 3u);
}

TEST_F(MetaStateMachineTest, DomainRejectConsumesIndexWithoutStateChange) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*opened);

  Commit(*machine, 1, MakeRegister(0x11));
  // A cleanly decoded command violating a domain rule (the principal is
  // already bound to another node) is REJECTED: index consumed, audit
  // written, state unchanged — the commit thread keeps going (no fail-stop).
  RegisterNode conflict = MakeRegister(0x22);
  conflict.principal_ = MakePrincipal(0x11);
  Commit(*machine, 2, conflict);

  const MetaStores stores = machine->StoresSnapshot();
  EXPECT_EQ(stores.identity_.NodeCount(), 1u);
  EXPECT_FALSE(stores.identity_.FindNode(MakeNodeId(0x22)).has_value());
  const auto audit = stores.audit_.Find(2);
  ASSERT_TRUE(audit.has_value());
  EXPECT_EQ(audit->verdict_, MetaAuditVerdict::kRejected);
  EXPECT_FALSE(audit->verdict_detail_.empty());
  EXPECT_EQ(machine->last_commit_index(), 2u);
}

TEST_F(MetaStateMachineTest, UndecodableCommitFailsStop) {
  // The other half of failure classification: bytes that fail the
  // command codec are fail-stop (system_exit policy: spdlog::critical + abort).
  // The same bytes fail identically on every node, so this cannot fork the
  // group.
  EXPECT_DEATH(
      {
        auto opened = MetaStateMachine::Open(dir_);
        if (!opened.ok()) return;
        std::unique_ptr<MetaStateMachine> machine = std::move(*opened);
        std::shared_ptr<lavik::meta::MetaRaftBuffer> garbage =
            lavik::meta::MetaRaftBuffer::alloc(3);
        std::memcpy(garbage->data_begin(), "xyz", 3);
        machine->commit(1, *garbage);
      },
      "");
}

TEST_F(MetaStateMachineTest, RestartWithoutSnapshotReplaysFromScratch) {
  std::shared_ptr<lavik::meta::MetaRaftBuffer> c1 =
      EncodeOrDie(MakeRegister(0x11));
  std::shared_ptr<lavik::meta::MetaRaftBuffer> c2 =
      EncodeOrDie(MakeRegister(0x22));
  ASSERT_NE(c1, nullptr);
  ASSERT_NE(c2, nullptr);
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<MetaStateMachine> machine = std::move(*opened);
    machine->commit(1, *c1);
    machine->commit(2, *c2);
  }

  // No snapshot was taken: the durable commit point is still zero and the
  // stores start empty. The Raft core replays the WAL forward from
  // last_commit_index(), whose durable watermark is the snapshot index. This
  // rebuilds the state — simulated here by re-committing the same entries.
  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*reopened);
  EXPECT_EQ(machine->last_commit_index(), 0u);
  EXPECT_FALSE(captured_.has_value());
  EXPECT_EQ(machine->StoresSnapshot().identity_.NodeCount(), 0u);

  machine->commit(1, *c1);
  machine->commit(2, *c2);
  const MetaStores stores = machine->StoresSnapshot();
  EXPECT_EQ(stores.identity_.NodeCount(), 2u);
  EXPECT_EQ(stores.audit_.size(), 2u);
  EXPECT_EQ(machine->last_commit_index(), 2u);
}

TEST_F(
    MetaStateMachineTest,
    ClusterCreateCompletionWithoutRequiredPoliciesRejectsOnLiveAndWalReplay) {
  lavik::meta::ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101", "tcp://127.0.0.1:7301",
                             "tcp://127.0.0.1:7201"}};
  manifest.data_nodes_ = {{MakeNodeId(0x11), "tcp://127.0.0.1:6379"}};
  manifest.groups_ = {{"g1", MakeNodeId(0x11), {}}};
  manifest.slot_ranges_ = {{0, 16383, "g1"}};

  SubmitOperation root;
  root.request_id_ = MakeRequestId(0x01);
  root.actor_.principal_ = std::string(kEntryPrincipal);
  root.actor_.readable_time_ = std::string(kEntryReadableTime);
  root.operation_id_ = MakeRequestId(0x02);
  root.kind_ = std::string(lavik::meta::kMetaClusterCreateOperationKind);
  const auto intent =
      lavik::meta::EncodeClusterCreateRequest(manifest, root.operation_id_);
  ASSERT_TRUE(intent.ok()) << intent.status();
  root.intent_ = *intent;
  root.intent_hash_ = lavik::meta::MetaSha256(root.intent_);

  lavik::meta::PutPolicy automatic;
  automatic.request_id_ = MakeRequestId(0x03);
  automatic.actor_ = root.actor_;
  automatic.policy_id_ =
      std::string(lavik::meta::kAutomaticUncontrolledFailoverPolicyId);
  automatic.version_ = 1;
  automatic.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";

  lavik::meta::CompleteOperation complete;
  complete.request_id_ = MakeRequestId(0x04);
  complete.actor_ = root.actor_;
  complete.operation_id_ = root.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";

  const std::shared_ptr<lavik::meta::MetaRaftBuffer> root_bytes =
      EncodeOrDie(root);
  const std::shared_ptr<lavik::meta::MetaRaftBuffer> automatic_bytes =
      EncodeOrDie(automatic);
  const std::shared_ptr<lavik::meta::MetaRaftBuffer> complete_bytes =
      EncodeOrDie(complete);
  ASSERT_NE(root_bytes, nullptr);
  ASSERT_NE(automatic_bytes, nullptr);
  ASSERT_NE(complete_bytes, nullptr);

  const auto apply_and_expect_rejected = [&](MetaStateMachine& machine) {
    ASSERT_NE(machine.commit(1, *root_bytes), nullptr);
    ASSERT_NE(machine.commit(2, *automatic_bytes), nullptr);
    ASSERT_NE(machine.commit(3, *complete_bytes), nullptr);
    const MetaStores stores = machine.StoresSnapshot();
    EXPECT_EQ(stores.topology_.ClusterLifecycle().state_,
              lavik::meta::MetaClusterLifecycle::kCreating);
    ASSERT_TRUE(
        stores.operation_.FindOperation(root.operation_id_).has_value());
    EXPECT_EQ(stores.operation_.FindOperation(root.operation_id_)->lifecycle_,
              lavik::meta::MetaOperationLifecycle::kSubmitted);
    const auto audit = stores.audit_.Find(3);
    ASSERT_TRUE(audit.has_value());
    EXPECT_EQ(audit->verdict_, MetaAuditVerdict::kRejected);
    EXPECT_NE(audit->verdict_detail_.find("all current global Policies"),
              std::string::npos);
    EXPECT_EQ(machine.last_commit_index(), 3u);
  };

  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<MetaStateMachine> machine = std::move(*opened);
    apply_and_expect_rejected(*machine);
  }

  // Without a snapshot the state machine reopens at zero; replaying the
  // durable WAL prefix must produce the same rejection and Creating state.
  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*reopened);
  EXPECT_EQ(machine->last_commit_index(), 0u);
  apply_and_expect_rejected(*machine);
}

TEST_F(MetaStateMachineTest, SnapshotInstallRestoresAllStores) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<MetaStateMachine> machine = std::move(*opened);
    Commit(*machine, 1, MakeRegister(0x11));
    Commit(*machine, 2, MakeRegister(0x22));
    Commit(*machine, 3, MakeCreateGroup("g1", 1));
    CreateSnapshot(*machine, /*log_idx=*/3, /*log_term=*/5);

    ASSERT_TRUE(captured_.has_value());
    EXPECT_EQ(captured_->index, 3u);
    EXPECT_EQ(captured_->term, 5u);
  }

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*reopened);
  EXPECT_EQ(machine->last_commit_index(), 3u);
  ASSERT_TRUE(captured_.has_value());
  EXPECT_EQ(captured_->index, 3u);
  const MetaStores stores = machine->StoresSnapshot();
  EXPECT_EQ(stores.identity_.NodeCount(), 2u);
  EXPECT_TRUE(stores.topology_.GroupExists("g1"));
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 1u);
  EXPECT_EQ(stores.audit_.size(), 3u);
}

TEST_F(MetaStateMachineTest,
       UncontrolledFailoverTransitionSurvivesSnapshotInstall) {
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*opened);

  const std::string owner = MakeNodeId(0x11);
  const std::string group_id = "g1\ntransition=forged value";
  const lavik::meta::MetaAssignmentId owner_assignment = MakeRequestId(0x31);

  lavik::meta::ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  manifest.meta_members_ = {{1, "tcp://127.0.0.1:7101", "tcp://127.0.0.1:7301",
                             "tcp://127.0.0.1:7201"}};
  manifest.data_nodes_ = {{owner, "tcp://127.0.0.1:6379"}};
  manifest.groups_ = {{group_id, owner, {}}};
  manifest.slot_ranges_ = {{0, 16383, group_id}};

  SubmitOperation root;
  root.request_id_ = MakeRequestId(0x01);
  root.actor_.principal_ = std::string(kEntryPrincipal);
  root.actor_.readable_time_ = std::string(kEntryReadableTime);
  root.operation_id_ = MakeRequestId(0x02);
  root.kind_ = std::string(lavik::meta::kMetaClusterCreateOperationKind);
  const auto intent =
      lavik::meta::EncodeClusterCreateRequest(manifest, root.operation_id_);
  ASSERT_TRUE(intent.ok()) << intent.status();
  root.intent_ = *intent;
  root.intent_hash_ = lavik::meta::MetaSha256(root.intent_);
  Commit(*machine, 1, root);

  lavik::meta::PutPolicy automatic;
  automatic.request_id_ = MakeRequestId(0x06);
  automatic.actor_ = root.actor_;
  automatic.policy_id_ =
      std::string(lavik::meta::kAutomaticUncontrolledFailoverPolicyId);
  automatic.version_ = 1;
  automatic.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":5000})";
  Commit(*machine, 2, automatic);

  lavik::meta::PutPolicy policy;
  policy.request_id_ = MakeRequestId(0x0a);
  policy.actor_ = root.actor_;
  policy.policy_id_ = std::string(lavik::meta::kAuthorityLeasePolicyId);
  policy.version_ = 1;
  policy.content_ = R"({"kind":"authority-lease-v1","duration_ms":5000})";
  Commit(*machine, 3, policy);

  lavik::meta::PutPolicy recovery;
  recovery.request_id_ = MakeRequestId(0x0b);
  recovery.actor_ = root.actor_;
  recovery.policy_id_ = std::string(lavik::meta::kCandidateRecoveryPolicyId);
  recovery.version_ = 1;
  recovery.content_ = R"({"kind":"candidate-recovery-v1","budget_ms":2000})";
  Commit(*machine, 4, recovery);

  lavik::meta::CompleteOperation complete;
  complete.request_id_ = MakeRequestId(0x03);
  complete.actor_ = root.actor_;
  complete.operation_id_ = root.operation_id_;
  complete.expected_revision_ = 0;
  complete.result_ = "cluster-created";
  Commit(*machine, 5, complete);

  RegisterNode node = MakeRegister(0x11);
  node.role_ = lavik::meta::MetaNodeRole::kPrimary;
  Commit(*machine, 6, node);

  CreateGroup group = MakeCreateGroup(group_id, 1);
  group.actor_ = root.actor_;
  Commit(*machine, 7, group);

  lavik::meta::AssignNodeToGroup assign;
  assign.request_id_ = MakeRequestId(0x05);
  assign.actor_ = root.actor_;
  assign.group_id_ = group_id;
  assign.node_id_ = owner;
  assign.assignment_id_ = owner_assignment;
  assign.role_ = lavik::meta::MetaNodeRole::kPrimary;
  assign.expected_revision_ = 1;
  assign.new_topology_epoch_ = 2;
  Commit(*machine, 8, assign);

  lavik::meta::BeginGroupTerm begin_term;
  begin_term.request_id_ = MakeRequestId(0x07);
  begin_term.actor_ = root.actor_;
  begin_term.group_id_ = group_id;
  begin_term.expected_term_ = 0;
  begin_term.new_term_ = 1;
  Commit(*machine, 9, begin_term);

  lavik::meta::ActivateAuthority activate;
  activate.request_id_ = MakeRequestId(0x08);
  activate.actor_ = root.actor_;
  activate.group_id_ = group_id;
  activate.expected_term_ = 1;
  activate.new_owner_ = owner;
  activate.new_topology_epoch_ = 3;
  Commit(*machine, 10, activate);

  lavik::meta::BeginUncontrolledFailover begin;
  begin.request_id_ = MakeRequestId(0x09);
  begin.actor_ = root.actor_;
  begin.group_id_ = group_id;
  begin.transition_id_ = MakeRequestId(0x41);
  begin.target_term_ = 2;
  begin.expected_owner_node_id_ = owner;
  begin.expected_owner_assignment_id_ = owner_assignment;
  begin.expected_membership_revision_ = 2;
  begin.expected_group_term_ = 1;
  begin.expected_population_manifest_revision_ = 0;
  begin.expected_population_manifest_digest_.fill(0);
  begin.expected_partition_replication_epoch_ = 0;
  Commit(*machine, 11, begin);

  const MetaStores committed = machine->StoresSnapshot();
  ASSERT_EQ(committed.topology_.ClusterLifecycle().state_,
            lavik::meta::MetaClusterLifecycle::kCreated);
  const auto committed_group = committed.topology_.FindGroup(group_id);
  ASSERT_TRUE(committed_group.has_value());
  ASSERT_TRUE(committed_group->failover_transition_.has_value());
  const lavik::meta::MetaFailoverTransition committed_transition =
      *committed_group->failover_transition_;
  EXPECT_EQ(committed_transition.transition_id_, begin.transition_id_);
  EXPECT_EQ(committed_transition.revision_, 11u);
  EXPECT_EQ(committed_transition.target_term_, 2u);
  EXPECT_FALSE(committed_transition.candidate_action_.has_value());

  const auto committed_grant = committed.topology_.AuthorityFor(group_id);
  ASSERT_TRUE(committed_grant.has_value());
  EXPECT_EQ(committed_grant->group_term_, 2u);
  EXPECT_FALSE(committed_grant->grant_.has_value());

  CreateSnapshot(*machine, /*log_idx=*/11, /*log_term=*/4);
  machine.reset();

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  machine = std::move(*reopened);
  EXPECT_EQ(machine->last_commit_index(), 11u);

  const MetaStores restored = machine->StoresSnapshot();
  const auto restored_group = restored.topology_.FindGroup(group_id);
  ASSERT_TRUE(restored_group.has_value());
  ASSERT_TRUE(restored_group->failover_transition_.has_value());
  EXPECT_EQ(*restored_group->failover_transition_, committed_transition);
  EXPECT_EQ(restored_group->record_.group_term_, 2u);
  const auto restored_grant = restored.topology_.AuthorityFor(group_id);
  ASSERT_TRUE(restored_grant.has_value());
  EXPECT_EQ(restored_grant->group_term_, 2u);
  EXPECT_FALSE(restored_grant->grant_.has_value());

  // If the Raft core presents the snapshot's final entry again, exact-index
  // replay must validate the installed post-state instead of advancing the
  // term or transition revision a second time.
  Commit(*machine, 11, begin);
  const MetaStores replayed = machine->StoresSnapshot();
  const auto replayed_group = replayed.topology_.FindGroup(group_id);
  ASSERT_TRUE(replayed_group.has_value());
  ASSERT_TRUE(replayed_group->failover_transition_.has_value());
  EXPECT_EQ(*replayed_group->failover_transition_, committed_transition);
  EXPECT_EQ(replayed_group->record_.group_term_, 2u);
  EXPECT_EQ(replayed.audit_.size(), 11u);
  EXPECT_EQ(machine->last_commit_index(), 11u);

  lavik::meta::MetaBootIncarnation boot{};
  boot.fill(0x61);
  lavik::meta::MetaReplicationHistoryId history{};
  history.fill(0x62);
  lavik::meta::MetaFailoverCandidateAction selected_action;
  selected_action.action_id_ = MakeRequestId(0x51);
  selected_action.candidate_ = {owner, owner_assignment, boot};
  selected_action.domain_ = {
      1, owner, owner_assignment, boot, history, 1,
  };

  lavik::meta::SetUncontrolledCandidate select;
  select.request_id_ = MakeRequestId(0x52);
  select.group_id_ = group_id;
  select.expected_transition_ = {begin.transition_id_, 11};
  select.candidate_action_ = selected_action;

  ScopedLogCapture logs;
  Commit(*machine, 12, select);
  const std::string selected_log = logs.Take();
  EXPECT_NE(selected_log.find("failover event=candidate-selected"),
            std::string::npos);
  EXPECT_NE(
      selected_log.find("group=g1%0Atransition%3Dforged%20value transition="),
      std::string::npos);
  EXPECT_EQ(selected_log.find("group=g1\ntransition=forged value"),
            std::string::npos);

  // State-dependent candidate event classification cannot be reconstructed
  // after the previous action is overwritten. Exact post-effect replay is
  // therefore intentionally silent instead of relabelling index 12 as a
  // replacement.
  Commit(*machine, 12, select);
  EXPECT_EQ(logs.Take().find("failover event="), std::string::npos);

  lavik::meta::MetaFailoverCandidateAction fallback_action = selected_action;
  fallback_action.action_id_ = MakeRequestId(0x53);
  fallback_action.domain_.source_history_id_.fill(0x63);
  lavik::meta::SetUncontrolledCandidate fallback = select;
  fallback.request_id_ = MakeRequestId(0x54);
  fallback.expected_transition_.revision_ = 12;
  fallback.candidate_action_ = fallback_action;
  Commit(*machine, 13, fallback);
  EXPECT_NE(logs.Take().find("failover event=domain-fallback"),
            std::string::npos);
  Commit(*machine, 13, fallback);
  EXPECT_EQ(logs.Take().find("failover event="), std::string::npos);
}

TEST_F(MetaStateMachineTest, SnapshotExactCutPoint) {
  // The captured state is exactly the snapshot's
  // last_log_idx state — commit N+1.. after create_snapshot() must not leak
  // into the snapshot file. Asserted on MetaStores CONTENT, not the index.
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*opened);
  Commit(*machine, 1, MakeRegister(0x11));
  Commit(*machine, 2, MakeRegister(0x22));
  CreateSnapshot(*machine, /*log_idx=*/2, /*log_term=*/7);
  // These commits land after the capture point and may race the async writer.
  Commit(*machine, 3, MakeRegister(0x33));
  Commit(*machine, 4, MakeRegister(0x44));
  EXPECT_EQ(machine->last_commit_index(), 4u);

  ASSERT_TRUE(captured_.has_value());
  ASSERT_EQ(captured_->index, 2u);
  const std::string envelope = captured_->bytes;
  EXPECT_FALSE(machine->Capture(2).ok());
  auto snap_stores = MetaStores::Deserialize(envelope);
  ASSERT_TRUE(snap_stores.ok()) << snap_stores.status();
  EXPECT_EQ(snap_stores->identity_.NodeCount(), 2u);
  EXPECT_TRUE(snap_stores->identity_.FindNode(MakeNodeId(0x11)).has_value());
  EXPECT_TRUE(snap_stores->identity_.FindNode(MakeNodeId(0x22)).has_value());
  EXPECT_FALSE(snap_stores->identity_.FindNode(MakeNodeId(0x33)).has_value());
  EXPECT_FALSE(snap_stores->identity_.FindNode(MakeNodeId(0x44)).has_value());
  EXPECT_EQ(snap_stores->audit_.size(), 2u);

  // The live state moved on past the snapshot point.
  EXPECT_EQ(machine->StoresSnapshot().identity_.NodeCount(), 4u);
}

TEST_F(MetaStateMachineTest, ReplayAfterSnapshotDoesNotGrowAudit) {
  // The commit watermark only advances with snapshots, so entries
  // applied after the last snapshot are REPLAYED after a crash. Replay of
  // the same log index must produce the identical audit record — the window
  // keyed by log index does not grow during replay.
  std::shared_ptr<lavik::meta::MetaRaftBuffer> c4 =
      EncodeOrDie(MakeRegister(0x44));
  std::shared_ptr<lavik::meta::MetaRaftBuffer> c5 =
      EncodeOrDie(MakeRegister(0x55));
  ASSERT_NE(c4, nullptr);
  ASSERT_NE(c5, nullptr);

  lavik::meta::MetaAuditRecord record4_before;
  std::string audit_before;
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<MetaStateMachine> machine = std::move(*opened);
    Commit(*machine, 1, MakeRegister(0x11));
    Commit(*machine, 2, MakeRegister(0x22));
    Commit(*machine, 3, MakeRegister(0x33));
    CreateSnapshot(*machine, 3, 1);
    machine->commit(4, *c4);
    machine->commit(5, *c5);
    const MetaStores stores = machine->StoresSnapshot();
    ASSERT_EQ(stores.audit_.size(), 5u);
    const auto record4 = stores.audit_.Find(4);
    ASSERT_TRUE(record4.has_value());
    record4_before = *record4;
    audit_before = *stores.audit_.Serialize();
  }

  // Crash without a newer snapshot: reopen restores @3; the core replays 4..5.
  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*reopened);
  EXPECT_EQ(machine->last_commit_index(), 3u);
  EXPECT_EQ(machine->StoresSnapshot().identity_.NodeCount(), 3u);
  EXPECT_EQ(machine->StoresSnapshot().audit_.size(), 3u);

  machine->commit(4, *c4);
  machine->commit(5, *c5);
  const MetaStores stores = machine->StoresSnapshot();
  EXPECT_EQ(stores.identity_.NodeCount(), 5u);
  // Uniqueness: replay rewrote the same records; the window did not grow.
  EXPECT_EQ(stores.audit_.size(), 5u);
  const auto record4 = stores.audit_.Find(4);
  ASSERT_TRUE(record4.has_value());
  EXPECT_EQ(*record4, record4_before);
  EXPECT_EQ(*stores.audit_.Serialize(), audit_before);
}

TEST_F(MetaStateMachineTest, SnapshotInstallValidatesBeforeReplacingState) {
  auto leader = Open();
  ASSERT_TRUE(leader.ok());
  Commit(**leader, 1, MakeRegister(0x11));
  Commit(**leader, 2, MakeRegister(0x22));
  Commit(**leader, 3, MakeCreateGroup("g1", 1));
  const auto image = (*leader)->Capture(3);
  ASSERT_TRUE(image.ok()) << image.status();
  auto follower = Open();
  ASSERT_TRUE(follower.ok());
  ASSERT_TRUE((*follower)->Install(3, *image).ok());
  EXPECT_EQ((*follower)->StoresSnapshot().audit_.Serialize(),
            (*leader)->StoresSnapshot().audit_.Serialize());
  EXPECT_EQ((*follower)->StoresSnapshot().identity_.NodeCount(), 2u);
  EXPECT_TRUE((*follower)->StoresSnapshot().topology_.GroupExists("g1"));
  EXPECT_EQ((*follower)->last_commit_index(), 3u);
  EXPECT_FALSE((*follower)->Install(4, "corrupt image").ok());
  EXPECT_FALSE((*follower)->Install(2, *image).ok());
  EXPECT_EQ((*follower)->last_commit_index(), 3u);
  EXPECT_EQ((*follower)->StoresSnapshot().audit_.Serialize(),
            (*leader)->StoresSnapshot().audit_.Serialize());
}

TEST_F(MetaStateMachineTest, CapturedImageRemainsOwnedWhileLiveStateAdvances) {
  auto machine = Open();
  ASSERT_TRUE(machine.ok());
  Commit(**machine, 1, MakeRegister(0x11));
  auto first = (*machine)->Capture(1);
  ASSERT_TRUE(first.ok());
  Commit(**machine, 2, MakeRegister(0x22));
  auto second = (*machine)->Capture(2);
  ASSERT_TRUE(second.ok());
  const auto old = MetaStores::Deserialize(*first);
  ASSERT_TRUE(old.ok());
  EXPECT_EQ(old->identity_.NodeCount(), 1u);
  EXPECT_NE(*first, *second);
}

TEST_F(MetaStateMachineTest, SubmitOperationSeqEqualsLogIndex) {
  // operation_seq is the SubmitOperation command's raft
  // log index — the state machine hands ApplyCommitted its commit index and
  // the journal keys on it directly (no counter).
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*opened);

  SubmitOperation submit;
  submit.request_id_ = MakeRequestId(0x41);
  submit.actor_.principal_ = std::string(kEntryPrincipal);
  submit.actor_.readable_time_ = std::string(kEntryReadableTime);
  submit.kind_ = "migration";
  submit.intent_ = "intent-bytes";
  Commit(*machine, 7, submit);

  {
    const MetaStores stores = machine->StoresSnapshot();
    const auto operation = stores.operation_.FindOperationBySeq(7);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(operation->operation_seq_, 7u);
    EXPECT_EQ(operation->kind_, "migration");
    // The journal persists the submitter's injected ActorContext, decoded off
    // the wire like any other field.
    EXPECT_EQ(operation->actor_.principal_, kEntryPrincipal);
    EXPECT_EQ(operation->actor_.readable_time_, kEntryReadableTime);
  }

  // Replay of the same index: idempotent accept, no state growth, no audit
  // growth during replay.
  Commit(*machine, 7, submit);
  const MetaStores stores = machine->StoresSnapshot();
  EXPECT_EQ(stores.audit_.size(), 1u);
  EXPECT_EQ(stores.operation_.FindOperationBySeq(7)->operation_seq_, 7u);
}

// ---------------------------------------------------------------------------
// MetaRaft integration over the real adapters (single node)
// ---------------------------------------------------------------------------

// Real C ABI integration: recovery and capture use the production Go WAL and
// snapshot executor, while application results remain typed C++ Meta verdicts.
class MetaServerIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = MakeTestDir("raft", "server"); }
  void TearDown() override {
    StopServer();
    RemoveTestDir(dir_);
  }
  void StartServer(uint64_t distance = 0) {
    auto machine = MetaStateMachine::Open(dir_);
    ASSERT_TRUE(machine.ok()) << machine.status();
    machine_ = std::move(*machine);
    auto options = lavik::test::SingleMetaOptions(dir_);
    options.snapshot_distance_ = distance;
    auto server = lavik::meta::MetaRaft::Open(std::move(options), *machine_);
    ASSERT_TRUE(server.ok()) << server.status();
    server_ = std::move(*server);
    ASSERT_TRUE(WaitFor([&] { return server_->is_leader(); },
                        std::chrono::seconds(15)));
  }
  void StopServer() {
    if (server_) server_->shutdown();
    server_.reset();
    machine_.reset();
  }
  void AppendAndWait(const MetaCommand& cmd) {
    const auto buffer = EncodeOrDie(cmd);
    auto result = server_->append_entries({buffer});
    ASSERT_TRUE(WaitFor([&] { return result->has_result(); },
                        std::chrono::seconds(10)));
    ASSERT_EQ(result->get_result_code(), lavik::meta::MetaRaftResultCode::OK);
  }
  std::filesystem::path dir_;
  std::shared_ptr<MetaStateMachine> machine_;
  std::shared_ptr<lavik::meta::MetaRaft> server_;
};

TEST_F(MetaServerIntegrationTest, CommitThenRestartReplaysLog) {
  StartServer();
  AppendAndWait(MakeRegister(0x11));
  AppendAndWait(MakeRegister(0x22));
  const auto term = server_->get_term();
  StopServer();
  StartServer();
  EXPECT_EQ(machine_->StoresSnapshot().identity_.NodeCount(), 2u);
  EXPECT_GT(server_->get_term(), term);
  AppendAndWait(MakeRegister(0x33));
  EXPECT_EQ(machine_->StoresSnapshot().identity_.NodeCount(), 3u);
}

TEST_F(MetaServerIntegrationTest, SnapshotCompactionAndRestart) {
  StartServer();
  for (uint8_t i = 0x10; i < 0x19; ++i) AppendAndWait(MakeRegister(i));
  const auto snapshot = server_->create_snapshot({});
  ASSERT_GT(snapshot, 0u);
  ASSERT_TRUE(WaitFor([&] { return server_->FirstLogIndex() == snapshot + 1; },
                      std::chrono::seconds(5)));
  StopServer();
  StartServer();
  EXPECT_GE(machine_->last_commit_index(), snapshot);
  EXPECT_EQ(machine_->StoresSnapshot().identity_.NodeCount(), 9u);
  EXPECT_GE(server_->FirstLogIndex(), snapshot + 1);
  AppendAndWait(MakeRegister(0x19));
  EXPECT_EQ(machine_->StoresSnapshot().identity_.NodeCount(), 10u);
}

TEST_F(MetaServerIntegrationTest, AutomaticSnapshotAndTailReplay) {
  StartServer(5);
  for (uint8_t i = 0x30; i < 0x3c; ++i) AppendAndWait(MakeRegister(i));
  ASSERT_TRUE(WaitFor([&] { return server_->FirstLogIndex() > 1; },
                      std::chrono::seconds(10)));
  const auto snapshot = server_->get_last_snapshot_idx();
  StopServer();
  StartServer(5);
  EXPECT_GE(server_->get_last_snapshot_idx(), snapshot);
  EXPECT_EQ(machine_->StoresSnapshot().identity_.NodeCount(), 12u);
  AppendAndWait(MakeRegister(0x3c));
  EXPECT_EQ(machine_->StoresSnapshot().identity_.NodeCount(), 13u);
}

TEST_F(MetaServerIntegrationTest, ShutdownJoinsAllCallbacks) {
  StartServer();
  AppendAndWait(MakeRegister(0x10));
  AppendAndWait(MakeRegister(0x11));
  auto closing = std::async(std::launch::async, [&] { server_->shutdown(); });
  EXPECT_EQ(closing.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);
  closing.get();
}

}  // namespace
