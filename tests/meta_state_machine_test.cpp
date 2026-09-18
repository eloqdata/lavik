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

// Tests for MetaStateMachine and its integration with the WAL v1
// NuraftLogStore through a real raft_server.
//
// Vertical slices:
//   1. Component contract: real commands committed directly into the state
//      machine; kill/reopen recovery (close + reopen stands in for process
//      restart; fdatasync-before-return is what makes it crash-safe);
//      snapshot exact cut point; snapshot -> compact -> reopen; logical
//      snapshot transmission; audit uniqueness under
//      replay; domain-reject vs fail-stop classification (death test).
//   2. Core-driven integration: a single-node raft_server running on the
//      real adapters (NuraftStateMgr + WAL v1 NuraftLogStore +
//      MetaStateMachine) proves the persistence ordering the Raft core
//      relies on, replay-based recovery after restart, and clean shutdown
//      across changes to peer commit tracking. These integration tests use
//      the production state machine with real commands.
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
#include "lavik/meta/nuraft_log_store.h"
#include "lavik/meta/nuraft_state_mgr.h"
#include "lavik/meta/state_apply.h"
#include "lavik/meta/state_machine.h"
#include "libnuraft/nuraft.hxx"
#include "libnuraft/raft_server_handler.hxx"
#include "spdlog/sinks/ostream_sink.h"
#include "spdlog/spdlog.h"
#include "support/test_data_path.h"

namespace {

using lavik::meta::CreateGroup;
using lavik::meta::MetaAuditVerdict;
using lavik::meta::MetaCommand;
using lavik::meta::MetaRequestId;
using lavik::meta::MetaStateMachine;
using lavik::meta::MetaStores;
using lavik::meta::NuraftLogStore;
using lavik::meta::NuraftStateMgr;
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
nuraft::ptr<nuraft::buffer> EncodeOrDie(const MetaCommand& cmd) {
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

  absl::StatusOr<std::unique_ptr<MetaStateMachine>> Open() {
    return MetaStateMachine::Open(dir_);
  }

  // Commits one encoded command at `log_idx` and returns the stores copy.
  void Commit(MetaStateMachine& machine, uint64_t log_idx,
              const MetaCommand& cmd) {
    nuraft::ptr<nuraft::buffer> buf = EncodeOrDie(cmd);
    ASSERT_NE(buf, nullptr);
    nuraft::ptr<nuraft::buffer> result = machine.commit(log_idx, *buf);
    ASSERT_NE(result, nullptr);
  }

  // Drives create_snapshot and waits for the async writer thread to invoke
  // when_done; snapshot file IO deliberately runs off the calling thread.
  void CreateSnapshot(MetaStateMachine& machine, uint64_t log_idx,
                      uint64_t log_term) {
    nuraft::ptr<nuraft::cluster_config> config =
        nuraft::cs_new<nuraft::cluster_config>();
    nuraft::snapshot snap(log_idx, log_term, config);
    std::mutex done_mutex;
    std::condition_variable done_cv;
    bool done = false;
    bool result = false;
    nuraft::async_result<bool>::handler_type handler =
        [&](bool& ret, nuraft::ptr<std::exception>& err) {
          EXPECT_EQ(err, nullptr);
          std::lock_guard<std::mutex> lock(done_mutex);
          done = true;
          result = ret;
          done_cv.notify_one();
        };
    machine.create_snapshot(snap, handler);
    std::unique_lock<std::mutex> lock(done_mutex);
    ASSERT_TRUE(done_cv.wait_for(lock, std::chrono::seconds(30), [&] {
      return done;
    })) << "snapshot writer thread never completed";
    ASSERT_TRUE(result);
  }

  // Streams one snapshot's logical objects into the serialized MetaStores
  // envelope (the exact bytes a follower would receive).
  std::string StreamSnapshot(MetaStateMachine& machine,
                             nuraft::snapshot& snap) {
    std::string bytes;
    void* ctx = nullptr;
    uint64_t obj_id = 0;
    bool is_last = false;
    size_t objects = 0;
    while (!is_last) {
      nuraft::ptr<nuraft::buffer> data;
      const int rc =
          machine.read_logical_snp_obj(snap, ctx, obj_id, data, is_last);
      EXPECT_EQ(rc, 0);
      if (rc != 0) break;
      if (data != nullptr && data->size() > 0) {
        bytes.append(reinterpret_cast<const char*>(data->data_begin()),
                     data->size());
      }
      ++obj_id;
      ++objects;
      EXPECT_LT(objects, 10000u);  // runaway protocol guard
      if (objects >= 10000u) break;
    }
    machine.free_user_snp_ctx(ctx);
    return bytes;
  }

  std::filesystem::path SnapshotPath(uint64_t log_idx) {
    return dir_ / ("snapshot_" + std::to_string(log_idx) + ".dat");
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
  nuraft::ptr<nuraft::cluster_config> config =
      nuraft::cs_new<nuraft::cluster_config>();
  machine->commit_config(/*log_idx=*/1, config);
  EXPECT_EQ(machine->last_commit_index(), 2u);

  machine->commit_config(/*log_idx=*/3, config);
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
        nuraft::ptr<nuraft::buffer> garbage = nuraft::buffer::alloc(3);
        std::memcpy(garbage->data_begin(), "xyz", 3);
        machine->commit(1, *garbage);
      },
      "");
}

TEST_F(MetaStateMachineTest, RestartWithoutSnapshotReplaysFromScratch) {
  nuraft::ptr<nuraft::buffer> c1 = EncodeOrDie(MakeRegister(0x11));
  nuraft::ptr<nuraft::buffer> c2 = EncodeOrDie(MakeRegister(0x22));
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
  EXPECT_EQ(machine->last_snapshot(), nullptr);
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

  const nuraft::ptr<nuraft::buffer> root_bytes = EncodeOrDie(root);
  const nuraft::ptr<nuraft::buffer> automatic_bytes = EncodeOrDie(automatic);
  const nuraft::ptr<nuraft::buffer> complete_bytes = EncodeOrDie(complete);
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

TEST_F(MetaStateMachineTest, SnapshotIsDurableAcrossRestart) {
  {
    auto opened = Open();
    ASSERT_TRUE(opened.ok()) << opened.status();
    std::unique_ptr<MetaStateMachine> machine = std::move(*opened);
    Commit(*machine, 1, MakeRegister(0x11));
    Commit(*machine, 2, MakeRegister(0x22));
    Commit(*machine, 3, MakeCreateGroup("g1", 1));
    CreateSnapshot(*machine, /*log_idx=*/3, /*log_term=*/5);

    nuraft::ptr<nuraft::snapshot> last = machine->last_snapshot();
    ASSERT_NE(last, nullptr);
    EXPECT_EQ(last->get_last_log_idx(), 3u);
    EXPECT_EQ(last->get_last_log_term(), 5u);
  }

  auto reopened = Open();
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*reopened);
  EXPECT_EQ(machine->last_commit_index(), 3u);
  nuraft::ptr<nuraft::snapshot> last = machine->last_snapshot();
  ASSERT_NE(last, nullptr);
  EXPECT_EQ(last->get_last_log_idx(), 3u);
  const MetaStores stores = machine->StoresSnapshot();
  EXPECT_EQ(stores.identity_.NodeCount(), 2u);
  EXPECT_TRUE(stores.topology_.GroupExists("g1"));
  EXPECT_EQ(stores.topology_.TopologyEpoch(), 1u);
  EXPECT_EQ(stores.audit_.size(), 3u);
}

TEST_F(MetaStateMachineTest,
       UncontrolledFailoverTransitionIsDurableAcrossSnapshotRestart) {
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

  nuraft::ptr<nuraft::snapshot> snap = machine->last_snapshot();
  ASSERT_NE(snap, nullptr);
  ASSERT_EQ(snap->get_last_log_idx(), 2u);
  const std::string envelope = StreamSnapshot(*machine, *snap);
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
  nuraft::ptr<nuraft::buffer> c4 = EncodeOrDie(MakeRegister(0x44));
  nuraft::ptr<nuraft::buffer> c5 = EncodeOrDie(MakeRegister(0x55));
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

TEST_F(MetaStateMachineTest, LogicalSnapshotTransmissionRoundTrip) {
  // Leader side: commit real commands and snapshot them.
  auto leader_opened = Open();
  ASSERT_TRUE(leader_opened.ok()) << leader_opened.status();
  std::unique_ptr<MetaStateMachine> leader = std::move(*leader_opened);
  Commit(*leader, 1, MakeRegister(0x11));
  Commit(*leader, 2, MakeRegister(0x22));
  Commit(*leader, 3, MakeCreateGroup("g1", 1));
  CreateSnapshot(*leader, 3, 2);
  nuraft::ptr<nuraft::snapshot> snap = leader->last_snapshot();
  ASSERT_NE(snap, nullptr);

  // Follower side: receive every logical object, then apply. The received
  // snapshot is durable before apply (save_logical_snp_obj writes the file;
  // apply_snapshot loads it).
  std::filesystem::path follower_dir = MakeTestDir("w3a", "sm_follower");
  auto follower_opened = MetaStateMachine::Open(follower_dir);
  ASSERT_TRUE(follower_opened.ok()) << follower_opened.status();
  std::unique_ptr<MetaStateMachine> follower = std::move(*follower_opened);

  void* read_ctx = nullptr;
  uint64_t obj_id = 0;
  bool is_last = false;
  bool is_first = true;
  size_t objects = 0;
  while (!is_last) {
    nuraft::ptr<nuraft::buffer> data;
    const int rc =
        leader->read_logical_snp_obj(*snap, read_ctx, obj_id, data, is_last);
    ASSERT_EQ(rc, 0);
    ASSERT_NE(data, nullptr);
    follower->save_logical_snp_obj(*snap, obj_id, *data, is_first, is_last);
    is_first = false;
    ++objects;
    ASSERT_LT(objects, 10000u);  // runaway protocol guard
  }
  leader->free_user_snp_ctx(read_ctx);
  EXPECT_GE(objects, 1u);

  ASSERT_TRUE(follower->apply_snapshot(*snap));
  {
    const MetaStores leader_stores = leader->StoresSnapshot();
    const MetaStores follower_stores = follower->StoresSnapshot();
    EXPECT_EQ(follower_stores.identity_.NodeCount(), 2u);
    EXPECT_TRUE(follower_stores.topology_.GroupExists("g1"));
    EXPECT_EQ(follower_stores.topology_.TopologyEpoch(), 1u);
    EXPECT_EQ(follower_stores.audit_.size(), 3u);
    EXPECT_EQ(follower_stores.audit_.Serialize(),
              leader_stores.audit_.Serialize());
  }
  EXPECT_EQ(follower->last_commit_index(), 3u);

  // The received snapshot is durable: a fresh open sees the same state.
  follower.reset();
  auto reopened = MetaStateMachine::Open(follower_dir);
  ASSERT_TRUE(reopened.ok()) << reopened.status();
  follower = std::move(*reopened);
  EXPECT_EQ(follower->StoresSnapshot().identity_.NodeCount(), 2u);
  EXPECT_EQ(follower->last_commit_index(), 3u);
  ASSERT_NE(follower->last_snapshot(), nullptr);
  EXPECT_EQ(follower->last_snapshot()->get_last_log_idx(), 3u);
  RemoveTestDir(follower_dir);
}

TEST_F(MetaStateMachineTest, MidStreamPruneKeepsPinnedSnapshotStreamable) {
  // Snapshot-sync livelock regression: pruning a snapshot whose read stream
  // is still open
  // fails the stream's next read, NuRaft resets the sync context on a failed
  // read and restarts from object zero with the newest snapshot, so a
  // follower whose stream time exceeded the snapshot interval could never
  // complete a sync. A snapshot with an open read stream must stay alive
  // until free_user_snp_ctx; only unpinned snapshots may be pruned.
  auto opened = Open();
  ASSERT_TRUE(opened.ok()) << opened.status();
  std::unique_ptr<MetaStateMachine> machine = std::move(*opened);

  Commit(*machine, 1, MakeRegister(0x11));
  Commit(*machine, 2, MakeRegister(0x22));
  Commit(*machine, 3, MakeRegister(0x33));
  CreateSnapshot(*machine, 3, 1);

  // Open a read stream on snapshot 3; the pin is held until free_user_snp_ctx
  // even though this small envelope streams as a single last object.
  nuraft::ptr<nuraft::snapshot> snap3 = machine->last_snapshot();
  ASSERT_NE(snap3, nullptr);
  ASSERT_EQ(snap3->get_last_log_idx(), 3u);
  void* ctx = nullptr;
  nuraft::ptr<nuraft::buffer> data;
  bool is_last = false;
  ASSERT_EQ(machine->read_logical_snp_obj(*snap3, ctx, 0, data, is_last), 0);
  ASSERT_NE(ctx, nullptr);

  // A newer snapshot arrives while the stream on 3 is (formally) open: the
  // pinned snapshot survives the prune.
  Commit(*machine, 4, MakeRegister(0x44));
  CreateSnapshot(*machine, 4, 1);
  EXPECT_TRUE(std::filesystem::exists(SnapshotPath(3)));
  EXPECT_TRUE(std::filesystem::exists(SnapshotPath(4)));
  // The open stream still serves its snapshot.
  EXPECT_TRUE(is_last);
  ASSERT_NE(data, nullptr);

  machine->free_user_snp_ctx(ctx);

  // Only after the pin is released does the next prune drop snapshots 3/4.
  Commit(*machine, 5, MakeRegister(0x55));
  CreateSnapshot(*machine, 5, 1);
  EXPECT_FALSE(std::filesystem::exists(SnapshotPath(3)));
  EXPECT_FALSE(std::filesystem::exists(SnapshotPath(4)));
  EXPECT_TRUE(std::filesystem::exists(SnapshotPath(5)));

  // A fresh read stream on a pruned snapshot fails with -1 (NuRaft's
  // retry-with-newer signal) and allocates no cursor.
  {
    nuraft::ptr<nuraft::cluster_config> config =
        nuraft::cs_new<nuraft::cluster_config>();
    nuraft::snapshot stale_snap(3, 1, config);
    void* stale_ctx = nullptr;
    nuraft::ptr<nuraft::buffer> stale_data;
    bool stale_last = false;
    EXPECT_EQ(machine->read_logical_snp_obj(stale_snap, stale_ctx, 0,
                                            stale_data, stale_last),
              -1);
    EXPECT_EQ(stale_ctx, nullptr);
  }
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
// raft_server integration over the real adapters (single node)
// ---------------------------------------------------------------------------

// Minimal real-time scheduler for driving the Raft core in tests: one thread
// per delayed task. Cancelled tasks still wake and exit cheaply because
// delayed_task::execute() checks the cancellation flag.
class ThreadScheduler : public nuraft::delayed_task_scheduler {
 public:
  ~ThreadScheduler() override { Shutdown(); }

  void schedule(nuraft::ptr<nuraft::delayed_task>& task,
                nuraft::int32 milliseconds) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopped_) return;
    threads_.emplace_back([this, task, milliseconds]() {
      {
        std::unique_lock<std::mutex> lock(mutex_);
        stopped_cv_.wait_for(lock, std::chrono::milliseconds(milliseconds),
                             [this] { return stopped_; });
      }
      if (!stopped_) task->execute();
    });
  }

  void Shutdown() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      stopped_ = true;
    }
    stopped_cv_.notify_all();
    for (std::thread& thread : threads_) {
      if (thread.joinable()) thread.join();
    }
  }

 private:
  // delayed_task::cancel() already flips the flag execute() checks, so no
  // scheduler-side bookkeeping is needed.
  void cancel_impl(nuraft::ptr<nuraft::delayed_task>& task) override {}

  std::mutex mutex_;
  std::condition_variable stopped_cv_;
  bool stopped_ = false;
  std::vector<std::thread> threads_;
};

// Single-node clusters never open peer connections.
class NullRpcClientFactory : public nuraft::rpc_client_factory {
 public:
  nuraft::ptr<nuraft::rpc_client> create_client(
      const std::string& endpoint) override {
    return nullptr;
  }
};

class MetaServerIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = MakeTestDir("w3a", "server"); }
  void TearDown() override {
    StopServer();
    RemoveTestDir(dir_);
  }

  // Opens the persistent state without launching the Raft core, so tests can
  // observe the recovered on-disk state before any replay kicks in.
  void OpenStorage() {
    const lavik::meta::NuraftMemberConfig local{
        1, "127.0.0.1:9601", "lavik://meta/1", "127.0.0.1:9701",
        "127.0.0.1:9801"};
    lavik::meta::NuraftStateMgrOpenOptions options{.data_dir_ = dir_,
                                                   .local_member_ = local};
    if (!std::filesystem::exists(std::filesystem::path(dir_) /
                                 "cluster_config.dat")) {
      options.initial_cluster_ = std::vector{local};
    }
    auto mgr = NuraftStateMgr::Open(std::move(options));
    ASSERT_TRUE(mgr.ok()) << mgr.status();
    mgr_ = nuraft::ptr<NuraftStateMgr>(std::move(*mgr));

    auto machine = MetaStateMachine::Open(dir_);
    ASSERT_TRUE(machine.ok()) << machine.status();
    machine_ = nuraft::ptr<MetaStateMachine>(std::move(*machine));
  }

  void LaunchServer(int snapshot_distance, bool elect_leader = true,
                    bool disable_peer_tracking_on_leadership = false) {
    scheduler_ = nuraft::cs_new<ThreadScheduler>();

    nuraft::raft_params params;
    params.with_election_timeout_lower(150);
    params.with_election_timeout_upper(300);
    params.with_hb_interval(50);
    params.with_snapshot_enabled(snapshot_distance);
    params.with_reserved_log_items(0);
    params.with_client_req_timeout(5000);
    params.track_peers_sm_commit_idx_ = disable_peer_tracking_on_leadership;
    params.wait_for_sm_catchup_on_becoming_leader_ =
        disable_peer_tracking_on_leadership;

    nuraft::context* ctx = new nuraft::context(
        mgr_, machine_, /*listener=*/nullptr, /*logger=*/nullptr,
        nuraft::cs_new<NullRpcClientFactory>(), scheduler_, params);
    nuraft::raft_server::init_options options;
    options.skip_initial_election_timeout_ = !elect_leader;
    if (disable_peer_tracking_on_leadership) {
      // Production's creation reconciler switches a caught-up leader from
      // all-peer confirmation to majority completion. Do it synchronously at
      // the role callback to leave a pending notifier target deterministically,
      // before the commit thread has scanned it. Context replacement is the
      // thread-safe parameter publication used by raft_server::update_params.
      options.raft_callback_ = [ctx](nuraft::cb_func::Type type,
                                     nuraft::cb_func::Param*) {
        if (type == nuraft::cb_func::BecomeLeader) {
          auto updated =
              nuraft::cs_new<nuraft::raft_params>(*ctx->get_params());
          updated->track_peers_sm_commit_idx_ = false;
          ctx->set_params(updated);
        }
        return nuraft::cb_func::Ok;
      };
    }
    server_ = nuraft::cs_new<nuraft::raft_server>(ctx, options);
    if (elect_leader) {
      ASSERT_TRUE(WaitFor([this] { return server_->is_leader(); },
                          std::chrono::seconds(15)));
    }
  }

  void StartServer(int snapshot_distance) {
    OpenStorage();
    LaunchServer(snapshot_distance);
  }

  void StopServer() {
    if (server_) {
      server_->shutdown();
    }
    if (machine_) {
      // Shutdown contract (state_machine.h): shutdown() joins the commit
      // thread, so no new snapshot jobs arrive; drain the SM writer so an
      // in-flight when_done lands on the still-alive core before reset().
      machine_->WaitForSnapshotWriterIdle();
    }
    if (server_) {
      server_.reset();
    }
    if (scheduler_) {
      scheduler_->Shutdown();
      scheduler_.reset();
    }
    machine_.reset();
    mgr_.reset();
  }

  // Proposes one committed command and waits for the commit result.
  void AppendAndWait(const MetaCommand& cmd) {
    nuraft::ptr<nuraft::buffer> buf = EncodeOrDie(cmd);
    ASSERT_NE(buf, nullptr);
    std::vector<nuraft::ptr<nuraft::buffer>> logs;
    logs.push_back(buf);
    nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> result =
        server_->append_entries(logs);
    ASSERT_TRUE(WaitFor([&] { return result->has_result(); },
                        std::chrono::seconds(10)));
    EXPECT_EQ(result->get_result_code(), nuraft::cmd_result_code::OK)
        << result->get_result_str();
    EXPECT_TRUE(result->get_accepted());
  }

  std::size_t NodeCount() {
    return machine_->StoresSnapshot().identity_.NodeCount();
  }

  std::filesystem::path dir_;
  nuraft::ptr<NuraftStateMgr> mgr_;
  nuraft::ptr<MetaStateMachine> machine_;
  nuraft::ptr<ThreadScheduler> scheduler_;
  nuraft::ptr<nuraft::raft_server> server_;
};

TEST_F(MetaServerIntegrationTest,
       DisablingPeerCommitTrackingDoesNotStrandShutdown) {
  StartServer(/*snapshot_distance=*/0);
  AppendAndWait(MakeRegister(0x10));
  StopServer();
  OpenStorage();
  ASSERT_NO_FATAL_FAILURE(LaunchServer(
      /*snapshot_distance=*/0, /*elect_leader=*/true,
      /*disable_peer_tracking_on_leadership=*/true));
  ASSERT_TRUE(WaitFor(
      [this] {
        return !server_->get_current_params().track_peers_sm_commit_idx_;
      },
      std::chrono::seconds(5)));
  AppendAndWait(MakeRegister(0x11));

  auto shutdown =
      std::async(std::launch::async, [this] { server_->shutdown(); });
  const auto stopped = shutdown.wait_for(std::chrono::seconds(2));
  if (stopped != std::future_status::ready) {
    // Unstick the old implementation so failure is an assertion, not a hung
    // test process: re-enabling tracking lets it retire its stale target and
    // reach the stop check. The production fix must not need this transition.
    auto params = server_->get_current_params();
    params.track_peers_sm_commit_idx_ = true;
    server_->update_params(params);
  }
  shutdown.get();
  server_.reset();
  EXPECT_EQ(stopped, std::future_status::ready);
}

TEST_F(MetaServerIntegrationTest, CommitThenRestartReplaysLog) {
  StartServer(/*snapshot_distance=*/0);
  AppendAndWait(MakeRegister(0x11));
  AppendAndWait(MakeRegister(0x22));
  EXPECT_EQ(NodeCount(), 2u);
  StopServer();

  // Restart on the same directory: no snapshot exists, so the recovered state
  // machine is empty at commit index 0. (Checked before the core starts: a
  // re-elected leader re-commits the durable log almost immediately.)
  OpenStorage();
  EXPECT_EQ(machine_->last_commit_index(), 0u);
  EXPECT_EQ(NodeCount(), 0u);

  // The new leader's first current-term entry lets it commit everything
  // before it; replay then re-applies the pre-restart entries after quorum
  // confirmation.
  LaunchServer(/*snapshot_distance=*/0);
  AppendAndWait(MakeRegister(0x33));
  ASSERT_TRUE(
      WaitFor([this] { return NodeCount() == 3u; }, std::chrono::seconds(10)));

  // The vote from the second election was persisted.
  nuraft::ptr<nuraft::srv_state> state = mgr_->read_state();
  ASSERT_NE(state, nullptr);
  EXPECT_GE(state->get_term(), 2u);
}

TEST_F(MetaServerIntegrationTest, SnapshotCompactionAndRestart) {
  StartServer(/*snapshot_distance=*/0);
  for (std::uint8_t ii = 0; ii < 9; ++ii) {
    AppendAndWait(MakeRegister(static_cast<std::uint8_t>(0x10 + ii)));
  }
  ASSERT_EQ(NodeCount(), 9u);

  // Manual snapshot on the latest committed index. serialize_commit_=true is
  // MANDATORY for the manual path: serialize_commit_=true, or equivalently
  // schedule_snapshot_creation(), excludes the commit thread
  // while the state machine captures, which is what makes the cut point
  // exact. The state machine writes the file asynchronously off the
  // commit thread, so compaction completes when the writer's when_done
  // reaches the core; wait for it.
  nuraft::raft_server::create_snapshot_options options;
  options.serialize_commit_ = true;
  const uint64_t snapshot_index = server_->create_snapshot(options);
  ASSERT_GT(snapshot_index, 0u);
  // The SM records last_snapshot_ before when_done reaches the core, so the
  // completed log compaction implies both.
  nuraft::ptr<nuraft::log_store> store = mgr_->load_log_store();
  ASSERT_TRUE(
      WaitFor([&] { return store->start_index() == snapshot_index + 1; },
              std::chrono::seconds(10)));
  ASSERT_NE(machine_->last_snapshot(), nullptr);
  EXPECT_EQ(machine_->last_snapshot()->get_last_log_idx(), snapshot_index);
  StopServer();

  // Restart: the stores and the durable commit index come from the snapshot,
  // before any replay. (Checked before the core starts for determinism.)
  OpenStorage();
  EXPECT_EQ(machine_->last_commit_index(), snapshot_index);
  EXPECT_EQ(NodeCount(), 9u);
  nuraft::ptr<nuraft::log_store> reopened_store = mgr_->load_log_store();
  EXPECT_EQ(reopened_store->start_index(), snapshot_index + 1);

  // The cluster still accepts writes after recovery. (Seed 0x19: MakeNodeId
  // folds to the low nibble, so 0x19 is the first pattern not colliding with
  // the 0x10..0x18 batch above.)
  LaunchServer(/*snapshot_distance=*/0);
  AppendAndWait(MakeRegister(0x19));
  ASSERT_TRUE(
      WaitFor([this] { return NodeCount() == 10u; }, std::chrono::seconds(10)));
}

TEST_F(MetaServerIntegrationTest, AutoSnapshotOnCommitThread) {
  // The automatic path: snapshot_and_compact runs create_snapshot ON the
  // commit thread at a commit boundary (handle_commit.cxx), so this exercises
  // the exact-cut capture plus the async writer hand-off where they actually
  // live in production — unlike the unit tests, which drive create_snapshot
  // directly.
  StartServer(/*snapshot_distance=*/5);
  for (std::uint8_t ii = 0; ii < 12; ++ii) {
    AppendAndWait(MakeRegister(static_cast<std::uint8_t>(0x30 + ii)));
  }
  nuraft::ptr<nuraft::log_store> store = mgr_->load_log_store();
  ASSERT_TRUE(WaitFor(
      [this] {
        return machine_->last_snapshot() != nullptr &&
               machine_->last_snapshot()->get_last_log_idx() > 0;
      },
      std::chrono::seconds(10)));
  // Compaction follows the durable snapshot (on_snapshot_completed), moving
  // the WAL start forward.
  ASSERT_TRUE(WaitFor([&] { return store->start_index() > 1; },
                      std::chrono::seconds(10)));
  EXPECT_EQ(NodeCount(), 12u);
  StopServer();

  // Restart recovers the snapshotted prefix from the snapshot file and the
  // post-snapshot tail by replay; the full state must come back.
  OpenStorage();
  ASSERT_NE(machine_->last_snapshot(), nullptr);
  // A later automatic snapshot may publish while the final appends drain.
  // Compare recovery with the newest durable cut, not the first cut observed
  // above.
  EXPECT_EQ(machine_->last_commit_index(),
            machine_->last_snapshot()->get_last_log_idx());
  LaunchServer(/*snapshot_distance=*/5);
  AppendAndWait(MakeRegister(0x3c));  // low-nibble pattern "c...", unused above
  ASSERT_TRUE(
      WaitFor([this] { return NodeCount() == 13u; }, std::chrono::seconds(10)));
}

// Exercise the same request dispatcher as the peer transport, including term
// updates and durable voted_for. Multi-member authentication and elections are
// covered by gate_snapshot_vote; this seam isolates log-freshness decisions.
class VoteRequestDriver : public nuraft::raft_server_handler {
 public:
  using nuraft::raft_server_handler::process_req;
};

class MetaVoteIntegrationTest
    : public MetaServerIntegrationTest,
      public ::testing::WithParamInterface<std::string> {};

TEST_P(MetaVoteIntegrationTest, ComparesLogicalLogAfterCompaction) {
  const std::string history = GetParam();
  ASSERT_NO_FATAL_FAILURE(OpenStorage());
  ASSERT_NO_FATAL_FAILURE(LaunchServer(/*snapshot_distance=*/0,
                                       /*elect_leader=*/history != "Empty"));
  uint64_t last_index = 0;
  uint64_t last_term = 0;
  if (history != "Empty") {
    AppendAndWait(MakeRegister(0x11));
    AppendAndWait(MakeRegister(0x22));
    auto store = mgr_->load_log_store();
    last_index = store->next_slot() - 1;
    last_term = store->last_entry()->get_term();
    ASSERT_GT(last_term, 0u);
    if (history != "Wal") {
      nuraft::raft_server::create_snapshot_options options;
      options.serialize_commit_ = true;
      ASSERT_EQ(server_->create_snapshot(options), last_index);
      ASSERT_TRUE(
          WaitFor([&] { return store->start_index() == last_index + 1; },
                  std::chrono::seconds(10)));
      ASSERT_EQ(store->next_slot(), store->start_index());
      ASSERT_EQ(store->last_entry()->get_term(), 0u);
      ASSERT_EQ(machine_->last_snapshot()->get_last_log_term(), last_term);
      if (history == "SnapshotRestart" || history == "SnapshotWithTail") {
        StopServer();
        ASSERT_NO_FATAL_FAILURE(OpenStorage());
        ASSERT_EQ(machine_->last_snapshot()->get_last_log_idx(), last_index);
        ASSERT_EQ(machine_->last_snapshot()->get_last_log_term(), last_term);
        ASSERT_NO_FATAL_FAILURE(
            LaunchServer(/*snapshot_distance=*/0,
                         /*elect_leader=*/history == "SnapshotWithTail"));
        if (history == "SnapshotWithTail") {
          AppendAndWait(MakeRegister(0x33));
          store = mgr_->load_log_store();
          ASSERT_GT(store->last_entry()->get_term(), last_term);
          last_index = store->next_slot() - 1;
          last_term = store->last_entry()->get_term();
        } else {
          ASSERT_EQ(mgr_->load_log_store()->next_slot(), last_index + 1);
          ASSERT_EQ(mgr_->load_log_store()->last_entry()->get_term(), 0u);
        }
      }
    }
  }

  // Keep real core threads, but stop timer callbacks so a local election
  // cannot race the synthetic requests. Each request starts a fresh election
  // term so a previous granted vote cannot mask the freshness decision.
  scheduler_->Shutdown();
  const auto vote = [&](uint64_t candidate_term, uint64_t candidate_index,
                        bool expected_grant) {
    SCOPED_TRACE(::testing::Message()
                 << "candidate (" << candidate_term << ", " << candidate_index
                 << "), voter (" << last_term << ", " << last_index << ")");
    const uint64_t election_term = mgr_->read_state()->get_term() + 1;
    nuraft::req_msg request(election_term,
                            nuraft::msg_type::request_vote_request,
                            /*src=*/2, /*dst=*/1, candidate_term,
                            candidate_index, /*commit_idx=*/0);
    auto response = VoteRequestDriver::process_req(server_.get(), request);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->get_accepted(), expected_grant);
    EXPECT_EQ(mgr_->read_state()->get_voted_for(), expected_grant ? 2 : -1);
  };
  if (last_index > 0) {
    vote(last_term - 1, last_index + 100, false);
    vote(last_term, last_index - 1, false);
  }
  vote(last_term, last_index, true);
  vote(last_term, last_index + 1, true);
  vote(last_term + 1, last_index > 0 ? last_index - 1 : 0, true);
}

INSTANTIATE_TEST_SUITE_P(History, MetaVoteIntegrationTest,
                         ::testing::Values("Empty", "Wal", "Snapshot",
                                           "SnapshotRestart",
                                           "SnapshotWithTail"),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                           return info.param;
                         });

}  // namespace
