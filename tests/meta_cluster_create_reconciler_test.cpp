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

#include <barrier>
#include <limits>
#include <thread>
#include <variant>

#include "gtest/gtest.h"
#include "lavik/cluster/lease_clock.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/cluster_create_reconciler.h"
#include "lavik/meta/control_projector.h"
#include "lavik/meta/hash.h"
#include "lavik/meta/state_machine.h"
#include "support/test_data_path.h"

namespace lavik::meta {
namespace {
class ClusterCreateV1RecoveryTest : public testing::Test {
 protected:
  void Apply(MetaCommand command) {
    SCOPED_TRACE("command_tag=" + std::to_string(static_cast<std::uint16_t>(
                                      MetaCommandTagOf(command))));
    std::visit([&](auto& c) { c.request_id_.fill(1); }, command);
    const auto result = ApplyCommitted(stores_, ++index_, command,
                                       "lavik://operator/test", "now");
    ASSERT_EQ(result.verdict_, MetaAuditVerdict::kAccepted) << result.detail_;
    auto bytes = stores_.Serialize();
    ASSERT_TRUE(bytes.ok()) << bytes.status();
    auto restored = MetaStores::Deserialize(*bytes);
    ASSERT_TRUE(restored.ok()) << restored.status();
    stores_ = std::move(*restored);
  }

  void SetUp() override {
    BindMetaMember member;
    member.server_id_ = 1;
    member.principal_ = "lavik://meta/1";
    member.data_control_endpoint_ = "127.0.0.1:7301";
    member.ctl_endpoint_ = "127.0.0.1:7201";
    Apply(member);
    for (const std::uint32_t id : {2U, 3U}) {
      member.server_id_ = id;
      member.principal_ = "lavik://meta/" + std::to_string(id);
      member.data_control_endpoint_ = "127.0.0.1:" + std::to_string(7300 + id);
      member.ctl_endpoint_ = "127.0.0.1:" + std::to_string(7200 + id);
      Apply(member);
    }

    manifest_.schema_version_ = 1;

    manifest_.client_mode_ = lavik::ClientMode::kCluster;
    for (const std::uint32_t id : {1U, 2U, 3U}) {
      manifest_.meta_members_.push_back(
          {id, "tcp://127.0.0.1:" + std::to_string(7100 + id),
           "tcp://127.0.0.1:" + std::to_string(7300 + id),
           "tcp://127.0.0.1:" + std::to_string(7200 + id)});
      raft_.members_.push_back({
          .id_ = id,
          .endpoint_ = "127.0.0.1:" + std::to_string(7100 + id),
          .principal_ = "lavik://meta/" + std::to_string(id),
          .data_control_endpoint_ = "127.0.0.1:" + std::to_string(7300 + id),
          .ctl_endpoint_ = "127.0.0.1:" + std::to_string(7200 + id),
      });
    }
    raft_.local_server_id_ = 1;
    raft_.max_response_age_us_ = 1'000'000;
    raft_.peer_progress_ = {
        {2, std::numeric_limits<std::uint64_t>::max(), 1},
        {3, std::numeric_limits<std::uint64_t>::max(), 1},
    };
    manifest_.data_nodes_ = {
        {std::string(40, '1'), "tcp://127.0.0.1:6371"},
        {std::string(40, '2'), "tcp://127.0.0.1:6372"},
        {std::string(40, '3'), "tcp://127.0.0.1:6373"},
        {std::string(40, '4'), "tcp://127.0.0.1:6374"},
    };
    ConfigureDataEndpoints();
    manifest_.groups_ = {
        {"group-a", std::string(40, '1'), {std::string(40, '2')}},
        {"group-b", std::string(40, '3'), {std::string(40, '4')}},
    };
    manifest_.slot_ranges_ = {
        {0, 8191, "group-a"},
        {8192, 16'383, "group-b"},
    };
    ConfigureGroups();
    root_.fill(8);
    auto intent = EncodeClusterCreateRequest(manifest_, root_);
    ASSERT_TRUE(intent.ok()) << intent.status();
    SubmitOperation submit;
    submit.operation_id_ = root_;
    submit.kind_ = kMetaClusterCreateOperationKind;
    submit.intent_ = *intent;
    submit.intent_hash_ = MetaSha256(*intent);
    Apply(submit);
  }

  absl::StatusOr<MetaClusterCreateView> CaptureView() {
    auto opened = MetaStateMachine::Open(
        lavik::test::TestDataPath("create_reconciler_capture"));
    if (!opened.ok()) return opened.status();
    auto image = stores_.Serialize();
    if (!image.ok()) return image.status();
    if (auto status = (*opened)->Install(index_, *image); !status.ok())
      return status;
    auto discovery = (*opened)->CaptureClusterCreateDiscovery();
    // Pure-planner tests also exercise an already-terminal root; production
    // discovery correctly stops returning it after Creating ends.
    if (!discovery.root_) discovery.root_ = (*opened)->FindOperation(root_);
    auto keys = detail::CaptureClusterCreateReadKeys(*discovery.root_);
    if (!keys.ok()) return keys.status();
    auto view = (*opened)->CaptureClusterCreateView(discovery, keys->children_,
                                                    keys->manifests_);
    if (!view)
      return absl::InternalError("uncontended creation capture changed");
    return std::move(*view);
  }

  absl::StatusOr<std::optional<MetaCommand>> Plan() {
    auto view = CaptureView();
    if (!view.ok()) return view.status();
    return detail::PlanClusterCreateStep(*view, runtime_, raft_);
  }

  virtual void ConfigureDataEndpoints() {}
  virtual void ConfigureGroups() {}

  void AdvanceToProjectionWait() {
    for (int step = 0; step < 100; ++step) {
      if (stores_.operation_.FindOperation(root_)->kind_phase_blob_ ==
          "wait-data-projection") {
        auto waiting = Plan();
        ASSERT_TRUE(waiting.ok()) << waiting.status();
        ASSERT_FALSE(waiting->has_value());
        return;
      }
      auto next = Plan();
      ASSERT_TRUE(next.ok() && next->has_value()) << next.status();
      Apply(std::move(**next));
      ASSERT_FALSE(HasFatalFailure());
    }
    FAIL() << "v1 creation did not reach Data projection wait";
  }

  void PublishRuntime() {
    runtime_.leader_term_ = 1;
    runtime_.leader_authority_eligible_ = true;
    for (std::size_t index = 0; index < manifest_.data_nodes_.size(); ++index) {
      const auto& declaration = manifest_.data_nodes_[index];
      const auto group_declaration = std::find_if(
          manifest_.groups_.begin(), manifest_.groups_.end(),
          [&](const auto& group) {
            return group.primary_node_id_ == declaration.node_id_ ||
                   std::find(group.replica_node_ids_.begin(),
                             group.replica_node_ids_.end(),
                             declaration.node_id_) !=
                       group.replica_node_ids_.end();
          });
      ASSERT_NE(group_declaration, manifest_.groups_.end());
      const auto group =
          stores_.topology_.FindGroup(group_declaration->group_id_);
      const auto grant =
          stores_.topology_.AuthorityFor(group_declaration->group_id_);
      ASSERT_TRUE(group.has_value());
      ASSERT_TRUE(grant.has_value() && grant->grant_.has_value());
      const auto member =
          std::find_if(group->members_.begin(), group->members_.end(),
                       [&](const auto& item) {
                         return item.node_id_ == declaration.node_id_;
                       });
      ASSERT_NE(member, group->members_.end());
      MetaDataControlRuntimeNode node;
      node.node_id_ = declaration.node_id_;
      node.boot_id_ = std::string(40, "0123456789abcdef"[(5 + index) % 16]);
      node.replication_history_id_.fill(static_cast<std::uint8_t>(10 + index));
      node.replication_flow_count_ = index == 0 ? 3 : 2;
      node.control_revision_ = index_;
      node.leader_term_ = runtime_.leader_term_;
      node.validated_committed_high_water_ =
          std::numeric_limits<std::uint64_t>::max();
      node.health_ = cluster::control::HeartbeatHealth{
          .storage_ready = true, .population_ready = true};
      if (declaration.node_id_ == group_declaration->primary_node_id_) {
        node.last_lease_decision_ = cluster::control::LeaseGranted{
            .leader_id = 1,
            .raft_term = node.leader_term_,
            .data_boot_id = node.boot_id_,
            .control_revision = node.control_revision_,
            .group_id = group->group_id_,
            .assignment_id = member->assignment_id_,
            .group_term = grant->group_term_,
            .granted_duration_ms = 60'000};
        node.lease_decision_heartbeat_received_lease_ms_ =
            cluster::LeaseClockMillis();
      }
      node.groups_.push_back({group->group_id_, member->assignment_id_,
                              group->record_.group_term_,
                              group->record_.population_manifest_revision_,
                              group->record_.population_manifest_digest_,
                              group->record_.partition_replication_epoch_});
      runtime_.nodes_.push_back(std::move(node));
    }
  }

  MetaOperationRecord GroupOperation(std::string_view group_id) const {
    const auto operation = stores_.operation_.FindOperation(
        detail::ClusterCreateV1GroupOperationId(root_, group_id));
    EXPECT_TRUE(operation.has_value());
    return operation.value_or(MetaOperationRecord{});
  }

  void ApplyPlanned() {
    auto next = Plan();
    ASSERT_TRUE(next.ok() && next->has_value()) << next.status();
    Apply(std::move(**next));
  }

  CommitDirectiveResult ResultFor(const MetaOperationRecord& operation,
                                  const MetaCurrentDirective& directive,
                                  MetaDirectiveResultStatus status) {
    CommitDirectiveResult result;
    result.operation_id_ = operation.operation_id_;
    result.directive_id_ = directive.spec_.directive_id_;
    result.attempt_id_ = directive.spec_.attempt_id_;
    result.directive_revision_ = directive.directive_revision_;
    result.recipient_node_id_ = directive.spec_.recipient_node_id_;
    result.recipient_boot_id_ =
        directive.spec_.kind_ == kMetaDirectiveAuthorizeSource
            ? directive.spec_.source_boot_id_
            : directive.spec_.target_boot_id_;
    result.assignment_id_ = directive.spec_.assignment_id_;
    result.status_ = status;
    result.result_ =
        status == MetaDirectiveResultStatus::kSucceeded ? "ready" : "failed";
    return result;
  }

  void CommitResult(const MetaOperationRecord& operation,
                    const MetaCurrentDirective& directive,
                    MetaDirectiveResultStatus status) {
    Apply(ResultFor(operation, directive, status));
  }

  void StartFirstAuthorizationBatch() {
    AdvanceToProjectionWait();
    PublishRuntime();
    ApplyPlanned();  // root: initialize-groups
    ApplyPlanned();  // group-a: submit
    ApplyPlanned();  // group-a: initialize primary
    auto child = GroupOperation("group-a");
    ASSERT_EQ(child.current_directives_.size(), 1);
    CommitResult(child, child.current_directives_.front(),
                 MetaDirectiveResultStatus::kSucceeded);
    ApplyPlanned();  // group-a: authorize replica source
  }

  void StartFirstReplicaBatch() {
    StartFirstAuthorizationBatch();
    auto child = GroupOperation("group-a");
    ASSERT_EQ(child.current_directives_.size(), 1);
    ASSERT_EQ(child.current_directives_.front().spec_.kind_,
              kMetaDirectiveAuthorizeSource);
    CommitResult(child, child.current_directives_.front(),
                 MetaDirectiveResultStatus::kSucceeded);
    ApplyPlanned();  // group-a: retain authorization and rebuild replica
  }

  void ExpectIncarnationFailure(std::string_view node_id) {
    const auto receipts = GroupOperation("group-a").terminal_receipts_;
    ApplyPlanned();  // retain failure and remove old directives
    auto child = GroupOperation("group-a");
    EXPECT_TRUE(child.kind_phase_blob_.starts_with("deterministic-failure:"));
    EXPECT_NE(child.kind_phase_blob_.find("node=" + std::string(node_id)),
              std::string::npos);
    EXPECT_TRUE(child.current_directives_.empty());
    EXPECT_EQ(child.terminal_receipts_, receipts);

    // Every Apply serializes/restores the stores. Dropping runtime as well
    // models a Meta restart after detection but before fencing the Group.
    runtime_ = {};
    ApplyPlanned();  // fence group-a
    EXPECT_FALSE(stores_.topology_.AuthorityFor("group-a")->grant_.has_value());
    EXPECT_TRUE(stores_.topology_.AuthorityFor("group-b")->grant_.has_value());
    ApplyPlanned();  // abort group-a
    ApplyPlanned();  // abort root
    EXPECT_EQ(GroupOperation("group-a").lifecycle_,
              MetaOperationLifecycle::kAborted);
    EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
              MetaOperationLifecycle::kAborted);
    EXPECT_FALSE(stores_.operation_.FindOperation(
        detail::ClusterCreateV1GroupOperationId(root_, "group-b")));
  }

  MetaStores stores_;
  std::uint64_t index_ = 0;
  MetaOperationId root_{};
  ClusterCreateManifestV1 manifest_;
  MetaDataControlRuntimeSnapshot runtime_;
  MetaClusterCreateRaftView raft_;
};

TEST_F(ClusterCreateV1RecoveryTest,
       CapturePreservesExtraDataAndGroupsForRejection) {
  AdvanceToProjectionWait();
  const auto clean = stores_;
  RegisterNode extra;
  extra.node_id_ = std::string(40, 'e');
  extra.principal_ = "lavik://node/" + extra.node_id_;
  extra.role_ = MetaNodeRole::kReplica;
  extra.endpoints_ = {"tcp://127.0.0.1:6399"};
  Apply(extra);
  auto view = CaptureView();
  ASSERT_TRUE(view.ok()) << view.status();
  EXPECT_EQ(view->nodes_.size(), manifest_.data_nodes_.size() + 1);
  EXPECT_NE(Plan().status().message().find("node differs from intent"),
            std::string_view::npos);
  stores_ = clean;
  CreateGroup group;
  group.group_id_ = "unexpected";
  group.new_topology_epoch_ = stores_.topology_.TopologyEpoch() + 1;
  Apply(group);
  EXPECT_NE(Plan().status().message().find("unknown Group"),
            std::string_view::npos);
}

TEST_F(ClusterCreateV1RecoveryTest,
       CapturesAbsoluteSlotsIncludingUnassignedGaps) {
  AdvanceToProjectionWait();
  auto original = CaptureView();
  ASSERT_TRUE(original.ok()) << original.status();
  SetSlotMap slots;
  slots.new_topology_epoch_ = stores_.topology_.TopologyEpoch() + 1;
  slots.ranges_ = {{1, 8191, "group-a"}, {8192, 16382, "group-b"}};
  // Exercise a valid absolute map with incorrect creation coverage. The pure
  // store deliberately omits the aggregate authority gate for this fixture.
  ASSERT_TRUE(stores_.topology_.Apply(slots).ok());
  auto current = CaptureView();
  ASSERT_TRUE(current.ok()) << current.status();
  EXPECT_FALSE(current->SlotOwner(0));
  EXPECT_FALSE(current->SlotOwner(16383));
  EXPECT_EQ(current->SlotOwner(8192), "group-b");
  EXPECT_EQ(original->SlotOwner(0), "group-a");
  EXPECT_EQ(original->SlotOwner(16383), "group-b");
  EXPECT_NE(Plan().status().message().find("Slot map differs"),
            std::string_view::npos);
}

TEST_F(ClusterCreateV1RecoveryTest, ArchivedChildIsNotRediscoveredAsNewWork) {
  StartFirstReplicaBatch();
  auto child = GroupOperation("group-a");
  CommitResult(child, child.current_directives_[1],
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();
  ApplyPlanned();
  child = GroupOperation("group-a");
  ASSERT_EQ(child.lifecycle_, MetaOperationLifecycle::kCompleted);
  ArchiveOperations archive;
  archive.operation_seqs_ = {child.operation_seq_};
  Apply(archive);
  const auto view = CaptureView();
  ASSERT_TRUE(view.ok()) << view.status();
  const auto& archived = view->children_.at(child.operation_id_);
  EXPECT_TRUE(archived.known_);
  EXPECT_FALSE(archived.operation_);
  const auto& missing = view->children_.at(
      detail::ClusterCreateV1GroupOperationId(root_, "group-b"));
  EXPECT_FALSE(missing.known_);
  EXPECT_FALSE(missing.operation_);
  EXPECT_NE(Plan().status().message().find("operation was archived"),
            std::string_view::npos);
}

TEST_F(ClusterCreateV1RecoveryTest,
       CaptureRetriesChangedRootAndOwnsChildReceipts) {
  StartFirstReplicaBatch();
  auto opened = MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  auto machine = std::move(*opened);
  auto image = stores_.Serialize();
  ASSERT_TRUE(image.ok());
  ASSERT_TRUE(machine->Install(index_, *image).ok());
  const auto discovery = machine->CaptureClusterCreateDiscovery();
  ASSERT_TRUE(discovery.root_);
  const auto keys = detail::CaptureClusterCreateReadKeys(*discovery.root_);
  ASSERT_TRUE(keys.ok());
  const auto before = machine->CaptureClusterCreateView(
      discovery, keys->children_, keys->manifests_);
  ASSERT_TRUE(before);
  const auto child = GroupOperation("group-a");
  CommitResult(child, child.current_directives_[1],
               MetaDirectiveResultStatus::kSucceeded);
  image = stores_.Serialize();
  ASSERT_TRUE(image.ok());
  ASSERT_TRUE(machine->Install(index_, *image).ok());
  // Child changes do not invalidate root-derived selectors; the complete new
  // child set and cursor are captured together, never reused from discovery.
  auto after = machine->CaptureClusterCreateView(discovery, keys->children_,
                                                 keys->manifests_);
  ASSERT_TRUE(after);
  EXPECT_EQ(after->applied_index(), index_);
  EXPECT_EQ(after->children_.at(child.operation_id_)
                .operation_->terminal_receipts_.size(),
            child.terminal_receipts_.size() + 1);
  TransitionOperationPhase change;
  change.operation_id_ = root_;
  change.expected_revision_ = discovery.root_->revision_;
  change.kind_phase_blob_ = "recovery-required:test";
  Apply(change);
  image = stores_.Serialize();
  ASSERT_TRUE(image.ok());
  ASSERT_TRUE(machine->Install(index_, *image).ok());
  EXPECT_FALSE(machine->CaptureClusterCreateView(discovery, keys->children_,
                                                 keys->manifests_));
  machine.reset();
  EXPECT_EQ(before->children_.at(child.operation_id_).operation_, child);
  EXPECT_EQ(
      after->children_.at(child.operation_id_).operation_->current_directives_,
      child.current_directives_);
}

TEST_F(ClusterCreateV1RecoveryTest,
       ConcurrentCommitAdvanceInstallKeepWorkflowCut) {
  StartFirstReplicaBatch();
  ASSERT_FALSE(HasFatalFailure());
  auto opened = MetaStateMachine::Open("");
  ASSERT_TRUE(opened.ok());
  auto machine = std::move(*opened);
  auto image = stores_.Serialize();
  ASSERT_TRUE(image.ok());
  ASSERT_TRUE(machine->Install(index_, *image).ok());
  const auto initial_index = index_;
  const auto child_id = GroupOperation("group-a").operation_id_;
  const auto keys = detail::CaptureClusterCreateReadKeys(
      *stores_.operation_.FindOperation(root_));
  ASSERT_TRUE(keys.ok());
  struct Expected {
    MetaOperationRecord root;
    MetaOperationRecord child;
    std::uint64_t epoch;
    std::uint64_t state_index;
  };
  std::vector<Expected> expected;
  auto remember = [&](std::uint64_t state_index) {
    expected.push_back({*stores_.operation_.FindOperation(root_),
                        *stores_.operation_.FindOperation(child_id),
                        stores_.topology_.TopologyEpoch(), state_index});
  };
  remember(index_);
  struct Step {
    std::shared_ptr<MetaRaftBuffer> command;
    std::string image;
  };
  std::vector<Step> steps;
  for (int round = 0; round < 12; ++round) {
    TransitionOperationPhase phase;
    phase.operation_id_ = root_;
    phase.expected_revision_ =
        stores_.operation_.FindOperation(root_)->revision_;
    phase.kind_phase_blob_ = "commit-" + std::to_string(round);
    auto command = MetaStateMachine::EncodeCommand(phase);
    ASSERT_TRUE(command.ok());
    Apply(phase);
    ASSERT_FALSE(HasFatalFailure());
    steps.push_back({*command, {}});
    remember(index_);
    ++index_;
    steps.push_back({{}, {}});  // configuration-only Advance
    remember(index_ - 1);
    phase.expected_revision_ =
        stores_.operation_.FindOperation(root_)->revision_;
    phase.kind_phase_blob_ = "install-" + std::to_string(round);
    ASSERT_TRUE(
        stores_.operation_.TransitionOperationPhase(phase, ++index_).ok());
    phase.operation_id_ = child_id;
    phase.expected_revision_ =
        stores_.operation_.FindOperation(child_id)->revision_;
    ASSERT_TRUE(
        stores_.operation_.TransitionOperationPhase(phase, index_).ok());
    ASSERT_TRUE(stores_.topology_
                    .SetTopologyEpoch(stores_.topology_.TopologyEpoch() + 1)
                    .ok());
    image = stores_.Serialize();
    ASSERT_TRUE(image.ok());
    steps.push_back({{}, *image});
    remember(index_);
  }
  auto check = [&](const MetaClusterCreateView& view) {
    const auto offset = view.applied_index() - initial_index;
    ASSERT_LT(offset, expected.size());
    const auto& want = expected[offset];
    EXPECT_EQ(view.root_, want.root);
    EXPECT_EQ(view.children_.at(child_id).operation_, want.child);
    EXPECT_EQ(view.topology_epoch_, want.epoch);
    EXPECT_EQ(view.cursor_.state_change_index(), want.state_index);
    EXPECT_EQ(view.nodes_.size(), 4u);
    EXPECT_EQ(view.groups_.size(), 2u);
    EXPECT_EQ(view.SlotOwner(16383), "group-b");
  };
  std::barrier barrier(3);
  auto reader = [&] {
    for (std::size_t step = 0; step < steps.size(); ++step) {
      barrier.arrive_and_wait();
      for (int attempt = 0; attempt < 8; ++attempt) {
        auto discovery = machine->CaptureClusterCreateDiscovery();
        if (auto view = machine->CaptureClusterCreateView(
                discovery, keys->children_, keys->manifests_))
          check(*view);
      }
      barrier.arrive_and_wait();
      auto discovery = machine->CaptureClusterCreateDiscovery();
      auto view = machine->CaptureClusterCreateView(discovery, keys->children_,
                                                    keys->manifests_);
      EXPECT_TRUE(view);
      if (view) {
        check(*view);
        EXPECT_EQ(view->applied_index(), initial_index + step + 1);
      }
    }
  };
  std::jthread first(reader), second(reader);
  for (std::size_t step = 0; step < steps.size(); ++step) {
    barrier.arrive_and_wait();
    const auto index = initial_index + step + 1;
    if (steps[step].command)
      EXPECT_NE(machine->commit(index, *steps[step].command), nullptr);
    else if (!steps[step].image.empty())
      EXPECT_TRUE(machine->Install(index, steps[step].image).ok());
    else
      machine->Advance(index);
    barrier.arrive_and_wait();
  }
}

class ClusterCreateTlsRecoveryTest : public ClusterCreateV1RecoveryTest {
 protected:
  void ConfigureDataEndpoints() override {
    manifest_.data_nodes_[0].tls_endpoint_ = "tls://127.0.0.1:16371";
    manifest_.data_nodes_[1].client_endpoint_.clear();
    manifest_.data_nodes_[1].tls_endpoint_ = "tls://127.0.0.1:16372";
  }
};

TEST_F(ClusterCreateTlsRecoveryTest, RecoversRegistrationAndProjectsTlsPorts) {
  // Apply() round-trips the aggregate after every step. Recovery must compare
  // both committed endpoints with the intent and preserve transport identity.
  AdvanceToProjectionWait();
  ASSERT_FALSE(HasFatalFailure());
  const auto primary = stores_.identity_.FindNode(std::string(40, '1'));
  const auto replica = stores_.identity_.FindNode(std::string(40, '2'));
  ASSERT_TRUE(primary.has_value());
  ASSERT_TRUE(replica.has_value());
  EXPECT_EQ(primary->endpoints_,
            (std::vector<std::string>{"tcp://127.0.0.1:6371",
                                      "tls://127.0.0.1:16371"}));
  EXPECT_EQ(replica->endpoints_,
            (std::vector<std::string>{"tls://127.0.0.1:16372"}));
  const auto projected = MetaControlProjector::ProjectNode(
      MetaCommittedView(stores_, index_), replica->node_id_);
  ASSERT_TRUE(projected.ok()) << projected.status();
  ASSERT_EQ(projected->full_state.nodes.size(), 4U);
  EXPECT_EQ(projected->full_state.nodes[0].port, 6371);
  EXPECT_EQ(projected->full_state.nodes[0].tls_port, 16371);
  EXPECT_EQ(projected->full_state.nodes[1].port, 0);
  EXPECT_EQ(projected->full_state.nodes[1].tls_port, 16372);
}

TEST_F(ClusterCreateV1RecoveryTest,
       WaitsAtStableMetaBarrierUntilEveryRemoteIsRecentAndApplied) {
  const std::uint64_t barrier =
      stores_.operation_.FindOperation(root_)->operation_seq_;

  raft_.peer_progress_.clear();
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  raft_.peer_progress_ = {
      {2, barrier - 1, 1},
      {3, barrier, 1},
  };
  waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  raft_.peer_progress_[0].last_sm_committed_index_ = barrier;
  raft_.peer_progress_[1].last_response_age_us_ =
      raft_.max_response_age_us_ + 1;
  waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  raft_.peer_progress_[1].last_response_age_us_ = 1;
  auto ready = Plan();
  ASSERT_TRUE(ready.ok() && ready->has_value()) << ready.status();
  const auto* phase = std::get_if<TransitionOperationPhase>(&**ready);
  ASSERT_NE(phase, nullptr);
  EXPECT_EQ(phase->kind_phase_blob_, "policy");
}

TEST_F(ClusterCreateV1RecoveryTest, InstallsRecoveryPolicyBeforeDataTopology) {
  AdvanceToProjectionWait();
  const auto policy = stores_.policy_.CurrentCandidateRecovery();
  ASSERT_TRUE(policy.has_value());
  EXPECT_EQ(policy->version_, 1u);
  EXPECT_EQ(policy->budget_ms_, 2000u);
}

TEST_F(ClusterCreateV1RecoveryTest,
       CommitsOneSlotMapAndSparsePopulationPerGroup) {
  std::size_t slot_map_commands = 0;
  for (int step = 0; step < 100; ++step) {
    if (stores_.operation_.FindOperation(root_)->kind_phase_blob_ ==
        "wait-data-projection")
      break;
    auto next = Plan();
    ASSERT_TRUE(next.ok() && next->has_value()) << next.status();
    if (const auto* slot_map = std::get_if<SetSlotMap>(&**next)) {
      ++slot_map_commands;
      EXPECT_EQ(slot_map->ranges_,
                (std::vector<MetaSlotAssignment>{{0, 8191, "group-a"},
                                                 {8192, 16'383, "group-b"}}));
    }
    Apply(std::move(**next));
    ASSERT_FALSE(HasFatalFailure());
  }
  EXPECT_EQ(slot_map_commands, 1);
  EXPECT_EQ(stores_.policy_.CurrentAutomaticUncontrolledFailover(),
            (MetaAutomaticUncontrolledFailoverPolicy{
                .version_ = 1,
                .suspect_after_ms_ = kDefaultAutomaticFailoverSuspectAfterMs}));
  EXPECT_EQ(
      stores_.policy_.CurrentAuthorityLease(),
      (MetaAuthorityLeasePolicy{
          .version_ = 1, .duration_ms_ = kDefaultAuthorityLeaseDurationMs}));
  ASSERT_EQ(stores_.population_manifest_.Size(), 2);
  for (const auto& declaration : manifest_.groups_) {
    const auto group = stores_.topology_.FindGroup(declaration.group_id_);
    ASSERT_TRUE(group.has_value());
    const auto document = stores_.population_manifest_.Find(
        group->record_.population_manifest_digest_);
    ASSERT_TRUE(document.has_value());
    ASSERT_EQ(document->entries_.size(), 8192);
    EXPECT_EQ(document->entries_.front().partition_id_,
              declaration.group_id_ == "group-a" ? 0 : 8192);
    EXPECT_EQ(document->entries_.back().partition_id_,
              declaration.group_id_ == "group-a" ? 8191 : 16'383);
  }
}

TEST_F(ClusterCreateV1RecoveryTest,
       PreservesValidPoliciesPreseededBeforeBootstrap) {
  PutPolicy automatic;
  automatic.policy_id_ = kAutomaticUncontrolledFailoverPolicyId;
  automatic.version_ = 1;
  automatic.content_ =
      R"({"kind":"automatic-uncontrolled-failover-v1","suspect_after_ms":9000})";
  Apply(automatic);
  PutPolicy lease;
  lease.policy_id_ = kAuthorityLeasePolicyId;
  lease.version_ = 1;
  lease.content_ = R"({"kind":"authority-lease-v1","duration_ms":7000})";
  Apply(lease);

  AdvanceToProjectionWait();
  EXPECT_EQ(stores_.policy_.CurrentAutomaticUncontrolledFailover(),
            (MetaAutomaticUncontrolledFailoverPolicy{
                .version_ = 1, .suspect_after_ms_ = 9000}));
  EXPECT_EQ(stores_.policy_.CurrentAuthorityLease(),
            (MetaAuthorityLeasePolicy{.version_ = 1, .duration_ms_ = 7000}));
}

TEST_F(ClusterCreateV1RecoveryTest,
       WaitsForLiveSourceLayoutBeforePlanningSourceAuthorization) {
  AdvanceToProjectionWait();
  PublishRuntime();
  ApplyPlanned();  // root: initialize-groups
  ApplyPlanned();  // group-a: submit
  ApplyPlanned();  // group-a: initialize primary
  auto child = GroupOperation("group-a");
  ASSERT_EQ(child.current_directives_.size(), 1);
  CommitResult(child, child.current_directives_.front(),
               MetaDirectiveResultStatus::kSucceeded);
  const auto primary = runtime_.nodes_.front();
  runtime_.nodes_.erase(runtime_.nodes_.begin());
  ApplyPlanned();  // independent group-b: submit
  ApplyPlanned();  // independent group-b: initialize primary
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  runtime_.nodes_.insert(runtime_.nodes_.begin(), primary);
  ApplyPlanned();
  child = GroupOperation("group-a");
  ASSERT_EQ(child.current_directives_.size(), 1);
  EXPECT_EQ(child.current_directives_.front().spec_.kind_,
            kMetaDirectiveAuthorizeSource);
  for (const auto& directive : child.current_directives_) {
    auto request =
        cluster::control::DecodeRebuildRequest(directive.spec_.payload_);
    ASSERT_TRUE(request.ok()) << request.status();
    EXPECT_EQ(request->source_flow_count, primary.replication_flow_count_);
  }
}

TEST_F(ClusterCreateV1RecoveryTest,
       AuthorizesEverySourceBeforeCommittingReplicaRebuilds) {
  AdvanceToProjectionWait();
  PublishRuntime();
  ApplyPlanned();  // root: initialize-groups
  ApplyPlanned();  // group-a: submit
  ApplyPlanned();  // group-a: initialize primary
  auto first = GroupOperation("group-a");
  ASSERT_EQ(first.current_directives_.size(), 1);
  CommitResult(first, first.current_directives_.front(),
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // group-a: authorize source only

  first = GroupOperation("group-a");
  ASSERT_EQ(first.current_directives_.size(), 1);
  ASSERT_EQ(first.current_directives_.front().spec_.kind_,
            kMetaDirectiveAuthorizeSource);
  const MetaCurrentDirective authorize = first.current_directives_.front();
  CommitResult(first, authorize, MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // group-a: retain authorization and add rebuild

  first = GroupOperation("group-a");
  ASSERT_EQ(first.current_directives_.size(), 2);
  EXPECT_EQ(first.current_directives_[0], authorize);
  EXPECT_EQ(first.current_directives_[1].spec_.kind_, kMetaDirectiveRebuild);
  EXPECT_GT(first.current_directives_[1].directive_revision_,
            authorize.directive_revision_);
  // The fixture's primary has three source flows and its target two workers.
  // Apply round-trips the stores, so this also checks that replay preserves
  // the source layout across both durable phases.
  for (const auto& directive : first.current_directives_) {
    auto request =
        cluster::control::DecodeRebuildRequest(directive.spec_.payload_);
    ASSERT_TRUE(request.ok()) << request.status();
    EXPECT_EQ(request->source_flow_count, 3);
  }
  EXPECT_FALSE(stores_.operation_.FindOperation(
      detail::ClusterCreateV1GroupOperationId(root_, "group-b")));

  CommitResult(first, first.current_directives_[1],
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // group-a: population-ready and remove directives
  ApplyPlanned();  // group-a: completed
  EXPECT_EQ(GroupOperation("group-a").lifecycle_,
            MetaOperationLifecycle::kCompleted);
  ApplyPlanned();  // group-b: submit when group-a no longer has runnable work
  EXPECT_EQ(GroupOperation("group-b").lifecycle_,
            MetaOperationLifecycle::kSubmitted);
}

TEST_F(ClusterCreateV1RecoveryTest,
       WaitsForCurrentSourceLeaseBeforeReplicaRebuild) {
  StartFirstAuthorizationBatch();
  const auto child = GroupOperation("group-a");
  ASSERT_EQ(child.current_directives_.size(), 1);
  CommitResult(child, child.current_directives_.front(),
               MetaDirectiveResultStatus::kSucceeded);

  auto& source = runtime_.nodes_.front();
  const auto grant = *source.last_lease_decision_;
  source.last_lease_decision_ = cluster::control::LeaseDenied{};
  ApplyPlanned();  // independent group-b: submit
  ApplyPlanned();  // independent group-b: initialize primary
  auto waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  source.last_lease_decision_ = grant;
  source.lease_decision_heartbeat_received_lease_ms_ =
      cluster::LeaseClockMillis() - 60'001;
  waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  source.lease_decision_heartbeat_received_lease_ms_ =
      cluster::LeaseClockMillis();
  std::get<cluster::control::LeaseGranted>(*source.last_lease_decision_)
      .data_boot_id = std::string(40, 'f');
  waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  source.last_lease_decision_ = grant;
  runtime_.leader_authority_eligible_ = false;
  waiting = Plan();
  ASSERT_TRUE(waiting.ok()) << waiting.status();
  EXPECT_FALSE(waiting->has_value());

  runtime_.leader_authority_eligible_ = true;
  ApplyPlanned();
  const auto rebuilt = GroupOperation("group-a");
  ASSERT_EQ(rebuilt.current_directives_.size(), 2);
  EXPECT_EQ(rebuilt.current_directives_[1].spec_.kind_, kMetaDirectiveRebuild);
}

TEST_F(ClusterCreateV1RecoveryTest, SlowReplicaDoesNotBlockIndependentGroup) {
  StartFirstReplicaBatch();
  const auto stalled = GroupOperation("group-a");
  auto next = Plan();
  ASSERT_TRUE(next.ok() && next->has_value()) << next.status();
  ASSERT_TRUE(std::holds_alternative<SubmitOperation>(**next));
  Apply(std::move(**next));  // group-b starts while group-a awaits its replica
  ApplyPlanned();            // initialize group-b primary
  auto child = GroupOperation("group-b");
  ASSERT_EQ(child.current_directives_.size(), 1);
  CommitResult(child, child.current_directives_.front(),
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // authorize group-b source
  child = GroupOperation("group-b");
  CommitResult(child, child.current_directives_.front(),
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // rebuild group-b replica
  child = GroupOperation("group-b");
  ASSERT_EQ(child.current_directives_.size(), 2);
  CommitResult(child, child.current_directives_[1],
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // ready group-b
  ApplyPlanned();  // complete group-b
  EXPECT_EQ(GroupOperation("group-b").lifecycle_,
            MetaOperationLifecycle::kCompleted);
  EXPECT_EQ(GroupOperation("group-a").revision_, stalled.revision_);
  EXPECT_EQ(GroupOperation("group-a").current_directives_,
            stalled.current_directives_);
  next = Plan();
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_FALSE(next->has_value());  // root still waits for group-a
  CommitResult(stalled, stalled.current_directives_[1],
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();
  ApplyPlanned();
  ApplyPlanned();
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
            MetaOperationLifecycle::kCompleted);
}

TEST_F(ClusterCreateV1RecoveryTest, MissingFirstPrimaryDoesNotBlockAdmission) {
  AdvanceToProjectionWait();
  PublishRuntime();
  ApplyPlanned();
  runtime_.nodes_.erase(runtime_.nodes_.begin());
  ApplyPlanned();
  EXPECT_EQ(GroupOperation("group-b").lifecycle_,
            MetaOperationLifecycle::kSubmitted);
  EXPECT_FALSE(stores_.operation_.FindOperation(
      detail::ClusterCreateV1GroupOperationId(root_, "group-a")));
}

TEST_F(ClusterCreateV1RecoveryTest, LaterFailurePreemptsEarlierRunnableChild) {
  StartFirstReplicaBatch();
  ApplyPlanned();  // group-b submit
  ApplyPlanned();  // group-b initialize
  const auto first = GroupOperation("group-a");
  const auto second = GroupOperation("group-b");
  CommitResult(first, first.current_directives_[1],
               MetaDirectiveResultStatus::kSucceeded);
  CommitResult(second, second.current_directives_.front(),
               MetaDirectiveResultStatus::kFailed);
  const auto next = Plan();
  ASSERT_TRUE(next.ok() && next->has_value()) << next.status();
  ASSERT_TRUE(std::holds_alternative<FenceGroup>(**next));
  EXPECT_EQ(std::get<FenceGroup>(**next).group_id_, "group-b");
  Apply(**next);
  ApplyPlanned();  // abort group-b
  runtime_ = {};
  ApplyPlanned();  // invalidate unfinished group-a
  ApplyPlanned();  // fence group-a
  ApplyPlanned();  // abort group-a
  ApplyPlanned();  // abort root
  EXPECT_EQ(stores_.topology_.ClusterLifecycle().state_,
            MetaClusterLifecycle::kProvisioningFailed);
  EXPECT_EQ(GroupOperation("group-a").lifecycle_,
            MetaOperationLifecycle::kAborted);
}

class ClusterCreateSiblingFailureTest
    : public ClusterCreateV1RecoveryTest,
      public testing::WithParamInterface<int> {};

TEST_P(ClusterCreateSiblingFailureTest, RetiresAllStartedWorkBeforeRootAbort) {
  AdvanceToProjectionWait();
  PublishRuntime();
  ApplyPlanned();                       // root: initialize-groups
  ApplyPlanned();                       // group-a: submit
  if (GetParam() >= 1) ApplyPlanned();  // primary initialization
  if (GetParam() >= 2) {
    const auto child = GroupOperation("group-a");
    CommitResult(child, child.current_directives_.front(),
                 MetaDirectiveResultStatus::kSucceeded);
    ApplyPlanned();  // source authorization
  }
  if (GetParam() >= 3) {
    const auto child = GroupOperation("group-a");
    CommitResult(child, child.current_directives_.front(),
                 MetaDirectiveResultStatus::kSucceeded);
    ApplyPlanned();  // replica rebuild
  }
  // For the submitted cut, inject a second child with the same durable intent
  // the planner would use. It must clean up even before any directive exists.
  SubmitOperation submit;
  submit.operation_id_ =
      detail::ClusterCreateV1GroupOperationId(root_, "group-b");
  submit.kind_ = kMetaClusterCreateV1GroupOperationKind;
  submit.intent_ = GroupOperation("group-a").intent_;
  submit.intent_.replace(submit.intent_.find("67726f75702d61"), 14,
                         "67726f75702d62");
  submit.intent_.replace(submit.intent_.size() - 40, 40,
                         runtime_.nodes_[2].boot_id_);
  submit.intent_hash_ = MetaSha256(submit.intent_);
  submit.replication_history_id_ = runtime_.nodes_[2].replication_history_id_;
  Apply(submit);
  // A committed abort is the recovery boundary after the failing Group's
  // fence. Drive the sibling cleanup with no volatile runtime evidence.
  FenceGroup fence;
  fence.group_id_ = "group-b";
  fence.expected_term_ = 1;
  fence.new_term_ = 2;
  Apply(fence);
  AbortOperation abort;
  abort.operation_id_ = submit.operation_id_;
  abort.expected_revision_ = GroupOperation("group-b").revision_;
  abort.reason_ = "group=group-b primary initialization: failed";
  Apply(abort);
  const auto original = GroupOperation("group-a");
  std::optional<CommitDirectiveResult> late;
  for (const auto& directive : original.current_directives_) {
    if (directive.spec_.kind_ != kMetaDirectiveAuthorizeSource)
      late =
          ResultFor(original, directive, MetaDirectiveResultStatus::kSucceeded);
  }
  runtime_ = {};
  ApplyPlanned();  // retire sibling directives, retain original cause
  EXPECT_TRUE(GroupOperation("group-a").current_directives_.empty());
  EXPECT_EQ(GroupOperation("group-a").terminal_receipts_,
            original.terminal_receipts_);
  if (late) {
    const auto result = ApplyCommitted(stores_, ++index_, *late,
                                       "lavik://operator/test", "now");
    EXPECT_EQ(result.verdict_, MetaAuditVerdict::kRejected);
  }
  ApplyPlanned();  // fence sibling
  EXPECT_FALSE(stores_.topology_.AuthorityFor("group-a")->grant_);
  ApplyPlanned();  // abort sibling
  EXPECT_EQ(GroupOperation("group-a").lifecycle_,
            MetaOperationLifecycle::kAborted);
  EXPECT_FALSE(stores_.operation_.FindOperation(root_)->lifecycle_ ==
               MetaOperationLifecycle::kAborted);
  ApplyPlanned();  // now abort root
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->terminal_result_,
            abort.reason_);
  EXPECT_EQ(stores_.topology_.ClusterLifecycle().state_,
            MetaClusterLifecycle::kProvisioningFailed);
}

INSTANTIATE_TEST_SUITE_P(AllActivePhases, ClusterCreateSiblingFailureTest,
                         testing::Values(0, 1, 2, 3));

class ClusterCreateManyGroupsTest : public ClusterCreateV1RecoveryTest {
 protected:
  void ConfigureGroups() override {
    manifest_.data_nodes_.clear();
    manifest_.groups_.clear();
    manifest_.slot_ranges_.clear();
    for (int index = 0; index < 6; ++index) {
      const std::string node(40, static_cast<char>('1' + index));
      const std::string group = "group-" + std::to_string(index);
      manifest_.data_nodes_.push_back(
          {node, "tcp://127.0.0.1:" + std::to_string(6371 + index)});
      manifest_.groups_.push_back({group, node, {}});
      manifest_.slot_ranges_.push_back(
          {static_cast<std::uint16_t>(index * 2048),
           static_cast<std::uint16_t>(index == 5 ? 16383
                                                 : (index + 1) * 2048 - 1),
           group});
    }
  }
};

TEST_F(ClusterCreateManyGroupsTest, BoundsActiveWorkAndReusesFreedCapacity) {
  AdvanceToProjectionWait();
  PublishRuntime();
  ApplyPlanned();
  for (int index = 0; index < 4; ++index) {
    ApplyPlanned();  // submit
    ApplyPlanned();  // initialize
  }
  auto next = Plan();
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_FALSE(next->has_value());
  EXPECT_FALSE(stores_.operation_.OperationKnown(
      detail::ClusterCreateV1GroupOperationId(root_, "group-4")));
  // Three stalled children keep their slots. The fourth slot lets all later
  // children finish, across serialization/recovery after every command.
  for (int index = 3; index < 6; ++index) {
    const auto child = GroupOperation("group-" + std::to_string(index));
    ASSERT_EQ(child.current_directives_.size(), 1);
    CommitResult(child, child.current_directives_.front(),
                 MetaDirectiveResultStatus::kSucceeded);
    ApplyPlanned();
    ApplyPlanned();
    if (index < 5) {
      ApplyPlanned();
      ApplyPlanned();
    }
  }
  for (int index = 0; index < 3; ++index) {
    const auto child = GroupOperation("group-" + std::to_string(index));
    EXPECT_EQ(child.kind_phase_blob_, "initializing-empty-population");
    CommitResult(child, child.current_directives_.front(),
                 MetaDirectiveResultStatus::kSucceeded);
    ApplyPlanned();
    ApplyPlanned();
  }
  ApplyPlanned();
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
            MetaOperationLifecycle::kCompleted);
}

TEST_F(ClusterCreateV1RecoveryTest,
       ReplicaFailureFencesOnlyItsGroupAndAbortsRoot) {
  StartFirstReplicaBatch();
  auto first = GroupOperation("group-a");
  ASSERT_EQ(first.current_directives_.size(), 2);
  CommitResult(first, first.current_directives_[1],
               MetaDirectiveResultStatus::kFailed);
  ApplyPlanned();  // fence group-a
  EXPECT_FALSE(stores_.topology_.AuthorityFor("group-a")->grant_.has_value());
  EXPECT_TRUE(stores_.topology_.AuthorityFor("group-b")->grant_.has_value());
  ApplyPlanned();  // abort group-a
  ApplyPlanned();  // abort root
  EXPECT_EQ(GroupOperation("group-a").lifecycle_,
            MetaOperationLifecycle::kAborted);
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
            MetaOperationLifecycle::kAborted);
  EXPECT_FALSE(stores_.operation_.FindOperation(
      detail::ClusterCreateV1GroupOperationId(root_, "group-b")));
}

TEST_F(ClusterCreateV1RecoveryTest,
       SourceAuthorizationFailureFencesOnlyItsGroupAndAbortsRoot) {
  StartFirstAuthorizationBatch();
  auto first = GroupOperation("group-a");
  ASSERT_EQ(first.current_directives_.size(), 1);
  CommitResult(first, first.current_directives_.front(),
               MetaDirectiveResultStatus::kFailed);
  ApplyPlanned();  // fence group-a
  EXPECT_FALSE(stores_.topology_.AuthorityFor("group-a")->grant_.has_value());
  EXPECT_TRUE(stores_.topology_.AuthorityFor("group-b")->grant_.has_value());
  ApplyPlanned();  // abort group-a
  ApplyPlanned();  // abort root
  EXPECT_EQ(GroupOperation("group-a").lifecycle_,
            MetaOperationLifecycle::kAborted);
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
            MetaOperationLifecycle::kAborted);
  EXPECT_FALSE(stores_.operation_.FindOperation(
      detail::ClusterCreateV1GroupOperationId(root_, "group-b")));
}

TEST_F(ClusterCreateV1RecoveryTest,
       PrimaryRestartFencesOnlyItsGroupAndAbortsRoot) {
  AdvanceToProjectionWait();
  PublishRuntime();
  ApplyPlanned();  // root: initialize-groups
  ApplyPlanned();  // group-a: submit
  ApplyPlanned();  // group-a: initialize primary
  runtime_.nodes_.front().boot_id_ = std::string(40, 'f');

  ApplyPlanned();  // retain the deterministic failure reason
  ApplyPlanned();  // fence group-a
  EXPECT_FALSE(stores_.topology_.AuthorityFor("group-a")->grant_.has_value());
  EXPECT_TRUE(stores_.topology_.AuthorityFor("group-b")->grant_.has_value());
  ApplyPlanned();  // abort group-a
  ApplyPlanned();  // abort root

  EXPECT_EQ(GroupOperation("group-a").lifecycle_,
            MetaOperationLifecycle::kAborted);
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
            MetaOperationLifecycle::kAborted);
  EXPECT_FALSE(stores_.operation_.FindOperation(
      detail::ClusterCreateV1GroupOperationId(root_, "group-b")));
  EXPECT_NE(stores_.operation_.FindOperation(root_)->terminal_result_.find(
                "group=group-a node=" + std::string(40, '1')),
            std::string::npos);
}

TEST_F(ClusterCreateV1RecoveryTest,
       UnexpectedFenceCannotMasqueradeAsAReplicaFailure) {
  StartFirstReplicaBatch();
  FenceGroup fence;
  fence.group_id_ = "group-a";
  fence.expected_term_ = 1;
  fence.new_term_ = 2;
  Apply(fence);

  const auto next = Plan();
  EXPECT_FALSE(next.ok());
  EXPECT_NE(next.status().message().find("authority changed"),
            std::string_view::npos);
}

TEST_F(ClusterCreateV1RecoveryTest,
       ReplicaRestartBeforeResultFencesItsGroupAndAbortsRoot) {
  StartFirstReplicaBatch();
  runtime_.nodes_[1].boot_id_ = std::string(40, 'f');
  ExpectIncarnationFailure(std::string(40, '2'));
}

TEST_F(ClusterCreateV1RecoveryTest,
       SourceRestartBeforeAuthorizationFencesItsGroupAndAbortsRoot) {
  StartFirstAuthorizationBatch();
  runtime_.nodes_[0].boot_id_ = std::string(40, 'f');
  ExpectIncarnationFailure(std::string(40, '1'));
}

TEST_F(ClusterCreateV1RecoveryTest,
       SourceHistoryChangeWhileRebuildingFencesItsGroupAndAbortsRoot) {
  StartFirstReplicaBatch();
  runtime_.nodes_[0].replication_history_id_.fill(99);
  ExpectIncarnationFailure(std::string(40, '1'));
}

TEST_F(ClusterCreateV1RecoveryTest,
       PrimaryRestartBeforeDirectiveDispatchDoesNotIssueStaleInitialization) {
  AdvanceToProjectionWait();
  PublishRuntime();
  ApplyPlanned();  // root: initialize-groups
  ApplyPlanned();  // group-a: submit with the original primary boot
  runtime_.nodes_[0].boot_id_ = std::string(40, 'f');
  ExpectIncarnationFailure(std::string(40, '1'));
}

TEST_F(ClusterCreateV1RecoveryTest,
       PrimaryRestartAfterInitializationDoesNotIssueStaleSourceAuthorization) {
  AdvanceToProjectionWait();
  PublishRuntime();
  ApplyPlanned();  // root: initialize-groups
  ApplyPlanned();  // group-a: submit
  ApplyPlanned();  // group-a: initialize primary
  const auto child = GroupOperation("group-a");
  CommitResult(child, child.current_directives_.front(),
               MetaDirectiveResultStatus::kSucceeded);
  runtime_.nodes_[0].boot_id_ = std::string(40, 'f');
  ExpectIncarnationFailure(std::string(40, '1'));
}

TEST_F(ClusterCreateV1RecoveryTest,
       MissingRuntimeAndSameBootReconnectDoNotImplyRestart) {
  StartFirstReplicaBatch();
  auto reconnected = runtime_;
  runtime_ = {};
  auto next = Plan();
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_FALSE(next->has_value());
  runtime_ = std::move(reconnected);
  for (auto& node : runtime_.nodes_) {
    node.session_id_.fill(99);
    ++node.session_generation_;
  }
  ApplyPlanned();  // independent group-b: submit
  ApplyPlanned();  // independent group-b: initialize primary
  next = Plan();
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_FALSE(next->has_value());

  runtime_.leader_authority_eligible_ = false;
  runtime_.nodes_[1].boot_id_ = std::string(40, 'f');
  next = Plan();
  ASSERT_TRUE(next.ok()) << next.status();
  EXPECT_FALSE(next->has_value());
}

TEST_F(ClusterCreateV1RecoveryTest,
       CommittedReplicaSuccessRemainsHistoryAfterBootChanges) {
  StartFirstReplicaBatch();
  const auto child = GroupOperation("group-a");
  CommitResult(child, child.current_directives_[1],
               MetaDirectiveResultStatus::kSucceeded);
  const auto receipts = GroupOperation("group-a").terminal_receipts_;
  runtime_.nodes_[0].boot_id_ = std::string(40, 'e');
  runtime_.nodes_[1].boot_id_ = std::string(40, 'f');
  ApplyPlanned();  // retire completed directives
  ApplyPlanned();  // complete the child without rewriting its results
  EXPECT_EQ(GroupOperation("group-a").lifecycle_,
            MetaOperationLifecycle::kCompleted);
  EXPECT_EQ(GroupOperation("group-a").terminal_receipts_, receipts);
  EXPECT_TRUE(stores_.topology_.AuthorityFor("group-a")->grant_.has_value());
}

TEST_F(ClusterCreateV1RecoveryTest,
       LateOldBootResultCannotCompleteAnInvalidatedAttempt) {
  StartFirstReplicaBatch();
  const auto child = GroupOperation("group-a");
  auto late = ResultFor(child, child.current_directives_[1],
                        MetaDirectiveResultStatus::kSucceeded);
  runtime_.nodes_[1].boot_id_ = std::string(40, 'f');
  ApplyPlanned();  // durably invalidate the attempt before its result arrives
  const auto failed = GroupOperation("group-a");
  late.request_id_.fill(2);
  const auto result =
      ApplyCommitted(stores_, ++index_, late, "lavik://operator/test", "now");
  EXPECT_EQ(result.verdict_, MetaAuditVerdict::kRejected) << result.detail_;
  EXPECT_EQ(GroupOperation("group-a").kind_phase_blob_,
            failed.kind_phase_blob_);
  EXPECT_EQ(GroupOperation("group-a").terminal_receipts_,
            failed.terminal_receipts_);
}

TEST_F(ClusterCreateV1RecoveryTest,
       LaterGroupFailureDoesNotRollbackCompletedGroup) {
  StartFirstReplicaBatch();
  auto first = GroupOperation("group-a");
  CommitResult(first, first.current_directives_[1],
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // group-a: population-ready
  ApplyPlanned();  // group-a: completed
  ApplyPlanned();  // group-b: submitted
  ApplyPlanned();  // group-b: initialize primary
  auto second = GroupOperation("group-b");
  CommitResult(second, second.current_directives_.front(),
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // group-b: source authorization
  second = GroupOperation("group-b");
  ASSERT_EQ(second.current_directives_.size(), 1);
  CommitResult(second, second.current_directives_.front(),
               MetaDirectiveResultStatus::kSucceeded);
  ApplyPlanned();  // group-b: replica rebuild
  second = GroupOperation("group-b");
  ASSERT_EQ(second.current_directives_.size(), 2);
  CommitResult(second, second.current_directives_[1],
               MetaDirectiveResultStatus::kFailed);
  ApplyPlanned();  // fence group-b
  ApplyPlanned();  // abort group-b
  ApplyPlanned();  // abort root

  EXPECT_EQ(GroupOperation("group-a").lifecycle_,
            MetaOperationLifecycle::kCompleted);
  EXPECT_TRUE(stores_.topology_.AuthorityFor("group-a")->grant_.has_value());
  EXPECT_FALSE(stores_.topology_.AuthorityFor("group-b")->grant_.has_value());
  EXPECT_EQ(stores_.operation_.FindOperation(root_)->lifecycle_,
            MetaOperationLifecycle::kAborted);
}
}  // namespace
}  // namespace lavik::meta
