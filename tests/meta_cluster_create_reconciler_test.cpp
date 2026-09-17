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

#include <limits>

#include "gtest/gtest.h"
#include "lavik/meta/cluster_create.h"
#include "lavik/meta/cluster_create_reconciler.h"
#include "lavik/meta/control_projector.h"
#include "lavik/meta/hash.h"

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

  auto Plan() {
    return detail::PlanClusterCreateStep(
        MetaCommittedView(stores_, index_),
        *stores_.operation_.FindOperation(root_), runtime_, raft_);
  }

  virtual void ConfigureDataEndpoints() {}

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
      node.boot_id_ = std::string(40, static_cast<char>('5' + index));
      node.replication_history_id_.fill(static_cast<std::uint8_t>(10 + index));
      node.replication_flow_count_ = index == 0 ? 3 : 2;
      node.control_revision_ = index_;
      node.validated_committed_high_water_ =
          std::numeric_limits<std::uint64_t>::max();
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
  ApplyPlanned();  // group-b: submit only after group-a completes
  EXPECT_EQ(GroupOperation("group-b").lifecycle_,
            MetaOperationLifecycle::kSubmitted);
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
