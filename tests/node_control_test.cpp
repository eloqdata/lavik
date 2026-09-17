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

#include "lavik/cluster/node_control.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "bycorf/net/server.h"
#include "bycorf/runtime/worker.h"
#include "lavik/metrics.h"

namespace lavik::cluster {
namespace {

using namespace std::chrono_literals;

constexpr std::string_view kNodeA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr std::string_view kNodeB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr std::string_view kBoot = "cccccccccccccccccccccccccccccccccccccccc";
constexpr std::uint64_t kPartitionReplicationEpoch = 23;

template <typename Id>
Id ParseId(std::string_view text) {
  const std::optional<Id> parsed = Id::Parse(text);
  EXPECT_TRUE(parsed.has_value());
  return parsed.value_or(Id{});
}

Sha256Digest Digest(std::uint8_t byte) {
  Sha256Digest digest{};
  digest.fill(byte);
  return digest;
}

NodeDescriptor MakeNode(std::string_view id, std::uint16_t port) {
  NodeDescriptor node;
  node.node_id_ = *NodeId::Parse(id);
  node.SetHost("127.0.0.1");
  node.port_ = port;
  return node;
}

AssignmentId Assignment(unsigned value) {
  std::array<std::uint8_t, AssignmentId::kByteSize> bytes{};
  bytes.back() = static_cast<std::uint8_t>(value);
  return AssignmentId::FromBytes(bytes);
}

SessionIdentity Session(unsigned value, std::uint64_t generation = 1) {
  std::array<std::uint8_t, SessionId::kByteSize> bytes{};
  bytes.back() = static_cast<std::uint8_t>(value);
  return SessionIdentity{
      .session_id_ = SessionId::FromBytes(bytes),
      .generation_ = generation,
      .data_boot_id_ = *NodeId::Parse(kBoot),
  };
}

template <typename Id>
Id ShortId(unsigned value) {
  typename Id::Bytes bytes{};
  bytes.back() = static_cast<std::uint8_t>(value);
  return Id::FromBytes(bytes);
}

template <typename T>
T RunTaskSync(bycorf::Task<T> task) {
  std::promise<void> done;
  std::future<void> signal = done.get_future();
  task.SetCompletionCallback(
      &done, [](void* context, std::coroutine_handle<>) noexcept {
        static_cast<std::promise<void>*>(context)->set_value();
      });
  auto handle = std::move(task).ReleaseHandle();
  EXPECT_TRUE(handle);
  if (!handle) return T{};
  handle.resume();
  EXPECT_EQ(signal.wait_for(5s), std::future_status::ready);
  T result = std::move(handle.promise().value_);
  handle.destroy();
  return result;
}

std::shared_ptr<const ServingState> MakeState(
    AssignmentId assignment = Assignment(1), std::uint64_t topology_epoch = 1,
    std::uint64_t term = 1, std::uint64_t manifest_revision = 1,
    bool granted = true, bool population_ready = true,
    bool mutations_paused = false) {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(topology_epoch)
      .SetSelfNodeIndex(0)
      .AddNode(MakeNode(kNodeA, 7000))
      .AddNode(MakeNode(kNodeB, 7001));
  GroupView group;
  group.group_id_ = "group-a";
  group.primary_node_index_ = 0;
  group.assignment_id_ = assignment;
  group.group_term_ = term;
  group.manifest_revision_ = manifest_revision;
  group.granted_ = granted;
  group.population_ready_ = population_ready;
  group.storage_ready_ = true;  // Installer replaces this local fact.
  group.mutations_paused_ = mutations_paused;
  group.slot_ranges_.push_back({0, kSlotCount - 1});
  builder.AddGroup(std::move(group));
  auto state = builder.Build();
  EXPECT_TRUE(state.ok()) << state.status();
  return state.ok() ? *state : nullptr;
}

std::shared_ptr<const ServingState> MakeReplicaState(
    AssignmentId owner_assignment, std::uint64_t manifest_revision = 1,
    bool population_ready = false) {
  ServingStateBuilder builder;
  NodeDescriptor owner = MakeNode(kNodeA, 7000);
  NodeDescriptor replica = MakeNode(kNodeB, 7001);
  replica.primary_node_index_ = 0;
  builder.SetTopologyEpoch(1)
      .SetSelfNodeIndex(1)
      .AddNode(std::move(owner))
      .AddNode(std::move(replica));
  GroupView group;
  group.group_id_ = "group-a";
  group.primary_node_index_ = 0;
  group.assignment_id_ = owner_assignment;
  group.group_term_ = 1;
  group.manifest_revision_ = manifest_revision;
  group.granted_ = true;
  group.population_ready_ = population_ready;
  group.storage_ready_ = true;
  group.replica_node_indices_.push_back(1);
  group.slot_ranges_.push_back({0, kSlotCount - 1});
  builder.AddGroup(std::move(group));
  auto state = builder.Build();
  EXPECT_TRUE(state.ok()) << state.status();
  return state.ok() ? *state : nullptr;
}

std::shared_ptr<const ServingState> MakeLocalSourceState(
    AssignmentId target_assignment, std::uint64_t topology_epoch,
    bool population_ready = true) {
  ServingStateBuilder builder;
  NodeDescriptor source = MakeNode(kNodeA, 7000);
  NodeDescriptor target = MakeNode(kNodeB, 7001);
  source.primary_node_index_ = 1;
  builder.SetTopologyEpoch(topology_epoch)
      .SetSelfNodeIndex(0)
      .AddNode(std::move(source))
      .AddNode(std::move(target));
  GroupView group;
  group.group_id_ = "group-a";
  group.primary_node_index_ = 1;
  group.assignment_id_ = target_assignment;
  group.group_term_ = 1;
  group.manifest_revision_ = 1;
  group.granted_ = true;
  group.population_ready_ = population_ready;
  group.storage_ready_ = true;
  group.replica_node_indices_.push_back(0);
  group.slot_ranges_.push_back({0, kSlotCount - 1});
  builder.AddGroup(std::move(group));
  auto state = builder.Build();
  EXPECT_TRUE(state.ok()) << state.status();
  return state.ok() ? *state : nullptr;
}

std::shared_ptr<const ServingState> MakeOwnerlessState() {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(2)
      .SetSelfNodeIndex(1)
      .AddNode(MakeNode(kNodeA, 7000))
      .AddNode(MakeNode(kNodeB, 7001));
  auto state = builder.Build();
  EXPECT_TRUE(state.ok()) << state.status();
  return state.ok() ? *state : nullptr;
}

std::shared_ptr<const ServingState> MakeLocalOwnerlessState(
    std::uint64_t topology_epoch = 2) {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(topology_epoch)
      .SetSelfNodeIndex(0)
      .AddNode(MakeNode(kNodeA, 7000))
      .AddNode(MakeNode(kNodeB, 7001));
  auto state = builder.Build();
  EXPECT_TRUE(state.ok()) << state.status();
  return state.ok() ? *state : nullptr;
}

ProjectionBasis Basis(std::uint64_t index) {
  return ProjectionBasis{.control_revision_ = index};
}

PreparedFullState FullState(
    std::shared_ptr<const ServingState> state,
    std::optional<AssignmentId> node_b_assignment = std::nullopt) {
  const GroupView* group = state->FindGroup("group-a");
  EXPECT_NE(group, nullptr);
  PreparedFullState prepared{
      .serving_state_ = std::move(state),
      .authority_lease_duration_ms_ = 5'000,
  };
  if (group != nullptr) {
    prepared.desired_cluster_controls_.push_back(DesiredClusterControl{
        .identity_ =
            PreparedGroupControlIdentity{
                .group_id_ = group->group_id_,
                .group_term_ = group->group_term_,
                .manifest_revision_ = group->manifest_revision_,
                .manifest_digest_ = Digest(4),
                .partition_replication_epoch_ = kPartitionReplicationEpoch,
                .members_ = {{.node_id_ = *NodeId::Parse(kNodeA),
                              .assignment_id_ = group->assignment_id_},
                             {.node_id_ = *NodeId::Parse(kNodeB),
                              .assignment_id_ = node_b_assignment.value_or(
                                  group->assignment_id_)}},
            },
        .owner_ =
            PreparedMemberAssignment{
                .node_id_ =
                    prepared.serving_state_->NodeAt(group->primary_node_index_)
                        ->node_id_,
                .assignment_id_ = group->assignment_id_},
        .grant_active_ = true,
    });
  }
  return prepared;
}

PreparedFullState WithLease(PreparedFullState state,
                            std::chrono::milliseconds duration) {
  state.authority_lease_duration_ms_ = duration.count();
  return state;
}

PreparedFullState LocalSourceFullState(
    std::shared_ptr<const ServingState> state, AssignmentId source_assignment,
    AssignmentId target_assignment) {
  const GroupView* group = state->FindGroup("group-a");
  EXPECT_NE(group, nullptr);
  PreparedFullState prepared{
      .serving_state_ = std::move(state),
      .authority_lease_duration_ms_ = 5'000,
  };
  if (group != nullptr) {
    prepared.desired_cluster_controls_.push_back(DesiredClusterControl{
        .identity_ =
            PreparedGroupControlIdentity{
                .group_id_ = group->group_id_,
                .group_term_ = group->group_term_,
                .manifest_revision_ = group->manifest_revision_,
                .manifest_digest_ = Digest(4),
                .partition_replication_epoch_ = kPartitionReplicationEpoch,
                .members_ = {{.node_id_ = *NodeId::Parse(kNodeA),
                              .assignment_id_ = source_assignment},
                             {.node_id_ = *NodeId::Parse(kNodeB),
                              .assignment_id_ = target_assignment}},
            },
        .owner_ =
            PreparedMemberAssignment{
                .node_id_ =
                    prepared.serving_state_->NodeAt(group->primary_node_index_)
                        ->node_id_,
                .assignment_id_ = group->assignment_id_},
        .grant_active_ = true,
    });
  }
  return prepared;
}

AuthorityAnchor Anchor(const ServingState& state) {
  const GroupView* group = state.FindGroup("group-a");
  EXPECT_NE(group, nullptr);
  return AuthorityAnchor{
      .group_id_ = "group-a",
      .assignment_id_ = group->assignment_id_,
      .group_term_ = group->group_term_,
  };
}

RequestView WriteRequest(std::span<const std::uint16_t> slots) {
  return RequestView{.slots_ = slots, .is_write_ = true};
}

class RecordingActions final : public NodeControlActions {
 public:
  bool ReceivesDirectives() const noexcept override {
    return receives_directives_;
  }

  absl::Status RevokeSourceAuthorizations() override {
    ++revocations_;
    return revoke_status_;
  }

  bycorf::Task<absl::Status> RevokeSourceAuthorizationsAndWait() override {
    ++async_revocations_;
    revocation_entered_ = true;
    if (on_async_revocation_) on_async_revocation_();
    while (block_revocation_) {
      bycorf::Worker* worker = bycorf::ThisWorker().self_;
      if (worker == nullptr) {
        co_return absl::FailedPreconditionError(
            "blocked test revocation requires a Bycorf worker");
      }
      co_await bycorf::Yield(*worker);
    }
    revocation_exited_ = true;
    co_return revoke_status_;
  }

  bycorf::Task<absl::Status>
  ClearSourceAuthorizationsForSessionReplacementAndWait(
      bool preserve_established_exports) override {
    ++session_clears_;
    preserve_established_exports_.push_back(preserve_established_exports);
    control_events_.push_back("clear-sources");
    session_clear_entered_ = true;
    if (on_async_revocation_) on_async_revocation_();
    while (block_session_clear_) {
      bycorf::Worker* worker = bycorf::ThisWorker().self_;
      if (worker == nullptr) {
        co_return absl::FailedPreconditionError(
            "blocked test session clear requires a Bycorf worker");
      }
      co_await bycorf::Yield(*worker);
    }
    session_clear_exited_ = true;
    co_return revoke_status_;
  }

  bycorf::Task<absl::Status>
  RefreshSourceAuthorizationsForFdsReplacementAndWait(
      bool preserve_current_population_exports,
      std::size_t expected_authorization_replays) override {
    expected_authorization_replays_.push_back(expected_authorization_replays);
    co_return co_await ClearSourceAuthorizationsForSessionReplacementAndWait(
        preserve_current_population_exports);
  }

  bycorf::Task<absl::Status> ReconcileClusterControl(
      std::optional<DesiredClusterControl> desired) override {
    ++cluster_control_reconciliations_;
    desired_cluster_control_ = std::move(desired);
    control_events_.push_back("reconcile-cluster-control");
    if (on_cluster_control_reconcile_) on_cluster_control_reconcile_();
    co_return cluster_control_reconcile_status_;
  }

  bycorf::Task<absl::Status> ReconcilePopulation(
      std::optional<PopulationReadiness> desired,
      bool population_transition_expected) override {
    ++population_reconciliations_;
    desired_population_ = std::move(desired);
    population_transition_expected_ = population_transition_expected;
    co_return population_reconcile_status_;
  }

  bycorf::Task<absl::Status> ActivatePreparedPromotion(
      PreparedFailoverActivation activation) override {
    ++promotion_activations_;
    promotion_activation_ = std::move(activation);
    control_events_.push_back("activate-promotion");
    promotion_activation_entered_ = true;
    if (on_promotion_activation_) on_promotion_activation_();
    while (block_promotion_activation_) {
      bycorf::Worker* worker = bycorf::ThisWorker().self_;
      if (worker == nullptr) {
        co_return absl::FailedPreconditionError(
            "blocked promotion activation requires a Bycorf worker");
      }
      co_await bycorf::Yield(*worker);
    }
    co_return promotion_activation_status_;
  }

  bycorf::Task<absl::Status> EnableExpirationAuthorityUntil(
      MonotonicTime deadline) override {
    ++expiration_authority_enables_;
    expiration_authority_deadline_ = deadline;
    control_events_.push_back("enable-expiration");
    expiration_authority_enable_entered_ = true;
    if (on_expiration_authority_enable_) {
      on_expiration_authority_enable_();
    }
    while (block_expiration_authority_enable_) {
      bycorf::Worker* worker = bycorf::ThisWorker().self_;
      if (worker == nullptr) {
        co_return absl::FailedPreconditionError(
            "blocked expiration enable requires a Bycorf worker");
      }
      co_await bycorf::Yield(*worker);
    }
    co_return expiration_authority_enable_status_;
  }

  bycorf::Task<absl::Status> EnableSourceAdmissionForLease(
      MonotonicTime deadline) override {
    ++source_admission_enables_;
    source_admission_deadline_ = deadline;
    control_events_.push_back("enable-source-admission");
    source_admission_enable_entered_ = true;
    if (on_source_admission_enable_) on_source_admission_enable_();
    while (block_source_admission_enable_) {
      bycorf::Worker* worker = bycorf::ThisWorker().self_;
      if (worker == nullptr) {
        co_return absl::FailedPreconditionError(
            "blocked source admission enable requires a Bycorf worker");
      }
      co_await bycorf::Yield(*worker);
    }
    co_return source_admission_enable_status_;
  }

  bycorf::Task<absl::Status> RevokeExpirationAuthority() override {
    ++expiration_authority_revocations_;
    control_events_.push_back("revoke-expiration");
    co_return expiration_authority_revoke_status_;
  }

  bycorf::Task<absl::Status> CancelInProgressPopulation(
      bool preserve_current_follow_attempt) override {
    ++population_cancellations_;
    population_cancellation_preserve_follow_.push_back(
        preserve_current_follow_attempt);
    population_events_.push_back("cancel");
    co_return population_reconcile_status_;
  }

  bycorf::Task<absl::Status> CancelPopulationForShutdown() override {
    ++population_shutdown_cancellations_;
    population_events_.push_back("shutdown-cancel");
    co_return population_reconcile_status_;
  }

  bycorf::Task<absl::Status> ApplyDirective(NodeDirective directive) override {
    if (on_apply_directive_) on_apply_directive_();
    population_events_.push_back("start");
    directives_.push_back(std::move(directive));
    co_return directive_status_;
  }

  bycorf::Task<NodeDirectiveCompletion> StartDirective(
      NodeDirective directive) override {
    directive_action_entered_ = true;
    while (block_directive_action_) {
      bycorf::Worker* worker = bycorf::ThisWorker().self_;
      if (worker == nullptr) {
        co_return NodeDirectiveCompletion::Rejected(
            absl::FailedPreconditionError(
                "blocked directive action requires a Bycorf worker"));
      }
      co_await bycorf::Yield(*worker);
    }
    const absl::Status admitted = co_await ApplyDirective(std::move(directive));
    if (!admitted.ok() || !defer_directive_completion_) {
      co_return NodeDirectiveCompletion::StartedTerminal(admitted);
    }
    co_return NodeDirectiveCompletion(
        [this] { return deferred_directive_result_; });
  }

  std::optional<NodeDirectiveCompletion> FindCompletedPopulation(
      const NodeDirective& directive) const override {
    if (completed_population_ != directive) return std::nullopt;
    return NodeDirectiveCompletion::StartedTerminal(absl::OkStatus());
  }

  absl::Status DrainAssignment(const AuthorityAnchor& anchor) override {
    drained_.push_back(anchor);
    return drain_status_;
  }

  int revocations_ = 0;
  int async_revocations_ = 0;
  int session_clears_ = 0;
  std::vector<bool> preserve_established_exports_;
  std::vector<std::size_t> expected_authorization_replays_;
  int cluster_control_reconciliations_ = 0;
  int population_reconciliations_ = 0;
  int population_cancellations_ = 0;
  std::vector<bool> population_cancellation_preserve_follow_;
  int population_shutdown_cancellations_ = 0;
  int promotion_activations_ = 0;
  int expiration_authority_enables_ = 0;
  int source_admission_enables_ = 0;
  int expiration_authority_revocations_ = 0;
  bool block_revocation_ = false;
  bool revocation_entered_ = false;
  bool revocation_exited_ = false;
  bool block_session_clear_ = false;
  bool session_clear_entered_ = false;
  bool session_clear_exited_ = false;
  bool block_directive_action_ = false;
  bool directive_action_entered_ = false;
  bool defer_directive_completion_ = false;
  std::vector<NodeDirective> directives_;
  std::vector<AuthorityAnchor> drained_;
  std::vector<std::string> population_events_;
  std::vector<std::string> control_events_;
  std::optional<DesiredClusterControl> desired_cluster_control_;
  std::optional<PopulationReadiness> desired_population_;
  std::optional<PreparedFailoverActivation> promotion_activation_;
  std::optional<MonotonicTime> expiration_authority_deadline_;
  std::optional<MonotonicTime> source_admission_deadline_;
  std::optional<absl::Status> deferred_directive_result_;
  std::optional<NodeDirective> completed_population_;
  bool population_transition_expected_ = false;
  bool receives_directives_ = false;
  bool block_promotion_activation_ = false;
  bool promotion_activation_entered_ = false;
  bool block_expiration_authority_enable_ = false;
  bool expiration_authority_enable_entered_ = false;
  bool block_source_admission_enable_ = false;
  bool source_admission_enable_entered_ = false;
  std::function<void()> on_async_revocation_;
  std::function<void()> on_cluster_control_reconcile_;
  std::function<void()> on_apply_directive_;
  std::function<void()> on_promotion_activation_;
  std::function<void()> on_expiration_authority_enable_;
  std::function<void()> on_source_admission_enable_;
  absl::Status revoke_status_ = absl::OkStatus();
  absl::Status directive_status_ = absl::OkStatus();
  absl::Status population_reconcile_status_ = absl::OkStatus();
  absl::Status cluster_control_reconcile_status_ = absl::OkStatus();
  absl::Status promotion_activation_status_ = absl::OkStatus();
  absl::Status expiration_authority_enable_status_ = absl::OkStatus();
  absl::Status source_admission_enable_status_ = absl::OkStatus();
  absl::Status expiration_authority_revoke_status_ = absl::OkStatus();
  absl::Status drain_status_ = absl::OkStatus();
};

struct DynamicControl {
  DynamicControl() : guard(cache), installer(cache, guard, actions) {}

  TopologyCache cache;
  AuthorityGuard guard;
  RecordingActions actions;
  NodeControlInstaller installer;
};

DesiredClusterControl DesiredControl(std::string group_id = "group-a") {
  return DesiredClusterControl{
      .identity_ =
          {
              .group_id_ = std::move(group_id),
              .group_term_ = 7,
              .manifest_revision_ = 11,
              .manifest_digest_ = Digest(4),
              .partition_replication_epoch_ = 12,
              .members_ = {{.node_id_ = *NodeId::Parse(kNodeA),
                            .assignment_id_ = Assignment(1)},
                           {.node_id_ = *NodeId::Parse(kNodeB),
                            .assignment_id_ = Assignment(2)}},
          },
      .owner_ =
          PreparedMemberAssignment{
              .node_id_ = *NodeId::Parse(kNodeA),
              .assignment_id_ = Assignment(1),
          },
      .grant_active_ = true,
      .steady_replication_enabled_ = true,
  };
}

PreparedFullState WithDesiredControl(PreparedFullState prepared) {
  EXPECT_EQ(prepared.desired_cluster_controls_.size(), 1U);
  if (prepared.desired_cluster_controls_.size() != 1U) return prepared;
  DesiredClusterControl desired = DesiredControl();
  desired.identity_ = prepared.desired_cluster_controls_.front().identity_;
  prepared.desired_cluster_controls_.front() = std::move(desired);
  return prepared;
}

PreparedFullState ActivationFullState(FailoverActionId action_id,
                                      std::uint32_t lease_duration_ms = 5000) {
  PreparedFullState prepared = WithDesiredControl(FullState(MakeState()));
  prepared.desired_cluster_controls_.front().activation_action_id_ = action_id;
  prepared.authority_lease_duration_ms_ = lease_duration_ms;
  return prepared;
}

PreparedFullState FencedFullState(DesiredClusterControl desired) {
  PreparedFullState prepared{
      .serving_state_ = MakeLocalOwnerlessState(),
      .authority_lease_duration_ms_ = 5'000,
      .desired_cluster_controls_ = {std::move(desired)},
  };
  return prepared;
}

PreparedFullState ControlledPauseFullState(bool paused,
                                           std::uint64_t topology_epoch) {
  // Match PrepareMetaFullState: Meta never projects the boot-local ReadyToken
  // back to Data, even when only failover control changes between snapshots.
  PreparedFullState prepared = WithDesiredControl(FullState(
      MakeState(Assignment(1), topology_epoch, 1, 1,
                /*granted=*/true, /*population_ready=*/false, paused)));
  if (paused) {
    prepared.desired_cluster_controls_.front().failover_transition_ =
        PreparedFailoverTransition{
            .transition_id_ = ShortId<FailoverTransitionId>(2),
            .revision_ = 19,
            .mode_ = PreparedFailoverMode::kControlled,
            .target_term_ = 2,
            .candidate_action_ =
                PreparedFailoverAction{
                    .action_id_ = ShortId<FailoverActionId>(3),
                    .candidate_ =
                        {
                            .node_id_ = *NodeId::Parse(kNodeB),
                            .assignment_id_ = Assignment(2),
                            .boot_id_ = *NodeId::Parse(kBoot),
                        },
                    .domain_ =
                        {
                            .source_group_term_ = 1,
                            .source_node_id_ = *NodeId::Parse(kNodeA),
                            .source_assignment_id_ = Assignment(1),
                            .source_boot_id_ = *NodeId::Parse(kBoot),
                            .source_history_id_ = *NodeId::Parse(kNodeA),
                            .flow_count_ = 1,
                        },
                },
    };
  }
  return prepared;
}

class ControlledPauseDrainService final : public bycorf::Service {
 public:
  explicit ControlledPauseDrainService(bycorf::Server* server)
      : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ = absl::FailedPreconditionError(
          "controlled pause drain test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }
    result_ = co_await control_.installer.InstallFullStateTransition(
        ControlledPauseFullState(false, 1), Basis(10));
    if (!result_.ok()) {
      server_->RequestStop();
      co_return result_;
    }

    old_state_ = control_.cache.Current();
    GroupInFlight* cell = old_state_->InFlightCellForSlot(12);
    if (cell == nullptr) {
      result_ = absl::InternalError("pause test has no in-flight cell");
      server_->RequestStop();
      co_return result_;
    }
    in_flight_.emplace(*cell, 0);
    control_.actions.on_async_revocation_ = [&] {
      worker.Spawn(ReleaseOldMutation(worker));
    };
    control_.actions.on_cluster_control_reconcile_ = [&] {
      reconciled_after_drain_ = old_state_->GroupInFlightCount("group-a") == 0;
    };

    result_ = co_await control_.installer.InstallFullStateTransition(
        ControlledPauseFullState(true, 2), Basis(11));
    full_state_returned_after_drain_ = old_mutation_released_;
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  bycorf::Server* server_;
  DynamicControl control_;
  bool prepared_ = false;
  bool old_mutation_released_ = false;
  bool pause_published_before_release_ = false;
  bool reconciled_after_drain_ = false;
  bool full_state_returned_after_drain_ = false;
  absl::Status result_ =
      absl::UnknownError("controlled pause drain test did not run");

 private:
  bycorf::Task<absl::Status> ReleaseOldMutation(bycorf::Worker& worker) {
    co_await bycorf::Yield(worker);
    const GroupView* current = control_.cache.Current()->FindGroup("group-a");
    pause_published_before_release_ =
        current != nullptr && current->mutations_paused_;
    in_flight_.reset();
    old_mutation_released_ = true;
    co_return absl::OkStatus();
  }

  std::shared_ptr<const ServingState> old_state_;
  std::optional<InFlightGuard> in_flight_;
};

TEST(EstablishedExportScopeTest,
     SeparatesStableDataFlowIdentityFromAuthorityAndFailoverControl) {
  const DesiredClusterControl established = DesiredControl();

  DesiredClusterControl control_only = established;
  ++control_only.identity_.group_term_;
  control_only.grant_active_ = false;
  control_only.activation_action_id_ = ShortId<FailoverActionId>(1);
  control_only.failover_transition_ = PreparedFailoverTransition{
      .transition_id_ = ShortId<FailoverTransitionId>(2),
      .revision_ = 19,
      .mode_ = PreparedFailoverMode::kUncontrolled,
      .target_term_ = 8,
  };
  EXPECT_TRUE(SameEstablishedExportScope(established, control_only));

  DesiredClusterControl changed = control_only;
  changed.owner_->assignment_id_ = Assignment(3);
  EXPECT_FALSE(SameEstablishedExportScope(established, changed));
  changed = control_only;
  changed.identity_.manifest_digest_ = Digest(5);
  EXPECT_FALSE(SameEstablishedExportScope(established, changed));
  changed = control_only;
  ++changed.identity_.partition_replication_epoch_;
  EXPECT_FALSE(SameEstablishedExportScope(established, changed));
  changed = control_only;
  changed.identity_.members_[1].assignment_id_ = Assignment(4);
  EXPECT_FALSE(SameEstablishedExportScope(established, changed));
}

TEST(NodeControlInstallerTest,
     FullStateReconcilesChangedClusterControlAfterSourceCleanupOnly) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());

  ASSERT_TRUE(
      RunTaskSync(control.installer.InstallFullStateTransition(
                      WithDesiredControl(FullState(MakeState())), Basis(10)))
          .ok());
  ASSERT_EQ(
      control.actions.control_events_,
      (std::vector<std::string>{"clear-sources", "reconcile-cluster-control"}));
  ASSERT_EQ(control.actions.cluster_control_reconciliations_, 1);
  ASSERT_TRUE(control.actions.desired_cluster_control_.has_value());
  EXPECT_EQ(control.actions.desired_cluster_control_->identity_.group_id_,
            "group-a");
  EXPECT_FALSE(control.actions.desired_cluster_control_
                   ->population_transition_expected_);

  // A byte-exact topology whose current directive set starts a population
  // transition must suppress ordinary Follow Owner even though the committed
  // Group portion is unchanged.
  ASSERT_TRUE(
      RunTaskSync(control.installer.InstallFullStateTransition(
                      WithDesiredControl(FullState(MakeState())), Basis(10),
                      /*local_population_transition_expected=*/true))
          .ok());
  EXPECT_EQ(control.actions.session_clears_, 2);
  EXPECT_EQ(control.actions.cluster_control_reconciliations_, 2);
  ASSERT_TRUE(control.actions.desired_cluster_control_.has_value());
  EXPECT_TRUE(control.actions.desired_cluster_control_
                  ->population_transition_expected_);

  PreparedFullState changed =
      WithDesiredControl(FullState(MakeState(Assignment(1), 2, 2)));
  changed.desired_cluster_controls_.front().failover_transition_ =
      PreparedFailoverTransition{
          .transition_id_ = ShortId<FailoverTransitionId>(2),
          .revision_ = 19,
          .mode_ = PreparedFailoverMode::kControlled,
          .target_term_ = 3,
  };
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              std::move(changed), Basis(11)))
                  .ok());
  EXPECT_EQ(control.actions.session_clears_, 3);
  EXPECT_EQ(control.actions.cluster_control_reconciliations_, 3);
  EXPECT_TRUE(control.actions.desired_cluster_control_->failover_transition_
                  .has_value());
}

TEST(NodeControlInstallerTest,
     GrantlessControlPreservesEstablishedExportWithStablePopulationScope) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  PreparedFullState initial = WithDesiredControl(FullState(MakeState()));
  DesiredClusterControl fenced = initial.desired_cluster_controls_.front();
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              std::move(initial), Basis(10)))
                  .ok());
  ASSERT_EQ(control.actions.preserve_established_exports_.size(), 1U);
  EXPECT_FALSE(control.actions.preserve_established_exports_.back());

  ++fenced.identity_.group_term_;
  fenced.grant_active_ = false;
  fenced.failover_transition_ = PreparedFailoverTransition{
      .transition_id_ = ShortId<FailoverTransitionId>(2),
      .revision_ = 19,
      .mode_ = PreparedFailoverMode::kUncontrolled,
      .target_term_ = fenced.identity_.group_term_,
  };

  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              FencedFullState(std::move(fenced)), Basis(11)))
                  .ok());
  ASSERT_EQ(control.actions.preserve_established_exports_.size(), 2U);
  EXPECT_TRUE(control.actions.preserve_established_exports_.back());
}

TEST(NodeControlInstallerTest,
     ControlledPausePreservesFiniteLeaseAndRejectsOnlyMutations) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              ControlledPauseFullState(false, 1), Basis(10)))
                  .ok());
  ASSERT_FALSE(
      control.cache.Current()->FindGroup("group-a")->population_ready_);
  ASSERT_TRUE(RunTaskSync(control.installer.SetPopulationReadinessTransition(
                              PopulationReadiness{
                                  .group_id_ = "group-a",
                                  .assignment_id_ = Assignment(1),
                                  .group_term_ = 1,
                                  .manifest_revision_ = 1,
                                  .manifest_digest_ = Digest(4),
                                  .partition_replication_epoch_ =
                                      kPartitionReplicationEpoch,
                              }))
                  .ok());
  const std::shared_ptr<const ServingState> active = control.cache.Current();
  ASSERT_NE(active, nullptr);
  ASSERT_TRUE(control.installer
                  .ApplyAuthority({.kind_ = AuthorityMessage::Kind::kLeaseGrant,
                                   .session_ = Session(1),
                                   .projection_ = Basis(10),
                                   .anchor_ = Anchor(*active),
                                   .sent_at_ = MonotonicTime{},
                                   .granted_duration_ = 5s},
                                  MonotonicTime{})
                  .ok());
  constexpr std::array<std::uint16_t, 1> slots{12};
  const AuthorityAdmission admitted_before_pause =
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 1s);
  ASSERT_EQ(admitted_before_pause.decision().kind_, Decision::Kind::kServe);

  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              ControlledPauseFullState(true, 2), Basis(11)))
                  .ok());
  ASSERT_NE(control.cache.Current()->FindGroup("group-a"), nullptr);
  EXPECT_TRUE(control.cache.Current()->FindGroup("group-a")->population_ready_);
  EXPECT_TRUE(control.cache.Current()->FindGroup("group-a")->mutations_paused_);
  AuthorityInFlightGuards guards;
  EXPECT_EQ(control.guard.RegisterAndRecheck(admitted_before_pause, 0,
                                             MonotonicTime{} + 2s, &guards),
            RecheckResult::kReject);
  EXPECT_TRUE(guards.empty());
  const RequestView read{.slots_ = slots, .is_write_ = false};
  EXPECT_EQ(control.guard.CaptureAndAdmit(read, MonotonicTime{} + 2s)
                .decision()
                .kind_,
            Decision::Kind::kServe);
  EXPECT_EQ(
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 2s)
          .decision()
          .kind_,
      Decision::Kind::kTryAgain);
  ASSERT_EQ(control.actions.preserve_established_exports_.size(), 2U);
  EXPECT_TRUE(control.actions.preserve_established_exports_.back());

  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              ControlledPauseFullState(false, 3), Basis(12)))
                  .ok());
  EXPECT_EQ(
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 3s)
          .decision()
          .kind_,
      Decision::Kind::kServe);
}

TEST(NodeControlInstallerTest,
     MetaProjectionDoesNotCarryReadinessAcrossPopulationAnchorChanges) {
  const auto expect_not_carried = [&](std::string_view changed_anchor,
                                      PreparedFullState replacement) {
    SCOPED_TRACE(changed_anchor);
    DynamicControl control;
    ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
    ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                                ControlledPauseFullState(false, 1), Basis(10)))
                    .ok());
    ASSERT_TRUE(RunTaskSync(control.installer.SetPopulationReadinessTransition(
                                PopulationReadiness{
                                    .group_id_ = "group-a",
                                    .assignment_id_ = Assignment(1),
                                    .group_term_ = 1,
                                    .manifest_revision_ = 1,
                                    .manifest_digest_ = Digest(4),
                                    .partition_replication_epoch_ =
                                        kPartitionReplicationEpoch,
                                }))
                    .ok());
    const auto ready = control.cache.Current();
    ASSERT_NE(ready, nullptr);
    ASSERT_TRUE(
        control.installer
            .ApplyAuthority({.kind_ = AuthorityMessage::Kind::kLeaseGrant,
                             .session_ = Session(1),
                             .projection_ = Basis(10),
                             .anchor_ = Anchor(*ready),
                             .sent_at_ = MonotonicTime{},
                             .granted_duration_ = 5s},
                            MonotonicTime{})
            .ok());

    ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                                std::move(replacement), Basis(11)))
                    .ok());
    const GroupView* group = control.cache.Current()->FindGroup("group-a");
    ASSERT_NE(group, nullptr);
    EXPECT_FALSE(group->population_ready_);
    constexpr std::array<std::uint16_t, 1> slots{12};
    const RequestView read{.slots_ = slots, .is_write_ = false};
    EXPECT_NE(control.guard.CaptureAndAdmit(read, MonotonicTime{} + 2s)
                  .decision()
                  .kind_,
              Decision::Kind::kServe);
  };

  PreparedFullState changed_term = ControlledPauseFullState(true, 2);
  changed_term.serving_state_ =
      MakeState(Assignment(1), 2, 2, 1,
                /*granted=*/true, /*population_ready=*/false,
                /*mutations_paused=*/true);
  changed_term.desired_cluster_controls_.front().identity_.group_term_ = 2;
  expect_not_carried("group term", std::move(changed_term));

  PreparedFullState changed_manifest = ControlledPauseFullState(true, 2);
  changed_manifest.serving_state_ =
      MakeState(Assignment(1), 2, 1, 2,
                /*granted=*/true, /*population_ready=*/false,
                /*mutations_paused=*/true);
  changed_manifest.desired_cluster_controls_.front()
      .identity_.manifest_revision_ = 2;
  changed_manifest.desired_cluster_controls_.front()
      .identity_.manifest_digest_ = Digest(5);
  expect_not_carried("manifest identity", std::move(changed_manifest));

  PreparedFullState changed_partition = ControlledPauseFullState(true, 2);
  ++changed_partition.desired_cluster_controls_.front()
        .identity_.partition_replication_epoch_;
  expect_not_carried("partition replication epoch",
                     std::move(changed_partition));

  PreparedFullState changed_local_assignment =
      ControlledPauseFullState(true, 2);
  changed_local_assignment.serving_state_ =
      MakeState(Assignment(2), 2, 1, 1,
                /*granted=*/true, /*population_ready=*/false,
                /*mutations_paused=*/true);
  changed_local_assignment.desired_cluster_controls_.front()
      .identity_.members_.front()
      .assignment_id_ = Assignment(2);
  changed_local_assignment.desired_cluster_controls_.front()
      .owner_->assignment_id_ = Assignment(2);
  expect_not_carried("local assignment", std::move(changed_local_assignment));

  PreparedFullState changed_owner =
      LocalSourceFullState(MakeLocalSourceState(Assignment(2), 2,
                                                /*population_ready=*/false),
                           Assignment(1), Assignment(2));
  DesiredClusterControl owner_control = DesiredControl();
  owner_control.identity_ =
      changed_owner.desired_cluster_controls_.front().identity_;
  owner_control.owner_ = PreparedMemberAssignment{
      .node_id_ = *NodeId::Parse(kNodeB),
      .assignment_id_ = Assignment(2),
  };
  owner_control.failover_transition_ = ControlledPauseFullState(true, 2)
                                           .desired_cluster_controls_.front()
                                           .failover_transition_;
  changed_owner.desired_cluster_controls_.front() = std::move(owner_control);
  expect_not_carried("owner", std::move(changed_owner));
}

TEST(NodeControlInstallerTest,
     ControlledPausePublishesThenDrainsBeforeReconcileAndFullStateAck) {
  bycorf::Server server;
  ControlledPauseDrainService service(&server);
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_TRUE(service.result_.ok()) << service.result_;
  EXPECT_TRUE(service.pause_published_before_release_);
  EXPECT_TRUE(service.old_mutation_released_);
  EXPECT_TRUE(service.reconciled_after_drain_);
  EXPECT_TRUE(service.full_state_returned_after_drain_);
}

class FenceDrainService final : public bycorf::Service {
 public:
  explicit FenceDrainService(bycorf::Server* server) : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ =
          absl::FailedPreconditionError("fence drain test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status installed = control_.installer.InstallFullState(
            FullState(MakeState()), Basis(10));
        !installed.ok()) {
      result_ = installed;
      server_->RequestStop();
      co_return result_;
    }
    old_ = control_.cache.Current();
    GroupInFlight* cell = old_->InFlightCellForSlot(12);
    if (cell == nullptr) {
      result_ = absl::InternalError("test state has no in-flight cell");
      server_->RequestStop();
      co_return result_;
    }
    in_flight_.emplace(*cell, 0);
    worker.Spawn(ReleaseAfterTransitionSuspends(worker));

    result_ = co_await control_.installer.ApplyFenceTransition(AuthorityMessage{
        .kind_ = AuthorityMessage::Kind::kFence,
        .session_ = Session(1),
        .projection_ = Basis(10),
        .anchor_ = Anchor(*old_),
    });
    transition_returned_ = true;
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }
  bool release_ran_while_waiting() const noexcept {
    return release_ran_while_waiting_;
  }
  bool saw_fenced_state() const noexcept { return saw_fenced_state_; }

 private:
  bycorf::Task<absl::Status> ReleaseAfterTransitionSuspends(
      bycorf::Worker& worker) {
    while (control_.actions.async_revocations_ == 0) {
      co_await bycorf::Yield(worker);
    }
    release_ran_while_waiting_ = !transition_returned_;
    const GroupView* fenced = control_.cache.Current()->FindGroup("group-a");
    saw_fenced_state_ = fenced != nullptr && !fenced->granted_;
    in_flight_.reset();
    co_return absl::OkStatus();
  }

  bycorf::Server* server_;
  DynamicControl control_;
  std::shared_ptr<const ServingState> old_;
  std::optional<InFlightGuard> in_flight_;
  bool prepared_ = false;
  bool transition_returned_ = false;
  bool release_ran_while_waiting_ = false;
  bool saw_fenced_state_ = false;
  absl::Status result_ = absl::UnknownError("fence drain service did not run");
};

class ProvisionalActivationService final : public bycorf::Service {
 public:
  enum class Scenario {
    kBlockedSuccess,
    kProjectionReplacement,
    kReplacementDuringExpirationEnable,
    kSessionLossDuringPromotionActivation,
    kSessionLossDuringExpirationEnable,
    kSessionLossDuringSourceAdmissionEnable,
    kExpiresDuringActivation,
    kRestartedWinnerWithPriorBootAction,
    kSteadyOwner,
    kRevocationBoundaries,
    kMismatchedDesiredAuthority,
    kLeaseShorterThanResolvedDuration,
    kLeaseLongerThanResolvedDuration,
  };

  ProvisionalActivationService(bycorf::Server* server, Scenario scenario)
      : server_(server), scenario_(scenario) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ = absl::FailedPreconditionError(
          "provisional activation test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    control_.actions.receives_directives_ = true;
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }

    const bool activation =
        scenario_ != Scenario::kSteadyOwner &&
        scenario_ != Scenario::kRevocationBoundaries &&
        scenario_ != Scenario::kMismatchedDesiredAuthority &&
        scenario_ != Scenario::kLeaseShorterThanResolvedDuration &&
        scenario_ != Scenario::kLeaseLongerThanResolvedDuration;
    const std::uint32_t desired_duration_ms =
        scenario_ == Scenario::kExpiresDuringActivation ? 5 : 5000;
    const std::uint32_t granted_duration_ms =
        scenario_ == Scenario::kLeaseShorterThanResolvedDuration ? 300
        : scenario_ == Scenario::kLeaseLongerThanResolvedDuration
            ? 6000
            : desired_duration_ms;
    PreparedFullState prepared =
        activation ? ActivationFullState(ShortId<FailoverActionId>(7),
                                         desired_duration_ms)
                   : WithDesiredControl(FullState(MakeState()));
    if (scenario_ == Scenario::kRestartedWinnerWithPriorBootAction) {
      // This service owns a fresh installer and action adapter, so it has no
      // prepared context from the boot that won the committed cutover. Meta's
      // projection still names that historical activation action, while
      // readiness is deliberately established below for this new boot.
      prepared.serving_state_ =
          MakeState(Assignment(1), 1, 1, 1,
                    /*granted=*/true, /*population_ready=*/false);
    }
    if (scenario_ == Scenario::kMismatchedDesiredAuthority) {
      prepared.desired_cluster_controls_.front().grant_active_ = false;
    }
    result_ = co_await control_.installer.InstallFullStateTransition(
        std::move(prepared), Basis(10));
    if (!result_.ok()) {
      server_->RequestStop();
      co_return result_;
    }

    if (scenario_ == Scenario::kRestartedWinnerWithPriorBootAction) {
      result_ = co_await control_.installer.SetPopulationReadinessTransition(
          PopulationReadiness{
              .group_id_ = "group-a",
              .assignment_id_ = Assignment(1),
              .group_term_ = 1,
              .manifest_revision_ = 1,
              .manifest_digest_ = Digest(4),
              .partition_replication_epoch_ = kPartitionReplicationEpoch,
          });
      if (!result_.ok()) {
        server_->RequestStop();
        co_return result_;
      }
      // Mirror ReplicationManager's rejection when a new process has no
      // boot-local retained action/prepared context for the persisted id.
      control_.actions.promotion_activation_status_ =
          absl::FailedPreconditionError(
              "cluster activation has no matching retained prepared action");
    }

    const std::shared_ptr<const ServingState> state = control_.cache.Current();
    grant_ = AuthorityMessage{
        .kind_ = AuthorityMessage::Kind::kLeaseGrant,
        .session_ = Session(1),
        .projection_ = Basis(10),
        .anchor_ = Anchor(*state),
        .sent_at_ = LeaseClockNow(),
        .granted_duration_ = std::chrono::milliseconds(granted_duration_ms),
    };
    control_.actions.on_source_admission_enable_ = [this] {
      lease_installed_before_source_admission_ = CanWrite();
    };

    if (scenario_ == Scenario::kBlockedSuccess ||
        scenario_ == Scenario::kProjectionReplacement ||
        scenario_ == Scenario::kReplacementDuringExpirationEnable ||
        scenario_ == Scenario::kSessionLossDuringPromotionActivation ||
        scenario_ == Scenario::kSessionLossDuringExpirationEnable ||
        scenario_ == Scenario::kSessionLossDuringSourceAdmissionEnable) {
      const bool block_expiration =
          scenario_ == Scenario::kReplacementDuringExpirationEnable ||
          scenario_ == Scenario::kSessionLossDuringExpirationEnable;
      const bool block_source =
          scenario_ == Scenario::kSessionLossDuringSourceAdmissionEnable;
      control_.actions.block_promotion_activation_ =
          !block_expiration && !block_source;
      control_.actions.block_expiration_authority_enable_ = block_expiration;
      control_.actions.block_source_admission_enable_ = block_source;
      worker.Spawn(ApplyGrant());
      while (control_.actions.block_promotion_activation_ &&
             !control_.actions.promotion_activation_entered_) {
        co_await bycorf::Yield(worker);
      }
      while (control_.actions.block_expiration_authority_enable_ &&
             !control_.actions.expiration_authority_enable_entered_) {
        co_await bycorf::Yield(worker);
      }
      while (control_.actions.block_source_admission_enable_ &&
             !control_.actions.source_admission_enable_entered_) {
        co_await bycorf::Yield(worker);
      }
      no_lease_before_activation_ = !CanWrite();
      expiration_disabled_before_activation_ =
          control_.actions.expiration_authority_enables_ == 0;
      if (scenario_ == Scenario::kProjectionReplacement ||
          scenario_ == Scenario::kReplacementDuringExpirationEnable) {
        PreparedFullState replacement =
            WithDesiredControl(FullState(MakeState(Assignment(1), 2, 2)));
        replacement_result_ =
            co_await control_.installer.InstallFullStateTransition(
                std::move(replacement), Basis(11));
      }
      if (scenario_ == Scenario::kSessionLossDuringPromotionActivation ||
          scenario_ == Scenario::kSessionLossDuringExpirationEnable ||
          scenario_ == Scenario::kSessionLossDuringSourceAdmissionEnable) {
        session_loss_result_ =
            control_.installer.InvalidateSessionNow(grant_.session_);
      }
      control_.actions.block_promotion_activation_ = false;
      control_.actions.block_expiration_authority_enable_ = false;
      control_.actions.block_source_admission_enable_ = false;
      while (!grant_returned_) co_await bycorf::Yield(worker);
    } else if (scenario_ == Scenario::kExpiresDuringActivation) {
      control_.actions.on_promotion_activation_ = [] {
        std::this_thread::sleep_for(20ms);
      };
      grant_result_ =
          co_await control_.installer.ApplyLeaseGrantTransition(grant_);
      grant_returned_ = true;
    } else {
      grant_result_ =
          co_await control_.installer.ApplyLeaseGrantTransition(grant_);
      grant_returned_ = true;
    }

    writable_after_ = CanWrite();
    if (scenario_ == Scenario::kRevocationBoundaries && grant_result_.ok()) {
      const int before_session =
          control_.actions.expiration_authority_revocations_;
      session_loss_result_ = co_await control_.installer.LoseSessionTransition(
          grant_.session_, "test disconnect");
      session_revoked_expiration_ =
          control_.actions.expiration_authority_revocations_ ==
          before_session + 1;
      writable_after_ = CanWrite();
    }
    result_ = absl::OkStatus();
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  bool CanWrite() const {
    constexpr std::array<std::uint16_t, 1> slots{12};
    return control_.guard.CaptureAndAdmit(WriteRequest(slots), LeaseClockNow())
               .decision()
               .kind_ == Decision::Kind::kServe;
  }

  bycorf::Task<absl::Status> ApplyGrant() {
    grant_result_ =
        co_await control_.installer.ApplyLeaseGrantTransition(grant_);
    grant_returned_ = true;
    co_return absl::OkStatus();
  }

  bycorf::Server* server_;
  Scenario scenario_;
  DynamicControl control_;
  bool prepared_ = false;
  bool no_lease_before_activation_ = false;
  bool expiration_disabled_before_activation_ = false;
  bool grant_returned_ = false;
  bool writable_after_ = false;
  bool session_revoked_expiration_ = false;
  bool lease_installed_before_source_admission_ = false;
  AuthorityMessage grant_;
  absl::Status grant_result_ =
      absl::UnknownError("provisional grant did not run");
  absl::Status replacement_result_ =
      absl::UnknownError("replacement FDS did not run");
  absl::Status session_loss_result_ =
      absl::UnknownError("session loss did not run");
  absl::Status result_ =
      absl::UnknownError("provisional activation service did not run");
};

void RunProvisionalActivationService(ProvisionalActivationService& service,
                                     bycorf::Server& server) {
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();
  ASSERT_TRUE(service.result_.ok()) << service.result_;
}

TEST(NodeControlInstallerTest,
     ProvisionalFailoverGrantDoesNotCreateLeaseBeforeExactActivation) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server, ProvisionalActivationService::Scenario::kBlockedSuccess);
  RunProvisionalActivationService(service, server);

  EXPECT_TRUE(service.no_lease_before_activation_);
  EXPECT_TRUE(service.expiration_disabled_before_activation_);
  ASSERT_TRUE(service.grant_result_.ok()) << service.grant_result_;
  EXPECT_TRUE(service.writable_after_);
  ASSERT_EQ(service.control_.actions.promotion_activations_, 1);
  ASSERT_TRUE(service.control_.actions.promotion_activation_.has_value());
  const PreparedFailoverActivation& activation =
      *service.control_.actions.promotion_activation_;
  EXPECT_EQ(activation.action_id_, ShortId<FailoverActionId>(7));
  EXPECT_EQ(activation.group_id_, "group-a");
  EXPECT_EQ(activation.candidate_node_id_, *NodeId::Parse(kNodeA));
  EXPECT_EQ(activation.candidate_assignment_id_, Assignment(1));
  EXPECT_EQ(activation.candidate_boot_id_, *NodeId::Parse(kBoot));
  EXPECT_EQ(activation.target_term_, 1U);
  EXPECT_EQ(activation.manifest_revision_, 1U);
  EXPECT_EQ(activation.manifest_digest_, Digest(4));
  EXPECT_EQ(activation.partition_replication_epoch_,
            kPartitionReplicationEpoch);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 1);
  EXPECT_EQ(service.control_.actions.expiration_authority_deadline_,
            service.grant_.sent_at_ + service.grant_.granted_duration_);
}

TEST(NodeControlInstallerTest,
     FdsReplacementDuringPromotionActivationFailsFinalRecheckWithoutLease) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server, ProvisionalActivationService::Scenario::kProjectionReplacement);
  RunProvisionalActivationService(service, server);

  EXPECT_TRUE(service.no_lease_before_activation_);
  ASSERT_TRUE(service.replacement_result_.ok()) << service.replacement_result_;
  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 0);
  EXPECT_GE(service.control_.actions.expiration_authority_revocations_, 1);
}

TEST(NodeControlInstallerTest,
     FdsReplacementDuringExpirationEnableFailsTheLastRecheckWithoutLease) {
  bycorf::Server server;
  ProvisionalActivationService service(&server,
                                       ProvisionalActivationService::Scenario::
                                           kReplacementDuringExpirationEnable);
  RunProvisionalActivationService(service, server);

  EXPECT_TRUE(service.no_lease_before_activation_);
  ASSERT_TRUE(service.replacement_result_.ok()) << service.replacement_result_;
  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.promotion_activations_, 1);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 1);
  EXPECT_GE(service.control_.actions.expiration_authority_revocations_, 1);
}

TEST(NodeControlInstallerTest,
     SessionLossDuringPromotionActivationFailsFinalRecheckWithoutLease) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server, ProvisionalActivationService::Scenario::
                   kSessionLossDuringPromotionActivation);
  RunProvisionalActivationService(service, server);

  ASSERT_TRUE(service.session_loss_result_.ok())
      << service.session_loss_result_;
  EXPECT_TRUE(service.no_lease_before_activation_);
  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(service.grant_result_.message(),
            "promotion activation crossed a changed control projection or "
            "session");
  EXPECT_FALSE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.promotion_activations_, 1);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 0);
  EXPECT_GE(service.control_.actions.expiration_authority_revocations_, 1);
}

TEST(NodeControlInstallerTest,
     SessionLossDuringExpirationEnableFailsFinalRecheckWithoutLease) {
  bycorf::Server server;
  ProvisionalActivationService service(&server,
                                       ProvisionalActivationService::Scenario::
                                           kSessionLossDuringExpirationEnable);
  RunProvisionalActivationService(service, server);

  ASSERT_TRUE(service.session_loss_result_.ok())
      << service.session_loss_result_;
  EXPECT_TRUE(service.no_lease_before_activation_);
  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(service.grant_result_.message(),
            "expiration activation crossed a changed control projection or "
            "session");
  EXPECT_FALSE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.promotion_activations_, 1);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 1);
  EXPECT_GE(service.control_.actions.expiration_authority_revocations_, 1);
}

TEST(NodeControlInstallerTest,
     SessionLossDuringSourceAdmissionEnableFailsClosedAfterInstalledLease) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server, ProvisionalActivationService::Scenario::
                   kSessionLossDuringSourceAdmissionEnable);
  RunProvisionalActivationService(service, server);

  ASSERT_TRUE(service.session_loss_result_.ok())
      << service.session_loss_result_;
  EXPECT_TRUE(service.lease_installed_before_source_admission_);
  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(service.grant_result_.message(),
            "source admission activation crossed a changed or expired "
            "control session");
  EXPECT_FALSE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.source_admission_enables_, 1);
  EXPECT_GE(service.control_.actions.expiration_authority_revocations_, 1);
}

TEST(NodeControlInstallerTest,
     GrantExpiringDuringPromotionActivationNeverEnablesExpirationOrLease) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server,
      ProvisionalActivationService::Scenario::kExpiresDuringActivation);
  RunProvisionalActivationService(service, server);

  EXPECT_EQ(service.grant_result_.code(), absl::StatusCode::kDeadlineExceeded);
  EXPECT_FALSE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.promotion_activations_, 1);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 0);
  EXPECT_EQ(service.control_.actions.expiration_authority_revocations_, 1);
}

TEST(NodeControlInstallerTest,
     RestartedCutoverWinnerCannotReusePriorBootActivationActionOrLease) {
  bycorf::Server server;
  ProvisionalActivationService service(&server,
                                       ProvisionalActivationService::Scenario::
                                           kRestartedWinnerWithPriorBootAction);
  RunProvisionalActivationService(service, server);

  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(service.grant_result_.message(),
            "cluster activation has no matching retained prepared action");
  EXPECT_FALSE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.promotion_activations_, 1);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 0);
  EXPECT_EQ(service.control_.actions.expiration_authority_revocations_, 1);
}

TEST(NodeControlInstallerTest,
     SteadyOwnerGrantSkipsPromotionAndEnablesFiniteExpirationFirst) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server, ProvisionalActivationService::Scenario::kSteadyOwner);
  RunProvisionalActivationService(service, server);

  ASSERT_TRUE(service.grant_result_.ok()) << service.grant_result_;
  EXPECT_TRUE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.promotion_activations_, 0);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 1);
  EXPECT_EQ(service.control_.actions.source_admission_enables_, 1);
  EXPECT_TRUE(service.lease_installed_before_source_admission_);
  ASSERT_GE(service.control_.actions.control_events_.size(), 2U);
  EXPECT_EQ(
      service.control_.actions
          .control_events_[service.control_.actions.control_events_.size() - 2],
      "enable-expiration");
  EXPECT_EQ(service.control_.actions.control_events_.back(),
            "enable-source-admission");
}

TEST(NodeControlInstallerTest,
     RejectedDesiredAuthorityIncludesMessageAndDesiredGrantContexts) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server,
      ProvisionalActivationService::Scenario::kMismatchedDesiredAuthority);
  RunProvisionalActivationService(service, server);

  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  const std::string message(service.grant_result_.message());
  EXPECT_NE(message.find("message={group=group-a"), std::string::npos)
      << message;
  EXPECT_NE(message.find("desired={group=group-a"), std::string::npos)
      << message;
  EXPECT_NE(message.find("grant_active=false"), std::string::npos) << message;
  EXPECT_NE(message.find("duration_ms=5000"), std::string::npos) << message;
}

TEST(NodeControlInstallerTest, RejectsLeaseShorterThanFdsResolvedDuration) {
  bycorf::Server server;
  ProvisionalActivationService service(&server,
                                       ProvisionalActivationService::Scenario::
                                           kLeaseShorterThanResolvedDuration);
  RunProvisionalActivationService(service, server);

  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(service.writable_after_);
  EXPECT_EQ(service.control_.actions.expiration_authority_enables_, 0);
  const std::string message(service.grant_result_.message());
  EXPECT_NE(message.find("duration_ms=300"), std::string::npos) << message;
  EXPECT_NE(message.find("effective_duration_ms=5000"), std::string::npos)
      << message;
}

TEST(NodeControlInstallerTest,
     RejectsLeaseLongerThanFdsResolvedDurationWithBothDurations) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server,
      ProvisionalActivationService::Scenario::kLeaseLongerThanResolvedDuration);
  RunProvisionalActivationService(service, server);

  EXPECT_EQ(service.grant_result_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(service.writable_after_);
  const std::string message(service.grant_result_.message());
  EXPECT_NE(message.find("duration_ms=6000"), std::string::npos) << message;
  EXPECT_NE(message.find("duration_ms=5000"), std::string::npos) << message;
}

TEST(NodeControlInstallerTest,
     SessionLossRevokesFiniteExpirationAuthorityAndTheWriteLease) {
  bycorf::Server server;
  ProvisionalActivationService service(
      &server, ProvisionalActivationService::Scenario::kRevocationBoundaries);
  RunProvisionalActivationService(service, server);

  ASSERT_TRUE(service.grant_result_.ok()) << service.grant_result_;
  ASSERT_TRUE(service.session_loss_result_.ok())
      << service.session_loss_result_;
  EXPECT_TRUE(service.session_revoked_expiration_);
  EXPECT_FALSE(service.writable_after_);
}

class LeaseExpiryService final : public bycorf::Service {
 public:
  explicit LeaseExpiryService(bycorf::Server* server) : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ =
          absl::FailedPreconditionError("lease timer test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }
    // Keep distinct original/renewed deadline windows with enough scheduling
    // slack for a shared runner. A delayed wakeup before renewal must not turn
    // this stale-timer check into the separate expired-lease renewal case.
    constexpr auto lease_duration = 1s;
    if (absl::Status installed = control_.installer.InstallFullState(
            WithLease(FullState(MakeState()), lease_duration), Basis(10));
        !installed.ok()) {
      result_ = installed;
      server_->RequestStop();
      co_return result_;
    }
    const std::shared_ptr<const ServingState> state = control_.cache.Current();
    AuthorityMessage grant{
        .kind_ = AuthorityMessage::Kind::kLeaseGrant,
        .session_ = Session(1),
        .projection_ = Basis(10),
        .anchor_ = Anchor(*state),
        .sent_at_ = LeaseClockNow(),
        .granted_duration_ = lease_duration,
    };
    expirations_before_ = GetClusterControlMetrics().lease_expirations_;
    result_ = co_await control_.installer.ApplyLeaseGrantTransition(grant);
    if (!result_.ok()) {
      server_->RequestStop();
      co_return result_;
    }
    absl::Status slept = co_await bycorf::SleepFor(worker, 600ms);
    if (!slept.ok()) {
      result_ = slept;
      server_->RequestStop();
      co_return result_;
    }
    grant.sent_at_ = LeaseClockNow();
    grant.granted_duration_ = lease_duration;
    result_ = co_await control_.installer.ApplyLeaseGrantTransition(grant);
    if (!result_.ok()) {
      server_->RequestStop();
      co_return result_;
    }

    // Cross the original deadline. Its timer must see the renewed deadline
    // and leave the replacement lease and its source capabilities intact.
    slept = co_await bycorf::SleepFor(worker, 600ms);
    if (!slept.ok()) {
      result_ = slept;
      server_->RequestStop();
      co_return result_;
    }
    constexpr std::array<std::uint16_t, 1> slots{12};
    old_timer_preserved_lease_ =
        control_.guard.CaptureAndAdmit(WriteRequest(slots), LeaseClockNow())
                .decision()
                .kind_ == Decision::Kind::kServe &&
        control_.actions.session_clears_ == 0;

    slept = co_await bycorf::SleepFor(worker, 700ms);
    if (!slept.ok()) {
      result_ = slept;
      server_->RequestStop();
      co_return result_;
    }
    expired_ =
        control_.guard.CaptureAndAdmit(WriteRequest(slots), LeaseClockNow())
            .decision()
            .kind_ == Decision::Kind::kClusterDownUnbound;
    expiry_preserved_source_capabilities_ =
        control_.actions.session_clears_ == 0;
    grant.sent_at_ = LeaseClockNow();
    grant.granted_duration_ = lease_duration;
    renewal_succeeded_after_expiry_ =
        (co_await control_.installer.ApplyLeaseGrantTransition(grant)).ok();
    revocations_ = control_.actions.expiration_authority_revocations_;
    expirations_after_ = GetClusterControlMetrics().lease_expirations_;
    result_ = absl::OkStatus();
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  bycorf::Server* server_;
  DynamicControl control_;
  bool prepared_ = false;
  bool old_timer_preserved_lease_ = false;
  bool expired_ = false;
  bool renewal_succeeded_after_expiry_ = false;
  bool expiry_preserved_source_capabilities_ = false;
  int revocations_ = 0;
  std::uint64_t expirations_before_ = 0;
  std::uint64_t expirations_after_ = 0;
  absl::Status result_ = absl::UnknownError("lease expiry service did not run");
};

// Blocks the sole worker across the first deadline so the ordinary timer
// cannot run before a renewal is delivered. This models a host resume or an
// event-loop stall without requiring a privileged suspend operation.
class DelayedLeaseExpiryRenewalService final : public bycorf::Service {
 public:
  explicit DelayedLeaseExpiryRenewalService(bycorf::Server* server)
      : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& /*worker*/,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ = absl::FailedPreconditionError(
          "delayed lease expiry test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status installed = control_.installer.InstallFullState(
            WithLease(FullState(MakeState()), 20ms), Basis(10));
        !installed.ok()) {
      result_ = installed;
      server_->RequestStop();
      co_return result_;
    }

    const auto state = control_.cache.Current();
    AuthorityMessage grant{
        .kind_ = AuthorityMessage::Kind::kLeaseGrant,
        .session_ = Session(1),
        .projection_ = Basis(10),
        .anchor_ = Anchor(*state),
        .sent_at_ = LeaseClockNow(),
        .granted_duration_ = 20ms,
    };
    result_ = co_await control_.installer.ApplyLeaseGrantTransition(grant);
    if (!result_.ok()) {
      server_->RequestStop();
      co_return result_;
    }

    constexpr std::array<std::uint16_t, 1> slots{12};
    old_admission_ =
        control_.guard.CaptureAndAdmit(WriteRequest(slots), LeaseClockNow());
    old_admitted_ = old_admission_->decision().kind_ == Decision::Kind::kServe;
    expirations_before_ = GetClusterControlMetrics().lease_expirations_;

    // Do not yield: the deadline passes while the timer remains queued.
    std::this_thread::sleep_for(40ms);
    grant.sent_at_ = LeaseClockNow();
    grant.granted_duration_ = 20ms;
    result_ = co_await control_.installer.ApplyLeaseGrantTransition(grant);
    if (result_.ok()) {
      old_rejected_after_renewal_ =
          control_.guard.Recheck(*old_admission_, LeaseClockNow()) ==
          RecheckResult::kReject;
      new_admission_serves_ =
          control_.guard.CaptureAndAdmit(WriteRequest(slots), LeaseClockNow())
              .decision()
              .kind_ == Decision::Kind::kServe;
    }
    expirations_after_ = GetClusterControlMetrics().lease_expirations_;
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  bycorf::Server* server_;
  DynamicControl control_;
  bool prepared_ = false;
  bool old_admitted_ = false;
  bool old_rejected_after_renewal_ = false;
  bool new_admission_serves_ = false;
  std::optional<AuthorityAdmission> old_admission_;
  std::uint64_t expirations_before_ = 0;
  std::uint64_t expirations_after_ = 0;
  absl::Status result_ =
      absl::UnknownError("delayed lease expiry renewal service did not run");
};

class FenceDirectiveAdmissionService final : public bycorf::Service {
 public:
  explicit FenceDirectiveAdmissionService(bycorf::Server* server)
      : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ = absl::FailedPreconditionError(
          "directive admission test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status installed = control_.installer.InstallFullState(
            FullState(MakeState(Assignment(1), 1, 1, 1,
                                /*granted=*/true,
                                /*population_ready=*/false)),
            Basis(10));
        !installed.ok()) {
      result_ = installed;
      server_->RequestStop();
      co_return result_;
    }

    const std::shared_ptr<const ServingState> state = control_.cache.Current();
    directive_ = NodeDirective{
        .projection_ = Basis(10),
        .anchor_ = Anchor(*state),
        .operation_id_ = ShortId<OperationId>(1),
        .directive_id_ = ShortId<DirectiveId>(2),
        .attempt_id_ = ShortId<AttemptId>(3),
        .directive_revision_ = 8,
        .kind_ = NodeDirective::Kind::kReplication,
        .target_node_id_ = *NodeId::Parse(kNodeA),
        .target_boot_id_ = *NodeId::Parse(kBoot),
        .source_node_id_ = *NodeId::Parse(kNodeB),
        .source_assignment_id_ = Assignment(1),
        .source_boot_id_ = *NodeId::Parse(kBoot),
        .source_replication_history_id_ = *NodeId::Parse(kNodeB),
        .source_host_ = "127.0.0.1",
        .source_port_ = 7001,
        .flow_count_ = 1,
        .manifest_revision_ = 1,
        .manifest_digest_ = Digest(4),
        .partition_replication_epoch_ = kPartitionReplicationEpoch,

    };
    control_.actions.block_directive_action_ = true;
    worker.Spawn(RunDirective());
    while (!control_.actions.directive_action_entered_) {
      co_await bycorf::Yield(worker);
    }
    worker.Spawn(ReleaseAfterFencePublication(worker));

    result_ = co_await control_.installer.ApplyFenceTransition(AuthorityMessage{
        .kind_ = AuthorityMessage::Kind::kFence,
        .session_ = Session(1),
        .projection_ = Basis(10),
        .anchor_ = Anchor(*state),
    });
    fence_returned_ = true;
    while (!directive_returned_) co_await bycorf::Yield(worker);
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  bycorf::Server* server_;
  DynamicControl control_;
  NodeDirective directive_;
  bool prepared_ = false;
  bool fence_returned_ = false;
  bool directive_returned_ = false;
  bool directive_started_ = false;
  bool fence_waited_for_action_ = false;
  bool saw_fenced_state_ = false;
  absl::Status directive_status_ =
      absl::UnknownError("directive admission did not return");
  absl::Status result_ =
      absl::UnknownError("fence admission service did not run");

 private:
  bycorf::Task<absl::Status> RunDirective() {
    NodeDirectiveCompletion completion =
        co_await control_.installer.StartDirective(directive_);
    directive_started_ = completion.started();
    directive_status_ = completion.result().value_or(
        absl::UnknownError("test directive completion was not terminal"));
    directive_returned_ = true;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ReleaseAfterFencePublication(
      bycorf::Worker& worker) {
    const GroupView* group = control_.cache.Current()->FindGroup("group-a");
    while (group != nullptr && group->granted_) {
      co_await bycorf::Yield(worker);
      group = control_.cache.Current()->FindGroup("group-a");
    }
    saw_fenced_state_ = group != nullptr && !group->granted_;
    fence_waited_for_action_ = !fence_returned_ &&
                               control_.actions.directives_.empty() &&
                               control_.actions.population_cancellations_ == 0;
    control_.actions.block_directive_action_ = false;
    co_return absl::OkStatus();
  }
};

class ReadinessAdmissionInvalidationService final : public bycorf::Service {
 public:
  explicit ReadinessAdmissionInvalidationService(bycorf::Server* server)
      : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ = absl::FailedPreconditionError(
          "readiness admission test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }
    const AssignmentId owner_assignment = Assignment(1);
    const AssignmentId replica_assignment = Assignment(2);
    if (absl::Status installed = control_.installer.InstallFullState(
            FullState(MakeReplicaState(owner_assignment, 1,
                                       /*population_ready=*/true),
                      replica_assignment),
            Basis(10));
        !installed.ok()) {
      result_ = installed;
      server_->RequestStop();
      co_return result_;
    }
    directive_ = NodeDirective{
        .projection_ = Basis(10),
        .anchor_ = {.group_id_ = "group-a",
                    .assignment_id_ = replica_assignment,
                    .group_term_ = 1},
        .operation_id_ = ShortId<OperationId>(1),
        .directive_id_ = ShortId<DirectiveId>(2),
        .attempt_id_ = ShortId<AttemptId>(3),
        .directive_revision_ = 8,
        .kind_ = NodeDirective::Kind::kReplication,
        .target_node_id_ = *NodeId::Parse(kNodeB),
        .target_boot_id_ = *NodeId::Parse(kBoot),
        .source_node_id_ = *NodeId::Parse(kNodeA),
        .source_assignment_id_ = owner_assignment,
        .source_boot_id_ = *NodeId::Parse(kBoot),
        .source_replication_history_id_ = *NodeId::Parse(kNodeA),
        .source_host_ = "127.0.0.1",
        .source_port_ = 7000,
        .flow_count_ = 1,
        .manifest_revision_ = 1,
        .manifest_digest_ = Digest(4),
        .partition_replication_epoch_ = kPartitionReplicationEpoch,

    };
    control_.actions.block_revocation_ = true;
    worker.Spawn(RunDirective());
    while (!control_.actions.revocation_entered_) {
      co_await bycorf::Yield(worker);
    }
    const GroupView* group = control_.cache.Current()->FindGroup("group-a");
    saw_unready_state_ = group != nullptr && !group->population_ready_;
    result_ = control_.installer.InvalidateSessionNow(Session(1));
    control_.actions.block_revocation_ = false;
    while (!directive_returned_) co_await bycorf::Yield(worker);
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  bycorf::Server* server_;
  DynamicControl control_;
  NodeDirective directive_;
  bool prepared_ = false;
  bool directive_returned_ = false;
  bool directive_started_ = false;
  bool saw_unready_state_ = false;
  absl::Status directive_status_ =
      absl::UnknownError("readiness admission did not return");
  absl::Status result_ =
      absl::UnknownError("readiness admission service did not run");

 private:
  bycorf::Task<absl::Status> RunDirective() {
    NodeDirectiveCompletion completion =
        co_await control_.installer.StartDirective(directive_);
    directive_started_ = completion.started();
    directive_status_ = completion.result().value_or(
        absl::UnknownError("test directive completion was not terminal"));
    directive_returned_ = true;
    co_return absl::OkStatus();
  }
};

class StorageLossAdmissionService final : public bycorf::Service {
 public:
  explicit StorageLossAdmissionService(bycorf::Server* server)
      : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ = absl::FailedPreconditionError(
          "storage loss admission test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status installed = control_.installer.InstallFullState(
            FullState(MakeState(Assignment(1), 1, 1, 1,
                                /*granted=*/true,
                                /*population_ready=*/false)),
            Basis(10));
        !installed.ok()) {
      result_ = installed;
      server_->RequestStop();
      co_return result_;
    }
    control_.actions.receives_directives_ = true;

    const std::shared_ptr<const ServingState> state = control_.cache.Current();
    directive_ = NodeDirective{
        .projection_ = Basis(10),
        .anchor_ = Anchor(*state),
        .operation_id_ = ShortId<OperationId>(1),
        .directive_id_ = ShortId<DirectiveId>(2),
        .attempt_id_ = ShortId<AttemptId>(3),
        .directive_revision_ = 8,
        .kind_ = NodeDirective::Kind::kReplication,
        .target_node_id_ = *NodeId::Parse(kNodeA),
        .target_boot_id_ = *NodeId::Parse(kBoot),
        .source_node_id_ = *NodeId::Parse(kNodeB),
        .source_assignment_id_ = Assignment(1),
        .source_boot_id_ = *NodeId::Parse(kBoot),
        .source_replication_history_id_ = *NodeId::Parse(kNodeB),
        .source_host_ = "127.0.0.1",
        .source_port_ = 7001,
        .flow_count_ = 1,
        .manifest_revision_ = 1,
        .manifest_digest_ = Digest(4),
        .partition_replication_epoch_ = kPartitionReplicationEpoch,

    };
    control_.actions.block_directive_action_ = true;
    worker.Spawn(RunDirective());
    while (!control_.actions.directive_action_entered_) {
      co_await bycorf::Yield(worker);
    }
    worker.Spawn(ReleaseAfterStorageLossPublication(worker));

    result_ = co_await control_.installer.LoseStorageReadinessTransition();
    transition_returned_ = true;
    while (!directive_returned_) co_await bycorf::Yield(worker);

    NodeDirectiveCompletion retry =
        co_await control_.installer.StartDirective(directive_);
    retry_started_ = retry.started();
    retry_status_ = retry.result().value_or(
        absl::UnknownError("storage loss retry did not terminate"));
    restore_status_ = control_.installer.SetStorageReady(true);
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  bycorf::Server* server_;
  DynamicControl control_;
  NodeDirective directive_;
  bool prepared_ = false;
  bool transition_returned_ = false;
  bool directive_returned_ = false;
  bool directive_started_ = false;
  bool retry_started_ = false;
  bool transition_waited_for_action_ = false;
  bool saw_storage_unready_ = false;
  absl::Status directive_status_ =
      absl::UnknownError("storage loss directive did not return");
  absl::Status retry_status_ =
      absl::UnknownError("storage loss retry did not run");
  absl::Status restore_status_ =
      absl::UnknownError("storage readiness restore did not run");
  absl::Status result_ =
      absl::UnknownError("storage loss admission service did not run");

 private:
  bycorf::Task<absl::Status> RunDirective() {
    NodeDirectiveCompletion completion =
        co_await control_.installer.StartDirective(directive_);
    directive_started_ = completion.started();
    directive_status_ = completion.result().value_or(
        absl::UnknownError("storage loss directive was not terminal"));
    directive_returned_ = true;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ReleaseAfterStorageLossPublication(
      bycorf::Worker& worker) {
    while (control_.installer.storage_ready()) {
      co_await bycorf::Yield(worker);
    }
    saw_storage_unready_ = true;
    transition_waited_for_action_ =
        !transition_returned_ && control_.actions.directives_.empty() &&
        control_.actions.population_shutdown_cancellations_ == 0;
    control_.actions.block_directive_action_ = false;
    co_return absl::OkStatus();
  }
};

class StorageLossControlTransitionService final : public bycorf::Service {
 public:
  explicit StorageLossControlTransitionService(bycorf::Server* server)
      : server_(server) {}

  void Prepare(unsigned thread_count) override {
    prepared_ = thread_count == 1;
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    if (!prepared_) {
      result_ = absl::FailedPreconditionError(
          "storage loss transition test requires one worker");
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status ready = control_.installer.SetStorageReady(true);
        !ready.ok()) {
      result_ = ready;
      server_->RequestStop();
      co_return result_;
    }
    if (absl::Status installed = control_.installer.InstallFullState(
            FullState(MakeState()), Basis(10));
        !installed.ok()) {
      result_ = installed;
      server_->RequestStop();
      co_return result_;
    }
    control_.actions.receives_directives_ = true;
    control_.actions.block_session_clear_ = true;
    worker.Spawn(RunFullState());
    while (!control_.actions.session_clear_entered_) {
      co_await bycorf::Yield(worker);
    }
    worker.Spawn(RunStorageLoss());
    while (control_.installer.storage_ready()) {
      co_await bycorf::Yield(worker);
    }
    storage_loss_waited_for_prior_transition_ =
        !storage_loss_returned_ &&
        control_.actions.population_shutdown_cancellations_ == 0;
    control_.actions.block_session_clear_ = false;
    while (!full_state_returned_ || !storage_loss_returned_) {
      co_await bycorf::Yield(worker);
    }
    result_ = storage_loss_result_;
    server_->RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  bycorf::Server* server_;
  DynamicControl control_;
  bool prepared_ = false;
  bool full_state_returned_ = false;
  bool storage_loss_returned_ = false;
  bool storage_loss_waited_for_prior_transition_ = false;
  absl::Status full_state_result_ =
      absl::UnknownError("full-state transition did not run");
  absl::Status storage_loss_result_ =
      absl::UnknownError("storage-loss transition did not run");
  absl::Status result_ =
      absl::UnknownError("storage-loss transition service did not run");

 private:
  bycorf::Task<absl::Status> RunFullState() {
    full_state_result_ = co_await control_.installer.InstallFullStateTransition(
        FullState(MakeState()), Basis(10));
    full_state_returned_ = true;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> RunStorageLoss() {
    storage_loss_result_ =
        co_await control_.installer.LoseStorageReadinessTransition();
    storage_loss_returned_ = true;
    co_return absl::OkStatus();
  }
};

TEST(NodeControlInstallerTest, FiniteAuthorityRequiresAnExactLeaseGrant) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());

  constexpr std::array<std::uint16_t, 1> slots{12};
  const MonotonicTime now{};
  EXPECT_EQ(
      control.guard.CaptureAndAdmit(WriteRequest(slots), now).decision().kind_,
      Decision::Kind::kClusterDownUnbound);

  const auto state = control.cache.Current();
  ASSERT_NE(state, nullptr);
  AuthorityMessage grant{
      .kind_ = AuthorityMessage::Kind::kLeaseGrant,
      .session_ = Session(1),
      .projection_ = Basis(10),
      .anchor_ = Anchor(*state),
      .sent_at_ = now,
      .granted_duration_ = 5s,
  };
  for (const std::uint64_t index : {9, 11}) {
    grant.projection_ = Basis(index);
    EXPECT_EQ(control.installer.ApplyAuthority(grant, now).code(),
              absl::StatusCode::kFailedPrecondition);
  }
  grant.projection_ = Basis(10);
  ASSERT_TRUE(control.installer.ApplyAuthority(grant, now).ok());

  const AuthorityAdmission admission =
      control.guard.CaptureAndAdmit(WriteRequest(slots), now + 1s);
  EXPECT_EQ(admission.decision().kind_, Decision::Kind::kServe);
  EXPECT_EQ(control.guard.Recheck(admission, now + 2s), RecheckResult::kOk);
}

TEST(NodeControlInstallerTest, SameIndexReinstallUpdatesLeaseCeiling) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(control.installer
                  .InstallFullState(WithDesiredControl(FullState(MakeState())),
                                    Basis(10))
                  .ok());
  const auto version = control.cache.version();
  auto reconnected = WithDesiredControl(FullState(MakeState()));
  reconnected.authority_lease_duration_ms_ = 1'000;
  ASSERT_TRUE(
      control.installer.InstallFullState(std::move(reconnected), Basis(10))
          .ok());
  EXPECT_EQ(control.cache.version(), version);
  AuthorityMessage grant{
      .kind_ = AuthorityMessage::Kind::kLeaseGrant,
      .session_ = Session(2),
      .projection_ = Basis(10),
      .anchor_ = Anchor(*control.cache.Current()),
      .sent_at_ = MonotonicTime{},
      .granted_duration_ = 2s,
  };
  EXPECT_EQ(control.installer.ApplyAuthority(grant, MonotonicTime{}).code(),
            absl::StatusCode::kFailedPrecondition);
  grant.granted_duration_ = 1s;
  EXPECT_TRUE(control.installer.ApplyAuthority(grant, MonotonicTime{}).ok());
}

TEST(NodeControlInstallerTest,
     ShorterFdsLeaseCeilingPreservesOldDeadlineAndBoundsRenewal) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(control.installer
                  .InstallFullState(WithDesiredControl(FullState(MakeState())),
                                    Basis(10))
                  .ok());

  const MonotonicTime now{};
  const auto state = control.cache.Current();
  AuthorityMessage grant{
      .kind_ = AuthorityMessage::Kind::kLeaseGrant,
      .session_ = Session(1),
      .projection_ = Basis(10),
      .anchor_ = Anchor(*state),
      .sent_at_ = now,
      .granted_duration_ = 5s,
  };
  ASSERT_TRUE(control.installer.ApplyAuthority(grant, now).ok());

  PreparedFullState replacement = WithDesiredControl(FullState(MakeState()));
  replacement.authority_lease_duration_ms_ = 1'000;
  ASSERT_TRUE(
      control.installer.InstallFullState(std::move(replacement), Basis(11))
          .ok());

  constexpr std::array<std::uint16_t, 1> slots{12};
  EXPECT_EQ(control.guard.CaptureAndAdmit(WriteRequest(slots), now + 2s)
                .decision()
                .kind_,
            Decision::Kind::kServe);

  grant.projection_ = Basis(11);
  grant.sent_at_ = now + 2s;
  grant.granted_duration_ = 2s;
  EXPECT_EQ(control.installer.ApplyAuthority(grant, now + 2s).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.guard.CaptureAndAdmit(WriteRequest(slots), now + 4s)
                .decision()
                .kind_,
            Decision::Kind::kServe);
  EXPECT_EQ(control.guard.CaptureAndAdmit(WriteRequest(slots), now + 6s)
                .decision()
                .kind_,
            Decision::Kind::kClusterDownUnbound);
}

TEST(NodeControlInstallerTest,
     SuspendAwareLeaseClockJumpRejectsOldAuthoritySynchronously) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  const MonotonicTime before_suspend{};
  const auto state = control.cache.Current();
  ASSERT_TRUE(control.installer
                  .ApplyAuthority(
                      AuthorityMessage{
                          .kind_ = AuthorityMessage::Kind::kLeaseGrant,
                          .session_ = Session(1),
                          .projection_ = Basis(10),
                          .anchor_ = Anchor(*state),
                          .sent_at_ = before_suspend,
                          .granted_duration_ = 5s,
                      },
                      before_suspend)
                  .ok());

  constexpr std::array<std::uint16_t, 1> slots{12};
  EXPECT_EQ(
      control.guard.CaptureAndAdmit(WriteRequest(slots), before_suspend + 1s)
          .decision()
          .kind_,
      Decision::Kind::kServe);
  // Production supplies this value from CLOCK_BOOTTIME. A host suspend can
  // jump it past the deadline even when Bycorf's CLOCK_MONOTONIC timer has not
  // resumed yet, so the request path itself must fail closed.
  EXPECT_EQ(
      control.guard.CaptureAndAdmit(WriteRequest(slots), before_suspend + 20s)
          .decision()
          .kind_,
      Decision::Kind::kClusterDownUnbound);
}

TEST(NodeControlInstallerTest,
     SameAuthorityRenewalExtendsDeadlineWithoutInvalidatingAdmission) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  const auto state = control.cache.Current();
  AuthorityMessage grant{
      .kind_ = AuthorityMessage::Kind::kLeaseGrant,
      .session_ = Session(1),
      .projection_ = Basis(10),
      .anchor_ = Anchor(*state),
      .sent_at_ = MonotonicTime{},
      .granted_duration_ = 5s,
  };
  ASSERT_TRUE(control.installer.ApplyAuthority(grant, MonotonicTime{}).ok());

  constexpr std::array<std::uint16_t, 1> slots{12};
  const AuthorityAdmission before =
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 1s);
  grant.sent_at_ = MonotonicTime{} + 4s;
  grant.granted_duration_ = 5s;
  ASSERT_TRUE(
      control.installer.ApplyAuthority(grant, MonotonicTime{} + 4s).ok());

  EXPECT_EQ(control.guard.Recheck(before, MonotonicTime{} + 8s),
            RecheckResult::kOk);
  EXPECT_EQ(control.guard.Recheck(before, MonotonicTime{} + 9s),
            RecheckResult::kReject);
}

TEST(NodeControlInstallerTest,
     PopulationProofLossInvalidatesLeaseAndRevokesOnlyOnTransition) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  const std::shared_ptr<const ServingState> state = control.cache.Current();
  const AuthorityAnchor anchor = Anchor(*state);
  ASSERT_TRUE(control.installer
                  .ApplyAuthority(
                      {
                          .kind_ = AuthorityMessage::Kind::kLeaseGrant,
                          .session_ = Session(1),
                          .projection_ = Basis(10),
                          .anchor_ = anchor,
                          .sent_at_ = MonotonicTime{},
                          .granted_duration_ = 5s,
                      },
                      MonotonicTime{})
                  .ok());
  constexpr std::array<std::uint16_t, 1> slots{12};
  EXPECT_EQ(
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 1s)
          .decision()
          .kind_,
      Decision::Kind::kServe);

  ASSERT_TRUE(RunTaskSync(control.installer.SetPopulationReadinessTransition(
                              std::nullopt))
                  .ok());
  EXPECT_FALSE(
      control.cache.Current()->FindGroup("group-a")->population_ready_);
  EXPECT_EQ(
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 2s)
          .decision()
          .kind_,
      Decision::Kind::kLoading);
  EXPECT_EQ(control.actions.async_revocations_, 1);
  EXPECT_EQ(control.actions.expiration_authority_revocations_, 1);

  // Repeated NOT_READY observations are no-ops, while only the exact current
  // boot-local population identity may make the assignment ready again.
  ASSERT_TRUE(RunTaskSync(control.installer.SetPopulationReadinessTransition(
                              std::nullopt))
                  .ok());
  EXPECT_EQ(control.actions.async_revocations_, 1);
  EXPECT_EQ(control.actions.expiration_authority_revocations_, 1);
  EXPECT_TRUE(RunTaskSync(control.installer.SetPopulationReadinessTransition(
                              PopulationReadiness{
                                  .group_id_ = "group-a",
                                  .assignment_id_ = anchor.assignment_id_,
                                  .group_term_ = anchor.group_term_,
                                  .manifest_revision_ = 1,
                                  .manifest_digest_ = Digest(4),
                                  .partition_replication_epoch_ =
                                      kPartitionReplicationEpoch,
                              }))
                  .ok());
  EXPECT_TRUE(control.cache.Current()->FindGroup("group-a")->population_ready_);
}

TEST(NodeControlInstallerTest, CountsOneLocalExpirationPerExpiredLease) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer
          .InstallFullState(WithLease(FullState(MakeState()), 1s), Basis(10))
          .ok());
  const auto state = control.cache.Current();
  ASSERT_TRUE(control.installer
                  .ApplyAuthority(
                      {
                          .kind_ = AuthorityMessage::Kind::kLeaseGrant,
                          .session_ = Session(1),
                          .projection_ = Basis(10),
                          .anchor_ = Anchor(*state),
                          .sent_at_ = MonotonicTime{},
                          .granted_duration_ = 1s,
                      },
                      MonotonicTime{})
                  .ok());
  constexpr std::array<std::uint16_t, 1> slots{12};
  const std::uint64_t before = GetClusterControlMetrics().lease_expirations_;
  EXPECT_EQ(
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 2s)
          .decision()
          .kind_,
      Decision::Kind::kClusterDownUnbound);
  (void)control.guard.CaptureAndAdmit(WriteRequest(slots),
                                      MonotonicTime{} + 3s);
  EXPECT_EQ(GetClusterControlMetrics().lease_expirations_, before + 1);
}

TEST(NodeControlInstallerTest,
     SessionLossInvalidatesWithoutPublishingTopology) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer
          .InstallFullState(WithLease(FullState(MakeState()), 10s), Basis(10))
          .ok());
  const auto state = control.cache.Current();
  const SessionIdentity session = Session(1);
  ASSERT_TRUE(control.installer
                  .ApplyAuthority({.kind_ = AuthorityMessage::Kind::kLeaseGrant,
                                   .session_ = session,
                                   .projection_ = Basis(10),
                                   .anchor_ = Anchor(*state),
                                   .sent_at_ = MonotonicTime{},
                                   .granted_duration_ = 10s},
                                  MonotonicTime{})
                  .ok());
  constexpr std::array<std::uint16_t, 1> slots{12};
  const AuthorityAdmission admission =
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 1s);
  const std::uint64_t version = control.cache.version();

  EXPECT_TRUE(control.installer.LoseSession(session, "transport closed").ok());
  EXPECT_EQ(control.cache.version(), version);
  EXPECT_EQ(control.guard.Recheck(admission, MonotonicTime{} + 2s),
            RecheckResult::kReject);
  EXPECT_EQ(control.actions.revocations_, 1);
}

TEST(NodeControlInstallerTest,
     FenceInvalidatesBeforePublishingAndRejectsOldAuthorityForThisBoot) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer
          .InstallFullState(WithLease(FullState(MakeState()), 10s), Basis(10))
          .ok());
  const auto active = control.cache.Current();
  AuthorityMessage grant{
      .kind_ = AuthorityMessage::Kind::kLeaseGrant,
      .session_ = Session(1),
      .projection_ = Basis(10),
      .anchor_ = Anchor(*active),
      .sent_at_ = MonotonicTime{},
      .granted_duration_ = 10s,
  };
  ASSERT_TRUE(control.installer.ApplyAuthority(grant, MonotonicTime{}).ok());
  constexpr std::array<std::uint16_t, 1> slots{12};
  const AuthorityAdmission admission =
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 1s);

  AuthorityMessage fence = grant;
  fence.kind_ = AuthorityMessage::Kind::kFence;
  ASSERT_TRUE(control.installer.ApplyAuthority(fence, MonotonicTime{}).ok());

  EXPECT_EQ(control.guard.Recheck(admission, MonotonicTime{} + 2s),
            RecheckResult::kReject);
  const GroupView* fenced = control.cache.Current()->FindGroup("group-a");
  ASSERT_NE(fenced, nullptr);
  EXPECT_FALSE(fenced->granted_);
  EXPECT_EQ(control.actions.revocations_, 1);
  ASSERT_EQ(control.actions.drained_.size(), 1U);
  EXPECT_EQ(control.actions.drained_.front(), fence.anchor_);
  EXPECT_EQ(control.installer.ApplyAuthority(grant, MonotonicTime{}).code(),
            absl::StatusCode::kFailedPrecondition);
}

TEST(NodeControlInstallerTest,
     AsyncFencePublishesBeforeRevocationAndCompletesAfterOldWorkDrains) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  const std::shared_ptr<const ServingState> old = control.cache.Current();
  ASSERT_NE(old, nullptr);
  GroupInFlight* cell = old->InFlightCellForSlot(12);
  ASSERT_NE(cell, nullptr);
  std::optional<InFlightGuard> in_flight;
  in_flight.emplace(*cell, 0);

  AuthorityMessage fence{
      .kind_ = AuthorityMessage::Kind::kFence,
      .session_ = Session(1),
      .projection_ = Basis(10),
      .anchor_ = Anchor(*old),
  };
  bool observed_fenced_before_revocation = false;
  control.actions.on_async_revocation_ = [&] {
    const GroupView* group = control.cache.Current()->FindGroup("group-a");
    observed_fenced_before_revocation = group != nullptr && !group->granted_;
    EXPECT_EQ(old->GroupInFlightCount("group-a"), 1U);
    in_flight.reset();
  };

  EXPECT_TRUE(RunTaskSync(control.installer.ApplyFenceTransition(fence)).ok());
  EXPECT_TRUE(observed_fenced_before_revocation);
  EXPECT_EQ(old->GroupInFlightCount("group-a"), 0U);
  EXPECT_EQ(control.actions.async_revocations_, 1);
  EXPECT_EQ(control.actions.revocations_, 0);
  EXPECT_EQ(control.actions.expiration_authority_revocations_, 1);
}

TEST(NodeControlInstallerTest,
     AsyncFenceSuspendsWorkerCoroutineUntilRetiredCounterReachesZero) {
  bycorf::Server server;
  FenceDrainService service(&server);
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_TRUE(service.result().ok()) << service.result();
  EXPECT_TRUE(service.release_ran_while_waiting());
  EXPECT_TRUE(service.saw_fenced_state());
}

TEST(NodeControlInstallerTest,
     FenceWaitsForSuspendedDirectiveAdmissionBeforeFinalCancellation) {
  bycorf::Server server;
  FenceDirectiveAdmissionService service(&server);
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_TRUE(service.result_.ok()) << service.result_;
  EXPECT_TRUE(service.saw_fenced_state_);
  EXPECT_TRUE(service.fence_waited_for_action_);
  EXPECT_TRUE(service.directive_started_);
  EXPECT_TRUE(service.directive_status_.ok()) << service.directive_status_;
  EXPECT_EQ(service.control_.actions.async_revocations_, 1);
  EXPECT_EQ(service.control_.actions.expiration_authority_revocations_, 1);
  EXPECT_EQ(service.control_.actions.population_cancellations_, 1);
  EXPECT_EQ(service.control_.actions.population_cancellation_preserve_follow_,
            (std::vector<bool>{false}));
  const std::vector<std::string> expected_events{"start", "cancel"};
  EXPECT_EQ(service.control_.actions.population_events_, expected_events);
}

TEST(NodeControlInstallerTest,
     SessionInvalidationDuringReadinessAwaitPreventsDirectiveAction) {
  bycorf::Server server;
  ReadinessAdmissionInvalidationService service(&server);
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_TRUE(service.result_.ok()) << service.result_;
  EXPECT_TRUE(service.saw_unready_state_);
  EXPECT_FALSE(service.directive_started_);
  EXPECT_EQ(service.directive_status_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(service.control_.actions.directives_.empty());
}

TEST(NodeControlInstallerTest,
     StorageLossWaitsForAdmissionCancelsTargetAndLatchesFailure) {
  bycorf::Server server;
  StorageLossAdmissionService service(&server);
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_TRUE(service.result_.ok()) << service.result_;
  EXPECT_TRUE(service.saw_storage_unready_);
  EXPECT_TRUE(service.transition_waited_for_action_);
  EXPECT_TRUE(service.directive_started_);
  EXPECT_TRUE(service.directive_status_.ok()) << service.directive_status_;
  EXPECT_FALSE(service.retry_started_);
  EXPECT_EQ(service.retry_status_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(service.restore_status_.code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(service.control_.installer.storage_ready());
  EXPECT_EQ(service.control_.actions.async_revocations_, 1);
  EXPECT_EQ(service.control_.actions.expiration_authority_revocations_, 1);
  EXPECT_EQ(service.control_.actions.population_shutdown_cancellations_, 1);
  const std::vector<std::string> expected_events{"start", "shutdown-cancel"};
  EXPECT_EQ(service.control_.actions.population_events_, expected_events);
}

TEST(NodeControlInstallerTest,
     StorageLossJoinsEarlierControlTransitionBeforeFinalCleanup) {
  bycorf::Server server;
  StorageLossControlTransitionService service(&server);
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_TRUE(service.full_state_result_.ok()) << service.full_state_result_;
  EXPECT_TRUE(service.result_.ok()) << service.result_;
  EXPECT_TRUE(service.storage_loss_waited_for_prior_transition_);
  EXPECT_TRUE(service.control_.actions.session_clear_exited_);
  EXPECT_FALSE(service.control_.actions.desired_population_.has_value());
  EXPECT_FALSE(service.control_.actions.population_transition_expected_);
  EXPECT_EQ(service.control_.actions.async_revocations_, 1);
  EXPECT_EQ(service.control_.actions.population_shutdown_cancellations_, 1);
}

TEST(NodeControlInstallerTest, StorageLossRetainsAnUncertainCleanupFailure) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  control.actions.revoke_status_ =
      absl::InternalError("injected uncertain source cleanup");

  const absl::Status first =
      RunTaskSync(control.installer.LoseStorageReadinessTransition());
  const absl::Status replay =
      RunTaskSync(control.installer.LoseStorageReadinessTransition());

  EXPECT_EQ(first.code(), absl::StatusCode::kInternal);
  EXPECT_EQ(replay, first);
  EXPECT_EQ(control.actions.async_revocations_, 1);
  EXPECT_EQ(control.actions.population_shutdown_cancellations_, 1);
  EXPECT_FALSE(control.installer.storage_ready());
}

TEST(NodeControlInstallerTest,
     LeaseTimerExpiresExactLeaseButCannotRevokeItsRenewal) {
  bycorf::Server server;
  LeaseExpiryService service(&server);
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_TRUE(service.result_.ok()) << service.result_;
  EXPECT_TRUE(service.old_timer_preserved_lease_);
  EXPECT_TRUE(service.expired_);
  EXPECT_TRUE(service.renewal_succeeded_after_expiry_);
  EXPECT_TRUE(service.expiry_preserved_source_capabilities_);
  EXPECT_EQ(service.revocations_, 1);
  EXPECT_EQ(service.expirations_after_, service.expirations_before_ + 1);
}

TEST(NodeControlInstallerTest,
     ExpiredLeaseCannotBeRevivedBeforeDelayedTimerRuns) {
  bycorf::Server server;
  DelayedLeaseExpiryRenewalService service(&server);
  server.AddService(&service);
  bycorf::ServerOptions options;
  options.thread_count_ = 1;
  options.pin_workers_ = false;
  options.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(options).ok());
  server.WaitUntilStopped();

  EXPECT_TRUE(service.result_.ok()) << service.result_;
  EXPECT_TRUE(service.old_admitted_);
  EXPECT_TRUE(service.old_rejected_after_renewal_);
  EXPECT_TRUE(service.new_admission_serves_);
  EXPECT_EQ(service.control_.actions.session_clears_, 0);
  EXPECT_TRUE(service.control_.actions.preserve_established_exports_.empty());
  EXPECT_EQ(service.control_.actions.expiration_authority_revocations_, 1);
  EXPECT_EQ(service.control_.actions.source_admission_enables_, 2);
  EXPECT_EQ(service.control_.actions.drained_.size(), 1u);
  EXPECT_EQ(service.expirations_after_, service.expirations_before_ + 1);
}

TEST(NodeControlInstallerTest,
     FullStateReplayIsIdempotentAndOlderIndexIsRejected) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  const std::uint64_t version = control.cache.version();
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  EXPECT_EQ(control.cache.version(), version);

  EXPECT_EQ(control.installer.InstallFullState(FullState(MakeState()), Basis(9))
                .code(),
            absl::StatusCode::kOutOfRange);
  EXPECT_EQ(control.actions.revocations_, 0);
}

TEST(NodeControlInstallerTest,
     AsyncOnlyAdapterRejectsEvenAnEmptySynchronousFullState) {
  DynamicControl control;
  control.actions.receives_directives_ = true;
  PreparedFullState empty_control_state{
      .serving_state_ = MakeState(),
  };

  EXPECT_EQ(control.installer
                .InstallFullState(std::move(empty_control_state), Basis(10))
                .code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.cache.Current(), nullptr);
}

TEST(NodeControlInstallerTest,
     DirectiveCapableAdapterRejectsSynchronousFenceAndSessionCleanup) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer
          .InstallFullState(WithLease(FullState(MakeState()), 10s), Basis(10))
          .ok());
  const std::shared_ptr<const ServingState> state = control.cache.Current();
  ASSERT_NE(state, nullptr);
  const SessionIdentity session = Session(1);
  const AuthorityMessage grant{
      .kind_ = AuthorityMessage::Kind::kLeaseGrant,
      .session_ = session,
      .projection_ = Basis(10),
      .anchor_ = Anchor(*state),
      .sent_at_ = MonotonicTime{},
      .granted_duration_ = 10s,
  };
  ASSERT_TRUE(control.installer.ApplyAuthority(grant, MonotonicTime{}).ok());
  control.actions.receives_directives_ = true;

  AuthorityMessage fence = grant;
  fence.kind_ = AuthorityMessage::Kind::kFence;
  EXPECT_EQ(control.installer.ApplyAuthority(fence, MonotonicTime{}).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(control.cache.Current()->FindGroup("group-a")->granted_);

  constexpr std::array<std::uint16_t, 1> slots{12};
  const AuthorityAdmission admission =
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 1s);
  ASSERT_EQ(admission.decision().kind_, Decision::Kind::kServe);
  EXPECT_EQ(control.installer.LoseSession(session, "transport closed").code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.guard.Recheck(admission, MonotonicTime{} + 2s),
            RecheckResult::kReject);
  EXPECT_EQ(control.actions.revocations_, 0);
  EXPECT_TRUE(control.actions.drained_.empty());
}

TEST(NodeControlInstallerTest, FullStateRejectsIndexAndDomainRegressions) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());

  EXPECT_EQ(control.installer.InstallFullState(FullState(MakeState()), Basis(9))
                .code(),
            absl::StatusCode::kOutOfRange);
  EXPECT_EQ(control.installer
                .InstallFullState(FullState(MakeState(Assignment(1), 2, 0)),
                                  Basis(11))
                .code(),
            absl::StatusCode::kFailedPrecondition);

  PreparedFullState regressed_population_epoch =
      FullState(MakeState(Assignment(1), 2));
  --regressed_population_epoch.desired_cluster_controls_[0]
        .identity_.partition_replication_epoch_;
  EXPECT_EQ(
      control.installer
          .InstallFullState(std::move(regressed_population_epoch), Basis(11))
          .code(),
      absl::StatusCode::kFailedPrecondition);

  // A fresh assignment is a new incarnation; its term may restart.
  EXPECT_TRUE(control.installer
                  .InstallFullState(FullState(MakeState(Assignment(2), 2, 0)),
                                    Basis(12))
                  .ok());
}

TEST(NodeControlInstallerTest,
     OwnerlessGroupRejectsSameMembershipControlCounterRegression) {
  DynamicControl control;
  PreparedFullState ownerless{
      .serving_state_ = MakeOwnerlessState(),
      .authority_lease_duration_ms_ = 5000,
      .desired_cluster_controls_ =
          {{.identity_ = {.group_id_ = "group-a",
                          .group_term_ = 5,
                          .manifest_revision_ = 5,
                          .manifest_digest_ = Digest(4),
                          .partition_replication_epoch_ = 5,
                          .members_ = {{.node_id_ = *NodeId::Parse(kNodeA),
                                        .assignment_id_ = Assignment(1)},
                                       {.node_id_ = *NodeId::Parse(kNodeB),
                                        .assignment_id_ = Assignment(2)}}}}},
  };
  ASSERT_TRUE(control.installer.InstallFullState(ownerless, Basis(10)).ok());

  const auto expect_rejected = [&](PreparedFullState candidate) {
    EXPECT_EQ(
        control.installer.InstallFullState(std::move(candidate), Basis(11))
            .code(),
        absl::StatusCode::kFailedPrecondition);
  };
  PreparedFullState candidate = ownerless;
  candidate.desired_cluster_controls_[0].identity_.group_term_ = 4;
  expect_rejected(std::move(candidate));
  candidate = ownerless;
  candidate.desired_cluster_controls_[0].identity_.manifest_revision_ = 4;
  expect_rejected(std::move(candidate));
  candidate = ownerless;
  candidate.desired_cluster_controls_[0]
      .identity_.partition_replication_epoch_ = 4;
  expect_rejected(std::move(candidate));

  candidate = ownerless;
  candidate.desired_cluster_controls_[0].identity_.manifest_digest_ = Digest(9);
  EXPECT_EQ(control.installer.InstallFullState(std::move(candidate), Basis(11))
                .code(),
            absl::StatusCode::kDataLoss);
}

TEST(NodeControlInstallerTest,
     StorageReadinessIsLocalAndRepublishedAtomically) {
  DynamicControl control;
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  ASSERT_NE(control.cache.Current(), nullptr);
  EXPECT_FALSE(control.cache.Current()->FindGroup("group-a")->storage_ready_);
  EXPECT_FALSE(control.installer.storage_ready());

  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  EXPECT_TRUE(control.cache.Current()->FindGroup("group-a")->storage_ready_);
  EXPECT_TRUE(control.installer.storage_ready());
  const std::uint64_t ready_version = control.cache.version();
  EXPECT_TRUE(control.installer.SetStorageReady(true).ok());
  EXPECT_EQ(control.cache.version(), ready_version);
  control.actions.receives_directives_ = true;
  EXPECT_EQ(control.installer.SetStorageReady(false).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(control.installer.storage_ready());
  EXPECT_EQ(control.cache.version(), ready_version);
}

TEST(NodeControlInstallerTest,
     NewAuthorityWaitsForRetiredInFlightMutationsToDrain) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  const std::shared_ptr<const ServingState> old = control.cache.Current();
  ASSERT_NE(old, nullptr);
  GroupInFlight* cell = old->InFlightCellForSlot(12);
  ASSERT_NE(cell, nullptr);
  std::optional<InFlightGuard> in_flight;
  in_flight.emplace(*cell, 0);

  ASSERT_TRUE(control.installer
                  .InstallFullState(FullState(MakeState(Assignment(1), 2, 2)),
                                    Basis(11))
                  .ok());
  const std::shared_ptr<const ServingState> current = control.cache.Current();
  AuthorityMessage grant{
      .kind_ = AuthorityMessage::Kind::kLeaseGrant,
      .session_ = Session(1),
      .projection_ = Basis(11),
      .anchor_ = Anchor(*current),
      .sent_at_ = MonotonicTime{},
      .granted_duration_ = 5s,
  };
  EXPECT_EQ(control.installer.ApplyAuthority(grant, MonotonicTime{}).code(),
            absl::StatusCode::kUnavailable);

  in_flight.reset();
  EXPECT_TRUE(control.installer.ApplyAuthority(grant, MonotonicTime{}).ok());
}

TEST(NodeControlInstallerTest,
     ExactFdsRefreshPreservesSourceExportsAndAuthorityChangeDrainsOldWork) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              FullState(MakeState()), Basis(10)))
                  .ok());
  EXPECT_EQ(control.actions.session_clears_, 1);
  ASSERT_EQ(control.actions.preserve_established_exports_.size(), 1U);
  EXPECT_FALSE(control.actions.preserve_established_exports_.back());
  EXPECT_EQ(control.actions.population_reconciliations_, 1);
  ASSERT_TRUE(control.actions.desired_population_.has_value());
  EXPECT_EQ(control.actions.desired_population_->group_id_, "group-a");
  EXPECT_EQ(control.actions.desired_population_->assignment_id_, Assignment(1));
  EXPECT_EQ(control.actions.desired_population_->partition_replication_epoch_,
            kPartitionReplicationEpoch);
  EXPECT_FALSE(control.actions.population_transition_expected_);

  // An exact FDS refresh preserves already-published source exports while
  // clearing the ledger and reserving every capability expected to replay.
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              FullState(MakeState()), Basis(10),
                              /*local_population_transition_expected=*/true,
                              /*expected_source_authorization_replays=*/2))
                  .ok());
  EXPECT_EQ(control.actions.session_clears_, 2);
  ASSERT_EQ(control.actions.preserve_established_exports_.size(), 2U);
  EXPECT_TRUE(control.actions.preserve_established_exports_.back());
  ASSERT_EQ(control.actions.expected_authorization_replays_.size(), 2U);
  EXPECT_EQ(control.actions.expected_authorization_replays_.back(), 2U);
  EXPECT_TRUE(control.actions.population_transition_expected_);

  // A replacement FDS omits the boot-local ReadyToken. The installer carries
  // an already verified bit across an exact population anchor, and the same
  // replacement must not tear down its already-online export.
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              FullState(MakeState(Assignment(1), 1, 1, 1,
                                                  /*granted=*/true,
                                                  /*population_ready=*/false)),
                              Basis(11)))
                  .ok());
  EXPECT_TRUE(control.cache.Current()->FindGroup("group-a")->population_ready_);
  EXPECT_EQ(control.actions.session_clears_, 3);
  ASSERT_EQ(control.actions.preserve_established_exports_.size(), 3U);
  EXPECT_TRUE(control.actions.preserve_established_exports_.back());

  const std::shared_ptr<const ServingState> old = control.cache.Current();
  ASSERT_NE(old, nullptr);
  GroupInFlight* cell = old->InFlightCellForSlot(12);
  ASSERT_NE(cell, nullptr);
  std::optional<InFlightGuard> in_flight;
  in_flight.emplace(*cell, 0);
  control.actions.on_async_revocation_ = [&] { in_flight.reset(); };
  EXPECT_TRUE(
      RunTaskSync(control.installer.InstallFullStateTransition(
                      FullState(MakeState(Assignment(1), 2, 2)), Basis(12)))
          .ok());
  EXPECT_EQ(control.actions.session_clears_, 4);
  ASSERT_EQ(control.actions.preserve_established_exports_.size(), 4U);
  EXPECT_TRUE(control.actions.preserve_established_exports_.back());
  EXPECT_EQ(old->GroupInFlightCount("group-a"), 0U);
}

TEST(NodeControlInstallerTest,
     GrantlessFullStateRetainsMemberPopulationIdentityAndAcceptsProof) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  PreparedFullState ownerless{
      .serving_state_ = MakeOwnerlessState(),
      .authority_lease_duration_ms_ = 5000,
      .desired_cluster_controls_ =
          {{.identity_ = {.group_id_ = "group-a",
                          .group_term_ = 2,
                          .manifest_revision_ = 1,
                          .manifest_digest_ = Digest(4),
                          .partition_replication_epoch_ =
                              kPartitionReplicationEpoch,
                          .members_ = {{.node_id_ = *NodeId::Parse(kNodeA),
                                        .assignment_id_ = Assignment(1)},
                                       {.node_id_ = *NodeId::Parse(kNodeB),
                                        .assignment_id_ = Assignment(2)}}}}},
  };
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              std::move(ownerless), Basis(11)))
                  .ok());
  EXPECT_TRUE(control.cache.Current()->Groups().empty());
  ASSERT_TRUE(control.actions.desired_population_.has_value());
  EXPECT_EQ(control.actions.desired_population_->group_id_, "group-a");
  EXPECT_EQ(control.actions.desired_population_->assignment_id_, Assignment(2));
  EXPECT_EQ(control.actions.desired_population_->group_term_, 2U);
  EXPECT_EQ(control.actions.desired_population_->partition_replication_epoch_,
            kPartitionReplicationEpoch);

  PopulationReadiness proof{
      .group_id_ = "group-a",
      .assignment_id_ = Assignment(2),
      .group_term_ = 2,
      .manifest_revision_ = 1,
      .manifest_digest_ = Digest(4),
      .partition_replication_epoch_ = kPartitionReplicationEpoch,
  };
  EXPECT_TRUE(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(proof))
          .ok());
  proof.group_term_ = 1;
  EXPECT_EQ(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(proof))
          .code(),
      absl::StatusCode::kFailedPrecondition);
}

TEST(NodeControlInstallerTest,
     SessionLossTracksOldWorkAndBlocksSourceAuthorizationUntilDrain) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              FullState(MakeState()), Basis(10)))
                  .ok());
  const std::shared_ptr<const ServingState> current = control.cache.Current();
  ASSERT_NE(current, nullptr);
  GroupInFlight* cell = current->InFlightCellForSlot(12);
  ASSERT_NE(cell, nullptr);
  std::optional<InFlightGuard> in_flight;
  in_flight.emplace(*cell, 0);
  ASSERT_TRUE(RunTaskSync(control.installer.LoseSessionTransition(
                              Session(1), "control transport closed"))
                  .ok());
  EXPECT_EQ(control.actions.population_cancellations_, 1);
  EXPECT_EQ(control.actions.population_cancellation_preserve_follow_,
            (std::vector<bool>{true}));

  NodeDirective authorize{
      .projection_ = Basis(10),
      .anchor_ = Anchor(*current),
      .operation_id_ = ShortId<OperationId>(1),
      .directive_id_ = ShortId<DirectiveId>(2),
      .attempt_id_ = ShortId<AttemptId>(3),
      .directive_revision_ = 8,
      .kind_ = NodeDirective::Kind::kAuthorizeSource,
      .target_node_id_ = *NodeId::Parse(kNodeB),
      .target_boot_id_ = *NodeId::Parse(kBoot),
      .source_node_id_ = *NodeId::Parse(kNodeA),
      .source_assignment_id_ = Assignment(1),
      .source_boot_id_ = *NodeId::Parse(kBoot),
      .source_replication_history_id_ = *NodeId::Parse(kNodeA),
      .source_host_ = "127.0.0.1",
      .source_port_ = 7001,
      .flow_count_ = 1,
      .manifest_revision_ = 1,
      .manifest_digest_ = Digest(4),
      .partition_replication_epoch_ = kPartitionReplicationEpoch,
  };
  EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(authorize)).code(),
            absl::StatusCode::kUnavailable);
  EXPECT_TRUE(control.actions.directives_.empty());

  in_flight.reset();
  EXPECT_TRUE(RunTaskSync(control.installer.ApplyDirective(authorize)).ok());
  ASSERT_EQ(control.actions.directives_.size(), 1U);
  EXPECT_EQ(control.actions.session_clears_, 2);
  ASSERT_EQ(control.actions.preserve_established_exports_.size(), 2U);
  EXPECT_TRUE(control.actions.preserve_established_exports_.back());
}

TEST(NodeControlInstallerTest,
     SessionInvalidationClosesLeaseBeforeAsynchronousCleanup) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer
          .InstallFullState(WithLease(FullState(MakeState()), 1h), Basis(10))
          .ok());
  const SessionIdentity session = Session(1);
  const std::shared_ptr<const ServingState> current = control.cache.Current();
  ASSERT_NE(current, nullptr);
  ASSERT_TRUE(control.installer
                  .ApplyAuthority(
                      AuthorityMessage{
                          .kind_ = AuthorityMessage::Kind::kLeaseGrant,
                          .session_ = session,
                          .projection_ = Basis(10),
                          .anchor_ = Anchor(*current),
                          .sent_at_ = LeaseClockNow(),
                          .granted_duration_ = 1h,
                      },
                      LeaseClockNow())
                  .ok());
  constexpr std::array<std::uint16_t, 1> slots{12};
  EXPECT_EQ(control.guard.CaptureAndAdmit(WriteRequest(slots), LeaseClockNow())
                .decision()
                .kind_,
            Decision::Kind::kServe);

  ASSERT_TRUE(control.installer.InvalidateSessionNow(session).ok());
  EXPECT_NE(control.guard.CaptureAndAdmit(WriteRequest(slots), LeaseClockNow())
                .decision()
                .kind_,
            Decision::Kind::kServe);
  EXPECT_EQ(control.actions.session_clears_, 0);
  EXPECT_EQ(control.actions.population_cancellations_, 0);
}

TEST(NodeControlInstallerTest,
     ShutdownPrejoinInvalidatesLeaseAndUsesDedicatedActionSeam) {
  DynamicControl control;
  control.actions.receives_directives_ = true;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              FullState(MakeState()), Basis(10)))
                  .ok());
  const std::shared_ptr<const ServingState> state = control.cache.Current();
  ASSERT_NE(state, nullptr);
  ASSERT_TRUE(control.installer
                  .ApplyAuthority(
                      {
                          .kind_ = AuthorityMessage::Kind::kLeaseGrant,
                          .session_ = Session(1),
                          .projection_ = Basis(10),
                          .anchor_ = Anchor(*state),
                          .sent_at_ = MonotonicTime{},
                          .granted_duration_ = 5s,
                      },
                      MonotonicTime{})
                  .ok());
  constexpr std::array<std::uint16_t, 1> slots{12};
  const AuthorityAdmission admitted =
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 1s);
  ASSERT_EQ(admitted.decision().kind_, Decision::Kind::kServe);

  EXPECT_TRUE(
      RunTaskSync(control.installer.CancelPopulationForShutdownTransition())
          .ok());
  EXPECT_EQ(control.actions.population_shutdown_cancellations_, 1);
  EXPECT_EQ(control.actions.population_cancellations_, 0);
  EXPECT_EQ(control.actions.expiration_authority_revocations_, 1);
  EXPECT_EQ(control.guard.Recheck(admitted, MonotonicTime{} + 2s),
            RecheckResult::kReject);
}

TEST(NodeControlInstallerTest, DirectiveRequiresCurrentProjectionAndAuthority) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer
          .InstallFullState(FullState(MakeState(Assignment(1), 1, 1, 1,
                                                /*granted=*/true,
                                                /*population_ready=*/false)),
                            Basis(10))
          .ok());
  const AuthorityAnchor anchor = Anchor(*control.cache.Current());
  NodeDirective directive{
      .projection_ = Basis(10),
      .anchor_ = anchor,
      .operation_id_ = ShortId<OperationId>(1),
      .directive_id_ = ShortId<DirectiveId>(2),
      .attempt_id_ = ShortId<AttemptId>(3),
      .directive_revision_ = 8,
      .kind_ = NodeDirective::Kind::kReplication,
      .target_node_id_ = *NodeId::Parse(kNodeA),
      .target_boot_id_ = *NodeId::Parse(kBoot),
      .source_node_id_ = *NodeId::Parse(kNodeB),
      .source_assignment_id_ = Assignment(1),
      .source_boot_id_ = *NodeId::Parse(kBoot),
      .source_replication_history_id_ = *NodeId::Parse(kNodeB),
      .source_host_ = "127.0.0.1",
      .source_port_ = 7001,
      .flow_count_ = 1,
      .manifest_revision_ = 1,
      .manifest_digest_ = Digest(4),
      .partition_replication_epoch_ = kPartitionReplicationEpoch,

  };
  EXPECT_TRUE(RunTaskSync(control.installer.ApplyDirective(directive)).ok());
  ASSERT_EQ(control.actions.directives_.size(), 1U);

  {
    NodeDirective unsupported = directive;
    unsupported.payload_ = "opaque";

    EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(unsupported)).code(),
              absl::StatusCode::kInvalidArgument);
    EXPECT_EQ(control.actions.directives_.size(), 1U);
  }

  directive.source_assignment_id_ = Assignment(2);
  EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(directive)).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.actions.directives_.size(), 1U);
  directive.source_assignment_id_ = Assignment(1);

  ++directive.partition_replication_epoch_;
  EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(directive)).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.actions.directives_.size(), 1U);
  directive.partition_replication_epoch_ = kPartitionReplicationEpoch;

  for (const std::uint64_t index : {9, 11}) {
    directive.projection_ = Basis(index);
    EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(directive)).code(),
              absl::StatusCode::kFailedPrecondition);
    EXPECT_EQ(control.actions.directives_.size(), 1U);
  }
  directive.projection_ = Basis(10);

  AuthorityMessage fence{
      .kind_ = AuthorityMessage::Kind::kFence,
      .session_ = Session(1),
      .projection_ = Basis(10),
      .anchor_ = anchor,
  };
  ASSERT_TRUE(RunTaskSync(control.installer.ApplyFenceTransition(fence)).ok());
  EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(directive)).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.actions.directives_.size(), 1U);

  // The floor fences one assignment through one term, not the group forever.
  // A committed higher term for the same incarnation is distinct.
  ASSERT_TRUE(
      control.installer
          .InstallFullState(FullState(MakeState(Assignment(1), 2, 2, 1,
                                                /*granted=*/true,
                                                /*population_ready=*/false)),
                            Basis(11))
          .ok());
  directive.projection_ = Basis(11);
  directive.anchor_ = Anchor(*control.cache.Current());
  EXPECT_TRUE(RunTaskSync(control.installer.ApplyDirective(directive)).ok());
  EXPECT_EQ(control.actions.directives_.size(), 2U);
}

TEST(NodeControlInstallerTest,
     EmptyPopulationUsesTheExistingPopulationAdmissionWithoutASource) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer
          .InstallFullState(FullState(MakeState(Assignment(1), 1, 1, 1,
                                                /*granted=*/true,
                                                /*population_ready=*/false)),
                            Basis(10))
          .ok());
  NodeDirective directive{
      .projection_ = Basis(10),
      .anchor_ = Anchor(*control.cache.Current()),
      .operation_id_ = ShortId<OperationId>(1),
      .directive_id_ = ShortId<DirectiveId>(2),
      .attempt_id_ = ShortId<AttemptId>(3),
      .directive_revision_ = 8,
      .kind_ = NodeDirective::Kind::kInitializeEmptyPopulation,
      .target_node_id_ = *NodeId::Parse(kNodeA),
      .target_boot_id_ = *NodeId::Parse(kBoot),
      .flow_count_ = 0,
      .manifest_revision_ = 1,
      .manifest_digest_ = Digest(4),
      .partition_replication_epoch_ = kPartitionReplicationEpoch,
      .payload_ = std::string(kNodeB),

  };

  NodeDirective stale_assignment = directive;
  stale_assignment.anchor_.assignment_id_ = Assignment(2);
  EXPECT_EQ(
      RunTaskSync(control.installer.ApplyDirective(stale_assignment)).code(),
      absl::StatusCode::kFailedPrecondition);

  NodeDirective stale_term = directive;
  ++stale_term.anchor_.group_term_;
  EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(stale_term)).code(),
            absl::StatusCode::kFailedPrecondition);

  NodeDirective stale_population = directive;
  ++stale_population.partition_replication_epoch_;
  EXPECT_EQ(
      RunTaskSync(control.installer.ApplyDirective(stale_population)).code(),
      absl::StatusCode::kFailedPrecondition);

  EXPECT_TRUE(RunTaskSync(control.installer.ApplyDirective(directive)).ok());
  ASSERT_EQ(control.actions.directives_.size(), 1U);
  EXPECT_EQ(control.actions.directives_.front().kind_,
            NodeDirective::Kind::kInitializeEmptyPopulation);
  EXPECT_TRUE(control.actions.directives_.front().source_node_id_.empty());

  // Lost-result replay while the target is already serving is a read of the
  // original completion, not another population mutation. Unknown attempts
  // still cannot enter the action adapter or take this Ready owner offline.
  ASSERT_TRUE(RunTaskSync(control.installer.SetPopulationReadinessTransition(
                              PopulationReadiness{"group-a", Assignment(1), 1,
                                                  1, Digest(4),
                                                  kPartitionReplicationEpoch}))
                  .ok());
  control.actions.completed_population_ = directive;
  ASSERT_TRUE(control.cache.Current()->FindGroup("group-a")->population_ready_);
  const auto before_replay = control.cache.Current();
  EXPECT_TRUE(RunTaskSync(control.installer.ApplyDirective(directive)).ok());
  EXPECT_EQ(control.cache.Current(), before_replay);
  EXPECT_EQ(control.actions.directives_.size(), 1U);
  NodeDirective different_attempt = directive;
  different_attempt.attempt_id_ = ShortId<AttemptId>(9);
  EXPECT_EQ(
      RunTaskSync(control.installer.ApplyDirective(different_attempt)).code(),
      absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.cache.Current(), before_replay);
  EXPECT_EQ(control.actions.directives_.size(), 1U);
  NodeDirective stale_replay = directive;
  ++stale_replay.anchor_.group_term_;
  control.actions.completed_population_ = stale_replay;
  EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(stale_replay)).code(),
            absl::StatusCode::kFailedPrecondition);

  NodeDirective sourced = directive;
  sourced.directive_id_ = ShortId<DirectiveId>(4);
  sourced.source_node_id_ = *NodeId::Parse(kNodeB);
  EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(sourced)).code(),
            absl::StatusCode::kInvalidArgument);

  NodeDirective malformed_history = directive;
  malformed_history.directive_id_ = ShortId<DirectiveId>(5);
  malformed_history.payload_ = std::string(40, 'G');
  EXPECT_EQ(
      RunTaskSync(control.installer.ApplyDirective(malformed_history)).code(),
      absl::StatusCode::kInvalidArgument);
}

TEST(NodeControlInstallerTest,
     SourceDirectiveFenceFloorUsesTheLocalSourceAssignment) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(
      control.installer.InstallFullState(FullState(MakeState()), Basis(10))
          .ok());
  const AuthorityAnchor former_owner = Anchor(*control.cache.Current());
  ASSERT_TRUE(
      RunTaskSync(control.installer.ApplyFenceTransition(AuthorityMessage{
                      .kind_ = AuthorityMessage::Kind::kFence,
                      .session_ = Session(1),
                      .projection_ = Basis(10),
                      .anchor_ = former_owner,
                  }))
          .ok());

  const AssignmentId target_assignment = Assignment(2);
  ASSERT_TRUE(
      control.installer
          .InstallFullState(LocalSourceFullState(
                                MakeLocalSourceState(target_assignment, 2),
                                former_owner.assignment_id_, target_assignment),
                            Basis(11))
          .ok());
  NodeDirective authorize{
      .projection_ = Basis(11),
      .anchor_ = {.group_id_ = "group-a",
                  .assignment_id_ = target_assignment,
                  .group_term_ = 1},
      .operation_id_ = ShortId<OperationId>(1),
      .directive_id_ = ShortId<DirectiveId>(2),
      .attempt_id_ = ShortId<AttemptId>(3),
      .directive_revision_ = 8,
      .kind_ = NodeDirective::Kind::kAuthorizeSource,
      .target_node_id_ = *NodeId::Parse(kNodeB),
      .target_boot_id_ = *NodeId::Parse(kBoot),
      .source_node_id_ = *NodeId::Parse(kNodeA),
      .source_assignment_id_ = former_owner.assignment_id_,
      .source_boot_id_ = *NodeId::Parse(kBoot),
      .source_replication_history_id_ = *NodeId::Parse(kNodeA),
      .source_host_ = "127.0.0.1",
      .source_port_ = 7000,
      .flow_count_ = 1,
      .manifest_revision_ = 1,
      .manifest_digest_ = Digest(4),
      .partition_replication_epoch_ = kPartitionReplicationEpoch,
  };
  EXPECT_EQ(RunTaskSync(control.installer.ApplyDirective(authorize)).code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_TRUE(control.actions.directives_.empty());

  // A new local membership incarnation does not inherit the former owner's
  // boot-local floor.
  const AssignmentId fresh_source = Assignment(3);
  ASSERT_TRUE(
      control.installer
          .InstallFullState(
              LocalSourceFullState(MakeLocalSourceState(target_assignment, 3),
                                   fresh_source, target_assignment),
              Basis(12))
          .ok());
  authorize.projection_ = Basis(12);
  authorize.source_assignment_id_ = fresh_source;
  EXPECT_TRUE(RunTaskSync(control.installer.ApplyDirective(authorize)).ok());
  ASSERT_EQ(control.actions.directives_.size(), 1U);
}

TEST(NodeControlInstallerTest,
     ReplicaReadinessAndRebuildUseTargetMemberAssignment) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  const AssignmentId owner_assignment = Assignment(1);
  const AssignmentId replica_assignment = Assignment(2);
  ASSERT_TRUE(
      control.installer
          .InstallFullState(
              FullState(MakeReplicaState(owner_assignment), replica_assignment),
              Basis(10))
          .ok());

  PopulationReadiness readiness{
      .group_id_ = "group-a",
      .assignment_id_ = replica_assignment,
      .group_term_ = 1,
      .manifest_revision_ = 1,
      .manifest_digest_ = Digest(4),
      .partition_replication_epoch_ = kPartitionReplicationEpoch,
  };
  ASSERT_TRUE(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(readiness))
          .ok());
  EXPECT_TRUE(control.cache.Current()->FindGroup("group-a")->population_ready_);

  PopulationReadiness stale = readiness;
  stale.manifest_digest_ = Digest(5);
  EXPECT_EQ(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(stale))
          .code(),
      absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(
      control.cache.Current()->FindGroup("group-a")->population_ready_);
  ASSERT_TRUE(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(readiness))
          .ok());

  stale = readiness;
  stale.assignment_id_ = owner_assignment;
  EXPECT_EQ(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(stale))
          .code(),
      absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(
      control.cache.Current()->FindGroup("group-a")->population_ready_);
  ASSERT_TRUE(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(readiness))
          .ok());

  stale = readiness;
  ++stale.partition_replication_epoch_;
  EXPECT_EQ(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(stale))
          .code(),
      absl::StatusCode::kFailedPrecondition);
  EXPECT_FALSE(
      control.cache.Current()->FindGroup("group-a")->population_ready_);
  ASSERT_TRUE(
      RunTaskSync(control.installer.SetPopulationReadinessTransition(readiness))
          .ok());

  NodeDirective rebuild{
      .projection_ = Basis(10),
      .anchor_ = {.group_id_ = "group-a",
                  .assignment_id_ = replica_assignment,
                  .group_term_ = 1},
      .operation_id_ = ShortId<OperationId>(1),
      .directive_id_ = ShortId<DirectiveId>(2),
      .attempt_id_ = ShortId<AttemptId>(3),
      .directive_revision_ = 8,
      .kind_ = NodeDirective::Kind::kReplication,
      .target_node_id_ = *NodeId::Parse(kNodeB),
      .target_boot_id_ = *NodeId::Parse(kBoot),
      .source_node_id_ = *NodeId::Parse(kNodeA),
      .source_assignment_id_ = owner_assignment,
      .source_boot_id_ = *NodeId::Parse(kBoot),
      .source_replication_history_id_ = *NodeId::Parse(kNodeA),
      .source_host_ = "127.0.0.1",
      .source_port_ = 7000,
      .flow_count_ = 1,
      .manifest_revision_ = 1,
      .manifest_digest_ = Digest(4),
      .partition_replication_epoch_ = kPartitionReplicationEpoch,

  };
  bool saw_not_ready_before_admission = false;
  control.actions.on_apply_directive_ = [&] {
    saw_not_ready_before_admission =
        !control.cache.Current()->FindGroup("group-a")->population_ready_;
  };
  control.actions.defer_directive_completion_ = true;
  NodeDirectiveCompletion first =
      RunTaskSync(control.installer.StartDirective(rebuild));
  EXPECT_TRUE(first.started());
  EXPECT_FALSE(first.result().has_value());
  EXPECT_TRUE(saw_not_ready_before_admission);
  ASSERT_EQ(control.actions.directives_.size(), 1U);

  NodeDirective replacement = rebuild;
  replacement.directive_revision_ = 9;
  replacement.attempt_id_ = ShortId<AttemptId>(4);
  NodeDirectiveCompletion second =
      RunTaskSync(control.installer.StartDirective(replacement));
  EXPECT_TRUE(second.started());
  EXPECT_FALSE(second.result().has_value());
  EXPECT_EQ(control.actions.directives_.size(), 2U);
  control.actions.deferred_directive_result_ =
      absl::CancelledError("superseded");
  ASSERT_TRUE(first.result().has_value());
  ASSERT_TRUE(second.result().has_value());
  EXPECT_EQ(first.result()->code(), absl::StatusCode::kCancelled);

  rebuild.anchor_.assignment_id_ = owner_assignment;
  NodeDirectiveCompletion rejected =
      RunTaskSync(control.installer.StartDirective(rebuild));
  EXPECT_FALSE(rejected.started());
  ASSERT_TRUE(rejected.result().has_value());
  EXPECT_EQ(rejected.result()->code(), absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.actions.directives_.size(), 2U);
}

}  // namespace

TEST(NodeControlInstallerTest, RoutingRefreshPreservesLocalLeaseAndExecution) {
  DynamicControl control;
  ASSERT_TRUE(control.installer.SetStorageReady(true).ok());
  ASSERT_TRUE(RunTaskSync(control.installer.InstallFullStateTransition(
                              FullState(MakeState()), Basis(10)))
                  .ok());
  const auto before = control.cache.Current();
  const AuthorityMessage grant{.kind_ = AuthorityMessage::Kind::kLeaseGrant,
                               .session_ = Session(1),
                               .projection_ = Basis(10),
                               .anchor_ = Anchor(*before),
                               .sent_at_ = MonotonicTime{},
                               .granted_duration_ = 5s};
  ASSERT_TRUE(control.installer.ApplyAuthority(grant, MonotonicTime{}).ok());
  constexpr std::array<std::uint16_t, 1> slots{12};
  const auto admission =
      control.guard.CaptureAndAdmit(WriteRequest(slots), MonotonicTime{} + 1s);
  const auto clears = control.actions.session_clears_;
  ASSERT_TRUE(
      control.installer.InstallRouting(FullState(MakeState(Assignment(1), 2)))
          .ok());
  EXPECT_EQ(control.actions.session_clears_, clears);
  EXPECT_EQ(control.guard.Recheck(admission, MonotonicTime{} + 2s),
            RecheckResult::kOk);
  EXPECT_EQ(control.installer
                .InstallRouting(FullState(MakeState(Assignment(1), 3, 2)))
                .code(),
            absl::StatusCode::kFailedPrecondition);
  EXPECT_EQ(control.cache.Current()->topology_epoch(), 2);
}

}  // namespace lavik::cluster
