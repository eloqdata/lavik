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

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/worker.h"
#include "spdlog/spdlog.h"

namespace lavik::cluster {
namespace {

// Bycorf's relative sleep uses CLOCK_MONOTONIC, which pauses across host
// suspend. Rechecking a CLOCK_BOOTTIME deadline in short slices bounds the
// post-resume source-capability cleanup delay instead of preserving the
// remainder of an arbitrarily long lease. Request admission itself checks
// CLOCK_BOOTTIME synchronously and has no such delay.
constexpr auto kLeaseExpiryRecheckInterval = std::chrono::milliseconds(25);

MonotonicDuration LeaseExpiryRecheckInterval(MonotonicDuration grant) {
  const MonotonicDuration quarter = std::max(grant / 4, MonotonicDuration{1});
  return std::min(quarter, std::chrono::duration_cast<MonotonicDuration>(
                               kLeaseExpiryRecheckInterval));
}

AuthorityAnchor AnchorFor(const GroupView& group) {
  return AuthorityAnchor{
      .group_id_ = group.group_id_,
      .assignment_id_ = group.assignment_id_,
      .group_term_ = group.group_term_,
  };
}

bool SameLocalAssignment(const ServingState& state, const GroupView& group) {
  return group.primary_node_index_ == state.SelfNodeIndex();
}

bool LocalMember(const ServingState& state, const GroupView& group) {
  if (SameLocalAssignment(state, group)) return true;
  return std::find(group.replica_node_indices_.begin(),
                   group.replica_node_indices_.end(),
                   state.SelfNodeIndex()) != group.replica_node_indices_.end();
}

struct BootLocalPopulationAnchor {
  std::string group_id_;
  NodeId local_node_id_;
  AssignmentId local_assignment_id_;
  NodeId owner_node_id_;
  AssignmentId owner_assignment_id_;
  std::uint64_t group_term_ = 0;
  std::uint64_t manifest_revision_ = 0;
  Sha256Digest manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;

  friend bool operator==(const BootLocalPopulationAnchor&,
                         const BootLocalPopulationAnchor&) = default;
};

const PreparedGroupControlIdentity* FindControlIdentity(
    std::span<const DesiredClusterControl> controls,
    std::string_view group_id) {
  const auto found =
      std::find_if(controls.begin(), controls.end(),
                   [group_id](const DesiredClusterControl& control) {
                     return control.identity_.group_id_ == group_id;
                   });
  return found == controls.end() ? nullptr : &found->identity_;
}

std::optional<BootLocalPopulationAnchor> PopulationAnchorFor(
    const ServingState& state, const GroupView& group,
    std::span<const DesiredClusterControl> controls) {
  const NodeDescriptor* self = state.Self();
  const NodeDescriptor* owner = state.NodeAt(group.primary_node_index_);
  if (self == nullptr || owner == nullptr || !LocalMember(state, group)) {
    return std::nullopt;
  }
  const auto* control_group = FindControlIdentity(controls, group.group_id_);
  if (control_group == nullptr) return std::nullopt;
  const auto local_member = std::find_if(
      control_group->members_.begin(), control_group->members_.end(),
      [&](const PreparedMemberAssignment& member) {
        return member.node_id_ == self->node_id_;
      });
  const auto owner_member = std::find_if(
      control_group->members_.begin(), control_group->members_.end(),
      [&](const PreparedMemberAssignment& member) {
        return member.node_id_ == owner->node_id_;
      });
  if (local_member == control_group->members_.end() ||
      owner_member == control_group->members_.end() ||
      owner_member->assignment_id_ != group.assignment_id_) {
    return std::nullopt;
  }
  return BootLocalPopulationAnchor{
      .group_id_ = group.group_id_,
      .local_node_id_ = self->node_id_,
      .local_assignment_id_ = local_member->assignment_id_,
      .owner_node_id_ = owner->node_id_,
      .owner_assignment_id_ = owner_member->assignment_id_,
      .group_term_ = control_group->group_term_,
      .manifest_revision_ = control_group->manifest_revision_,
      .manifest_digest_ = control_group->manifest_digest_,
      .partition_replication_epoch_ =
          control_group->partition_replication_epoch_,
  };
}

// Rebuilds an immutable state while preserving every semantic field and the
// worker stripe layout. Mutators are used only for local readiness and a
// committed fence, keeping those transitions atomic at TopologyCache.
template <typename Mutate>
std::shared_ptr<const ServingState> RebuildState(const ServingState& state,
                                                 Mutate&& mutate) {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(state.topology_epoch())
      .IncludeGroupTerm(state.max_group_term())
      .SetSelfNodeIndex(state.SelfNodeIndex())
      .SetInFlightStripeCount(state.InFlightStripeCount());
  for (const NodeDescriptor& node : state.Nodes()) builder.AddNode(node);
  for (const GroupView& source : state.Groups()) {
    GroupView group = source;
    mutate(group);
    builder.AddGroup(std::move(group));
  }
  auto rebuilt = builder.Build();
  // The source already passed the same structural builder invariants and the
  // local installer mutations cannot invalidate node/slot relationships.
  if (!rebuilt.ok()) return nullptr;
  return *std::move(rebuilt);
}

absl::Status FirstFailure(absl::Status first, absl::Status next) {
  return first.ok() ? std::move(next) : first;
}

MonotonicTime SaturatingLeaseDeadline(const AuthorityMessage& message) {
  const MonotonicDuration remaining = MonotonicTime::max() - message.sent_at_;
  return message.granted_duration_ >= remaining
             ? MonotonicTime::max()
             : message.sent_at_ + message.granted_duration_;
}

std::optional<EstablishedExportScope> ExportScopeFor(
    const DesiredClusterControl& desired) {
  if (!desired.owner_.has_value()) return std::nullopt;
  EstablishedExportScope result{
      .group_id_ = desired.identity_.group_id_,
      .source_ = *desired.owner_,
      .manifest_revision_ = desired.identity_.manifest_revision_,
      .manifest_digest_ = desired.identity_.manifest_digest_,
      .partition_replication_epoch_ =
          desired.identity_.partition_replication_epoch_,
      .downstream_members_ = {},
  };
  for (const PreparedMemberAssignment& member : desired.identity_.members_) {
    if (member.node_id_ != desired.owner_->node_id_) {
      result.downstream_members_.push_back(member);
    }
  }
  std::sort(
      result.downstream_members_.begin(), result.downstream_members_.end(),
      [](const PreparedMemberAssignment& a, const PreparedMemberAssignment& b) {
        if (a.node_id_ != b.node_id_) return a.node_id_ < b.node_id_;
        return a.assignment_id_ < b.assignment_id_;
      });
  return result;
}

}  // namespace

bool SameEstablishedExportScope(const DesiredClusterControl& left,
                                const DesiredClusterControl& right) {
  return ExportScopeFor(left) == ExportScopeFor(right);
}

NodeDirectiveCompletion NodeDirectiveCompletion::Rejected(absl::Status result) {
  if (result.ok()) {
    result = absl::InternalError(
        "a rejected directive completion cannot contain success");
  }
  auto terminal = std::make_shared<const absl::Status>(std::move(result));
  return NodeDirectiveCompletion(
      [terminal = std::move(terminal)]() { return *terminal; }, false);
}

NodeDirectiveCompletion NodeDirectiveCompletion::StartedTerminal(
    absl::Status result) {
  auto terminal = std::make_shared<const absl::Status>(std::move(result));
  return NodeDirectiveCompletion(
      [terminal = std::move(terminal)]() { return *terminal; }, true);
}

std::optional<absl::Status> NodeDirectiveCompletion::result() const {
  if (!poll_) {
    return absl::FailedPreconditionError(
        "directive completion handle is empty");
  }
  return poll_();
}

bycorf::Task<absl::Status> NodeDirectiveCompletion::Await() const {
  for (;;) {
    if (std::optional<absl::Status> terminal = result(); terminal.has_value()) {
      co_return *terminal;
    }
    bycorf::Worker* worker = bycorf::ThisWorker().self_;
    if (worker == nullptr) {
      co_return absl::FailedPreconditionError(
          "pending directive completion requires a Bycorf worker");
    }
    const absl::Status waited =
        co_await bycorf::SleepFor(*worker, std::chrono::milliseconds(10));
    if (!waited.ok()) co_return waited;
  }
}

absl::Status NullNodeControlActions::RevokeSourceAuthorizations() {
  return absl::OkStatus();
}

bycorf::Task<absl::Status>
NodeControlActions::RevokeSourceAuthorizationsAndWait() {
  co_return RevokeSourceAuthorizations();
}

bycorf::Task<absl::Status>
NodeControlActions::ClearSourceAuthorizationsForSessionReplacementAndWait(
    bool /*preserve_established_exports*/) {
  co_return RevokeSourceAuthorizations();
}

bycorf::Task<absl::Status>
NodeControlActions::RefreshSourceAuthorizationsForFdsReplacementAndWait(
    bool preserve_current_population_exports,
    std::size_t /*expected_authorization_replays*/) {
  co_return co_await ClearSourceAuthorizationsForSessionReplacementAndWait(
      preserve_current_population_exports);
}

bycorf::Task<absl::Status> NodeControlActions::ReconcileClusterControl(
    std::optional<DesiredClusterControl> /*desired*/) {
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> NodeControlActions::ActivatePreparedPromotion(
    PreparedFailoverActivation /*activation*/) {
  co_return absl::FailedPreconditionError(
      "the control adapter cannot activate a prepared promotion");
}

bycorf::Task<absl::Status> NodeControlActions::EnableExpirationAuthorityUntil(
    MonotonicTime /*deadline*/) {
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> NodeControlActions::EnableSourceAdmissionForLease(
    MonotonicTime /*deadline*/) {
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> NodeControlActions::RevokeExpirationAuthority() {
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> NodeControlActions::ReconcilePopulation(
    std::optional<PopulationReadiness> /*desired*/,
    bool /*population_transition_expected*/) {
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> NodeControlActions::CancelInProgressPopulation(
    bool /*preserve_current_follow_attempt*/) {
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> NodeControlActions::CancelPopulationForShutdown() {
  co_return co_await CancelInProgressPopulation(
      /*preserve_current_follow_attempt=*/false);
}

bycorf::Task<NodeDirectiveCompletion> NodeControlActions::StartDirective(
    NodeDirective directive) {
  co_return NodeDirectiveCompletion::StartedTerminal(
      co_await ApplyDirective(std::move(directive)));
}

bycorf::Task<absl::Status> NullNodeControlActions::ApplyDirective(
    NodeDirective /*directive*/) {
  co_return absl::FailedPreconditionError(
      "the test control adapter does not execute Meta directives");
}

absl::Status NullNodeControlActions::DrainAssignment(
    const AuthorityAnchor& /*anchor*/) {
  return absl::OkStatus();
}

NodeControlInstaller::NodeControlInstaller(TopologyCache& topology,
                                           AuthorityGuard& authority,
                                           NodeControlActions& actions)
    : topology_(topology), authority_(authority), actions_(actions) {}

NodeControlInstaller::~NodeControlInstaller() {
  assert(std::all_of(lease_timer_lifetimes_.begin(),
                     lease_timer_lifetimes_.end(),
                     [](const auto& lifetime) { return lifetime.expired(); }) &&
         "NodeControlInstaller must outlive its worker-owned lease timers");
}

absl::Status NodeControlInstaller::ValidateProjection(
    const ProjectionBasis& basis) const {
  if (!projection_basis_.has_value()) {
    return absl::FailedPreconditionError(
        "no full desired state has been installed");
  }
  if (basis.control_revision_ != projection_basis_->control_revision_) {
    return absl::FailedPreconditionError(
        "control message does not reference the installed Meta projection");
  }
  return absl::OkStatus();
}

absl::Status NodeControlInstaller::ValidateAnchor(
    const AuthorityAnchor& anchor, bool require_local_owner) const {
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current == nullptr) {
    return absl::FailedPreconditionError("no serving state is installed");
  }
  const GroupView* group = current->FindGroup(anchor.group_id_);
  if (group == nullptr) {
    return absl::FailedPreconditionError(
        absl::StrCat("unknown authority group '", anchor.group_id_, "'"));
  }
  if (require_local_owner &&
      group->primary_node_index_ != current->SelfNodeIndex()) {
    return absl::FailedPreconditionError(
        "authority message does not target a locally owned group");
  }
  if (AnchorFor(*group) != anchor) {
    return absl::FailedPreconditionError(
        "authority message anchor is not current");
  }
  return absl::OkStatus();
}

const PreparedGroupControlIdentity* NodeControlInstaller::FindControlGroup(
    std::string_view group_id) const {
  return FindControlIdentity(desired_cluster_controls_, group_id);
}

const PreparedMemberAssignment* NodeControlInstaller::FindMemberAssignment(
    const PreparedGroupControlIdentity& group, const NodeId& node_id) const {
  const auto member =
      std::find_if(group.members_.begin(), group.members_.end(),
                   [&node_id](const PreparedMemberAssignment& candidate) {
                     return candidate.node_id_ == node_id;
                   });
  return member == group.members_.end() ? nullptr : &*member;
}

absl::StatusOr<std::optional<PopulationReadiness>>
NodeControlInstaller::DesiredLocalPopulation() const {
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current == nullptr || current->Self() == nullptr) {
    return std::optional<PopulationReadiness>{};
  }
  std::optional<PopulationReadiness> desired;
  for (const auto& control : desired_cluster_controls_) {
    const auto& control_group = control.identity_;
    const PreparedMemberAssignment* local_member =
        FindMemberAssignment(control_group, current->Self()->node_id_);
    if (local_member == nullptr) continue;
    if (desired.has_value()) {
      return absl::FailedPreconditionError(
          "one Data process cannot reconcile more than one local population");
    }
    desired = PopulationReadiness{
        .group_id_ = control_group.group_id_,
        .assignment_id_ = local_member->assignment_id_,
        .group_term_ = control_group.group_term_,
        .manifest_revision_ = control_group.manifest_revision_,
        .manifest_digest_ = control_group.manifest_digest_,
        .partition_replication_epoch_ =
            control_group.partition_replication_epoch_,
    };
  }
  return desired;
}

absl::StatusOr<std::optional<DesiredClusterControl>>
NodeControlInstaller::DesiredLocalClusterControl() const {
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current == nullptr || current->Self() == nullptr) {
    return std::optional<DesiredClusterControl>{};
  }
  std::optional<DesiredClusterControl> desired;
  for (const DesiredClusterControl& control : desired_cluster_controls_) {
    const bool local_member = std::any_of(
        control.identity_.members_.begin(), control.identity_.members_.end(),
        [&](const PreparedMemberAssignment& member) {
          return member.node_id_ == current->Self()->node_id_;
        });
    if (!local_member) continue;
    if (desired.has_value()) {
      return absl::FailedPreconditionError(
          "one Data process cannot reconcile more than one local Group");
    }
    desired = control;
  }
  return desired;
}

absl::StatusOr<std::optional<DesiredClusterControl>>
NodeControlInstaller::ValidateLeaseGrantContext(const AuthorityMessage& message,
                                                MonotonicTime now) {
  if (!projection_basis_.has_value() ||
      message.projection_ != *projection_basis_) {
    return absl::FailedPreconditionError(
        "lease grant does not name the exact installed Meta projection");
  }
  if (const absl::Status anchor =
          ValidateAnchor(message.anchor_, /*require_local_owner=*/true);
      !anchor.ok()) {
    return anchor;
  }
  if (storage_failed_) {
    return absl::FailedPreconditionError(
        "storage failed during this boot; lease recovery requires restart");
  }
  if (RejectedByFence(message.anchor_)) {
    return absl::FailedPreconditionError(
        "lease grant does not advance the in-memory fence floor");
  }
  if (!message.session_.complete()) {
    return absl::InvalidArgumentError(
        "lease grant session identity is incomplete");
  }
  if (message.granted_duration_ <= MonotonicDuration::zero()) {
    return absl::InvalidArgumentError("lease grant duration must be positive");
  }
  const MonotonicTime deadline = SaturatingLeaseDeadline(message);
  if (deadline == MonotonicTime::max()) {
    return absl::InvalidArgumentError(
        "Meta lease grant must have a finite deadline");
  }
  if (deadline <= now) {
    return absl::DeadlineExceededError(
        "lease grant expired before it could be installed");
  }
  if (source_revocation_transitions_ != 0 ||
      DrainPending(message.anchor_.group_id_)) {
    return absl::UnavailableError(
        "authority cleanup or retired assignment requests have not drained");
  }

  const std::shared_ptr<const ServingState> current = topology_.Current();
  const GroupView* group = current == nullptr
                               ? nullptr
                               : current->FindGroup(message.anchor_.group_id_);
  if (current == nullptr || current->Self() == nullptr || group == nullptr ||
      group->primary_node_index_ != current->SelfNodeIndex() ||
      !group->granted_ || !group->population_ready_ || !group->storage_ready_) {
    return absl::FailedPreconditionError(
        "lease grant targets an unready or fenced assignment");
  }

  auto desired = DesiredLocalClusterControl();
  if (!desired.ok()) return desired.status();
  // Static and older in-process test adapters have no desired-control layer.
  // Meta-managed projections always populate it and therefore take the
  // stronger committed owner/action validation below.
  if (!desired->has_value()) return *desired;
  const DesiredClusterControl& control = **desired;
  // The local control carries the duration already resolved from the current
  // global Policy and the Meta Leader's leadership-validity limit. The grant
  // must repeat that scalar exactly so projection identity and finite authority
  // cannot describe different lease contracts.
  if (control.identity_.group_id_ != message.anchor_.group_id_ ||
      control.identity_.group_term_ != message.anchor_.group_term_ ||
      !control.owner_.has_value() ||
      control.owner_->node_id_ != current->Self()->node_id_ ||
      control.owner_->assignment_id_ != message.anchor_.assignment_id_ ||
      !control.grant_active_ || authority_lease_duration_ms_ == 0 ||
      message.granted_duration_ !=
          std::chrono::duration_cast<MonotonicDuration>(
              std::chrono::milliseconds(authority_lease_duration_ms_))) {
    const std::string desired_owner =
        control.owner_.has_value() ? control.owner_->node_id_.ToHexString()
                                   : "<none>";
    const std::string desired_assignment =
        control.owner_.has_value()
            ? control.owner_->assignment_id_.ToHexString()
            : "<none>";
    return absl::FailedPreconditionError(absl::StrCat(
        "lease grant disagrees with committed desired owner authority: ",
        "message={group=", message.anchor_.group_id_,
        ",assignment=", message.anchor_.assignment_id_.ToHexString(),
        ",term=", message.anchor_.group_term_, ",duration_ms=",
        std::chrono::duration_cast<std::chrono::milliseconds>(
            message.granted_duration_)
            .count(),
        "} desired={group=", control.identity_.group_id_,
        ",owner=", desired_owner, ",owner_assignment=", desired_assignment,
        ",term=", control.identity_.group_term_,
        ",grant_active=", control.grant_active_ ? "true" : "false",
        ",effective_duration_ms=", authority_lease_duration_ms_,
        "} local_node=", current->Self()->node_id_.ToHexString()));
  }
  if (control.activation_action_id_.has_value() &&
      control.activation_action_id_->empty()) {
    return absl::DataLossError(
        "failover-installed current grant has an empty activation action id");
  }
  return *desired;
}

void NodeControlInstaller::RetireLeaseSchedule(std::string_view group_id) {
  const auto schedule = lease_expiry_schedules_.find(std::string(group_id));
  if (schedule == lease_expiry_schedules_.end()) return;
  schedule->second->active_ = false;
  if (schedule->second->timer_generation_ ==
      std::numeric_limits<std::uint64_t>::max()) {
    std::terminate();
  }
  ++schedule->second->timer_generation_;
  lease_expiry_schedules_.erase(schedule);
}

void NodeControlInstaller::RetireAllLeaseSchedules() {
  for (auto& [unused_group_id, schedule] : lease_expiry_schedules_) {
    (void)unused_group_id;
    schedule->active_ = false;
    if (schedule->timer_generation_ ==
        std::numeric_limits<std::uint64_t>::max()) {
      std::terminate();
    }
    ++schedule->timer_generation_;
  }
  lease_expiry_schedules_.clear();
}

bycorf::Task<absl::Status> NodeControlInstaller::FailClosedLeaseGrantTransition(
    const AuthorityMessage& message, absl::Status failure) {
  assert(!failure.ok());
  ControlTransitionGuard transition_guard(*this);
  InvalidateDirectiveAdmissions();
  authority_.InvalidateSession(message.session_);
  RetireLeaseSchedule(message.anchor_.group_id_);

  std::vector<AuthorityAnchor> anchors;
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current != nullptr) {
    const GroupView* group = current->FindGroup(message.anchor_.group_id_);
    if (group != nullptr &&
        group->primary_node_index_ == current->SelfNodeIndex() &&
        AnchorFor(*group) == message.anchor_) {
      RememberDrain(current, message.anchor_);
      anchors.push_back(message.anchor_);
    }
  }

  absl::Status cleanup = co_await actions_.RevokeExpirationAuthority();
  cleanup =
      FirstFailure(std::move(cleanup), co_await WaitForDirectiveAdmissions());
  cleanup = FirstFailure(
      std::move(cleanup),
      co_await actions_.ClearSourceAuthorizationsForSessionReplacementAndWait(
          /*preserve_established_exports=*/false));
  for (const AuthorityAnchor& anchor : anchors) {
    cleanup =
        FirstFailure(std::move(cleanup), actions_.DrainAssignment(anchor));
  }
  cleanup =
      FirstFailure(std::move(cleanup), co_await WaitForPendingDrains(anchors));
  co_return FirstFailure(std::move(failure), std::move(cleanup));
}

absl::Status NodeControlInstaller::ValidateDirectiveAnchor(
    const NodeDirective& directive) const {
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current == nullptr || current->Self() == nullptr) {
    return absl::FailedPreconditionError("no serving state is installed");
  }
  const PreparedGroupControlIdentity* control_group =
      FindControlGroup(directive.anchor_.group_id_);
  if (control_group == nullptr) {
    return absl::FailedPreconditionError(
        "directive group is absent from the installed local control "
        "identities");
  }
  if (control_group->group_term_ != directive.anchor_.group_term_ ||
      control_group->partition_replication_epoch_ !=
          directive.partition_replication_epoch_) {
    return absl::FailedPreconditionError(
        "directive authority identity is not current");
  }
  const PreparedMemberAssignment* target =
      FindMemberAssignment(*control_group, directive.target_node_id_);
  if (target == nullptr ||
      target->assignment_id_ != directive.anchor_.assignment_id_) {
    return absl::FailedPreconditionError(
        "directive target assignment is not current");
  }
  const PreparedMemberAssignment* source =
      FindMemberAssignment(*control_group, directive.source_node_id_);
  const bool initializes_empty =
      directive.kind_ == NodeDirective::Kind::kInitializeEmptyPopulation;
  if (!initializes_empty &&
      (source == nullptr ||
       source->assignment_id_ != directive.source_assignment_id_)) {
    return absl::FailedPreconditionError(
        "directive source assignment is not current");
  }

  const NodeId& local_node_id = current->Self()->node_id_;
  if (directive.kind_ == NodeDirective::Kind::kReplication ||
      initializes_empty) {
    if (directive.target_node_id_ != local_node_id) {
      return absl::FailedPreconditionError(
          "population directive does not execute on its target");
    }
  } else if (directive.source_node_id_ != local_node_id) {
    return absl::FailedPreconditionError(
        "source directive does not execute on its source");
  }
  return absl::OkStatus();
}

absl::Status NodeControlInstaller::ValidateDirectiveForStart(
    const NodeDirective& directive, bool replay_lookup) {
  if (storage_failed_) {
    return absl::FailedPreconditionError(
        "storage failed during this boot; directive execution requires "
        "restart recovery");
  }
  if (source_revocation_transitions_ != 0) {
    return absl::UnavailableError(
        "source authority cleanup is still in progress");
  }
  const bool initializes_empty =
      directive.kind_ == NodeDirective::Kind::kInitializeEmptyPopulation;
  const bool initialization_payload_valid =
      directive.payload_.size() == 40 &&
      std::all_of(directive.payload_.begin(), directive.payload_.end(),
                  [](unsigned char value) {
                    return (value >= '0' && value <= '9') ||
                           (value >= 'a' && value <= 'f');
                  });
  if (initializes_empty ? !initialization_payload_valid
                        : !directive.payload_.empty()) {
    return absl::InvalidArgumentError(
        "directive kind does not match its opaque field schema");
  }
  if (const absl::Status projection = ValidateProjection(directive.projection_);
      !projection.ok()) {
    return projection;
  }
  if (directive.operation_id_.empty() || directive.directive_id_.empty() ||
      directive.attempt_id_.empty() || directive.directive_revision_ == 0 ||
      directive.target_node_id_.empty() || directive.target_boot_id_.empty() ||
      (!initializes_empty &&
       (directive.source_node_id_.empty() ||
        directive.source_assignment_id_.empty() ||
        directive.source_boot_id_.empty() ||
        directive.source_replication_history_id_.empty()))) {
    return absl::InvalidArgumentError(
        "directive execution identity is incomplete");
  }
  if (initializes_empty &&
      (!directive.source_node_id_.empty() ||
       !directive.source_assignment_id_.empty() ||
       !directive.source_boot_id_.empty() ||
       !directive.source_replication_history_id_.empty() ||
       !directive.source_host_.empty() || directive.source_port_ != 0)) {
    return absl::InvalidArgumentError(
        "empty population directive must not carry a source identity");
  }
  if (const absl::Status anchor = ValidateDirectiveAnchor(directive);
      !anchor.ok()) {
    return anchor;
  }
  // A fence names the assignment local to the node that received it. Rebuilds
  // run on the target and therefore use the common target anchor; source-side
  // directives must compare the source member's assignment against the same
  // group counter floor. Otherwise a former owner could reopen an export
  // capability merely because a later directive names a different target.
  AuthorityAnchor local_anchor = directive.anchor_;
  if (directive.kind_ != NodeDirective::Kind::kReplication &&
      !initializes_empty) {
    local_anchor.assignment_id_ = directive.source_assignment_id_;
  }
  if (RejectedByFence(local_anchor)) {
    return absl::FailedPreconditionError(
        "directive does not advance the boot-local fence floor");
  }
  const std::shared_ptr<const ServingState> current = topology_.Current();
  const GroupView* group = current->FindGroup(directive.anchor_.group_id_);
  if (directive.kind_ == NodeDirective::Kind::kReplication ||
      initializes_empty) {
    const bool local_serving_owner =
        group != nullptr &&
        group->primary_node_index_ == current->SelfNodeIndex() &&
        group->granted_ && group->population_ready_ && group->storage_ready_;
    if (!replay_lookup && local_serving_owner) {
      return absl::FailedPreconditionError(
          "population mutation requires a non-serving local target assignment");
    }
    if (!replay_lookup &&
        (DrainPending(directive.anchor_.group_id_) ||
         current->GroupInFlightCount(directive.anchor_.group_id_) != 0)) {
      return absl::UnavailableError(
          "population target still has in-flight requests");
    }
  } else if (directive.kind_ == NodeDirective::Kind::kAuthorizeSource &&
             DrainPending(directive.anchor_.group_id_)) {
    return absl::UnavailableError(
        "source authorization waits for the retired assignment to drain");
  }
  if (directive.kind_ != NodeDirective::Kind::kRevokeSources &&
      ((initializes_empty ? directive.flow_count_ != 0
                          : directive.flow_count_ == 0) ||
       directive.manifest_revision_ == 0 ||
       std::all_of(directive.manifest_digest_.begin(),
                   directive.manifest_digest_.end(),
                   [](std::uint8_t byte) { return byte == 0; }))) {
    return absl::InvalidArgumentError(
        "population directive is missing its flow or manifest identity");
  }
  if (directive.kind_ == NodeDirective::Kind::kReplication &&
      (directive.source_host_.empty() || directive.source_port_ == 0)) {
    return absl::InvalidArgumentError(
        "rebuild directive has no dialable source endpoint");
  }
  return absl::OkStatus();
}

bool NodeControlInstaller::DrainPending(std::string_view group_id) {
  std::erase_if(pending_drains_, [](const PendingDrain& drain) {
    return drain.state_->GroupInFlightCount(drain.group_id_) == 0;
  });
  return std::any_of(pending_drains_.begin(), pending_drains_.end(),
                     [group_id](const PendingDrain& drain) {
                       return drain.group_id_ == group_id;
                     });
}

void NodeControlInstaller::InvalidateDirectiveAdmissions() {
  // Reusing a generation could let an ancient suspended admission pass a new
  // authority boundary. The counter cannot realistically exhaust; fail-stop
  // is safer than wrapping into a potentially valid old token.
  if (directive_admission_generation_ ==
      std::numeric_limits<std::uint64_t>::max()) {
    std::terminate();
  }
  ++directive_admission_generation_;
}

bycorf::Task<absl::Status> NodeControlInstaller::WaitForDirectiveAdmissions() {
  bycorf::Worker* worker = bycorf::ThisWorker().self_;
  if (worker == nullptr && directive_admissions_in_flight_ != 0) {
    co_return absl::FailedPreconditionError(
        "directive admission drain requires a Bycorf worker");
  }
  while (directive_admissions_in_flight_ != 0) {
    co_await bycorf::Yield(*worker);
  }
  co_return absl::OkStatus();
}

NodeControlInstaller::ControlTransitionGuard::ControlTransitionGuard(
    NodeControlInstaller& owner, bool active) {
  if (!active) return;
  if (owner.source_revocation_transitions_ ==
          std::numeric_limits<unsigned>::max() ||
      owner.next_control_transition_id_ ==
          std::numeric_limits<std::uint64_t>::max()) {
    std::terminate();
  }
  owner_ = &owner;
  id_ = ++owner.next_control_transition_id_;
  ++owner.source_revocation_transitions_;
  const auto [unused, inserted] = owner.active_control_transitions_.insert(id_);
  (void)unused;
  assert(inserted);
}

NodeControlInstaller::ControlTransitionGuard::~ControlTransitionGuard() {
  if (owner_ == nullptr) return;
  assert(owner_->source_revocation_transitions_ != 0);
  // The drain barrier reads this set independently of the count below. Retire
  // the transition in Release too, where assert expressions are not evaluated.
  const auto erased = owner_->active_control_transitions_.erase(id_);
  assert(erased == 1);
  (void)erased;
  --owner_->source_revocation_transitions_;
}

bycorf::Task<absl::Status>
NodeControlInstaller::WaitForControlTransitionsBefore(
    std::uint64_t transition_id) {
  bycorf::Worker* worker = bycorf::ThisWorker().self_;
  while (!active_control_transitions_.empty() &&
         *active_control_transitions_.begin() < transition_id) {
    if (worker == nullptr) {
      co_return absl::FailedPreconditionError(
          "control transition drain requires a Bycorf worker");
    }
    co_await bycorf::Yield(*worker);
  }
  co_return absl::OkStatus();
}

void NodeControlInstaller::RememberDrain(
    std::shared_ptr<const ServingState> state, const AuthorityAnchor& anchor) {
  if (state == nullptr || state->GroupInFlightCount(anchor.group_id_) == 0) {
    return;
  }
  const auto duplicate =
      std::find_if(pending_drains_.begin(), pending_drains_.end(),
                   [&](const PendingDrain& drain) {
                     return drain.state_.get() == state.get() &&
                            drain.group_id_ == anchor.group_id_;
                   });
  if (duplicate == pending_drains_.end()) {
    pending_drains_.push_back(PendingDrain{
        .state_ = std::move(state),
        .group_id_ = anchor.group_id_,
        .retired_anchor_ = anchor,
    });
  }
}

bycorf::Task<absl::Status> NodeControlInstaller::WaitForPendingDrains(
    std::span<const AuthorityAnchor> anchors) {
  while (std::any_of(anchors.begin(), anchors.end(),
                     [this](const AuthorityAnchor& anchor) {
                       return DrainPending(anchor.group_id_);
                     })) {
    bycorf::Worker* worker = bycorf::ThisWorker().self_;
    if (worker == nullptr) {
      co_return absl::FailedPreconditionError(
          "asynchronous assignment drain requires a Bycorf worker");
    }
    const absl::Status waited =
        co_await bycorf::SleepFor(*worker, std::chrono::milliseconds(1));
    if (!waited.ok()) co_return waited;
  }
  co_return absl::OkStatus();
}

std::vector<AuthorityAnchor>
NodeControlInstaller::RememberCurrentLocalDrains() {
  std::vector<AuthorityAnchor> anchors;
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current == nullptr) return anchors;
  for (const GroupView& group : current->Groups()) {
    if (!SameLocalAssignment(*current, group)) continue;
    AuthorityAnchor anchor = AnchorFor(group);
    RememberDrain(current, anchor);
    anchors.push_back(std::move(anchor));
  }
  return anchors;
}

bool NodeControlInstaller::RejectedByFence(
    const AuthorityAnchor& anchor) const {
  const auto it = reject_through_.find(anchor.group_id_);
  if (it == reject_through_.end() ||
      it->second.assignment_id_ != anchor.assignment_id_) {
    return false;
  }
  return anchor.group_term_ <= it->second.group_term_;
}

std::shared_ptr<const ServingState> NodeControlInstaller::WithStorageReady(
    const ServingState& state, bool ready) const {
  return RebuildState(
      state, [ready](GroupView& group) { group.storage_ready_ = ready; });
}

std::shared_ptr<const ServingState> NodeControlInstaller::WithFence(
    const ServingState& state, const AuthorityAnchor& anchor) const {
  return RebuildState(state, [&anchor](GroupView& group) {
    if (group.group_id_ != anchor.group_id_) return;
    group.assignment_id_ = anchor.assignment_id_;
    group.group_term_ = anchor.group_term_;
    group.granted_ = false;
  });
}

std::shared_ptr<const ServingState>
NodeControlInstaller::WithPopulationReadiness(
    const ServingState& state,
    const std::optional<PopulationReadiness>& readiness) const {
  const NodeDescriptor* self = state.Self();
  return RebuildState(state, [&](GroupView& group) {
    if (!LocalMember(state, group)) return;
    const PreparedGroupControlIdentity* control_group =
        FindControlGroup(group.group_id_);
    const PreparedMemberAssignment* local_member =
        self == nullptr || control_group == nullptr
            ? nullptr
            : FindMemberAssignment(*control_group, self->node_id_);
    group.population_ready_ =
        readiness.has_value() && group.group_id_ == readiness->group_id_ &&
        group.group_term_ == readiness->group_term_ &&
        group.manifest_revision_ == readiness->manifest_revision_ &&
        control_group != nullptr &&
        control_group->manifest_digest_ == readiness->manifest_digest_ &&
        control_group->partition_replication_epoch_ ==
            readiness->partition_replication_epoch_ &&
        local_member != nullptr &&
        local_member->assignment_id_ == readiness->assignment_id_;
  });
}

absl::Status NodeControlInstaller::InstallFullState(
    PreparedFullState prepared_state, ProjectionBasis projection_basis) {
  if (actions_.ReceivesDirectives()) {
    return absl::FailedPreconditionError(
        "directive-capable full desired state requires the asynchronous "
        "NodeControl transition");
  }
  // The synchronous test seam has no directive producer, but advancing the
  // token keeps this lower-level entry fail-closed if a caller violates that
  // assembly contract while an async admission is suspended.
  InvalidateDirectiveAdmissions();
  FullStateEffects effects;
  absl::Status local = InstallFullStateLocal(std::move(prepared_state),
                                             projection_basis, &effects);
  if (!effects.revoke_sources_) return local;

  absl::Status actions = actions_.RevokeSourceAuthorizations();
  for (const AuthorityAnchor& anchor : effects.retired_) {
    actions =
        FirstFailure(std::move(actions), actions_.DrainAssignment(anchor));
  }
  return FirstFailure(std::move(local), std::move(actions));
}

absl::Status NodeControlInstaller::InstallFullStateLocal(
    PreparedFullState prepared_state, ProjectionBasis projection_basis,
    FullStateEffects* effects) {
  *effects = FullStateEffects{};
  if (prepared_state.serving_state_ == nullptr) {
    return absl::InvalidArgumentError("full desired state is empty");
  }
  if (!prepared_state.desired_cluster_controls_.empty()) {
    std::set<std::string> group_ids;
    for (const auto& control : prepared_state.desired_cluster_controls_) {
      const auto& control_group = control.identity_;
      if (control_group.group_id_.empty() ||
          !group_ids.insert(control_group.group_id_).second) {
        return absl::InvalidArgumentError(
            "prepared local control control group identity is empty or "
            "duplicated");
      }
      const bool empty_manifest =
          std::all_of(control_group.manifest_digest_.begin(),
                      control_group.manifest_digest_.end(),
                      [](std::uint8_t byte) { return byte == 0; });
      if ((control_group.manifest_revision_ == 0) != empty_manifest) {
        return absl::InvalidArgumentError(
            "prepared local control manifest identity is partial");
      }
      std::set<NodeId> member_ids;
      for (const PreparedMemberAssignment& member : control_group.members_) {
        if (member.node_id_.empty() || member.assignment_id_.empty() ||
            !member_ids.insert(member.node_id_).second) {
          return absl::InvalidArgumentError(
              "prepared local control member identity is incomplete or "
              "duplicated");
        }
      }
      const GroupView* serving_group =
          prepared_state.serving_state_->FindGroup(control_group.group_id_);
      if (serving_group != nullptr &&
          (serving_group->group_term_ != control_group.group_term_ ||
           serving_group->manifest_revision_ !=
               control_group.manifest_revision_)) {
        return absl::InvalidArgumentError(
            "prepared local control control identity disagrees with its "
            "serving group");
      }
    }
  }
  if (!prepared_state.desired_cluster_controls_.empty()) {
    if (prepared_state.authority_lease_duration_ms_ == 0) {
      return absl::InvalidArgumentError(
          "prepared Meta local control has no effective Authority Lease "
          "duration");
    }
    std::set<std::string> desired_group_ids;
    for (const DesiredClusterControl& desired :
         prepared_state.desired_cluster_controls_) {
      if (desired.identity_.group_id_.empty() ||
          !desired_group_ids.insert(desired.identity_.group_id_).second) {
        return absl::InvalidArgumentError(
            "prepared desired cluster control is empty or duplicated");
      }
      if (desired.owner_.has_value() &&
          std::find(desired.identity_.members_.begin(),
                    desired.identity_.members_.end(),
                    *desired.owner_) == desired.identity_.members_.end()) {
        return absl::InvalidArgumentError(
            "prepared desired cluster control owner is not a member");
      }
      if (desired.grant_active_ && !desired.owner_.has_value()) {
        return absl::InvalidArgumentError(
            "prepared active cluster control has no owner");
      }
    }
  }
  if (projection_basis_.has_value()) {
    if (projection_basis.control_revision_ <
        projection_basis_->control_revision_) {
      return absl::OutOfRangeError("full desired state source index regressed");
    }
    // An equal committed index may be reinstalled after reconnect. Its local
    // lease ceiling can differ on the new Meta leader, so run the ordinary
    // replacement path rather than restoring the prior session's lease context.
  }

  const bool readiness_already_normalized =
      std::all_of(prepared_state.serving_state_->Groups().begin(),
                  prepared_state.serving_state_->Groups().end(),
                  [this](const GroupView& group) {
                    return group.storage_ready_ == storage_ready_;
                  });
  std::shared_ptr<const ServingState> next =
      readiness_already_normalized
          ? prepared_state.serving_state_
          : WithStorageReady(*prepared_state.serving_state_, storage_ready_);
  if (next == nullptr) {
    return absl::InternalError("cannot rebuild prepared serving state");
  }

  const std::shared_ptr<const ServingState> before = topology_.Current();
  if (before != nullptr) {
    std::set<std::string> carried_ready_groups;
    for (const GroupView& old_group : before->Groups()) {
      if (!old_group.population_ready_) continue;
      const GroupView* new_group = next->FindGroup(old_group.group_id_);
      if (new_group == nullptr || new_group->population_ready_) continue;
      const auto old_anchor = PopulationAnchorFor(
          *before, old_group, std::span(desired_cluster_controls_));
      const auto new_anchor = PopulationAnchorFor(
          *next, *new_group,
          std::span(prepared_state.desired_cluster_controls_));
      if (old_anchor.has_value() && old_anchor == new_anchor) {
        carried_ready_groups.insert(old_group.group_id_);
      }
    }
    if (!carried_ready_groups.empty()) {
      // Meta deliberately omits boot-local ReadyToken state, so its local-
      // member projection is always false. That normalization is not a new
      // negative observation. Preserve an already verified bit only within
      // this installer boot and only across the exact population identity;
      // SetPopulationReadinessTransition remains the sole path that can clear
      // it without changing a durable population anchor.
      next = RebuildState(*next, [&](GroupView& group) {
        if (carried_ready_groups.contains(group.group_id_)) {
          group.population_ready_ = true;
        }
      });
      if (next == nullptr) {
        return absl::InternalError(
            "cannot carry boot-local population readiness");
      }
    }
    if (next->topology_epoch() < before->topology_epoch()) {
      return absl::FailedPreconditionError("topology epoch regressed");
    }
    for (const auto& old_control : desired_cluster_controls_) {
      const auto& old_group = old_control.identity_;
      const auto* new_group = FindControlIdentity(
          prepared_state.desired_cluster_controls_, old_group.group_id_);
      if (new_group == nullptr) continue;
      if (new_group->partition_replication_epoch_ <
          old_group.partition_replication_epoch_) {
        return absl::FailedPreconditionError(
            absl::StrCat("partition replication epoch regressed for group '",
                         old_group.group_id_, "'"));
      }
      const bool shares_member_incarnation = std::any_of(
          old_group.members_.begin(), old_group.members_.end(),
          [&](const PreparedMemberAssignment& old_member) {
            return std::any_of(new_group->members_.begin(),
                               new_group->members_.end(),
                               [&](const PreparedMemberAssignment& new_member) {
                                 return old_member == new_member;
                               });
          });
      if (shares_member_incarnation &&
          (new_group->group_term_ < old_group.group_term_ ||
           new_group->manifest_revision_ < old_group.manifest_revision_)) {
        return absl::FailedPreconditionError(absl::StrCat(
            "same-membership control counter regressed for group '",
            old_group.group_id_, "'"));
      }
      if (shares_member_incarnation &&
          new_group->manifest_revision_ == old_group.manifest_revision_ &&
          new_group->manifest_digest_ != old_group.manifest_digest_) {
        return absl::DataLossError(
            absl::StrCat("same manifest revision changed digest for group '",
                         old_group.group_id_, "'"));
      }
    }
    for (const GroupView& old_group : before->Groups()) {
      const GroupView* new_group = next->FindGroup(old_group.group_id_);
      if (new_group == nullptr ||
          new_group->assignment_id_ != old_group.assignment_id_) {
        continue;
      }
      if (new_group->group_term_ < old_group.group_term_ ||
          new_group->manifest_revision_ < old_group.manifest_revision_) {
        return absl::FailedPreconditionError(absl::StrCat(
            "same-assignment control counter regressed for group '",
            old_group.group_id_, "'"));
      }
    }
  }

  std::vector<AuthorityAnchor> retired;
  bool revoke_sources = false;
  bool preserve_current_population_exports = false;
  if (before != nullptr) {
    bool saw_desired_export_scope = false;
    bool all_desired_export_scopes_preserved = true;
    if (before->Self() != nullptr && !desired_cluster_controls_.empty() &&
        !prepared_state.desired_cluster_controls_.empty()) {
      for (const DesiredClusterControl& old_control :
           desired_cluster_controls_) {
        if (!old_control.owner_.has_value() ||
            old_control.owner_->node_id_ != before->Self()->node_id_) {
          continue;
        }
        saw_desired_export_scope = true;
        const auto new_control =
            std::find_if(prepared_state.desired_cluster_controls_.begin(),
                         prepared_state.desired_cluster_controls_.end(),
                         [&](const DesiredClusterControl& candidate) {
                           return candidate.identity_.group_id_ ==
                                  old_control.identity_.group_id_;
                         });
        all_desired_export_scopes_preserved =
            all_desired_export_scopes_preserved &&
            new_control != prepared_state.desired_cluster_controls_.end() &&
            SameEstablishedExportScope(old_control, *new_control);
      }
    }
    if (saw_desired_export_scope) {
      preserve_current_population_exports = all_desired_export_scopes_preserved;
    }

    bool saw_legacy_export_scope = false;
    bool all_legacy_export_scopes_preserved = true;
    for (const GroupView& old_group : before->Groups()) {
      if (!SameLocalAssignment(*before, old_group)) continue;
      const GroupView* new_group = next->FindGroup(old_group.group_id_);
      const PreparedGroupControlIdentity* old_control =
          FindControlGroup(old_group.group_id_);
      const auto* new_control = FindControlIdentity(
          prepared_state.desired_cluster_controls_, old_group.group_id_);
      // Desired-state projection excludes the boot-local ReadyToken. The
      // installer above reattaches it only for an equal population anchor;
      // legacy export retention is stricter and additionally requires the
      // complete durable group/member identity to remain byte-exact.
      if (!saw_desired_export_scope) {
        saw_legacy_export_scope = true;
        all_legacy_export_scopes_preserved =
            all_legacy_export_scopes_preserved &&
            old_group.primary_node_index_ == before->SelfNodeIndex() &&
            new_group != nullptr &&
            new_group->primary_node_index_ == next->SelfNodeIndex() &&
            new_group->granted_ == old_group.granted_ &&
            new_group->storage_ready_ == old_group.storage_ready_ &&
            old_control != nullptr && new_control != nullptr &&
            *old_control == *new_control;
      }
      const bool authority_changed =
          new_group == nullptr ||
          new_group->primary_node_index_ != next->SelfNodeIndex() ||
          AnchorFor(*new_group) != AnchorFor(old_group) ||
          new_group->granted_ != old_group.granted_ ||
          new_group->population_ready_ != old_group.population_ready_ ||
          new_group->storage_ready_ != old_group.storage_ready_;
      const bool pause_started =
          new_group != nullptr &&
          new_group->primary_node_index_ == next->SelfNodeIndex() &&
          AnchorFor(*new_group) == AnchorFor(old_group) &&
          !old_group.mutations_paused_ && new_group->mutations_paused_;
      if (pause_started) {
        effects->pause_drains_.push_back(AnchorFor(old_group));
      }
      if (authority_changed) {
        retired.push_back(AnchorFor(old_group));
        revoke_sources = true;
      }
    }
    if (!saw_desired_export_scope && saw_legacy_export_scope) {
      preserve_current_population_exports = all_legacy_export_scopes_preserved;
    }
  }

  // Invalidation precedes publication, closing the race in which a request
  // could observe old topology after the controller has accepted a revoking
  // assignment change.
  // Projection changes gate future Meta messages through ValidateProjection,
  // but they are not authority changes. In particular, publishing a
  // Controlled Pause must keep the current finite lease alive so reads and
  // heartbeats continue while mutations drain.
  authority_.InvalidateAnchorsChanged(before.get(), *next);
  topology_.Publish(next);
  projection_basis_ = projection_basis;
  authority_lease_duration_ms_ = prepared_state.authority_lease_duration_ms_;
  desired_cluster_controls_ =
      std::move(prepared_state.desired_cluster_controls_);
  for (const AuthorityAnchor& anchor : retired) {
    RememberDrain(before, anchor);
  }
  for (const AuthorityAnchor& anchor : effects->pause_drains_) {
    RememberDrain(before, anchor);
  }
  effects->revoke_sources_ = revoke_sources;
  effects->preserve_current_population_exports_ =
      preserve_current_population_exports;
  effects->retired_ = std::move(retired);
  return absl::OkStatus();
}

absl::Status NodeControlInstaller::InstallRouting(PreparedFullState prepared) {
  if (!projection_basis_ ||
      prepared.desired_cluster_controls_ != desired_cluster_controls_ ||
      prepared.authority_lease_duration_ms_ != authority_lease_duration_ms_) {
    return absl::FailedPreconditionError(
        "routing update changes local control");
  }
  FullStateEffects effects;
  return InstallFullStateLocal(std::move(prepared), *projection_basis_,
                               &effects);
}

bycorf::Task<absl::Status> NodeControlInstaller::InstallFullStateTransition(
    PreparedFullState prepared_state, ProjectionBasis projection_basis,
    bool local_population_transition_expected,
    std::size_t expected_source_authorization_replays) {
  ControlTransitionGuard transition_guard(*this);
  InvalidateDirectiveAdmissions();

  FullStateEffects effects;
  absl::Status local = InstallFullStateLocal(std::move(prepared_state),
                                             projection_basis, &effects);
  if (!local.ok() && !effects.revoke_sources_) {
    co_return FirstFailure(std::move(local),
                           co_await WaitForDirectiveAdmissions());
  }

  absl::StatusOr<std::optional<PopulationReadiness>> desired_population(
      std::optional<PopulationReadiness>{});
  if (local.ok()) desired_population = DesiredLocalPopulation();
  absl::StatusOr<std::optional<DesiredClusterControl>> desired_cluster_control(
      std::optional<DesiredClusterControl>{});
  if (local.ok()) desired_cluster_control = DesiredLocalClusterControl();

  // Every replacement clears current export capabilities. An exact live local
  // control may keep every already-published POPULATION export, including one
  // not yet ONLINE, because the replacement has proven the same
  // source/population scope. Any stronger transition joins and revokes the old
  // session. The async seam makes FullStateApplied a real join boundary instead
  // of an enqueue receipt. Target population reconciliation runs under the same
  // exclusion counter: no directive may start after local control publication
  // but before an invalidated native session and partial storage root are
  // joined.
  absl::Status actions = co_await WaitForDirectiveAdmissions();
  if (effects.revoke_sources_) {
    RetireAllLeaseSchedules();
    actions = FirstFailure(std::move(actions),
                           co_await actions_.RevokeExpirationAuthority());
  }
  actions = FirstFailure(
      std::move(actions),
      co_await actions_.RefreshSourceAuthorizationsForFdsReplacementAndWait(
          local.ok() && effects.preserve_current_population_exports_,
          local.ok() ? expected_source_authorization_replays : 0));
  if (!effects.pause_drains_.empty()) {
    actions =
        FirstFailure(std::move(actions),
                     co_await WaitForPendingDrains(effects.pause_drains_));
  }
  const std::optional<DesiredClusterControl> reconciled_cluster_control =
      !storage_failed_ && desired_cluster_control.ok()
          ? *desired_cluster_control
          : std::optional<DesiredClusterControl>{};
  std::optional<DesiredClusterControl> effective_cluster_control =
      reconciled_cluster_control;
  if (effective_cluster_control.has_value()) {
    effective_cluster_control->population_transition_expected_ =
        local_population_transition_expected;
  }
  if (!cluster_control_reconciled_ ||
      effective_cluster_control != reconciled_cluster_control_) {
    absl::Status reconciled =
        co_await actions_.ReconcileClusterControl(effective_cluster_control);
    if (reconciled.ok()) {
      reconciled_cluster_control_ = std::move(effective_cluster_control);
      cluster_control_reconciled_ = true;
    }
    actions = FirstFailure(std::move(actions), std::move(reconciled));
  }
  // Even an internally inconsistent local control must leave the local
  // population fail-closed and join the old session's work before the failed
  // transfer tears down its socket. Storage loss may have raced the preceding
  // session-clear await. The local control is still useful as fail-closed
  // topology truth, but this boot must never feed a non-empty desired
  // population back into ReplicationManager afterwards.
  const std::optional<PopulationReadiness> reconciled_population =
      !storage_failed_ && desired_population.ok()
          ? *desired_population
          : std::optional<PopulationReadiness>{};
  actions = FirstFailure(
      std::move(actions),
      co_await actions_.ReconcilePopulation(
          reconciled_population,
          !storage_failed_ && local_population_transition_expected));
  for (const AuthorityAnchor& anchor : effects.retired_) {
    actions =
        FirstFailure(std::move(actions), actions_.DrainAssignment(anchor));
  }
  if (!effects.retired_.empty()) {
    actions = FirstFailure(std::move(actions),
                           co_await WaitForPendingDrains(effects.retired_));
  }
  absl::Status desired =
      desired_population.ok() ? absl::OkStatus() : desired_population.status();
  if (!desired_cluster_control.ok()) {
    desired =
        FirstFailure(std::move(desired), desired_cluster_control.status());
  }
  co_return FirstFailure(std::move(local),
                         FirstFailure(std::move(desired), std::move(actions)));
}

absl::Status NodeControlInstaller::ApplyAuthority(
    const AuthorityMessage& message, MonotonicTime now) {
  if (const absl::Status projection = ValidateProjection(message.projection_);
      !projection.ok()) {
    return projection;
  }
  if (const absl::Status anchor =
          ValidateAnchor(message.anchor_, /*require_local_owner=*/true);
      !anchor.ok()) {
    return anchor;
  }

  if (message.kind_ == AuthorityMessage::Kind::kLeaseGrant) {
    auto desired = ValidateLeaseGrantContext(message, now);
    if (!desired.ok()) return desired.status();
    const MonotonicTime deadline = SaturatingLeaseDeadline(message);
    return authority_.RenewLease(message.session_, message.anchor_, deadline,
                                 now);
  }

  if (actions_.ReceivesDirectives()) {
    return absl::FailedPreconditionError(
        "directive-capable fence requires the asynchronous NodeControl "
        "transition");
  }
  InvalidateDirectiveAdmissions();
  auto transitioned = ApplyFenceLocal(message);
  if (!transitioned.ok()) return transitioned.status();
  absl::Status result = actions_.RevokeSourceAuthorizations();
  if (*transitioned) {
    result = FirstFailure(std::move(result),
                          actions_.DrainAssignment(message.anchor_));
  }
  return result;
}

bycorf::Task<absl::Status> NodeControlInstaller::ApplyLeaseGrantTransition(
    const AuthorityMessage& message) {
  if (message.kind_ != AuthorityMessage::Kind::kLeaseGrant) {
    co_return absl::InvalidArgumentError(
        "Meta lease transition received a non-grant authority message");
  }
  bycorf::Worker* worker = bycorf::ThisWorker().self_;
  if (worker == nullptr) {
    co_return absl::FailedPreconditionError(
        "Meta lease transition requires a Bycorf worker");
  }
  const MonotonicTime deadline = SaturatingLeaseDeadline(message);
  MonotonicTime now = LeaseClockNow();
  auto initial = ValidateLeaseGrantContext(message, now);
  if (!initial.ok()) co_return initial.status();

  // A CLOCK_MONOTONIC-backed worker timer may still be asleep after host
  // suspend even though CLOCK_BOOTTIME says the lease is already due. Finish
  // that exact expiration transition here before renewal can preserve the old
  // generation and revive pre-expiry admissions.
  if (const auto existing =
          lease_expiry_schedules_.find(message.anchor_.group_id_);
      existing != lease_expiry_schedules_.end() && existing->second->active_ &&
      existing->second->deadline_ <= now) {
    if (absl::Status expired =
            co_await FinishExpiredLeaseTransition(existing->second, now);
        !expired.ok()) {
      co_return expired;
    }
    now = LeaseClockNow();
    initial = ValidateLeaseGrantContext(message, now);
    if (!initial.ok()) co_return initial.status();
  }

  const std::optional<DesiredClusterControl> pinned_desired = *initial;
  const std::uint64_t pinned_generation = directive_admission_generation_;
  if (pinned_desired.has_value() &&
      pinned_desired->activation_action_id_.has_value()) {
    const std::shared_ptr<const ServingState> current = topology_.Current();
    assert(current != nullptr && current->Self() != nullptr &&
           pinned_desired->owner_.has_value());
    PreparedFailoverActivation activation{
        .action_id_ = *pinned_desired->activation_action_id_,
        .group_id_ = message.anchor_.group_id_,
        .candidate_node_id_ = current->Self()->node_id_,
        .candidate_assignment_id_ = message.anchor_.assignment_id_,
        .candidate_boot_id_ = message.session_.data_boot_id_,
        .target_term_ = pinned_desired->identity_.group_term_,
        .manifest_revision_ = pinned_desired->identity_.manifest_revision_,
        .manifest_digest_ = pinned_desired->identity_.manifest_digest_,
        .partition_replication_epoch_ =
            pinned_desired->identity_.partition_replication_epoch_,
    };
    if (absl::Status activated =
            co_await actions_.ActivatePreparedPromotion(std::move(activation));
        !activated.ok()) {
      co_return co_await FailClosedLeaseGrantTransition(message,
                                                        std::move(activated));
    }
    now = LeaseClockNow();
    auto rechecked = ValidateLeaseGrantContext(message, now);
    if (!rechecked.ok()) {
      co_return co_await FailClosedLeaseGrantTransition(message,
                                                        rechecked.status());
    }
    if (directive_admission_generation_ != pinned_generation ||
        *rechecked != pinned_desired) {
      co_return co_await FailClosedLeaseGrantTransition(
          message, absl::FailedPreconditionError(
                       "promotion activation crossed a changed control "
                       "projection or session"));
    }
  }

  if (absl::Status expiration =
          co_await actions_.EnableExpirationAuthorityUntil(deadline);
      !expiration.ok()) {
    co_return co_await FailClosedLeaseGrantTransition(message,
                                                      std::move(expiration));
  }
  now = LeaseClockNow();
  auto final = ValidateLeaseGrantContext(message, now);
  if (!final.ok()) {
    co_return co_await FailClosedLeaseGrantTransition(message, final.status());
  }
  if (directive_admission_generation_ != pinned_generation ||
      *final != pinned_desired) {
    co_return co_await FailClosedLeaseGrantTransition(
        message,
        absl::FailedPreconditionError(
            "expiration activation crossed a changed control projection or "
            "session"));
  }
  if (absl::Status installed = authority_.RenewLease(
          message.session_, message.anchor_, deadline, now);
      !installed.ok()) {
    co_return co_await FailClosedLeaseGrantTransition(message,
                                                      std::move(installed));
  }
  if (absl::Status source_admission =
          co_await actions_.EnableSourceAdmissionForLease(deadline);
      !source_admission.ok()) {
    co_return co_await FailClosedLeaseGrantTransition(
        message, std::move(source_admission));
  }
  now = LeaseClockNow();
  final = ValidateLeaseGrantContext(message, now);
  if (!final.ok()) {
    co_return co_await FailClosedLeaseGrantTransition(message, final.status());
  }
  if (directive_admission_generation_ != pinned_generation ||
      *final != pinned_desired ||
      !authority_.HasExactLease(message.session_, message.anchor_, deadline,
                                now)) {
    co_return co_await FailClosedLeaseGrantTransition(
        message, absl::FailedPreconditionError(
                     "source admission activation crossed a changed or expired "
                     "control session"));
  }

  std::shared_ptr<LeaseExpirySchedule> schedule;
  bool spawn_timer = false;
  const auto existing = lease_expiry_schedules_.find(message.anchor_.group_id_);
  if (existing == lease_expiry_schedules_.end() || !existing->second->active_) {
    schedule = std::make_shared<LeaseExpirySchedule>(LeaseExpirySchedule{
        .session_ = message.session_,
        .anchor_ = message.anchor_,
        .deadline_ = deadline,
        .recheck_interval_ =
            LeaseExpiryRecheckInterval(message.granted_duration_),
    });
    lease_expiry_schedules_.insert_or_assign(message.anchor_.group_id_,
                                             schedule);
    spawn_timer = true;
  } else {
    schedule = existing->second;
    const MonotonicTime old_deadline = schedule->deadline_;
    const MonotonicDuration old_recheck_interval = schedule->recheck_interval_;
    const MonotonicDuration recheck_interval =
        LeaseExpiryRecheckInterval(message.granted_duration_);
    schedule->session_ = message.session_;
    schedule->anchor_ = message.anchor_;
    schedule->deadline_ = deadline;
    schedule->recheck_interval_ = recheck_interval;
    if (deadline < old_deadline || recheck_interval < old_recheck_interval) {
      ++schedule->timer_generation_;
      spawn_timer = true;
    }
  }
  if (spawn_timer) {
    std::erase_if(lease_timer_lifetimes_,
                  [](const auto& lifetime) { return lifetime.expired(); });
    auto lifetime = std::make_shared<const LeaseTimerLifetime>();
    lease_timer_lifetimes_.push_back(lifetime);
    worker->Spawn(ExpireLeaseAt(schedule, schedule->timer_generation_,
                                std::move(lifetime)));
  }
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> NodeControlInstaller::ExpireLeaseAt(
    std::shared_ptr<LeaseExpirySchedule> schedule,
    std::uint64_t timer_generation,
    std::shared_ptr<const LeaseTimerLifetime> lifetime) {
  // `lifetime` deliberately stays in this coroutine frame across every
  // suspension. Its destruction makes the public outlive-worker contract
  // mechanically checkable without coupling this module to Worker shutdown.
  (void)lifetime;
  bycorf::Worker* worker = bycorf::ThisWorker().self_;
  if (worker == nullptr) {
    co_return absl::FailedPreconditionError(
        "lease expiration requires a Bycorf worker");
  }
  while (schedule->active_ && schedule->timer_generation_ == timer_generation) {
    const MonotonicTime deadline = schedule->deadline_;
    const MonotonicTime now = LeaseClockNow();
    if (now < deadline) {
      const absl::Status slept = co_await bycorf::SleepFor(
          *worker, std::min(deadline - now, schedule->recheck_interval_));
      if (!slept.ok()) co_return slept;
      continue;
    }
    break;
  }
  if (!schedule->active_ || schedule->timer_generation_ != timer_generation) {
    co_return absl::OkStatus();
  }
  // Renewal updates the shared schedule. An extension keeps this timer unless
  // the new grant requires a shorter recheck slice; a deadline shortening or
  // smaller slice replaces its generation, so this stale task cannot touch
  // the replacement lease.
  co_return co_await FinishExpiredLeaseTransition(schedule, LeaseClockNow());
}

bycorf::Task<absl::Status> NodeControlInstaller::FinishExpiredLeaseTransition(
    std::shared_ptr<LeaseExpirySchedule> schedule, MonotonicTime now) {
  if (!schedule->active_) co_return absl::OkStatus();
  if (now < schedule->deadline_) {
    co_return absl::FailedPreconditionError(
        "lease expiration transition ran before its exact deadline");
  }
  ControlTransitionGuard transition_guard(*this);
  InvalidateDirectiveAdmissions();
  const bool expired = authority_.ExpireLease(
      schedule->session_, schedule->anchor_, schedule->deadline_, now);
  if (expired) {
    spdlog::warn(
        "cluster lease expired for group {}; suspending new source admission",
        schedule->anchor_.group_id_);
  }
  schedule->active_ = false;
  const auto installed =
      lease_expiry_schedules_.find(schedule->anchor_.group_id_);
  if (installed != lease_expiry_schedules_.end() &&
      installed->second == schedule) {
    lease_expiry_schedules_.erase(installed);
  }
  absl::Status result = co_await actions_.RevokeExpirationAuthority();
  if (!expired) co_return result;
  const AuthorityAnchor anchor = schedule->anchor_;
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current != nullptr) {
    const GroupView* group = current->FindGroup(anchor.group_id_);
    if (group != nullptr && AnchorFor(*group) == anchor) {
      RememberDrain(current, anchor);
    }
  }
  result =
      FirstFailure(std::move(result), co_await WaitForDirectiveAdmissions());
  // Expiration closes new source admission inside RevokeExpirationAuthority,
  // but retains current local control capabilities and every already-published
  // POPULATION session. A committed fence, session loss, or population
  // identity change uses the stronger capability/session cleanup path.
  result = FirstFailure(std::move(result), actions_.DrainAssignment(anchor));
  co_return result;
}

absl::StatusOr<bool> NodeControlInstaller::ApplyFenceLocal(
    const AuthorityMessage& message) {
  if (message.kind_ != AuthorityMessage::Kind::kFence) {
    return absl::InvalidArgumentError(
        "asynchronous authority transition requires a fence");
  }

  RejectThrough& floor = reject_through_[message.anchor_.group_id_];
  if (floor.assignment_id_ != message.anchor_.assignment_id_ ||
      floor.group_term_ < message.anchor_.group_term_) {
    floor = RejectThrough{
        .assignment_id_ = message.anchor_.assignment_id_,
        .group_term_ = message.anchor_.group_term_,
    };
  }

  const std::shared_ptr<const ServingState> before = topology_.Current();
  const GroupView* before_group = before->FindGroup(message.anchor_.group_id_);
  const bool newly_fenced = before_group != nullptr && before_group->granted_;
  authority_.Fence(message.anchor_);
  const std::shared_ptr<const ServingState> fenced =
      WithFence(*topology_.Current(), message.anchor_);
  if (fenced == nullptr) {
    return absl::InternalError("cannot build fenced state");
  }
  topology_.Publish(fenced);
  if (newly_fenced) {
    RememberDrain(before, message.anchor_);
  }
  return newly_fenced;
}

bycorf::Task<absl::Status> NodeControlInstaller::ApplyFenceTransition(
    const AuthorityMessage& message) {
  if (message.kind_ != AuthorityMessage::Kind::kFence) {
    co_return absl::InvalidArgumentError(
        "Meta fence transition received a non-fence authority message");
  }
  if (const absl::Status projection = ValidateProjection(message.projection_);
      !projection.ok()) {
    co_return projection;
  }
  if (const absl::Status anchor =
          ValidateAnchor(message.anchor_, /*require_local_owner=*/true);
      !anchor.ok()) {
    co_return anchor;
  }

  ControlTransitionGuard transition_guard(*this);
  // Close admission before publishing the fence. Earlier admissions retain a
  // guard through NodeControlActions::StartDirective; after they either reject
  // or finish registering, the final cancel below sees every target attempt.
  InvalidateDirectiveAdmissions();
  RetireLeaseSchedule(message.anchor_.group_id_);
  auto transitioned = ApplyFenceLocal(message);
  if (!transitioned.ok()) co_return transitioned.status();
  absl::Status result = co_await WaitForDirectiveAdmissions();
  result = FirstFailure(std::move(result),
                        co_await actions_.RevokeExpirationAuthority());
  result = FirstFailure(std::move(result),
                        co_await actions_.RevokeSourceAuthorizationsAndWait());
  result = FirstFailure(std::move(result),
                        co_await actions_.CancelInProgressPopulation(
                            /*preserve_current_follow_attempt=*/false));
  if (*transitioned) {
    result = FirstFailure(std::move(result),
                          actions_.DrainAssignment(message.anchor_));
  }
  const std::array<AuthorityAnchor, 1> anchor{message.anchor_};
  result =
      FirstFailure(std::move(result), co_await WaitForPendingDrains(anchor));
  co_return result;
}

bycorf::Task<NodeDirectiveCompletion> NodeControlInstaller::StartDirective(
    NodeDirective directive) {
  const auto terminal = [](absl::Status status) {
    return NodeDirectiveCompletion::Rejected(std::move(status));
  };
  // Re-reporting an exact completed result is not a storage mutation. Keep
  // all session/local control/fence/identity checks, but do not require an
  // already-Ready owner to stop serving or drain client writes merely to repeat
  // its result. The adapter's lookup cannot start work; no match uses every
  // normal guard.
  if (directive.kind_ == NodeDirective::Kind::kReplication ||
      directive.kind_ == NodeDirective::Kind::kInitializeEmptyPopulation) {
    if (auto valid =
            ValidateDirectiveForStart(directive, /*replay_lookup=*/true);
        !valid.ok())
      co_return terminal(std::move(valid));
    if (auto completed = actions_.FindCompletedPopulation(directive))
      co_return std::move(*completed);
  }
  if (absl::Status valid = ValidateDirectiveForStart(directive); !valid.ok()) {
    co_return terminal(std::move(valid));
  }
  if (directive_admissions_in_flight_ ==
      std::numeric_limits<std::uint64_t>::max()) {
    std::terminate();
  }
  const std::uint64_t admission_generation = directive_admission_generation_;
  ++directive_admissions_in_flight_;
  struct AdmissionGuard {
    std::uint64_t* count_;
    ~AdmissionGuard() {
      assert(*count_ != 0);
      --*count_;
    }
  } admission_guard{&directive_admissions_in_flight_};
  if (directive.kind_ == NodeDirective::Kind::kReplication ||
      directive.kind_ == NodeDirective::Kind::kInitializeEmptyPopulation) {
    // Publish fail-closed state before ReplicationManager reaches any
    // destructive reset. A rejected or failed admission deliberately leaves
    // the target unready; only a matching ReadyToken may reopen it.
    absl::Status unready = co_await SetPopulationReadinessTransitionImpl(
        std::nullopt, /*invalidate_directive_admissions=*/false);
    if (!unready.ok()) co_return terminal(std::move(unready));
  }
  // Both readiness publication and NodeControlActions admission can suspend.
  // Recheck immediately before crossing into the action adapter; a fence,
  // local control, or session-loss generation observed in between owns this
  // attempt.
  if (admission_generation != directive_admission_generation_) {
    co_return terminal(absl::FailedPreconditionError(
        "directive admission was invalidated by a control transition"));
  }
  if (absl::Status valid = ValidateDirectiveForStart(directive); !valid.ok()) {
    co_return terminal(std::move(valid));
  }
  co_return co_await actions_.StartDirective(std::move(directive));
}

bycorf::Task<absl::Status> NodeControlInstaller::ApplyDirective(
    NodeDirective directive) {
  NodeDirectiveCompletion completion =
      co_await StartDirective(std::move(directive));
  co_return co_await completion.Await();
}

absl::Status NodeControlInstaller::LoseSession(const SessionIdentity& session,
                                               std::string_view /*reason*/) {
  if (absl::Status invalidated = InvalidateSessionNow(session);
      !invalidated.ok()) {
    return invalidated;
  }
  if (actions_.ReceivesDirectives()) {
    return absl::FailedPreconditionError(
        "directive-capable session loss requires the asynchronous NodeControl "
        "transition");
  }
  const std::vector<AuthorityAnchor> anchors = RememberCurrentLocalDrains();
  absl::Status result = actions_.RevokeSourceAuthorizations();
  for (const AuthorityAnchor& anchor : anchors) {
    result = FirstFailure(std::move(result), actions_.DrainAssignment(anchor));
  }
  return result;
}

absl::Status NodeControlInstaller::InvalidateSessionNow(
    const SessionIdentity& session) {
  if (!session.complete()) {
    return absl::InvalidArgumentError("lost session identity is incomplete");
  }
  // Socket loss must close directive admission before the caller awaits its
  // child tasks. LoseSessionTransition later joins admissions that were
  // already inside NodeControlActions when this generation advanced.
  InvalidateDirectiveAdmissions();
  authority_.InvalidateSession(session);
  RetireAllLeaseSchedules();
  return absl::OkStatus();
}

bycorf::Task<absl::Status>
NodeControlInstaller::CancelPopulationForShutdownTransition() {
  ControlTransitionGuard transition_guard(*this);
  InvalidateDirectiveAdmissions();
  authority_.InvalidateAll();
  RetireAllLeaseSchedules();
  absl::Status result = co_await WaitForDirectiveAdmissions();
  result = FirstFailure(std::move(result),
                        co_await actions_.RevokeExpirationAuthority());
  co_return FirstFailure(std::move(result),
                         co_await actions_.CancelPopulationForShutdown());
}

bycorf::Task<absl::Status> NodeControlInstaller::LoseSessionTransition(
    const SessionIdentity& session, std::string_view /*reason*/) {
  if (absl::Status invalidated = InvalidateSessionNow(session);
      !invalidated.ok()) {
    co_return invalidated;
  }
  // Remember the replaced snapshot before the first suspension. A reconnect
  // may install an equal projection, but no grant or destructive directive
  // can overtake writes admitted by the lost session.
  const std::vector<AuthorityAnchor> anchors = RememberCurrentLocalDrains();
  ControlTransitionGuard transition_guard(*this);
  absl::Status result = co_await WaitForDirectiveAdmissions();
  result = FirstFailure(std::move(result),
                        co_await actions_.RevokeExpirationAuthority());
  result = FirstFailure(
      std::move(result),
      co_await actions_.ClearSourceAuthorizationsForSessionReplacementAndWait(
          /*preserve_established_exports=*/true));
  result = FirstFailure(std::move(result),
                        co_await actions_.CancelInProgressPopulation(
                            /*preserve_current_follow_attempt=*/true));
  for (const AuthorityAnchor& anchor : anchors) {
    result = FirstFailure(std::move(result), actions_.DrainAssignment(anchor));
  }
  co_return result;
}

bycorf::Task<absl::Status>
NodeControlInstaller::RevokeSourceAuthorizationsTransition() {
  ControlTransitionGuard transition_guard(*this);
  InvalidateDirectiveAdmissions();
  absl::Status result = co_await WaitForDirectiveAdmissions();
  co_return FirstFailure(std::move(result),
                         co_await actions_.RevokeSourceAuthorizationsAndWait());
}

bycorf::Task<absl::Status>
NodeControlInstaller::SetPopulationReadinessTransition(
    std::optional<PopulationReadiness> readiness) {
  absl::Status result = co_await SetPopulationReadinessTransitionImpl(
      std::move(readiness), /*invalidate_directive_admissions=*/true);
  co_return result;
}

bycorf::Task<absl::Status>
NodeControlInstaller::SetPopulationReadinessTransitionImpl(
    std::optional<PopulationReadiness> readiness,
    bool invalidate_directive_admissions) {
  if (storage_failed_ && readiness.has_value()) {
    co_return absl::FailedPreconditionError(
        "storage failed during this boot; population readiness requires "
        "restart recovery");
  }
  const std::shared_ptr<const ServingState> before = topology_.Current();
  if (before == nullptr) {
    co_return absl::FailedPreconditionError(
        "population readiness requires an installed serving state");
  }
  bool matched = !readiness.has_value();
  if (readiness.has_value()) {
    const NodeDescriptor* self = before->Self();
    const PreparedGroupControlIdentity* control_group =
        FindControlGroup(readiness->group_id_);
    const PreparedMemberAssignment* local_member =
        self == nullptr || control_group == nullptr
            ? nullptr
            : FindMemberAssignment(*control_group, self->node_id_);
    if (control_group != nullptr && local_member != nullptr &&
        local_member->assignment_id_ == readiness->assignment_id_ &&
        control_group->group_term_ == readiness->group_term_ &&
        control_group->manifest_revision_ == readiness->manifest_revision_ &&
        control_group->manifest_digest_ == readiness->manifest_digest_ &&
        control_group->partition_replication_epoch_ ==
            readiness->partition_replication_epoch_) {
      matched = true;
    }
  }
  const std::shared_ptr<const ServingState> next =
      WithPopulationReadiness(*before, matched ? readiness : std::nullopt);
  if (next == nullptr) {
    co_return absl::InternalError("cannot publish local population readiness");
  }

  std::vector<AuthorityAnchor> owner_drains;
  bool changed = false;
  bool readiness_lost = false;
  for (const GroupView& old_group : before->Groups()) {
    if (!LocalMember(*before, old_group)) continue;
    const GroupView* new_group = next->FindGroup(old_group.group_id_);
    if (new_group == nullptr ||
        new_group->population_ready_ != old_group.population_ready_) {
      changed = true;
    }
    if (old_group.population_ready_ &&
        (new_group == nullptr || !new_group->population_ready_)) {
      // A replica may export a rebuild source even though it owns no serving
      // lease. Every local member's proof loss revokes that capability; only
      // a serving owner has request/lease state that also needs a drain.
      readiness_lost = true;
      if (SameLocalAssignment(*before, old_group)) {
        owner_drains.push_back(AnchorFor(old_group));
      }
    }
  }
  if (!changed) {
    co_return matched
        ? absl::OkStatus()
        : absl::FailedPreconditionError(
              "population proof does not match the installed local control");
  }
  const bool revoking = readiness_lost;
  if (revoking) {
    if (invalidate_directive_admissions) InvalidateDirectiveAdmissions();
    if (!owner_drains.empty()) {
      authority_.InvalidateLeases();
      for (const AuthorityAnchor& anchor : owner_drains) {
        RetireLeaseSchedule(anchor.group_id_);
      }
    }
  }
  ControlTransitionGuard transition_guard(*this, revoking);
  topology_.Publish(next);
  for (const AuthorityAnchor& anchor : owner_drains) {
    RememberDrain(before, anchor);
  }

  absl::Status result =
      matched
          ? absl::OkStatus()
          : absl::FailedPreconditionError(
                "population proof does not match the installed local control");
  if (revoking) {
    if (invalidate_directive_admissions) {
      result = FirstFailure(std::move(result),
                            co_await WaitForDirectiveAdmissions());
    }
    if (!owner_drains.empty()) {
      result = FirstFailure(std::move(result),
                            co_await actions_.RevokeExpirationAuthority());
    }
    result =
        FirstFailure(std::move(result),
                     co_await actions_.RevokeSourceAuthorizationsAndWait());
    for (const AuthorityAnchor& anchor : owner_drains) {
      result =
          FirstFailure(std::move(result), actions_.DrainAssignment(anchor));
    }
  }
  co_return result;
}

absl::Status NodeControlInstaller::SetStorageReady(bool ready) {
  if (storage_ready_ == ready) return absl::OkStatus();
  if (storage_failed_) {
    return absl::FailedPreconditionError(
        "storage failed during this boot and cannot become ready before "
        "restart");
  }
  if (!ready && actions_.ReceivesDirectives()) {
    return absl::FailedPreconditionError(
        "runtime storage loss requires the asynchronous NodeControl "
        "transition");
  }
  storage_ready_ = ready;
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current == nullptr) return absl::OkStatus();

  absl::Status result = absl::OkStatus();
  if (!ready) {
    InvalidateDirectiveAdmissions();
    authority_.InvalidateAll();
    RetireAllLeaseSchedules();
    result = actions_.RevokeSourceAuthorizations();
  }
  const std::shared_ptr<const ServingState> changed =
      WithStorageReady(*current, ready);
  if (changed == nullptr) {
    return FirstFailure(
        std::move(result),
        absl::InternalError("cannot publish storage readiness"));
  }
  topology_.Publish(changed);
  if (!ready) {
    for (const GroupView& group : current->Groups()) {
      if (group.primary_node_index_ != current->SelfNodeIndex()) continue;
      const AuthorityAnchor anchor = AnchorFor(group);
      RememberDrain(current, anchor);
      result =
          FirstFailure(std::move(result), actions_.DrainAssignment(anchor));
    }
  }
  return result;
}

bycorf::Task<absl::Status>
NodeControlInstaller::LoseStorageReadinessTransition() {
  if (storage_failed_) {
    if (storage_loss_result_.has_value()) co_return *storage_loss_result_;
    co_return absl::UnavailableError(
        "storage-loss cleanup is already in progress");
  }

  ControlTransitionGuard transition_guard(*this);

  // The latch and authority fence precede every suspension. In particular,
  // startup-style rebuild admission normally tolerates storage_ready=false;
  // the separate failure latch prevents that rule from reopening this boot.
  storage_failed_ = true;
  storage_ready_ = false;
  InvalidateDirectiveAdmissions();
  authority_.InvalidateAll();
  RetireAllLeaseSchedules();
  const std::shared_ptr<const ServingState> current = topology_.Current();
  const std::vector<AuthorityAnchor> anchors = RememberCurrentLocalDrains();

  absl::Status result = absl::OkStatus();
  if (current != nullptr) {
    const std::shared_ptr<const ServingState> changed =
        WithStorageReady(*current, false);
    if (changed == nullptr) {
      result = absl::InternalError("cannot publish lost storage readiness");
    } else {
      topology_.Publish(changed);
    }
  }

  result =
      FirstFailure(std::move(result), co_await WaitForDirectiveAdmissions());
  // A transition already past its own admission barrier can still be inside
  // the ReplicationManager adapter. Join every transition that crossed the
  // storage-loss cut before the final revoke/cancel pass, so none can publish
  // a target or source capability after this terminal barrier returns.
  result = FirstFailure(
      std::move(result),
      co_await WaitForControlTransitionsBefore(transition_guard.id()));
  result = FirstFailure(std::move(result),
                        co_await actions_.RevokeExpirationAuthority());
  result = FirstFailure(std::move(result),
                        co_await actions_.RevokeSourceAuthorizationsAndWait());
  // Admissions that had already returned no longer own a token, but their
  // target population (including a Ready online tail) may still be running.
  // Storage loss is terminal for this boot, so use the same preserve-nothing
  // join as graceful process shutdown rather than session loss's
  // in-progress-only cancellation.
  result = FirstFailure(std::move(result),
                        co_await actions_.CancelPopulationForShutdown());
  for (const AuthorityAnchor& anchor : anchors) {
    result = FirstFailure(std::move(result), actions_.DrainAssignment(anchor));
  }
  result =
      FirstFailure(std::move(result), co_await WaitForPendingDrains(anchors));
  storage_loss_result_ = std::move(result);
  co_return *storage_loss_result_;
}

}  // namespace lavik::cluster
