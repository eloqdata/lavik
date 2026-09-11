#include "keylane/cluster/node_control.h"

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
#include "celer/io/storage.h"
#include "celer/runtime/worker.h"

namespace keylane::cluster {
namespace {

// Celer's relative sleep uses CLOCK_MONOTONIC, which pauses across host
// suspend. Rechecking a CLOCK_BOOTTIME deadline in short slices bounds the
// post-resume source-capability cleanup delay instead of preserving the
// remainder of an arbitrarily long lease. Request admission itself checks
// CLOCK_BOOTTIME synchronously and has no such delay.
constexpr auto kLeaseExpiryRecheckInterval = std::chrono::milliseconds(25);

MonotonicDuration LeaseExpiryRecheckInterval(MonotonicDuration grant) {
  const MonotonicDuration quarter = std::max(grant / 4, MonotonicDuration{1});
  return std::min(quarter,
                  std::chrono::duration_cast<MonotonicDuration>(
                      kLeaseExpiryRecheckInterval));
}

AuthorityAnchor AnchorFor(const GroupView& group) {
  return AuthorityAnchor{
      .group_id_ = group.group_id_,
      .assignment_id_ = group.assignment_id_,
      .group_term_ = group.group_term_,
      .authority_version_ = group.authority_version_,
      .grant_revision_ = group.grant_revision_,
  };
}

auto CounterTuple(const AuthorityAnchor& anchor) {
  return std::tuple{anchor.group_term_, anchor.authority_version_,
                    anchor.grant_revision_};
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

// Rebuilds an immutable state while preserving every semantic field and the
// worker stripe layout. Mutators are used only for local readiness and a
// committed fence, keeping those transitions atomic at TopologyCache.
template <typename Mutate>
std::shared_ptr<const ServingState> RebuildState(const ServingState& state,
                                                 Mutate&& mutate) {
  ServingStateBuilder builder;
  builder.SetTopologyEpoch(state.topology_epoch())
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

}  // namespace

NodeDirectiveCompletion NodeDirectiveCompletion::Rejected(absl::Status result) {
  if (result.ok()) {
    result = absl::InternalError(
        "a rejected directive completion cannot contain success");
  }
  auto terminal =
      std::make_shared<const TerminalResult>(std::move(result));
  return NodeDirectiveCompletion(
      [terminal = std::move(terminal)]() { return *terminal; }, false,
      ResultPollTag{});
}

NodeDirectiveCompletion NodeDirectiveCompletion::StartedTerminal(
    absl::Status result) {
  TerminalResult terminal_result = result.ok()
                                       ? TerminalResult(std::string{})
                                       : TerminalResult(std::move(result));
  return StartedTerminalResult(std::move(terminal_result));
}

NodeDirectiveCompletion NodeDirectiveCompletion::StartedTerminalResult(
    TerminalResult result) {
  auto terminal =
      std::make_shared<const TerminalResult>(std::move(result));
  return NodeDirectiveCompletion(
      [terminal = std::move(terminal)]() { return *terminal; }, true,
      ResultPollTag{});
}

NodeDirectiveCompletion NodeDirectiveCompletion::FromResultPoll(
    ResultPoll poll) {
  return NodeDirectiveCompletion(std::move(poll), true, ResultPollTag{});
}

std::optional<NodeDirectiveCompletion::TerminalResult>
NodeDirectiveCompletion::terminal_result() const {
  if (!result_poll_) {
    return TerminalResult(absl::FailedPreconditionError(
        "directive completion handle is empty"));
  }
  return result_poll_();
}

std::optional<absl::Status> NodeDirectiveCompletion::result() const {
  std::optional<TerminalResult> terminal = terminal_result();
  if (!terminal.has_value()) return std::nullopt;
  return terminal->ok() ? absl::OkStatus() : terminal->status();
}

celer::Task<absl::Status> NodeDirectiveCompletion::Await() const {
  for (;;) {
    if (std::optional<absl::Status> terminal = result(); terminal.has_value()) {
      co_return *terminal;
    }
    celer::Worker* worker = celer::ThisWorker().self_;
    if (worker == nullptr) {
      co_return absl::FailedPreconditionError(
          "pending directive completion requires a Celer worker");
    }
    const absl::Status waited =
        co_await celer::SleepFor(*worker, std::chrono::milliseconds(10));
    if (!waited.ok()) co_return waited;
  }
}

absl::Status NullNodeControlActions::RevokeSourceAuthorizations() {
  return absl::OkStatus();
}

celer::Task<absl::Status>
NodeControlActions::RevokeSourceAuthorizationsAndWait() {
  co_return RevokeSourceAuthorizations();
}

celer::Task<absl::Status>
NodeControlActions::ClearSourceAuthorizationsForSessionReplacementAndWait(
    bool /*preserve_established_exports*/) {
  co_return RevokeSourceAuthorizations();
}

celer::Task<absl::Status> NodeControlActions::ReconcilePopulation(
    std::optional<PopulationReadiness> /*desired*/,
    bool /*population_transition_expected*/) {
  co_return absl::OkStatus();
}

celer::Task<absl::Status> NodeControlActions::CancelInProgressPopulation() {
  co_return absl::OkStatus();
}

celer::Task<absl::Status> NodeControlActions::CancelPopulationForShutdown() {
  co_return co_await CancelInProgressPopulation();
}

celer::Task<NodeDirectiveCompletion> NodeControlActions::StartDirective(
    NodeDirective directive) {
  co_return NodeDirectiveCompletion::StartedTerminal(
      co_await ApplyDirective(std::move(directive)));
}

celer::Task<absl::Status> NullNodeControlActions::ApplyDirective(
    NodeDirective /*directive*/) {
  co_return absl::FailedPreconditionError(
      "the static control adapter does not execute Meta directives");
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
  if (basis.source_meta_applied_index_ >
      projection_basis_->source_meta_applied_index_) {
    return absl::FailedPreconditionError(
        "control message depends on a future Meta projection");
  }
  if (basis.projection_hash_ != projection_basis_->projection_hash_) {
    return absl::FailedPreconditionError(
        "control message projection hash is not current");
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
  const auto group =
      std::find_if(control_groups_.begin(), control_groups_.end(),
                   [group_id](const PreparedGroupControlIdentity& candidate) {
                     return candidate.group_id_ == group_id;
                   });
  return group == control_groups_.end() ? nullptr : &*group;
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
  for (const PreparedGroupControlIdentity& control_group : control_groups_) {
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
        "directive group is absent from the installed FDS identities");
  }
  if (control_group->group_term_ != directive.anchor_.group_term_ ||
      control_group->authority_version_ !=
          directive.anchor_.authority_version_ ||
      control_group->grant_revision_ != directive.anchor_.grant_revision_ ||
      control_group->partition_replication_epoch_ !=
          directive.partition_replication_epoch_) {
    return absl::FailedPreconditionError(
        "directive authority counters are not current");
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
      initializes_empty ||
      directive.kind_ == NodeDirective::Kind::kPromotionPrepare) {
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
  const bool promotion =
      directive.kind_ == NodeDirective::Kind::kPromotionPrepare;
  if (directive.force_) {
    return absl::InvalidArgumentError(
        "directive force is reserved in control protocol v1");
  }
  if (promotion != directive.promotion_prepare_.has_value()) {
    return absl::InvalidArgumentError(
        "directive kind does not match its decoded payload schema");
  }
  const bool initialization_payload_valid =
      directive.payload_.size() == 40 &&
      std::all_of(directive.payload_.begin(), directive.payload_.end(),
                  [](unsigned char value) {
                    return (value >= '0' && value <= '9') ||
                           (value >= 'a' && value <= 'f');
                  });
  if (initializes_empty
          ? (!initialization_payload_valid ||
             !directive.preconditions_.empty())
          : (!directive.payload_.empty() ||
             !directive.preconditions_.empty())) {
    return absl::InvalidArgumentError(
        "directive kind does not match its opaque field schema");
  }
  if (promotion) {
    const PromotionPrepareInput& input = *directive.promotion_prepare_;
    const bool zero_exclusion =
        std::all_of(input.old_authority_exclusion_hash_.begin(),
                    input.old_authority_exclusion_hash_.end(),
                    [](std::uint8_t byte) { return byte == 0; });
    if (input.parent_history_id_.empty() ||
        input.parent_history_id_ !=
            directive.source_replication_history_id_.ToHexString() ||
        input.required_applied_next_lsns_.empty() ||
        std::any_of(input.required_applied_next_lsns_.begin(),
                    input.required_applied_next_lsns_.end(),
                    [](std::uint64_t cursor) { return cursor == 0; }) ||
        input.excluded_group_term_ != directive.anchor_.group_term_ ||
        zero_exclusion) {
      return absl::InvalidArgumentError(
          "promotion prepare payload or preconditions are incomplete");
    }
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
      !initializes_empty &&
      directive.kind_ != NodeDirective::Kind::kPromotionPrepare) {
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
    if (!directive.storage_mutating_ ||
        (!replay_lookup && local_serving_owner)) {
      return absl::FailedPreconditionError(
          "population mutation requires a non-serving local target assignment");
    }
    if (!replay_lookup &&
        (DrainPending(directive.anchor_.group_id_) ||
         current->GroupInFlightCount(directive.anchor_.group_id_) != 0)) {
      return absl::UnavailableError(
          "population target still has in-flight requests");
    }
  } else if (directive.kind_ == NodeDirective::Kind::kPromotionPrepare) {
    // Grantless FDS deliberately omits the group from ServingState. The
    // control identity checked above still binds candidate, term, manifest,
    // and population epoch; ReplicationManager owns the boot-local ReadyToken
    // check before durability work begins.
    if (!directive.storage_mutating_ || group != nullptr) {
      return absl::FailedPreconditionError(
          "promotion prepare requires a fenced ownerless group");
    }
    if (DrainPending(directive.anchor_.group_id_) ||
        current->GroupInFlightCount(directive.anchor_.group_id_) != 0) {
      return absl::UnavailableError(
          "promotion candidate still has in-flight requests");
    }
  } else if (directive.kind_ == NodeDirective::Kind::kAuthorizeSource &&
             DrainPending(directive.anchor_.group_id_)) {
    return absl::UnavailableError(
        "source authorization waits for the retired assignment to drain");
  } else if (directive.storage_mutating_) {
    return absl::InvalidArgumentError(
        "only population directives may mutate storage");
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

celer::Task<absl::Status> NodeControlInstaller::WaitForDirectiveAdmissions() {
  celer::Worker* worker = celer::ThisWorker().self_;
  if (worker == nullptr && directive_admissions_in_flight_ != 0) {
    co_return absl::FailedPreconditionError(
        "directive admission drain requires a Celer worker");
  }
  while (directive_admissions_in_flight_ != 0) {
    co_await celer::Yield(*worker);
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
  assert(owner_->active_control_transitions_.erase(id_) == 1);
  --owner_->source_revocation_transitions_;
}

celer::Task<absl::Status> NodeControlInstaller::WaitForControlTransitionsBefore(
    std::uint64_t transition_id) {
  celer::Worker* worker = celer::ThisWorker().self_;
  while (!active_control_transitions_.empty() &&
         *active_control_transitions_.begin() < transition_id) {
    if (worker == nullptr) {
      co_return absl::FailedPreconditionError(
          "control transition drain requires a Celer worker");
    }
    co_await celer::Yield(*worker);
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

celer::Task<absl::Status> NodeControlInstaller::WaitForPendingDrains(
    std::span<const AuthorityAnchor> anchors) {
  while (std::any_of(anchors.begin(), anchors.end(),
                     [this](const AuthorityAnchor& anchor) {
                       return DrainPending(anchor.group_id_);
                     })) {
    celer::Worker* worker = celer::ThisWorker().self_;
    if (worker == nullptr) {
      co_return absl::FailedPreconditionError(
          "asynchronous assignment drain requires a Celer worker");
    }
    const absl::Status waited =
        co_await celer::SleepFor(*worker, std::chrono::milliseconds(1));
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
  return CounterTuple(anchor) <= std::tuple{it->second.group_term_,
                                            it->second.authority_version_,
                                            it->second.grant_revision_};
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
    group.authority_version_ = anchor.authority_version_;
    group.grant_revision_ = anchor.grant_revision_;
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
  // Static topology has no directive producer, but advancing the token keeps
  // this lower-level entry fail-closed if a caller violates that assembly
  // contract while an async admission is suspended.
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
  if (!prepared_state.control_groups_.empty()) {
    std::set<std::string> group_ids;
    for (const PreparedGroupControlIdentity& control_group :
         prepared_state.control_groups_) {
      if (control_group.group_id_.empty() ||
          !group_ids.insert(control_group.group_id_).second) {
        return absl::InvalidArgumentError(
            "prepared FDS control group identity is empty or duplicated");
      }
      const bool empty_manifest =
          std::all_of(control_group.manifest_digest_.begin(),
                      control_group.manifest_digest_.end(),
                      [](std::uint8_t byte) { return byte == 0; });
      if ((control_group.manifest_revision_ == 0) != empty_manifest) {
        return absl::InvalidArgumentError(
            "prepared FDS manifest identity is partial");
      }
      std::set<NodeId> member_ids;
      for (const PreparedMemberAssignment& member : control_group.members_) {
        if (member.node_id_.empty() || member.assignment_id_.empty() ||
            !member_ids.insert(member.node_id_).second) {
          return absl::InvalidArgumentError(
              "prepared FDS member identity is incomplete or duplicated");
        }
      }
      const GroupView* serving_group =
          prepared_state.serving_state_->FindGroup(control_group.group_id_);
      if (serving_group != nullptr &&
          (serving_group->group_term_ != control_group.group_term_ ||
           serving_group->authority_version_ !=
               control_group.authority_version_ ||
           serving_group->grant_revision_ != control_group.grant_revision_ ||
           serving_group->config_epoch_ != control_group.config_epoch_ ||
           serving_group->manifest_revision_ !=
               control_group.manifest_revision_)) {
        return absl::InvalidArgumentError(
            "prepared FDS control identity disagrees with its serving group");
      }
    }
    for (const GroupView& serving_group :
         prepared_state.serving_state_->Groups()) {
      if (!group_ids.contains(serving_group.group_id_)) {
        return absl::InvalidArgumentError(
            "prepared FDS serving group has no control identity");
      }
    }
  }
  if (projection_basis_.has_value()) {
    if (projection_basis.source_meta_applied_index_ <
        projection_basis_->source_meta_applied_index_) {
      return absl::OutOfRangeError("full desired state source index regressed");
    }
    if (projection_basis.source_meta_applied_index_ ==
        projection_basis_->source_meta_applied_index_) {
      if (projection_basis == *projection_basis_ && object_hash_.has_value() &&
          prepared_state.object_hash_ == *object_hash_) {
        // Exact replay can follow a control-session refresh. Source admission
        // was cleared at disconnect, but a population export that was already
        // ONLINE remains safe while the old lease is invalid and this byte-
        // exact desired state is being re-established.
        effects->preserve_established_exports_ = true;
        return absl::OkStatus();
      }
      // One applied index cannot name two objects. Drop all memory authority;
      // retaining the connection would let a corrupt or Byzantine peer keep
      // extending a lease after equivocation.
      authority_.InvalidateAll();
      effects->revoke_sources_ = true;
      return absl::DataLossError(
          "full desired state equivocated at one Meta applied index");
    }
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
    if (next->topology_epoch() < before->topology_epoch()) {
      return absl::FailedPreconditionError("topology epoch regressed");
    }
    for (const PreparedGroupControlIdentity& old_group : control_groups_) {
      const auto new_group = std::find_if(
          prepared_state.control_groups_.begin(),
          prepared_state.control_groups_.end(),
          [&old_group](const PreparedGroupControlIdentity& candidate) {
            return candidate.group_id_ == old_group.group_id_;
          });
      if (new_group == prepared_state.control_groups_.end()) continue;
      if (new_group->partition_replication_epoch_ <
          old_group.partition_replication_epoch_) {
        return absl::FailedPreconditionError(absl::StrCat(
            "partition replication epoch regressed for group '",
            old_group.group_id_, "'"));
      }
      const bool shares_member_incarnation =
          std::any_of(old_group.members_.begin(), old_group.members_.end(),
                      [&](const PreparedMemberAssignment& old_member) {
                        return std::any_of(
                            new_group->members_.begin(),
                            new_group->members_.end(),
                            [&](const PreparedMemberAssignment& new_member) {
                              return old_member == new_member;
                            });
                      });
      if (shares_member_incarnation &&
          (new_group->config_epoch_ < old_group.config_epoch_ ||
           new_group->group_term_ < old_group.group_term_ ||
           new_group->authority_version_ < old_group.authority_version_ ||
           new_group->grant_revision_ < old_group.grant_revision_ ||
           new_group->manifest_revision_ < old_group.manifest_revision_)) {
        return absl::FailedPreconditionError(absl::StrCat(
            "same-membership control counter regressed for group '",
            old_group.group_id_, "'"));
      }
      if (shares_member_incarnation &&
          new_group->manifest_revision_ == old_group.manifest_revision_ &&
          new_group->manifest_digest_ != old_group.manifest_digest_) {
        return absl::DataLossError(absl::StrCat(
            "same manifest revision changed digest for group '",
            old_group.group_id_, "'"));
      }
    }
    for (const GroupView& old_group : before->Groups()) {
      const GroupView* new_group = next->FindGroup(old_group.group_id_);
      if (new_group == nullptr ||
          new_group->assignment_id_ != old_group.assignment_id_) {
        continue;
      }
      if (new_group->config_epoch_ < old_group.config_epoch_ ||
          new_group->group_term_ < old_group.group_term_ ||
          new_group->authority_version_ < old_group.authority_version_ ||
          new_group->grant_revision_ < old_group.grant_revision_ ||
          new_group->manifest_revision_ < old_group.manifest_revision_) {
        return absl::FailedPreconditionError(absl::StrCat(
            "same-assignment control counter regressed for group '",
            old_group.group_id_, "'"));
      }
    }
  }

  std::vector<AuthorityAnchor> retired;
  bool revoke_sources = false;
  bool preserve_established_exports = false;
  if (before != nullptr) {
    for (const GroupView& old_group : before->Groups()) {
      if (!SameLocalAssignment(*before, old_group)) continue;
      const GroupView* new_group = next->FindGroup(old_group.group_id_);
      const PreparedGroupControlIdentity* old_control =
          FindControlGroup(old_group.group_id_);
      const auto new_control = std::find_if(
          prepared_state.control_groups_.begin(),
          prepared_state.control_groups_.end(),
          [&](const PreparedGroupControlIdentity& candidate) {
            return candidate.group_id_ == old_group.group_id_;
          });
      // Desired-state projection intentionally excludes the boot-local
      // ReadyToken. A live replacement therefore normalizes population_ready
      // to false until the heartbeat reapplies the proof. An already-online
      // native export may span only that normalization, and only when the
      // complete durable group/member/population identity remains byte-exact.
      preserve_established_exports =
          old_group.primary_node_index_ == before->SelfNodeIndex() &&
          new_group != nullptr &&
          new_group->primary_node_index_ == next->SelfNodeIndex() &&
          new_group->granted_ == old_group.granted_ &&
          new_group->storage_ready_ == old_group.storage_ready_ &&
          old_control != nullptr &&
          new_control != prepared_state.control_groups_.end() &&
          *old_control == *new_control;
      const bool authority_changed =
          new_group == nullptr ||
          new_group->primary_node_index_ != next->SelfNodeIndex() ||
          AnchorFor(*new_group) != AnchorFor(old_group) ||
          new_group->granted_ != old_group.granted_ ||
          new_group->population_ready_ != old_group.population_ready_ ||
          new_group->storage_ready_ != old_group.storage_ready_;
      if (authority_changed) {
        retired.push_back(AnchorFor(old_group));
        revoke_sources = true;
      }
    }
  }

  // Invalidation precedes publication, closing the race in which a request
  // could observe old topology after the controller has accepted a revoking
  // assignment change.
  if (projection_basis_.has_value() && projection_basis_->projection_hash_ !=
                                           projection_basis.projection_hash_) {
    // Every live lease names the node-specific semantic projection. A higher
    // diagnostic applied index with the same projection is harmless; a hash
    // change requires a fresh challenge even when authority counters happen
    // to be unchanged.
    authority_.InvalidateLeases();
  }
  authority_.InvalidateAnchorsChanged(before.get(), *next);
  topology_.Publish(next);
  projection_basis_ = projection_basis;
  object_hash_ = prepared_state.object_hash_;
  control_groups_ = std::move(prepared_state.control_groups_);
  for (const AuthorityAnchor& anchor : retired) {
    RememberDrain(before, anchor);
  }
  effects->revoke_sources_ = revoke_sources;
  effects->preserve_established_exports_ = preserve_established_exports;
  effects->retired_ = std::move(retired);
  return absl::OkStatus();
}

celer::Task<absl::Status> NodeControlInstaller::InstallFullStateTransition(
    PreparedFullState prepared_state, ProjectionBasis projection_basis,
    bool local_population_transition_expected) {
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

  // Every replacement clears new export admission. An exact live FDS may keep
  // an established ONLINE export quarantined until this replacement validates
  // the same Group and population; any stronger transition joins and revokes
  // the old session. The async seam makes FullStateApplied a real join boundary
  // instead of an enqueue receipt. Target population reconciliation runs under
  // the same exclusion counter: no directive may start after FDS publication
  // but before an invalidated native session and partial storage root are
  // joined.
  absl::Status actions = co_await WaitForDirectiveAdmissions();
  actions = FirstFailure(
      std::move(actions),
      co_await actions_
          .ClearSourceAuthorizationsForSessionReplacementAndWait(
              local.ok() && effects.preserve_established_exports_));
  // Even an internally inconsistent FDS must leave the local population
  // fail-closed and join the old session's work before the failed transfer
  // tears down its socket.
  // Storage loss may have raced the preceding session-clear await. The FDS is
  // still useful as fail-closed topology truth, but this boot must never feed
  // a non-empty desired population back into ReplicationManager afterwards.
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
    if (storage_failed_) {
      return absl::FailedPreconditionError(
          "storage failed during this boot; lease recovery requires restart");
    }
    if (RejectedByFence(message.anchor_)) {
      return absl::FailedPreconditionError(
          "lease grant does not advance the in-memory fence floor");
    }
    if (message.granted_duration_ <= MonotonicDuration::zero()) {
      return absl::InvalidArgumentError(
          "lease grant duration must be positive");
    }
    if (source_revocation_transitions_ != 0 ||
        DrainPending(message.anchor_.group_id_)) {
      return absl::UnavailableError(
          "authority cleanup or retired assignment requests have not drained");
    }
    const GroupView* group =
        topology_.Current()->FindGroup(message.anchor_.group_id_);
    if (group == nullptr || !group->granted_ || !group->population_ready_ ||
        !group->storage_ready_) {
      return absl::FailedPreconditionError(
          "lease grant targets an unready or fenced assignment");
    }
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

celer::Task<absl::Status> NodeControlInstaller::ApplyLeaseGrantTransition(
    const AuthorityMessage& message) {
  if (message.kind_ != AuthorityMessage::Kind::kLeaseGrant) {
    co_return absl::InvalidArgumentError(
        "Meta lease transition received a non-grant authority message");
  }
  celer::Worker* worker = celer::ThisWorker().self_;
  if (worker == nullptr) {
    co_return absl::FailedPreconditionError(
        "Meta lease transition requires a Celer worker");
  }
  const MonotonicTime deadline = SaturatingLeaseDeadline(message);
  MonotonicTime now = LeaseClockNow();
  if (deadline <= now) {
    co_return absl::DeadlineExceededError(
        "lease grant expired before it could be installed");
  }

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
    if (deadline <= now) {
      co_return absl::DeadlineExceededError(
          "lease grant expired during prior-authority cleanup");
    }
  }

  if (absl::Status installed = ApplyAuthority(message, LeaseClockNow());
      !installed.ok()) {
    co_return installed;
  }
  if (deadline != MonotonicTime::max()) {
    std::shared_ptr<LeaseExpirySchedule> schedule;
    bool spawn_timer = false;
    const auto existing =
        lease_expiry_schedules_.find(message.anchor_.group_id_);
    if (existing == lease_expiry_schedules_.end() ||
        !existing->second->active_) {
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
      const MonotonicDuration old_recheck_interval =
          schedule->recheck_interval_;
      const MonotonicDuration recheck_interval =
          LeaseExpiryRecheckInterval(message.granted_duration_);
      schedule->session_ = message.session_;
      schedule->anchor_ = message.anchor_;
      schedule->deadline_ = deadline;
      schedule->recheck_interval_ = recheck_interval;
      if (deadline < old_deadline ||
          recheck_interval < old_recheck_interval) {
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
  }
  co_return absl::OkStatus();
}

celer::Task<absl::Status> NodeControlInstaller::ExpireLeaseAt(
    std::shared_ptr<LeaseExpirySchedule> schedule,
    std::uint64_t timer_generation,
    std::shared_ptr<const LeaseTimerLifetime> lifetime) {
  // `lifetime` deliberately stays in this coroutine frame across every
  // suspension. Its destruction makes the public outlive-worker contract
  // mechanically checkable without coupling this module to Worker shutdown.
  (void)lifetime;
  celer::Worker* worker = celer::ThisWorker().self_;
  if (worker == nullptr) {
    co_return absl::FailedPreconditionError(
        "lease expiration requires a Celer worker");
  }
  while (schedule->active_ && schedule->timer_generation_ == timer_generation) {
    const MonotonicTime deadline = schedule->deadline_;
    const MonotonicTime now = LeaseClockNow();
    if (now < deadline) {
      const absl::Status slept = co_await celer::SleepFor(
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

celer::Task<absl::Status>
NodeControlInstaller::FinishExpiredLeaseTransition(
    std::shared_ptr<LeaseExpirySchedule> schedule, MonotonicTime now) {
  if (!schedule->active_) co_return absl::OkStatus();
  if (now < schedule->deadline_) {
    co_return absl::FailedPreconditionError(
        "lease expiration transition ran before its exact deadline");
  }
  const bool expired = authority_.ExpireLease(
      schedule->session_, schedule->anchor_, schedule->deadline_, now);
  schedule->active_ = false;
  const auto installed =
      lease_expiry_schedules_.find(schedule->anchor_.group_id_);
  if (installed != lease_expiry_schedules_.end() &&
      installed->second == schedule) {
    lease_expiry_schedules_.erase(installed);
  }
  if (!expired) co_return absl::OkStatus();
  const AuthorityAnchor anchor = schedule->anchor_;
  const std::shared_ptr<const ServingState> current = topology_.Current();
  if (current != nullptr) {
    const GroupView* group = current->FindGroup(anchor.group_id_);
    if (group != nullptr && AnchorFor(*group) == anchor) {
      RememberDrain(current, anchor);
    }
  }
  ControlTransitionGuard transition_guard(*this);
  InvalidateDirectiveAdmissions();
  absl::Status result = co_await WaitForDirectiveAdmissions();
  // Lease expiry removes mutation authority and prevents every new source
  // handshake, but an exact population export that is already ONLINE may stay
  // quarantined until renewal. A committed fence or population-identity change
  // uses the stronger revocation path and joins it.
  result = FirstFailure(
      std::move(result),
      co_await actions_.ClearSourceAuthorizationsForSessionReplacementAndWait(
          /*preserve_established_exports=*/true));
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
      std::tuple{floor.group_term_, floor.authority_version_,
                 floor.grant_revision_} < CounterTuple(message.anchor_)) {
    floor = RejectThrough{
        .assignment_id_ = message.anchor_.assignment_id_,
        .group_term_ = message.anchor_.group_term_,
        .authority_version_ = message.anchor_.authority_version_,
        .grant_revision_ = message.anchor_.grant_revision_,
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

celer::Task<absl::Status> NodeControlInstaller::ApplyFenceTransition(
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
  auto transitioned = ApplyFenceLocal(message);
  if (!transitioned.ok()) co_return transitioned.status();
  absl::Status result = co_await WaitForDirectiveAdmissions();
  result = FirstFailure(std::move(result),
                        co_await actions_.RevokeSourceAuthorizationsAndWait());
  result = FirstFailure(std::move(result),
                        co_await actions_.CancelInProgressPopulation());
  if (*transitioned) {
    result = FirstFailure(std::move(result),
                          actions_.DrainAssignment(message.anchor_));
  }
  const std::array<AuthorityAnchor, 1> anchor{message.anchor_};
  result =
      FirstFailure(std::move(result), co_await WaitForPendingDrains(anchor));
  co_return result;
}

celer::Task<NodeDirectiveCompletion> NodeControlInstaller::StartDirective(
    NodeDirective directive) {
  const auto terminal = [](absl::Status status) {
    return NodeDirectiveCompletion::Rejected(std::move(status));
  };
  // Re-reporting an exact completed result is not a storage mutation. Keep
  // all session/FDS/fence/identity checks, but do not require an already-Ready
  // owner to stop serving or drain client writes merely to repeat its result.
  // The adapter's lookup cannot start work; no match uses every normal guard.
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
  // FDS, or session-loss generation observed in between owns this attempt.
  if (admission_generation != directive_admission_generation_) {
    co_return terminal(absl::FailedPreconditionError(
        "directive admission was invalidated by a control transition"));
  }
  if (absl::Status valid = ValidateDirectiveForStart(directive); !valid.ok()) {
    co_return terminal(std::move(valid));
  }
  co_return co_await actions_.StartDirective(std::move(directive));
}

celer::Task<absl::Status> NodeControlInstaller::ApplyDirective(
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
  return absl::OkStatus();
}

celer::Task<absl::Status>
NodeControlInstaller::CancelPopulationForShutdownTransition() {
  ControlTransitionGuard transition_guard(*this);
  InvalidateDirectiveAdmissions();
  authority_.InvalidateAll();
  absl::Status result = co_await WaitForDirectiveAdmissions();
  co_return FirstFailure(std::move(result),
                         co_await actions_.CancelPopulationForShutdown());
}

celer::Task<absl::Status> NodeControlInstaller::LoseSessionTransition(
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
  result = FirstFailure(
      std::move(result),
      co_await actions_
          .ClearSourceAuthorizationsForSessionReplacementAndWait(
              /*preserve_established_exports=*/true));
  result = FirstFailure(std::move(result),
                        co_await actions_.CancelInProgressPopulation());
  for (const AuthorityAnchor& anchor : anchors) {
    result = FirstFailure(std::move(result), actions_.DrainAssignment(anchor));
  }
  co_return result;
}

celer::Task<absl::Status>
NodeControlInstaller::RevokeSourceAuthorizationsTransition() {
  ControlTransitionGuard transition_guard(*this);
  InvalidateDirectiveAdmissions();
  absl::Status result = co_await WaitForDirectiveAdmissions();
  co_return FirstFailure(std::move(result),
                         co_await actions_.RevokeSourceAuthorizationsAndWait());
}

celer::Task<absl::Status>
NodeControlInstaller::SetPopulationReadinessTransition(
    std::optional<PopulationReadiness> readiness) {
  absl::Status result = co_await SetPopulationReadinessTransitionImpl(
      std::move(readiness), /*invalidate_directive_admissions=*/true);
  co_return result;
}

celer::Task<absl::Status>
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
              "population proof does not match the installed FDS");
  }
  const bool revoking = readiness_lost;
  if (revoking) {
    if (invalidate_directive_admissions) InvalidateDirectiveAdmissions();
    if (!owner_drains.empty()) authority_.InvalidateLeases();
  }
  ControlTransitionGuard transition_guard(*this, revoking);
  topology_.Publish(next);
  for (const AuthorityAnchor& anchor : owner_drains) {
    RememberDrain(before, anchor);
  }

  absl::Status result =
      matched ? absl::OkStatus()
              : absl::FailedPreconditionError(
                    "population proof does not match the installed FDS");
  if (revoking) {
    if (invalidate_directive_admissions) {
      result = FirstFailure(std::move(result),
                            co_await WaitForDirectiveAdmissions());
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

celer::Task<absl::Status>
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

}  // namespace keylane::cluster
