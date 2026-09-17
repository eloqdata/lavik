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

#include "lavik/meta/failover_reconciler.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <limits>
#include <map>
#include <mutex>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/cluster/lease_clock.h"
#include "lavik/fault_injection.h"
#include "lavik/meta/candidate_plan.h"
#include "lavik/meta/failover.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {
namespace {

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::ranges::all_of(value,
                             [](std::uint8_t byte) { return byte == 0; });
}

bool Terminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kCompleted ||
         lifecycle == MetaOperationLifecycle::kAborted;
}

bool WarmupComplete(const MetaFailoverPlannerContext& context) {
  if (context.observation_grace_ms_ == 0) return true;
  if (context.leadership_started_unix_ms_ >
      std::numeric_limits<std::int64_t>::max() -
          context.observation_grace_ms_) {
    return false;
  }
  return context.now_unix_ms_ >=
         context.leadership_started_unix_ms_ + context.observation_grace_ms_;
}

#if LAVIK_FAULTS_ENABLED
std::chrono::milliseconds TestPauseDelay(const char* variable) {
  const char* configured = std::getenv(variable);
  std::uint64_t delay_ms = 0;
  if (configured == nullptr) return std::chrono::milliseconds::zero();
  const char* end = configured + std::strlen(configured);
  const auto parsed = std::from_chars(configured, end, delay_ms);
  if (parsed.ec != std::errc{} || parsed.ptr != end || delay_ms == 0 ||
      delay_ms > 60'000) {
    return std::chrono::milliseconds::zero();
  }
  return std::chrono::milliseconds(delay_ms);
}
#endif

std::optional<MetaAssignmentId> AssignmentFor(
    const MetaTopologyGroupView& group, std::string_view node_id) {
  const auto found =
      std::ranges::find_if(group.members_, [&](const MetaGroupMember& member) {
        return member.node_id_ == node_id;
      });
  if (found == group.members_.end()) return std::nullopt;
  return found->assignment_id_;
}

template <typename Command>
void SetGroupAnchors(Command& command, const MetaTopologyGroupView& group) {
  command.expected_owner_node_id_ = group.record_.owner_;
  command.expected_owner_assignment_id_ =
      *AssignmentFor(group, group.record_.owner_);
  command.expected_membership_revision_ = group.revision_;
  command.expected_group_term_ = group.record_.group_term_;
  command.expected_population_manifest_revision_ =
      group.record_.population_manifest_revision_;
  command.expected_population_manifest_digest_ =
      group.record_.population_manifest_digest_;
  command.expected_partition_replication_epoch_ =
      group.record_.partition_replication_epoch_;
}

absl::StatusOr<MetaRequestId> NextId(
    const MetaFailoverPlannerContext& context) {
  if (!context.next_id_) {
    return absl::FailedPreconditionError(
        "failover planner has no id generator");
  }
  auto id = context.next_id_();
  if (!id.ok()) return id.status();
  if (IsZero(*id)) {
    return absl::FailedPreconditionError(
        "failover planner generated a zero id");
  }
  return *id;
}

bool ActiveGrantMatchesGroup(const MetaTopologyGroupView& group,
                             const MetaGroupAuthorityView& grant) {
  return grant.grant_.has_value() &&
         grant.group_term_ == group.record_.group_term_ &&
         grant.grant_->owner_ == group.record_.owner_;
}

bool ControlledDomainMatchesOwner(const MetaFailoverCompatibilityDomain& domain,
                                  const MetaTopologyGroupView& group,
                                  const MetaAssignmentId& owner_assignment) {
  return domain.source_group_term_ == group.record_.group_term_ &&
         domain.source_node_id_ == group.record_.owner_ &&
         domain.source_assignment_id_ == owner_assignment;
}

enum class SourceAvailability { kHealthy, kGrace, kUnavailable, kReplaced };
enum class CandidateAvailability { kHealthy, kWarmup, kUnavailable };

bool GraceExpired(std::int64_t since,
                  const MetaFailoverPlannerContext& context) {
  if (context.now_unix_ms_ < since) return false;
  if (since > std::numeric_limits<std::int64_t>::max() -
                  context.observation_grace_ms_) {
    return false;
  }
  return context.now_unix_ms_ >= since + context.observation_grace_ms_;
}

SourceAvailability SourceState(const MetaFailoverCandidateAction& action,
                               const MetaCommittedFacts& facts,
                               const MetaObservationStore& observations,
                               const MetaFailoverPlannerContext& context) {
  const auto& domain = action.domain_;
  const auto session = observations.SessionStateFor(domain.source_node_id_);
  if (!session.has_value()) {
    return WarmupComplete(context) ? SourceAvailability::kUnavailable
                                   : SourceAvailability::kGrace;
  }
  if (session->current_boot_id_ != domain.source_boot_id_ ||
      (session->current_history_id_.has_value() &&
       session->current_history_id_ !=
           std::optional(domain.source_history_id_))) {
    return SourceAvailability::kReplaced;
  }
  if (!session->current_history_id_.has_value()) {
    return WarmupComplete(context) ? SourceAvailability::kUnavailable
                                   : SourceAvailability::kGrace;
  }
  if (!session->connected_) {
    if (session->disconnected_boot_id_ ==
            std::optional(domain.source_boot_id_) &&
        session->disconnected_unix_ms_.has_value() &&
        !GraceExpired(*session->disconnected_unix_ms_, context)) {
      return SourceAvailability::kGrace;
    }
    return SourceAvailability::kUnavailable;
  }
  const auto latest = observations.LatestForNode(domain.source_node_id_, facts);
  if (!latest.has_value()) {
    // A same-incarnation reconnect purges the older generation's reports.
    // Give the replacement session only the remainder of the original
    // disconnect grace to produce its first heartbeat; adopting a session
    // does not itself establish that the source is healthy, and repeated
    // reconnects do not restart the clock.
    if (session->disconnected_boot_id_ ==
            std::optional(domain.source_boot_id_) &&
        session->disconnected_generation_.has_value() &&
        session->current_generation_ > *session->disconnected_generation_ &&
        session->disconnected_unix_ms_.has_value() &&
        !GraceExpired(*session->disconnected_unix_ms_, context)) {
      return SourceAvailability::kGrace;
    }
    return WarmupComplete(context) ? SourceAvailability::kUnavailable
                                   : SourceAvailability::kGrace;
  }
  if (latest->identity_.boot_incarnation_ != domain.source_boot_id_) {
    return SourceAvailability::kReplaced;
  }
  return GraceExpired(latest->received_unix_ms_, context)
             ? SourceAvailability::kUnavailable
             : SourceAvailability::kHealthy;
}

struct CandidateState {
  CandidateAvailability availability_ = CandidateAvailability::kWarmup;
  bool explicit_failure_ = false;
  std::optional<MetaCandidateProgressObs> progress_;
};

CandidateState UnavailableCandidate(bool explicit_failure = false) {
  return {.availability_ = CandidateAvailability::kUnavailable,
          .explicit_failure_ = explicit_failure,
          .progress_ = std::nullopt};
}

CandidateState WarmupCandidate() {
  return {.availability_ = CandidateAvailability::kWarmup,
          .progress_ = std::nullopt};
}

CandidateState HealthyCandidate(
    std::optional<MetaCandidateProgressObs> progress = std::nullopt) {
  return {.availability_ = CandidateAvailability::kHealthy,
          .progress_ = std::move(progress)};
}

CandidateState CurrentCandidateState(
    std::string_view group_id, const MetaFailoverTransition& transition,
    const MetaCommittedFacts& facts, const MetaObservationStore& observations,
    const MetaFailoverPlannerContext& context) {
  const MetaFailoverCandidateAction& action = *transition.candidate_action_;
  const auto session = observations.SessionStateFor(action.candidate_.node_id_);
  if (!session.has_value()) {
    return WarmupComplete(context) ? UnavailableCandidate() : WarmupCandidate();
  }
  if (!session->connected_ ||
      session->current_boot_id_ != action.candidate_.boot_id_) {
    return UnavailableCandidate(true);
  }
  if (observations
          .ActionFailedFor(transition.transition_id_, action.action_id_, facts,
                           context.now_unix_ms_)
          .has_value()) {
    return UnavailableCandidate(true);
  }

  const auto prepared = observations.CandidatePreparedFor(
      transition.transition_id_, action.action_id_, facts,
      context.now_unix_ms_);
  const bool same_boot_disconnect = session->disconnected_boot_id_ ==
                                    std::optional(action.candidate_.boot_id_);
  // ReplaceHeartbeat clears a pre-action disconnect only while committed
  // state has no action for this node/boot. Therefore any same-boot latch
  // visible here was recorded after this action became active and is terminal
  // for the action, even if a later session reports Prepared again.
  if (same_boot_disconnect) return UnavailableCandidate(true);
  if (prepared.has_value()) {
    return HealthyCandidate();
  }

  const MetaObservedFailoverProjection expected_projection{
      .group_id_ = std::string(group_id),
      .group_term_ = facts.CurrentGroupTerm(group_id),
      .transition_id_ = transition.transition_id_,
      .transition_revision_ = transition.revision_,
      .action_id_ = action.action_id_,
      .candidate_node_id_ = action.candidate_.node_id_,
      .candidate_assignment_id_ = action.candidate_.assignment_id_,
      .candidate_boot_id_ = action.candidate_.boot_id_,
  };
  const bool heartbeat_saw_exact_action =
      session->heartbeat_failover_projection_ ==
      std::optional(expected_projection);
  const bool omission_is_terminal =
      heartbeat_saw_exact_action && !action.authorization_.has_value();

  const auto candidates = observations.LiveCandidateProgressFor(
      group_id, facts, context.now_unix_ms_, action.operator_recovery_);
  const auto same_node = std::ranges::find_if(
      candidates, [&](const MetaCandidateProgressObs& candidate) {
        return candidate.node_id_ == action.candidate_.node_id_;
      });
  if (same_node != candidates.end()) {
    const bool exact =
        same_node->assignment_id_ == action.candidate_.assignment_id_ &&
        same_node->boot_incarnation_ == action.candidate_.boot_id_ &&
        same_node->operator_recovery_ == action.operator_recovery_ &&
        (action.operator_recovery_ ||
         CandidateCompatibilityDomain(*same_node) == action.domain_);
    if (!exact) {
      // A generic role belongs to whichever FDS Data had fully applied when
      // it produced this heartbeat. Only an exact, still-unauthorized action
      // turns a changed role into affirmative ineligibility; an older
      // projection must not clear a candidate selected atomically with an
      // uncontrolled term fence, and authorized preparation has its own
      // typed failure/watchdog outcome.
      return omission_is_terminal ? UnavailableCandidate(true)
                                  : WarmupCandidate();
    }
    return HealthyCandidate(*same_node);
  }
  if (observations
          .LatestForNode(action.candidate_.node_id_, facts,
                         context.now_unix_ms_)
          .has_value()) {
    // An exact installed-action marker and the replace-or-clear role update
    // were recorded under one observation-store lock. Absence is therefore
    // affirmative only in that case; old/unknown projection heartbeats keep
    // this action in warmup until publisher progress or disconnect resolves
    // the ambiguity.
    // Once authorization starts, Data may rotate to its child history and
    // intentionally suppress the ordinary role while prepare is in flight.
    // At that point only disconnect or typed ActionFailed is terminal; the
    // action watchdog supplies the bounded failure outcome.
    return omission_is_terminal ? UnavailableCandidate(true)
                                : WarmupCandidate();
  }
  return WarmupComplete(context) ? UnavailableCandidate() : WarmupCandidate();
}

MetaFailoverTransitionRef TransitionRef(
    const MetaFailoverTransition& transition) {
  return {transition.transition_id_, transition.revision_};
}

bool FrontierCovers(const std::vector<std::uint64_t>& applied,
                    const std::vector<std::uint64_t>& stable) {
  if (applied.size() != stable.size()) return false;
  for (std::size_t flow = 0; flow < stable.size(); ++flow) {
    if (applied[flow] < stable[flow]) return false;
  }
  return true;
}

MetaFailoverCandidateAction CandidateActionFrom(
    const MetaCandidateProgressObs& candidate,
    const MetaFailoverActionId& action_id) {
  MetaFailoverCandidateAction action;
  action.action_id_ = action_id;
  action.candidate_ = {.node_id_ = candidate.node_id_,
                       .assignment_id_ = candidate.assignment_id_,
                       .boot_id_ = candidate.boot_incarnation_};
  action.domain_ = CandidateCompatibilityDomain(candidate);
  return action;
}

absl::StatusOr<std::uint64_t> Increment(std::uint64_t value,
                                        std::string_view field) {
  if (value == std::numeric_limits<std::uint64_t>::max()) {
    return absl::FailedPreconditionError(std::string(field) +
                                         " cannot advance");
  }
  return value + 1;
}

absl::StatusOr<std::optional<MetaCommand>> AbortControlled(
    const MetaOperationRecord& operation, std::string_view group_id,
    std::optional<MetaFailoverTransitionRef> transition,
    std::string_view reason, const MetaFailoverPlannerContext& context) {
  auto request_id = NextId(context);
  if (!request_id.ok()) return request_id.status();
  AbortControlledFailover command;
  command.request_id_ = *request_id;
  command.operation_id_ = operation.operation_id_;
  command.expected_operation_revision_ = operation.revision_;
  command.group_id_ = std::string(group_id);
  command.expected_transition_ = transition;
  command.reason_ = std::string(reason);
  return MetaCommand{std::move(command)};
}

absl::StatusOr<std::optional<MetaCommand>> Authorize(
    std::string_view group_id, const MetaFailoverTransition& transition,
    MetaFailoverLoss loss, const MetaFailoverPlannerContext& context) {
  auto request_id = NextId(context);
  if (!request_id.ok()) return request_id.status();
  AuthorizeFailoverPrepare command;
  command.request_id_ = *request_id;
  command.group_id_ = std::string(group_id);
  command.expected_transition_ = TransitionRef(transition);
  command.action_id_ = transition.candidate_action_->action_id_;
  command.loss_if_cutover_ = loss;
  return MetaCommand{std::move(command)};
}

absl::StatusOr<std::optional<MetaCommand>> CommitControlled(
    const MetaCommittedView& view, const MetaTopologyGroupView& group,
    const MetaFailoverTransition& transition,
    const MetaOperationRecord& operation,
    const MetaFailoverPlannerContext& context) {
  const auto& action = *transition.candidate_action_;
  if (!action.authorization_.has_value() ||
      action.authorization_->loss_if_cutover_ != MetaFailoverLoss::kNone) {
    return absl::FailedPreconditionError(
        "controlled commit requires lossless authorization");
  }
  if (!AssignmentFor(group, group.record_.owner_).has_value()) {
    return absl::FailedPreconditionError(
        "controlled commit owner assignment is absent");
  }
  auto topology = Increment(view.topology().TopologyEpoch(), "topology epoch");
  if (!topology.ok()) return topology.status();
  auto request_id = NextId(context);
  if (!request_id.ok()) return request_id.status();

  CommitControlledFailover command;
  command.request_id_ = *request_id;
  command.operation_id_ = operation.operation_id_;
  command.expected_operation_revision_ = operation.revision_;
  command.group_id_ = group.group_id_;
  command.expected_transition_ = TransitionRef(transition);
  command.action_id_ = action.action_id_;
  command.authorized_revision_ = action.authorization_->authorized_revision_;
  command.expected_candidate_ = action.candidate_;
  SetGroupAnchors(command, group);
  command.new_topology_epoch_ = *topology;
  return MetaCommand{std::move(command)};
}

absl::StatusOr<std::optional<MetaCommand>> CommitUncontrolled(
    const MetaCommittedView& view, const MetaTopologyGroupView& group,
    const MetaFailoverTransition& transition,
    const MetaFailoverPlannerContext& context) {
  const auto& action = *transition.candidate_action_;
  if (!action.authorization_.has_value()) {
    return absl::FailedPreconditionError(
        "uncontrolled commit requires authorization");
  }
  if (!AssignmentFor(group, group.record_.owner_).has_value()) {
    return absl::FailedPreconditionError(
        "uncontrolled commit owner assignment is absent");
  }
  auto topology = Increment(view.topology().TopologyEpoch(), "topology epoch");
  if (!topology.ok()) return topology.status();
  auto request_id = NextId(context);
  if (!request_id.ok()) return request_id.status();

  CommitUncontrolledFailover command;
  command.request_id_ = *request_id;
  command.group_id_ = group.group_id_;
  command.expected_transition_ = TransitionRef(transition);
  command.action_id_ = action.action_id_;
  command.authorized_revision_ = action.authorization_->authorized_revision_;
  command.loss_if_cutover_ = action.authorization_->loss_if_cutover_;
  command.expected_candidate_ = action.candidate_;
  SetGroupAnchors(command, group);
  command.new_topology_epoch_ = *topology;
  return MetaCommand{std::move(command)};
}

absl::StatusOr<std::optional<MetaCommand>> PlanControlledTransition(
    const MetaCommittedView& view, const MetaTopologyGroupView& group,
    const MetaFailoverTransition& transition,
    const MetaObservationStore& observations,
    const MetaFailoverPlannerContext& context) {
  if (!transition.controlled_.has_value() ||
      !transition.candidate_action_.has_value()) {
    return absl::FailedPreconditionError(
        "controlled failover transition is incomplete");
  }
  const auto operation =
      view.operation().FindOperation(transition.controlled_->operation_id_);
  if (!operation.has_value() || Terminal(operation->lifecycle_)) {
    return absl::FailedPreconditionError(
        "controlled failover operation is unavailable");
  }
  if (context.now_unix_ms_ >= 0 &&
      static_cast<std::uint64_t>(context.now_unix_ms_) >=
          transition.controlled_->absolute_deadline_unix_ms_) {
    return AbortControlled(*operation, group.group_id_,
                           TransitionRef(transition),
                           "controlled failover deadline expired", context);
  }

  MetaStoresFacts facts(view.stores());
  const SourceAvailability source =
      SourceState(*transition.candidate_action_, facts, observations, context);
  const CandidateState candidate = CurrentCandidateState(
      group.group_id_, transition, facts, observations, context);
  if (source == SourceAvailability::kUnavailable ||
      source == SourceAvailability::kReplaced) {
    const bool retain =
        candidate.availability_ == CandidateAvailability::kHealthy &&
        transition.candidate_action_->authorization_.has_value() &&
        transition.candidate_action_->authorization_->loss_if_cutover_ ==
            MetaFailoverLoss::kNone;
    auto request_id = NextId(context);
    if (!request_id.ok()) return request_id.status();
    DegradeControlledFailover command;
    command.request_id_ = *request_id;
    command.operation_id_ = operation->operation_id_;
    command.expected_operation_revision_ = operation->revision_;
    command.group_id_ = group.group_id_;
    command.expected_transition_ = TransitionRef(transition);
    command.expected_candidate_action_ = transition.candidate_action_;
    command.retain_candidate_action_ = retain;
    command.reason_ = source == SourceAvailability::kReplaced
                          ? "controlled failover source was replaced"
                          : "controlled failover source became unavailable";
    return MetaCommand{std::move(command)};
  }
  // An affirmative Candidate failure terminates the operator-requested
  // attempt even while Source liveness is inside grace. Inferred Candidate
  // absence is terminal only while Source is known healthy; once Source is
  // definitively unavailable or replaced, the branch above must degrade into
  // the recovery needed to restore service.
  if (candidate.availability_ == CandidateAvailability::kUnavailable &&
      (source == SourceAvailability::kHealthy || candidate.explicit_failure_)) {
    return AbortControlled(
        *operation, group.group_id_, TransitionRef(transition),
        "controlled failover candidate became unavailable", context);
  }

  if (candidate.availability_ != CandidateAvailability::kHealthy) {
    return std::nullopt;
  }
  const auto& action = *transition.candidate_action_;
  if (!action.authorization_.has_value()) {
    const auto paused = observations.SourcePausedFor(
        transition.transition_id_, facts, context.now_unix_ms_);
    if (!paused.has_value() || !candidate.progress_.has_value() ||
        !FrontierCovers(candidate.progress_->applied_next_lsns_,
                        paused->stable_next_lsns_)) {
      return std::nullopt;
    }
    return Authorize(group.group_id_, transition, MetaFailoverLoss::kNone,
                     context);
  }
  if (action.authorization_->loss_if_cutover_ != MetaFailoverLoss::kNone) {
    return absl::FailedPreconditionError(
        "controlled transition has lossy authorization");
  }
  if (!observations
           .CandidatePreparedFor(transition.transition_id_, action.action_id_,
                                 facts, context.now_unix_ms_)
           .has_value()) {
    return std::nullopt;
  }
  const auto grant = view.topology().AuthorityFor(group.group_id_);
  if (!grant.has_value()) {
    return absl::FailedPreconditionError(
        "controlled transition grant state is absent");
  }
  return CommitControlled(view, group, transition, *operation, context);
}

absl::StatusOr<std::optional<MetaCommand>> SetUncontrolledAction(
    std::string_view group_id, const MetaFailoverTransition& transition,
    std::optional<MetaCandidateProgressObs> candidate,
    const MetaFailoverPlannerContext& context) {
  auto request_id = NextId(context);
  if (!request_id.ok()) return request_id.status();
  SetUncontrolledCandidate command;
  command.request_id_ = *request_id;
  command.group_id_ = std::string(group_id);
  command.expected_transition_ = TransitionRef(transition);
  if (candidate.has_value()) {
    auto action_id = NextId(context);
    if (!action_id.ok()) return action_id.status();
    command.candidate_action_ = CandidateActionFrom(*candidate, *action_id);
  }
  return MetaCommand{std::move(command)};
}

absl::StatusOr<std::optional<MetaCommand>> PlanUncontrolledTransition(
    const MetaCommittedView& view, const MetaTopologyGroupView& group,
    const MetaFailoverTransition& transition,
    const MetaObservationStore& observations,
    const MetaFailoverPlannerContext& context) {
  if (transition.controlled_.has_value()) {
    return absl::FailedPreconditionError(
        "uncontrolled transition retains controlled state");
  }
  MetaStoresFacts facts(view.stores());
  if (!transition.candidate_action_.has_value()) {
    const CandidatePlan plan = UncontrolledCandidatePlanFor(
        group.group_id_, facts, observations, context.now_unix_ms_);
    if (!plan.selected_.has_value()) return std::nullopt;
    return SetUncontrolledAction(group.group_id_, transition, plan.selected_,
                                 context);
  }

  const CandidateState candidate = CurrentCandidateState(
      group.group_id_, transition, facts, observations, context);
  if (candidate.availability_ == CandidateAvailability::kWarmup) {
    return std::nullopt;
  }
  if (candidate.availability_ == CandidateAvailability::kUnavailable) {
    const CandidatePlan replacement = UncontrolledCandidatePlanFor(
        group.group_id_, facts, observations, context.now_unix_ms_,
        transition.candidate_action_);
    return SetUncontrolledAction(group.group_id_, transition,
                                 replacement.selected_, context);
  }

  const auto& action = *transition.candidate_action_;
  if (!action.authorization_.has_value()) {
    if (!candidate.progress_.has_value()) return std::nullopt;
    // Explicit operator recovery has no historical domain or donor frontier.
    // Preserve its separate unknown-loss authorization over readable storage.
    if (action.operator_recovery_)
      return Authorize(group.group_id_, transition, MetaFailoverLoss::kUnknown,
                       context);
    if (!transition.recovery_deadline_unix_ms_.has_value()) {
      if (!context.authority_excluded_ ||
          !context.authority_excluded_(group.group_id_,
                                       transition.target_term_)) {
        return std::nullopt;
      }
      const auto policy = view.stores().policy_.CurrentCandidateRecovery();
      if (!policy.has_value())
        return absl::FailedPreconditionError("recovery policy is missing");
      if (context.now_unix_ms_ <= 0 ||
          policy->budget_ms_ > static_cast<std::uint64_t>(
                                   std::numeric_limits<std::int64_t>::max() -
                                   context.now_unix_ms_)) {
        return absl::InvalidArgumentError(
            "recovery deadline overflows Unix milliseconds");
      }
      auto id = NextId(context);
      if (!id.ok()) return id.status();
      StartCandidateRecovery start;
      start.request_id_ = *id;
      start.group_id_ = group.group_id_;
      start.expected_transition_ = TransitionRef(transition);
      start.action_id_ = action.action_id_;
      start.recovery_deadline_unix_ms_ =
          static_cast<std::uint64_t>(context.now_unix_ms_) + policy->budget_ms_;
      return MetaCommand{std::move(start)};
    }
    // Only the drained complete Applied cut can authorize prepare. A timeout
    // or a donor's advertised endpoint is never local population evidence.
    const auto complete = observations.CandidateRecoveryCompleteFor(
        transition.transition_id_, action.action_id_, facts,
        context.now_unix_ms_);
    if (!complete.has_value() ||
        complete->session_generation_ !=
            candidate.progress_->session_generation_ ||
        complete->applied_next_lsns_ !=
            candidate.progress_->applied_next_lsns_) {
      return std::nullopt;
    }
    return Authorize(group.group_id_, transition, MetaFailoverLoss::kUnknown,
                     context);
  }
  if (!observations
           .CandidatePreparedFor(transition.transition_id_, action.action_id_,
                                 facts, context.now_unix_ms_)
           .has_value()) {
    return std::nullopt;
  }
  const auto grant = view.topology().AuthorityFor(group.group_id_);
  if (!grant.has_value()) {
    return absl::FailedPreconditionError(
        "uncontrolled transition grant state is absent");
  }
  return CommitUncontrolled(view, group, transition, context);
}

bool OperationOwnsControlledTransition(const MetaCommittedView& view,
                                       const MetaOperationId& operation_id) {
  return std::ranges::any_of(
      view.topology().Groups(), [&](const MetaTopologyGroupView& group) {
        return group.failover_transition_.has_value() &&
               group.failover_transition_->mode_ ==
                   MetaFailoverMode::kControlled &&
               group.failover_transition_->controlled_.has_value() &&
               group.failover_transition_->controlled_->operation_id_ ==
                   operation_id;
      });
}

std::optional<MetaFailoverCompatibilityDomain> ControlledDomain(
    const MetaTopologyGroupView& group,
    const MetaObservationStore& observations, const MetaCommittedFacts& facts,
    const MetaFailoverPlannerContext& context) {
  const auto owner_assignment = AssignmentFor(group, group.record_.owner_);
  const auto owner_session = observations.SessionStateFor(group.record_.owner_);
  if (!owner_assignment.has_value() || !owner_session.has_value() ||
      !owner_session->connected_ ||
      !owner_session->current_history_id_.has_value()) {
    return std::nullopt;
  }
  const auto owner_latest =
      observations.LatestForNode(group.record_.owner_, facts);
  if (!owner_latest.has_value() ||
      GraceExpired(owner_latest->received_unix_ms_, context)) {
    return std::nullopt;
  }

  std::vector<MetaFailoverCompatibilityDomain> domains;
  for (const auto& candidate : observations.LiveCandidateProgressFor(
           group.group_id_, facts, context.now_unix_ms_)) {
    const auto domain = CandidateCompatibilityDomain(candidate);
    if (domain.source_group_term_ != group.record_.group_term_ ||
        domain.source_node_id_ != group.record_.owner_ ||
        domain.source_assignment_id_ != *owner_assignment ||
        domain.source_boot_id_ != owner_session->current_boot_id_ ||
        domain.source_history_id_ != *owner_session->current_history_id_) {
      continue;
    }
    if (std::ranges::find(domains, domain) == domains.end()) {
      domains.push_back(domain);
    }
  }
  if (domains.size() != 1) return std::nullopt;
  return domains.front();
}

absl::StatusOr<std::optional<MetaCommand>> PlanSubmittedControlled(
    const MetaCommittedView& view, const MetaOperationRecord& operation,
    const MetaObservationStore& observations,
    const MetaFailoverPlannerContext& context) {
  auto intent = DecodeFailoverOperationIntent(operation.intent_);
  if (!intent.ok()) return intent.status();
  if (static_cast<std::uint64_t>(context.now_unix_ms_) >=
      intent->absolute_deadline_unix_ms_) {
    return AbortControlled(operation, intent->group_id_, std::nullopt,
                           "controlled failover deadline expired before begin",
                           context);
  }
  if (!WarmupComplete(context)) return std::nullopt;

  const auto group = view.topology().FindGroup(intent->group_id_);
  if (!group.has_value()) {
    return AbortControlled(operation, intent->group_id_, std::nullopt,
                           "controlled failover group is unavailable", context);
  }
  if (group->failover_transition_.has_value()) {
    return AbortControlled(operation, intent->group_id_, std::nullopt,
                           "controlled failover group already has a transition",
                           context);
  }
  const auto grant = view.topology().AuthorityFor(intent->group_id_);
  if (!grant.has_value() || !ActiveGrantMatchesGroup(*group, *grant)) {
    return AbortControlled(operation, intent->group_id_, std::nullopt,
                           "controlled failover grant is unavailable", context);
  }
  if (group->record_.group_term_ == std::numeric_limits<std::uint64_t>::max()) {
    return AbortControlled(operation, intent->group_id_, std::nullopt,
                           "controlled failover group term cannot advance",
                           context);
  }
  const auto owner_assignment = AssignmentFor(*group, group->record_.owner_);
  if (!owner_assignment.has_value()) {
    return AbortControlled(
        operation, intent->group_id_, std::nullopt,
        "controlled failover owner assignment is unavailable", context);
  }

  MetaStoresFacts facts(view.stores());
  const auto domain = ControlledDomain(*group, observations, facts, context);
  if (!domain.has_value() ||
      !ControlledDomainMatchesOwner(*domain, *group, *owner_assignment)) {
    return AbortControlled(operation, intent->group_id_, std::nullopt,
                           "controlled failover has no exact-source candidate",
                           context);
  }
  const CandidatePlan candidate = CandidatePlanForDomain(
      intent->group_id_, *domain, facts, observations, context.now_unix_ms_);
  if (!candidate.selected_.has_value()) {
    return AbortControlled(operation, intent->group_id_, std::nullopt,
                           "controlled failover has no eligible candidate",
                           context);
  }

  auto request_id = NextId(context);
  if (!request_id.ok()) return request_id.status();
  auto transition_id = NextId(context);
  if (!transition_id.ok()) return transition_id.status();
  auto action_id = NextId(context);
  if (!action_id.ok()) return action_id.status();

  BeginControlledFailover command;
  command.request_id_ = *request_id;
  command.group_id_ = intent->group_id_;
  command.transition_id_ = *transition_id;
  command.target_term_ = group->record_.group_term_ + 1;
  command.candidate_action_ =
      CandidateActionFrom(*candidate.selected_, *action_id);
  command.operation_id_ = operation.operation_id_;
  command.expected_operation_revision_ = operation.revision_;
  command.absolute_deadline_unix_ms_ = intent->absolute_deadline_unix_ms_;
  SetGroupAnchors(command, *group);
  return MetaCommand{std::move(command)};
}

}  // namespace

absl::StatusOr<std::optional<MetaCommand>> PlanFailoverStep(
    const MetaCommittedView& view, const MetaObservationStore& observations,
    const MetaFailoverPlannerContext& context) {
  if (context.now_unix_ms_ < 0 || context.leadership_started_unix_ms_ < 0 ||
      context.observation_grace_ms_ < 0 ||
      context.leadership_started_unix_ms_ > context.now_unix_ms_) {
    return absl::InvalidArgumentError("invalid failover planning clock cut");
  }

  std::vector<MetaOperationRecord> operations =
      view.operation().LiveOperations();
  std::ranges::sort(operations, {}, &MetaOperationRecord::operation_seq_);
  for (const MetaOperationRecord& operation : operations) {
    if (operation.kind_ != kFailoverOperationKind ||
        Terminal(operation.lifecycle_)) {
      continue;
    }
    if (OperationOwnsControlledTransition(view, operation.operation_id_)) {
      continue;
    }
    if (operation.lifecycle_ != MetaOperationLifecycle::kSubmitted ||
        operation.revision_ != 0 || !operation.kind_phase_blob_.empty() ||
        !operation.current_directives_.empty()) {
      return absl::FailedPreconditionError(
          "active failover operation is not a pristine submitted request");
    }
    auto planned =
        PlanSubmittedControlled(view, operation, observations, context);
    if (!planned.ok() || planned->has_value()) return planned;
  }

  for (const MetaTopologyGroupView& group : view.topology().Groups()) {
    if (!group.failover_transition_.has_value()) continue;
    auto planned =
        group.failover_transition_->mode_ == MetaFailoverMode::kControlled
            ? PlanControlledTransition(view, group, *group.failover_transition_,
                                       observations, context)
            : PlanUncontrolledTransition(view, group,
                                         *group.failover_transition_,
                                         observations, context);
    if (!planned.ok() || planned->has_value()) return planned;
  }
  return std::nullopt;
}

bool MetaFencedAuthorityGuard::ObserveFence(std::string_view group_id,
                                            std::uint64_t target_term,
                                            std::int64_t now_lease_clock_ms) {
  if (group_id.empty() || target_term == 0 || now_lease_clock_ms < 0)
    return false;
  auto entry = std::ranges::find(entries_, group_id, &Entry::group_id_);
  if (entry == entries_.end()) {
    // Losing an entry only restarts a wait, never permits authority early.
    if (entries_.size() >= cluster::control::kMaxProjectedGroups)
      entries_.erase(entries_.begin());
    entries_.push_back(
        {std::string(group_id), target_term, now_lease_clock_ms});
    entry = std::prev(entries_.end());
  } else if (entry->target_term_ != target_term ||
             now_lease_clock_ms < entry->since_ms_) {
    *entry = {std::string(group_id), target_term, now_lease_clock_ms};
  }
  return static_cast<std::uint64_t>(now_lease_clock_ms - entry->since_ms_) >=
         quarantine_ms_;
}

struct MetaFailoverReconciler::Core {
  bycorf::ForeignExecutor executor_;
  MetaFailoverReconcilerOptions options_;
  // Proposal hooks run on the coordinator thread; planning runs on Bycorf.
  std::mutex recovery_mutex_;
  bool recovery_active_ = false;
  std::unique_ptr<MetaFencedAuthorityGuard> authority_guard_;
  struct RecoveryStart {
    MetaFailoverTransitionId transition_id_;
    std::uint64_t deadline_unix_ms_;
  };
  // Latches the first proposed cutoff even when append times out or policy
  // changes before commit. Entries are bounded by current committed groups.
  std::map<std::string, RecoveryStart> recovery_starts_;
  // Start/stop state is owned by the executor worker. Atomics are limited to
  // the cross-thread fast paths used when Notify can no longer be accepted.
  bool running_ = false;
  bool cancelled_ = true;
  bool shutdown_ = false;
  std::vector<std::shared_ptr<std::promise<void>>> waiters_;
  std::atomic<bool> stopping_{false};
  std::atomic<bool> stopped_{false};
#if LAVIK_FAULTS_ENABLED
  // One leader-local deterministic cut lets process tests observe Data's
  // committed post-Begin pause without changing the durable transition.
  bool test_pause_after_begin_applied_ = false;
  // Candidate-less automatic Begin commits before the ordinary failover
  // reconciler owns the transition. This cut lets process tests kill that
  // leader at the exact candidate-less transition recovery boundary.
  bool test_pause_after_automatic_begin_applied_ = false;
  // Lease fencing needs a later cut where the exact prepared action is
  // already durably authorized and therefore retained across degradation.
  bool test_pause_after_authorize_applied_ = false;
  // Leader recovery must collect CandidatePrepared again from Data rather
  // than treating the previous leader's volatile observation as durable.
  // This cut fires only after the current leader has ingested that exact
  // transition/action observation.
  bool test_pause_after_prepared_observed_ = false;
#endif
};

MetaFailoverReconciler::MetaFailoverReconciler(
    bycorf::ForeignExecutor executor, MetaFailoverReconcilerOptions options)
    : core_(std::make_shared<Core>()) {
  if (options.observation_grace_ms_ < 0 ||
      options.poll_interval_.count() <= 0) {
    throw std::invalid_argument("invalid failover reconciler timing options");
  }
  if (!options.now_unix_ms_) {
    options.now_unix_ms_ = [] {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
          .count();
    };
  }
  if (!options.next_id_) {
    options.next_id_ = [] { return cluster::control::GenerateId128(); };
  }
  if (!options.now_lease_clock_ms_)
    options.now_lease_clock_ms_ = cluster::LeaseClockMillis;
  core_->authority_guard_ = std::make_unique<MetaFencedAuthorityGuard>(
      options.authority_exclusion_ms_);
  core_->executor_ = std::move(executor);
  core_->options_ = std::move(options);
}

MetaFailoverReconciler::~MetaFailoverReconciler() { Shutdown(); }

MetaValidateHook MetaFailoverReconciler::validation_hook() const {
  return [core = core_](
             const MetaCommand& command, const MetaCommittedView& view,
             const MetaObservationStore&, std::int64_t) -> absl::Status {
    const auto* start = std::get_if<StartCandidateRecovery>(&command);
    if (start == nullptr) return absl::OkStatus();
    const auto group = view.topology().FindGroup(start->group_id_);
    const auto grant = view.topology().AuthorityFor(start->group_id_);
    if (!group.has_value() || !grant.has_value() || grant->grant_.has_value() ||
        !group->failover_transition_.has_value()) {
      return MetaDomainRejectError(
          "recovery requires a committed authority fence");
    }
    std::lock_guard lock(core->recovery_mutex_);
    const auto pending = core->recovery_starts_.find(start->group_id_);
    if (!core->recovery_active_ ||
        !core->authority_guard_->ObserveFence(
            start->group_id_, group->record_.group_term_,
            core->options_.now_lease_clock_ms_()) ||
        pending == core->recovery_starts_.end() ||
        pending->second.transition_id_ !=
            start->expected_transition_.transition_id_ ||
        pending->second.deadline_unix_ms_ !=
            start->recovery_deadline_unix_ms_) {
      return MetaDomainRejectError(
          "recovery prior authority exclusion is pending");
    }
    return absl::OkStatus();
  };
}

void MetaFailoverReconciler::Start(MetaLeaderContext& context) {
  const auto core = core_;
  const std::int64_t leadership_started = core->options_.now_unix_ms_();
  if (!core->executor_.Notify(
          [core, context = &context, leadership_started]() noexcept {
            if (core->shutdown_) return;
            if (core->running_) std::terminate();
            core->cancelled_ = false;
            core->running_ = true;
            {
              std::lock_guard lock(core->recovery_mutex_);
              core->authority_guard_->Reset();
              core->recovery_starts_.clear();
              core->recovery_active_ = true;
            }
            bycorf::ThisWorker().self_->Spawn(
                Run(core, context, leadership_started));
          })) {
    std::terminate();
  }
}

void MetaFailoverReconciler::Stop(bool permanent) {
  const auto core = core_;
  if (core->stopped_.load(std::memory_order_acquire)) return;
  if (permanent) core->stopping_.store(true, std::memory_order_release);
  auto waiter = std::make_shared<std::promise<void>>();
  auto done = waiter->get_future();
  if (!core->executor_.Notify([core, waiter, permanent]() noexcept {
        core->shutdown_ |= permanent;
        core->cancelled_ = true;
        {
          std::lock_guard lock(core->recovery_mutex_);
          core->recovery_active_ = false;
          core->authority_guard_->Reset();
          core->recovery_starts_.clear();
        }
        if (core->running_) {
          core->waiters_.push_back(waiter);
        } else {
          waiter->set_value();
        }
      })) {
    if (core->stopped_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
  if (permanent) core->stopped_.store(true, std::memory_order_release);
}

void MetaFailoverReconciler::CancelAndWait() { Stop(false); }

void MetaFailoverReconciler::Shutdown() { Stop(true); }

bool MetaFailoverReconciler::accepting() const {
  return !core_->stopping_.load(std::memory_order_acquire);
}

bycorf::Task<absl::Status> MetaFailoverReconciler::Run(
    std::shared_ptr<Core> core, MetaLeaderContext* context,
    std::int64_t leadership_started_unix_ms) {
  auto changed = std::make_shared<std::atomic<bool>>(false);
  auto subscribe = [&] {
    return context->SubscribeCommitted([changed](const MetaCommitEvent&) {
      changed->store(true, std::memory_order_release);
    });
  };
  auto subscribed = subscribe();
  std::string last_error;
  while (!core->cancelled_) {
    if (subscribed.subscription_->needs_resync()) subscribed = subscribe();
    if (changed->exchange(false, std::memory_order_acq_rel)) {
      subscribed.view_ = context->CommittedView();
    }

#if LAVIK_FAULTS_ENABLED
    if (!core->test_pause_after_automatic_begin_applied_) {
      const auto delay =
          TestPauseDelay("LAVIK_TEST_PAUSE_FAILOVER_AFTER_AUTOMATIC_BEGIN_MS");
      const auto groups = subscribed.view_.topology().Groups();
      const auto paused_group =
          std::ranges::find_if(groups, [](const auto& group) {
            if (!group.failover_transition_.has_value()) return false;
            const MetaFailoverTransition& transition =
                *group.failover_transition_;
            return transition.mode_ == MetaFailoverMode::kUncontrolled &&
                   !transition.candidate_action_.has_value();
          });
      if (delay != std::chrono::milliseconds::zero() &&
          paused_group != groups.end()) {
        core->test_pause_after_automatic_begin_applied_ = true;
        spdlog::info(
            "failover reconciliation paused after automatic uncontrolled "
            "Begin group={} delay_ms={}",
            paused_group->group_id_, delay.count());
        auto remaining = delay;
        constexpr auto kSlice = std::chrono::milliseconds(25);
        while (!core->cancelled_ && remaining > std::chrono::milliseconds(0)) {
          const auto slice = std::min(remaining, kSlice);
          const auto slept =
              co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, slice);
          if (!slept.ok()) {
            core->cancelled_ = true;
            break;
          }
          remaining -= slice;
        }
        changed->store(true, std::memory_order_release);
        continue;
      }
    }
    if (!core->test_pause_after_begin_applied_) {
      const auto delay =
          TestPauseDelay("LAVIK_TEST_PAUSE_FAILOVER_AFTER_BEGIN_MS");
      const auto groups = subscribed.view_.topology().Groups();
      const auto paused_group =
          std::ranges::find_if(groups, [](const auto& group) {
            if (!group.failover_transition_.has_value()) return false;
            const MetaFailoverTransition& transition =
                *group.failover_transition_;
            return transition.mode_ == MetaFailoverMode::kControlled &&
                   transition.controlled_.has_value() &&
                   transition.candidate_action_.has_value() &&
                   !transition.candidate_action_->authorization_.has_value();
          });
      if (delay != std::chrono::milliseconds::zero() &&
          paused_group != groups.end()) {
        core->test_pause_after_begin_applied_ = true;
        spdlog::info(
            "failover reconciliation paused after controlled Begin group={} "
            "delay_ms={}",
            paused_group->group_id_, delay.count());
        auto remaining = delay;
        constexpr auto kSlice = std::chrono::milliseconds(25);
        while (!core->cancelled_ && remaining > std::chrono::milliseconds(0)) {
          const auto slice = std::min(remaining, kSlice);
          const auto slept =
              co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, slice);
          if (!slept.ok()) {
            core->cancelled_ = true;
            break;
          }
          remaining -= slice;
        }
        changed->store(true, std::memory_order_release);
        continue;
      }
    }
    if (!core->test_pause_after_authorize_applied_) {
      const auto delay =
          TestPauseDelay("LAVIK_TEST_PAUSE_FAILOVER_AFTER_AUTHORIZE_MS");
      const auto groups = subscribed.view_.topology().Groups();
      const auto paused_group =
          std::ranges::find_if(groups, [](const auto& group) {
            if (!group.failover_transition_.has_value()) return false;
            const MetaFailoverTransition& transition =
                *group.failover_transition_;
            return transition.mode_ == MetaFailoverMode::kControlled &&
                   transition.controlled_.has_value() &&
                   transition.candidate_action_.has_value() &&
                   transition.candidate_action_->authorization_.has_value() &&
                   transition.candidate_action_->authorization_
                           ->loss_if_cutover_ == MetaFailoverLoss::kNone;
          });
      if (delay != std::chrono::milliseconds::zero() &&
          paused_group != groups.end()) {
        core->test_pause_after_authorize_applied_ = true;
        spdlog::info(
            "failover reconciliation paused after controlled Authorize "
            "group={} delay_ms={}",
            paused_group->group_id_, delay.count());
        auto remaining = delay;
        constexpr auto kSlice = std::chrono::milliseconds(25);
        while (!core->cancelled_ && remaining > std::chrono::milliseconds(0)) {
          const auto slice = std::min(remaining, kSlice);
          const auto slept =
              co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, slice);
          if (!slept.ok()) {
            core->cancelled_ = true;
            break;
          }
          remaining -= slice;
        }
        changed->store(true, std::memory_order_release);
        continue;
      }
    }
    if (!core->test_pause_after_prepared_observed_) {
      const auto delay =
          TestPauseDelay("LAVIK_TEST_PAUSE_FAILOVER_AFTER_PREPARED_MS");
      const std::int64_t observed_at = core->options_.now_unix_ms_();
      const MetaStoresFacts facts(subscribed.view_.stores());
      const auto groups = subscribed.view_.topology().Groups();
      const auto paused_group =
          std::ranges::find_if(groups, [&](const auto& group) {
            if (!group.failover_transition_.has_value()) return false;
            const MetaFailoverTransition& transition =
                *group.failover_transition_;
            if (transition.mode_ != MetaFailoverMode::kControlled ||
                !transition.controlled_.has_value() ||
                !transition.candidate_action_.has_value() ||
                !transition.candidate_action_->authorization_.has_value()) {
              return false;
            }
            return context->Observations()
                .CandidatePreparedFor(transition.transition_id_,
                                      transition.candidate_action_->action_id_,
                                      facts, observed_at)
                .has_value();
          });
      if (delay != std::chrono::milliseconds::zero() &&
          paused_group != groups.end()) {
        core->test_pause_after_prepared_observed_ = true;
        spdlog::info(
            "failover reconciliation paused after exact CandidatePrepared "
            "observation group={} delay_ms={}",
            paused_group->group_id_, delay.count());
        auto remaining = delay;
        constexpr auto kSlice = std::chrono::milliseconds(25);
        while (!core->cancelled_ && remaining > std::chrono::milliseconds(0)) {
          const auto slice = std::min(remaining, kSlice);
          const auto slept =
              co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, slice);
          if (!slept.ok()) {
            core->cancelled_ = true;
            break;
          }
          remaining -= slice;
        }
        changed->store(true, std::memory_order_release);
        continue;
      }
    }
#endif

    {
      std::lock_guard lock(core->recovery_mutex_);
      std::erase_if(core->recovery_starts_, [&](const auto& entry) {
        const auto group = subscribed.view_.topology().FindGroup(entry.first);
        return !group.has_value() || !group->failover_transition_.has_value() ||
               group->failover_transition_->transition_id_ !=
                   entry.second.transition_id_ ||
               group->failover_transition_->recovery_deadline_unix_ms_
                   .has_value();
      });
    }
    const std::int64_t now = core->options_.now_unix_ms_();
    auto planned = PlanFailoverStep(
        subscribed.view_, context->Observations(),
        {.now_unix_ms_ = now,
         .leadership_started_unix_ms_ = leadership_started_unix_ms,
         .observation_grace_ms_ = core->options_.observation_grace_ms_,
         .next_id_ = core->options_.next_id_,
         .authority_excluded_ = [core](std::string_view group,
                                       std::uint64_t term) {
           std::lock_guard lock(core->recovery_mutex_);
           return core->recovery_active_ &&
                  core->authority_guard_->ObserveFence(
                      group, term, core->options_.now_lease_clock_ms_());
         }});
    if (!planned.ok()) {
      if (last_error != planned.status().message()) {
        spdlog::warn("failover reconciliation blocked: {}",
                     planned.status().message());
        last_error = std::string(planned.status().message());
      }
    } else if (planned->has_value() && !core->cancelled_) {
      last_error.clear();
      if (auto* start = std::get_if<StartCandidateRecovery>(&**planned)) {
        std::lock_guard lock(core->recovery_mutex_);
        auto [it, inserted] = core->recovery_starts_.try_emplace(
            start->group_id_,
            Core::RecoveryStart{start->expected_transition_.transition_id_,
                                start->recovery_deadline_unix_ms_});
        if (!inserted && it->second.transition_id_ !=
                             start->expected_transition_.transition_id_) {
          it->second = {start->expected_transition_.transition_id_,
                        start->recovery_deadline_unix_ms_};
        }
        start->recovery_deadline_unix_ms_ = it->second.deadline_unix_ms_;
      }
      const auto applied = co_await context->Propose(std::move(**planned));
      if (core->cancelled_) break;
      // A timeout, rejection, or accepted reply is never interpreted as
      // committed truth. Every outcome forces a fresh aggregate read; exact
      // transition/operation revisions then decide the next level-triggered
      // step, including after a reply is lost but the entry committed.
      changed->store(true, std::memory_order_release);
      if (applied.ok() && applied->verdict_ == MetaAuditVerdict::kAccepted) {
        continue;
      }
      const std::string detail = applied.ok()
                                     ? applied->detail_
                                     : std::string(applied.status().message());
      if (last_error != detail) {
        spdlog::warn("failover proposal deferred: {}", detail);
        last_error = detail;
      }
    } else {
      last_error.clear();
    }

    const auto slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                                 core->options_.poll_interval_);
    if (!slept.ok()) break;
  }

  core->running_ = false;
  for (const auto& waiter : core->waiters_) waiter->set_value();
  core->waiters_.clear();
  co_return absl::OkStatus();
}

}  // namespace lavik::meta
