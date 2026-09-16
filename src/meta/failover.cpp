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

#include "lavik/meta/failover.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/meta/candidate_plan.h"
#include "lavik/meta/coordinator.h"
#include "lavik/meta/encoding.h"
#include "lavik/meta/hash.h"

namespace lavik::meta {
namespace {

constexpr std::string_view kOperationIntentMagic = "LVFO";
constexpr std::uint16_t kFailoverSchemaVersion = 1;

absl::Status Invalid(std::string_view message) {
  return MetaDomainRejectError(message);
}

absl::Status Corrupt(std::string_view message) {
  return MetaFailStopError(message);
}

void WriteHeader(MetaWriter& writer) {
  writer.WriteRaw(kOperationIntentMagic);
  writer.WriteU16(kFailoverSchemaVersion);
}

absl::Status ReadHeader(MetaReader& reader) {
  auto magic = reader.ReadRaw(kOperationIntentMagic.size());
  if (!magic.ok()) return magic.status();
  if (*magic != kOperationIntentMagic) {
    return Corrupt("unknown failover blob magic");
  }
  auto version = reader.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kFailoverSchemaVersion) {
    return Corrupt("unknown failover blob version");
  }
  return absl::OkStatus();
}

absl::Status DecodeValidation(absl::Status status) {
  if (status.ok()) return status;
  return Corrupt(status.message());
}

bool IsZero(const MetaReplicationHistoryId& history) {
  return std::ranges::all_of(history,
                             [](std::uint8_t byte) { return byte == 0; });
}

const MetaOperationId* GenericOperationId(const MetaCommand& command) {
  if (const auto* phase = std::get_if<TransitionOperationPhase>(&command)) {
    return &phase->operation_id_;
  }
  if (const auto* complete = std::get_if<CompleteOperation>(&command)) {
    return &complete->operation_id_;
  }
  if (const auto* abort = std::get_if<AbortOperation>(&command)) {
    return &abort->operation_id_;
  }
  return nullptr;
}

absl::StatusOr<MetaCandidateProgressObs> ExactCandidateProgress(
    std::string_view group_id, const MetaFailoverCandidateAction& action,
    const MetaCommittedFacts& facts, const MetaObservationStore& observations,
    std::int64_t now_unix_ms, bool action_is_committed) {
  if (now_unix_ms < 0) {
    return Invalid("failover proposal time is invalid");
  }
  const auto session = observations.SessionStateFor(action.candidate_.node_id_);
  if (!session.has_value() || !session->connected_ ||
      session->current_boot_id_ != action.candidate_.boot_id_) {
    return Invalid("failover candidate session is absent or replaced");
  }
  // A disconnect before Begin/Set does not poison a newly allocated action:
  // current-session progress is the observation from which that action is
  // created. Once the action is committed, however, same-boot generic progress
  // cannot erase an observed disconnect and revive the old action.
  if (action_is_committed && session->disconnected_boot_id_ ==
                                 std::optional(action.candidate_.boot_id_)) {
    return Invalid("failover candidate action was disconnected");
  }
  if (action.operator_recovery_ && !action_is_committed &&
      !observations.LiveCandidateProgressFor(group_id, facts, now_unix_ms)
           .empty()) {
    return Invalid(
        "operator recovery requires no eligible automatic candidate");
  }
  const auto candidates = observations.LiveCandidateProgressFor(
      group_id, facts, now_unix_ms, action.operator_recovery_);
  const auto candidate = std::ranges::find_if(
      candidates, [&](const MetaCandidateProgressObs& progress) {
        return progress.node_id_ == action.candidate_.node_id_ &&
               progress.assignment_id_ == action.candidate_.assignment_id_ &&
               progress.boot_incarnation_ == action.candidate_.boot_id_ &&
               progress.session_generation_ == session->current_generation_ &&
               progress.operator_recovery_ == action.operator_recovery_ &&
               (action.operator_recovery_ ||
                CandidateCompatibilityDomain(progress) == action.domain_);
      });
  if (candidate == candidates.end()) {
    return Invalid(
        "failover candidate progress is expired or incompatible with the "
        "action");
  }
  return *candidate;
}

absl::Status RequireExactCurrentSource(
    std::string_view group_id, const MetaFailoverCompatibilityDomain& domain,
    const MetaCommittedFacts& facts, const MetaObservationStore& observations,
    std::int64_t now_unix_ms) {
  const auto session = observations.SessionStateFor(domain.source_node_id_);
  if (!session.has_value() || !session->connected_ ||
      session->current_boot_id_ != domain.source_boot_id_) {
    return Invalid("controlled failover source session is absent or replaced");
  }
  if (session->current_history_id_ !=
          std::optional(domain.source_history_id_) ||
      domain.source_group_term_ != facts.CurrentGroupTerm(group_id) ||
      !facts.IsOwnerAssignment(group_id, domain.source_node_id_,
                               domain.source_assignment_id_)) {
    return Invalid("controlled failover source lineage is no longer current");
  }
  const auto latest =
      observations.LatestForNode(domain.source_node_id_, facts, now_unix_ms);
  if (!latest.has_value() ||
      latest->identity_.boot_incarnation_ != domain.source_boot_id_ ||
      latest->identity_.session_generation_ != session->current_generation_) {
    return Invalid(
        "controlled failover source has not reported from its current "
        "session");
  }
  return absl::OkStatus();
}

std::optional<MetaFailoverTransition> ExactTransition(
    const MetaCommittedView& view, const std::string& group_id,
    const MetaFailoverTransitionRef& expected) {
  const auto group = view.topology().FindGroup(group_id);
  if (!group.has_value() || !group->failover_transition_.has_value()) {
    return std::nullopt;
  }
  const MetaFailoverTransition& transition = *group->failover_transition_;
  if (transition.transition_id_ != expected.transition_id_ ||
      transition.revision_ != expected.revision_) {
    return std::nullopt;
  }
  return transition;
}

bool FrontierCovers(const std::vector<std::uint64_t>& applied,
                    const std::vector<std::uint64_t>& stable) {
  if (applied.size() != stable.size()) return false;
  for (std::size_t flow = 0; flow < stable.size(); ++flow) {
    if (applied[flow] < stable[flow]) return false;
  }
  return true;
}

bool SameCandidatePopulation(const MetaFailoverCandidateAction& lhs,
                             const MetaFailoverCandidateAction& rhs) {
  return lhs.candidate_ == rhs.candidate_ && lhs.domain_ == rhs.domain_;
}

absl::StatusOr<MetaCandidatePreparedObs> ExactCandidatePrepared(
    const MetaFailoverTransition& transition,
    const MetaFailoverCandidateAction& action, const MetaCommittedFacts& facts,
    const MetaObservationStore& observations, std::int64_t now_unix_ms) {
  if (now_unix_ms < 0) {
    return Invalid("failover proposal time is invalid");
  }
  const auto session = observations.SessionStateFor(action.candidate_.node_id_);
  if (!session.has_value() || !session->connected_ ||
      session->current_boot_id_ != action.candidate_.boot_id_) {
    return Invalid("failover candidate session is absent or replaced");
  }
  const auto prepared = observations.CandidatePreparedFor(
      transition.transition_id_, action.action_id_, facts, now_unix_ms);
  if (!prepared.has_value() ||
      prepared->candidate_node_id_ != action.candidate_.node_id_ ||
      prepared->candidate_assignment_id_ != action.candidate_.assignment_id_ ||
      prepared->candidate_boot_id_ != action.candidate_.boot_id_ ||
      prepared->session_generation_ != session->current_generation_) {
    return Invalid(
        "failover candidate prepared observation is absent or inexact");
  }
  if (session->disconnected_boot_id_ ==
      std::optional(action.candidate_.boot_id_)) {
    return Invalid("failover candidate action was disconnected");
  }
  return *prepared;
}

template <typename CommitCommand>
absl::Status ValidateCommitProposal(const CommitCommand& commit,
                                    const MetaCommittedView& view,
                                    const MetaObservationStore& observations,
                                    std::int64_t proposal_now_unix_ms) {
  constexpr bool kControlled =
      std::is_same_v<CommitCommand, CommitControlledFailover>;
  const auto transition =
      ExactTransition(view, commit.group_id_, commit.expected_transition_);
  if (!transition.has_value() ||
      transition->mode_ != (kControlled ? MetaFailoverMode::kControlled
                                        : MetaFailoverMode::kUncontrolled) ||
      !transition->candidate_action_.has_value()) {
    return Invalid("failover commit transition pre-state is stale");
  }
  const MetaFailoverCandidateAction& action = *transition->candidate_action_;
  if (action.action_id_ != commit.action_id_ ||
      action.candidate_ != commit.expected_candidate_ ||
      !action.authorization_.has_value() ||
      action.authorization_->authorized_revision_ !=
          commit.authorized_revision_) {
    return Invalid("failover commit action pre-state is stale");
  }
  if constexpr (kControlled) {
    if (!transition->controlled_.has_value() ||
        transition->controlled_->operation_id_ != commit.operation_id_ ||
        action.authorization_->loss_if_cutover_ != MetaFailoverLoss::kNone ||
        proposal_now_unix_ms < 0 ||
        static_cast<std::uint64_t>(proposal_now_unix_ms) >=
            transition->controlled_->absolute_deadline_unix_ms_) {
      return Invalid("controlled failover commit deadline or mode is invalid");
    }
  } else if (action.authorization_->loss_if_cutover_ !=
             commit.loss_if_cutover_) {
    return Invalid("uncontrolled failover commit loss authorization is stale");
  }

  MetaStoresFacts facts(view.stores());
  if (observations
          .ActionFailedFor(transition->transition_id_, action.action_id_, facts,
                           proposal_now_unix_ms)
          .has_value()) {
    return Invalid("failover candidate action has reported failure");
  }
  auto prepared = ExactCandidatePrepared(*transition, action, facts,
                                         observations, proposal_now_unix_ms);
  if (!prepared.ok()) return prepared.status();
  return absl::OkStatus();
}

}  // namespace

absl::Status ValidateFailoverOperationIntent(
    const FailoverOperationIntent& intent) {
  if (intent.group_id_.empty() ||
      intent.group_id_.size() > kMaxMetaGroupIdBytes ||
      intent.absolute_deadline_unix_ms_ == 0 ||
      intent.absolute_deadline_unix_ms_ >
          static_cast<std::uint64_t>(
              std::numeric_limits<std::int64_t>::max())) {
    return Invalid("failover operation intent is invalid");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string> EncodeFailoverOperationIntent(
    const FailoverOperationIntent& intent) {
  if (absl::Status status = ValidateFailoverOperationIntent(intent);
      !status.ok()) {
    return status;
  }
  MetaWriter writer;
  WriteHeader(writer);
  writer.WriteString(intent.group_id_);
  writer.WriteU64(intent.absolute_deadline_unix_ms_);
  return std::move(writer).TakeBuffer();
}

absl::StatusOr<FailoverOperationIntent> DecodeFailoverOperationIntent(
    std::string_view encoded) {
  MetaReader reader(encoded);
  if (absl::Status status = ReadHeader(reader); !status.ok()) return status;

  FailoverOperationIntent intent;
  auto group_id = reader.ReadString(kMaxMetaGroupIdBytes);
  if (!group_id.ok()) return group_id.status();
  intent.group_id_ = std::move(*group_id);
  auto deadline = reader.ReadU64();
  if (!deadline.ok()) return deadline.status();
  intent.absolute_deadline_unix_ms_ = *deadline;
  if (absl::Status status = reader.Finish(); !status.ok()) return status;
  if (absl::Status status =
          DecodeValidation(ValidateFailoverOperationIntent(intent));
      !status.ok()) {
    return status;
  }
  return intent;
}

absl::Status ValidateFailoverProposal(const MetaCommand& command,
                                      const MetaCommittedView& view,
                                      const MetaObservationStore& observations,
                                      std::int64_t proposal_now_unix_ms) {
  if (const auto* submit = std::get_if<SubmitOperation>(&command)) {
    if (submit->kind_ != kFailoverOperationKind) return absl::OkStatus();
    const auto intent = DecodeFailoverOperationIntent(submit->intent_);
    if (!intent.ok() || submit->intent_hash_ != MetaSha256(submit->intent_) ||
        !IsZero(submit->replication_history_id_)) {
      return Invalid("failover submit requires canonical request-only intent");
    }
    return absl::OkStatus();
  }

  if (const auto* begin = std::get_if<BeginControlledFailover>(&command)) {
    if (proposal_now_unix_ms < 0 ||
        static_cast<std::uint64_t>(proposal_now_unix_ms) >=
            begin->absolute_deadline_unix_ms_) {
      return Invalid("controlled failover begin deadline has expired");
    }
    const auto group = view.topology().FindGroup(begin->group_id_);
    if (!group.has_value() || group->failover_transition_.has_value()) {
      return Invalid("controlled failover begin pre-state is stale");
    }
    MetaStoresFacts facts(view.stores());
    if (absl::Status source = RequireExactCurrentSource(
            begin->group_id_, begin->candidate_action_.domain_, facts,
            observations, proposal_now_unix_ms);
        !source.ok()) {
      return source;
    }
    if (auto candidate = ExactCandidateProgress(
            begin->group_id_, begin->candidate_action_, facts, observations,
            proposal_now_unix_ms, /*action_is_committed=*/false);
        !candidate.ok()) {
      return candidate.status();
    }
    return absl::OkStatus();
  }

  if (const auto* begin = std::get_if<BeginUncontrolledFailover>(&command)) {
    const auto group = view.topology().FindGroup(begin->group_id_);
    if (!group.has_value() || group->failover_transition_.has_value()) {
      return Invalid("uncontrolled failover begin pre-state is stale");
    }
    if (begin->candidate_action_.has_value() &&
        begin->candidate_action_->operator_recovery_) {
      const auto owner =
          observations.OwnerObservationFor(group->record_.owner_);
      if (owner.has_value() && owner->connected_ &&
          (!owner->health_.has_value() ||
           (owner->health_->population_ready_ &&
            owner->health_->storage_ready_ && !owner->health_->draining_))) {
        return Invalid(
            "operator recovery cannot replace a serving or unobserved "
            "connected Owner");
      }
    }
    if (begin->candidate_action_.has_value()) {
      MetaStoresFacts facts(view.stores());
      if (auto candidate = ExactCandidateProgress(
              begin->group_id_, *begin->candidate_action_, facts, observations,
              proposal_now_unix_ms,
              /*action_is_committed=*/false);
          !candidate.ok()) {
        return candidate.status();
      }
    }
    return absl::OkStatus();
  }

  if (const auto* set = std::get_if<SetUncontrolledCandidate>(&command)) {
    const auto transition =
        ExactTransition(view, set->group_id_, set->expected_transition_);
    if (!transition.has_value() ||
        transition->mode_ != MetaFailoverMode::kUncontrolled) {
      return Invalid("uncontrolled candidate set pre-state is stale");
    }
    if (proposal_now_unix_ms < 0) {
      return Invalid("failover proposal time is invalid");
    }
    if (!transition->candidate_action_.has_value() &&
        !set->candidate_action_.has_value()) {
      return Invalid("uncontrolled failover candidate is absent");
    }
    MetaStoresFacts facts(view.stores());
    if (transition->candidate_action_.has_value()) {
      const MetaFailoverCandidateAction& current =
          *transition->candidate_action_;
      const bool failed =
          observations
              .ActionFailedFor(transition->transition_id_, current.action_id_,
                               facts, proposal_now_unix_ms)
              .has_value();
      if (!failed && (ExactCandidateProgress(set->group_id_, current, facts,
                                             observations, proposal_now_unix_ms,
                                             /*action_is_committed=*/true)
                          .ok() ||
                      ExactCandidatePrepared(*transition, current, facts,
                                             observations, proposal_now_unix_ms)
                          .ok())) {
        return Invalid(
            "uncontrolled candidate set would replace a healthy action");
      }
      if (set->candidate_action_.has_value() &&
          SameCandidatePopulation(current, *set->candidate_action_)) {
        return Invalid(
            "uncontrolled candidate replacement reuses the failed "
            "population");
      }
    }
    if (set->candidate_action_.has_value()) {
      if (auto candidate = ExactCandidateProgress(
              set->group_id_, *set->candidate_action_, facts, observations,
              proposal_now_unix_ms, /*action_is_committed=*/false);
          !candidate.ok()) {
        return candidate.status();
      }
    }
    return absl::OkStatus();
  }

  if (const auto* authorize = std::get_if<AuthorizeFailoverPrepare>(&command)) {
    const auto transition = ExactTransition(view, authorize->group_id_,
                                            authorize->expected_transition_);
    if (!transition.has_value() || !transition->candidate_action_.has_value() ||
        transition->candidate_action_->action_id_ != authorize->action_id_ ||
        transition->candidate_action_->authorization_.has_value()) {
      return Invalid("failover authorization pre-state is stale");
    }
    if (transition->mode_ == MetaFailoverMode::kControlled) {
      if (!transition->controlled_.has_value() || proposal_now_unix_ms < 0 ||
          static_cast<std::uint64_t>(proposal_now_unix_ms) >=
              transition->controlled_->absolute_deadline_unix_ms_) {
        return Invalid("controlled failover authorization deadline expired");
      }
      if (authorize->loss_if_cutover_ != MetaFailoverLoss::kNone) {
        return Invalid("controlled failover authorization must be lossless");
      }
    } else if (authorize->loss_if_cutover_ != MetaFailoverLoss::kUnknown) {
      return Invalid("uncontrolled failover authorization loss is invalid");
    }

    MetaStoresFacts facts(view.stores());
    const MetaFailoverCandidateAction& action = *transition->candidate_action_;
    if (observations
            .ActionFailedFor(transition->transition_id_, action.action_id_,
                             facts, proposal_now_unix_ms)
            .has_value()) {
      return Invalid("failover candidate action has reported failure");
    }
    auto progress = ExactCandidateProgress(authorize->group_id_, action, facts,
                                           observations, proposal_now_unix_ms,
                                           /*action_is_committed=*/true);
    if (!progress.ok()) return progress.status();

    if (transition->mode_ == MetaFailoverMode::kControlled) {
      const auto paused = observations.SourcePausedFor(
          transition->transition_id_, facts, proposal_now_unix_ms);
      if (!paused.has_value()) {
        return Invalid(
            "controlled failover source pause observation is absent");
      }
      if (!FrontierCovers(progress->applied_next_lsns_,
                          paused->stable_next_lsns_)) {
        return Invalid(
            "controlled failover candidate no longer covers the paused "
            "frontier");
      }
    }
    return absl::OkStatus();
  }

  if (const auto* degrade = std::get_if<DegradeControlledFailover>(&command)) {
    const auto transition = ExactTransition(view, degrade->group_id_,
                                            degrade->expected_transition_);
    if (!transition.has_value() ||
        transition->mode_ != MetaFailoverMode::kControlled ||
        !transition->controlled_.has_value() ||
        transition->controlled_->operation_id_ != degrade->operation_id_ ||
        transition->candidate_action_ != degrade->expected_candidate_action_) {
      return Invalid("controlled failover degrade pre-state is stale");
    }
    if (proposal_now_unix_ms < 0 ||
        static_cast<std::uint64_t>(proposal_now_unix_ms) >=
            transition->controlled_->absolute_deadline_unix_ms_) {
      return Invalid("controlled failover degrade deadline has expired");
    }

    MetaStoresFacts facts(view.stores());
    if (!transition->candidate_action_.has_value()) {
      return Invalid("controlled failover degrade has no candidate action");
    }
    // Source unavailability remains a negative, grace-derived planner
    // decision: the command deliberately carries no timestamp certificate and
    // proposal validation does not require evidence that an absent source
    // stayed absent. An exact, fresh recovery is positive contrary evidence,
    // however, and must invalidate a Degrade planned before that recovery. The
    // proposal hook separately rechecks any candidate capability the command
    // retains; deterministic apply/CAS still arbitrates a concurrent Abort or
    // Commit.
    if (RequireExactCurrentSource(degrade->group_id_,
                                  transition->candidate_action_->domain_, facts,
                                  observations, proposal_now_unix_ms)
            .ok()) {
      return Invalid("controlled failover source recovered before degradation");
    }
    if (!degrade->retain_candidate_action_) return absl::OkStatus();

    if (!transition->candidate_action_->authorization_.has_value() ||
        transition->candidate_action_->authorization_->loss_if_cutover_ !=
            MetaFailoverLoss::kNone) {
      return Invalid(
          "controlled failover degrade cannot retain an unauthorized or "
          "lossy action");
    }
    const MetaFailoverCandidateAction& action = *transition->candidate_action_;
    if (observations
            .ActionFailedFor(transition->transition_id_, action.action_id_,
                             facts, proposal_now_unix_ms)
            .has_value()) {
      return Invalid("failover candidate action has reported failure");
    }
    if (ExactCandidateProgress(degrade->group_id_, action, facts, observations,
                               proposal_now_unix_ms,
                               /*action_is_committed=*/true)
            .ok() ||
        ExactCandidatePrepared(*transition, action, facts, observations,
                               proposal_now_unix_ms)
            .ok()) {
      return absl::OkStatus();
    }
    return Invalid(
        "controlled failover retained candidate observations are "
        "stale or inexact");
  }

  if (const auto* commit = std::get_if<CommitControlledFailover>(&command)) {
    return ValidateCommitProposal(*commit, view, observations,
                                  proposal_now_unix_ms);
  }
  if (const auto* commit = std::get_if<CommitUncontrolledFailover>(&command)) {
    return ValidateCommitProposal(*commit, view, observations,
                                  proposal_now_unix_ms);
  }

  const MetaOperationId* operation_id = GenericOperationId(command);
  if (operation_id == nullptr) return absl::OkStatus();
  const auto operation = view.operation().FindOperation(*operation_id);
  if (operation.has_value() && operation->kind_ == kFailoverOperationKind) {
    return Invalid("failover operation is owned by typed commands");
  }
  return absl::OkStatus();
}

}  // namespace lavik::meta
