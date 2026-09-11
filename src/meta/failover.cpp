#include "keylane/meta/failover.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/candidate_plan.h"
#include "keylane/meta/coordinator.h"
#include "keylane/meta/encoding.h"
#include "keylane/meta/hash.h"

namespace keylane::meta {
namespace {

constexpr std::string_view kIntentMagic = "KLFI";
constexpr std::string_view kPhaseMagic = "KLFP";
constexpr std::string_view kOutcomeMagic = "KLFO";
constexpr std::string_view kUnavailableProofMagic = "KLFU";
constexpr std::uint16_t kFailoverSchemaVersion = 2;
constexpr std::uint16_t kOutcomeVersion = 1;
constexpr std::uint16_t kUnavailableProofVersion = 1;

int64_t UnixMillisNow() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::optional<FailoverPhaseStage> NextControlledStage(
    FailoverPhaseStage stage) {
  switch (stage) {
    case FailoverPhaseStage::kSourceHolding:
      return FailoverPhaseStage::kSourceHeld;
    case FailoverPhaseStage::kSourceHeld:
      return FailoverPhaseStage::kOldAuthorityExcluding;
    case FailoverPhaseStage::kOldAuthorityExcluding:
      return FailoverPhaseStage::kOldAuthorityExcluded;
    case FailoverPhaseStage::kOldAuthorityExcluded:
      return FailoverPhaseStage::kCandidateCaughtUp;
    case FailoverPhaseStage::kCandidateCaughtUp:
      return FailoverPhaseStage::kPromotionPreparing;
    case FailoverPhaseStage::kPromotionPreparing:
      return FailoverPhaseStage::kPromotionPrepared;
    case FailoverPhaseStage::kPromotionPrepared:
      return FailoverPhaseStage::kAuthorityActivated;
    case FailoverPhaseStage::kAuthorityActivated:
      return FailoverPhaseStage::kServing;
    case FailoverPhaseStage::kServing:
      return std::nullopt;
  }
  return std::nullopt;
}

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

absl::Status Invalid(std::string_view message) {
  return MetaDomainRejectError(message);
}

absl::Status Corrupt(std::string_view message) {
  return MetaFailStopError(message);
}

void WriteHeader(MetaWriter& writer, std::string_view magic) {
  writer.WriteRaw(magic);
  writer.WriteU16(kFailoverSchemaVersion);
}

absl::Status ReadHeader(MetaReader& reader, std::string_view magic) {
  auto actual = reader.ReadRaw(magic.size());
  if (!actual.ok()) return actual.status();
  if (*actual != magic) return Corrupt("unknown failover blob magic");
  auto version = reader.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kFailoverSchemaVersion) {
    return Corrupt("unknown failover blob version");
  }
  return absl::OkStatus();
}

absl::Status ValidateIntent(const FailoverIntent& intent) {
  if (intent.group_id_.empty() || intent.recovery_generation_ == 0 ||
      intent.attempt_timeout_ms_ == 0 ||
      intent.attempt_timeout_ms_ > kMaxControlledFailoverAttemptTimeoutMs ||
      intent.group_id_.size() > kMaxMetaGroupIdBytes ||
      !cluster::control::IsCanonicalIdentity160(intent.former_owner_node_id_) ||
      !cluster::control::IsCanonicalIdentity160(intent.candidate_node_id_) ||
      intent.former_owner_node_id_ == intent.candidate_node_id_ ||
      IsZero(intent.former_owner_assignment_id_) ||
      IsZero(intent.former_owner_boot_id_) ||
      IsZero(intent.candidate_assignment_id_) ||
      IsZero(intent.candidate_boot_id_) || intent.group_term_ < 2 ||
      intent.authority_version_ == 0 ||
      intent.authority_version_ == std::numeric_limits<std::uint64_t>::max() ||
      intent.grant_revision_ == 0 ||
      intent.old_grant_.lease_duration_ms_ == 0 ||
      intent.old_grant_.lease_duration_ms_ >
          std::numeric_limits<std::uint32_t>::max() ||
      intent.old_grant_.policy_id_.empty() ||
      intent.old_grant_.policy_id_.size() > kMaxMetaPolicyIdBytes ||
      intent.old_grant_.policy_version_ == 0 ||
      intent.population_manifest_revision_ == 0 ||
      IsZero(intent.population_manifest_digest_) ||
      intent.partition_replication_epoch_ == 0 ||
      IsZero(intent.parent_history_id_) || intent.flow_count_ == 0 ||
      intent.flow_count_ > kMaxMetaFailoverRecoveryFlows) {
    return Invalid("failover intent identity is incomplete");
  }
  return absl::OkStatus();
}

absl::Status ValidatePhase(const FailoverPhase& phase) {
  const auto stage = static_cast<std::uint8_t>(phase.stage_);
  if (stage < static_cast<std::uint8_t>(FailoverPhaseStage::kSourceHolding) ||
      stage > static_cast<std::uint8_t>(FailoverPhaseStage::kServing)) {
    return Invalid("failover phase identity is incomplete");
  }
  const bool has_frontier = !phase.required_applied_next_lsns_.empty();
  if (phase.required_applied_next_lsns_.size() >
          kMaxMetaFailoverRecoveryFlows ||
      std::any_of(phase.required_applied_next_lsns_.begin(),
                  phase.required_applied_next_lsns_.end(),
                  [](std::uint64_t cursor) { return cursor == 0; })) {
    return Invalid("failover phase frontier is invalid");
  }
  const bool authority_excluded =
      stage >=
      static_cast<std::uint8_t>(FailoverPhaseStage::kOldAuthorityExcluded);
  if (!authority_excluded &&
      (!IsZero(phase.old_authority_exclusion_hash_) || has_frontier)) {
    return Invalid("pre-exclusion failover phase carries exclusion evidence");
  }
  if (authority_excluded &&
      (IsZero(phase.old_authority_exclusion_hash_) || !has_frontier)) {
    return Invalid("post-exclusion failover phase is missing frozen evidence");
  }
  const bool promotion_prepared =
      stage >=
      static_cast<std::uint8_t>(FailoverPhaseStage::kPromotionPrepared);
  if (promotion_prepared == IsZero(phase.prepared_result_hash_)) {
    return Invalid("failover phase has an inconsistent prepared result hash");
  }
  return absl::OkStatus();
}

absl::Status ValidateOutcome(const ControlledFailoverOutcome& outcome) {
  const auto stage = static_cast<std::uint8_t>(outcome.terminal_stage_);
  const auto loss = static_cast<std::uint8_t>(outcome.loss_);
  if (stage < static_cast<std::uint8_t>(FailoverPhaseStage::kSourceHolding) ||
      stage > static_cast<std::uint8_t>(FailoverPhaseStage::kServing) ||
      loss < static_cast<std::uint8_t>(FailoverLossClassification::kExact) ||
      loss > static_cast<std::uint8_t>(FailoverLossClassification::kUnknown) ||
      outcome.proven_next_lsns_.size() > kMaxMetaFailoverRecoveryFlows ||
      std::any_of(outcome.proven_next_lsns_.begin(),
                  outcome.proven_next_lsns_.end(),
                  [](std::uint64_t cursor) { return cursor == 0; }) ||
      outcome.reason_.empty() ||
      outcome.reason_.size() > kMaxMetaAbortReasonBytes) {
    return Invalid("controlled failover outcome is incomplete");
  }
  if (outcome.succeeded_ &&
      outcome.terminal_stage_ != FailoverPhaseStage::kServing) {
    return Invalid("successful failover outcome did not reach serving");
  }
  if (outcome.succeeded_ && outcome.recovery_required_) {
    return Invalid("successful failover outcome cannot require recovery");
  }
  if (outcome.loss_ == FailoverLossClassification::kBounded &&
      outcome.proven_next_lsns_.empty()) {
    return Invalid("bounded failover outcome has no proven frontier");
  }
  return absl::OkStatus();
}

absl::Status DecodeValidation(absl::Status status) {
  if (status.ok()) return status;
  return Corrupt(status.message());
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& bytes) {
  constexpr std::string_view kHex = "0123456789abcdef";
  std::string result(bytes.size() * 2, '\0');
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    result[i * 2] = kHex[bytes[i] >> 4];
    result[i * 2 + 1] = kHex[bytes[i] & 0x0f];
  }
  return result;
}

enum class CommittedAnchorState { kOldAuthorityActive, kFenced, kActivated };

absl::Status ValidateCommittedAnchors(const FailoverIntent& intent,
                                      const MetaStores& stores,
                                      CommittedAnchorState expected,
                                      bool require_candidate_member = true) {
  const auto group = stores.topology_.FindGroup(intent.group_id_);
  const auto grant = stores.grant_.GroupState(intent.group_id_);
  if (!group.has_value() || !grant.has_value() ||
      group->record_.population_manifest_revision_ !=
          intent.population_manifest_revision_ ||
      group->record_.population_manifest_digest_ !=
          intent.population_manifest_digest_ ||
      group->record_.partition_replication_epoch_ !=
          intent.partition_replication_epoch_) {
    return Invalid("failover population anchors are stale");
  }
  const auto member_matches = [&](std::string_view node_id,
                                  const MetaAssignmentId& assignment) {
    return std::any_of(group->members_.begin(), group->members_.end(),
                       [&](const MetaGroupMember& member) {
                         return member.node_id_ == node_id &&
                                member.assignment_id_ == assignment;
                       });
  };
  if (!member_matches(intent.former_owner_node_id_,
                      intent.former_owner_assignment_id_) ||
      (require_candidate_member &&
       !member_matches(intent.candidate_node_id_,
                       intent.candidate_assignment_id_))) {
    return Invalid("failover member assignment is stale");
  }

  if (expected == CommittedAnchorState::kOldAuthorityActive) {
    if (intent.group_term_ < 2 || grant->fenced_ ||
        !grant->grant_.has_value() ||
        grant->group_term_ != intent.group_term_ - 1 ||
        grant->last_authority_version_ != intent.authority_version_ ||
        grant->last_grant_revision_ != intent.grant_revision_ ||
        grant->grant_->owner_ != intent.former_owner_node_id_ ||
        grant->grant_->term_ != intent.group_term_ - 1 ||
        grant->grant_->authority_version_ != intent.authority_version_ ||
        grant->grant_->grant_revision_ != intent.grant_revision_ ||
        grant->grant_->spec_ != intent.old_grant_ ||
        group->record_.owner_ != intent.former_owner_node_id_ ||
        group->record_.group_term_ != intent.group_term_ - 1 ||
        group->record_.authority_version_ != intent.authority_version_) {
      return Invalid(
          "failover anchors do not match the active former authority");
    }
    return absl::OkStatus();
  }

  if (expected == CommittedAnchorState::kFenced) {
    if (!grant->fenced_ || grant->grant_.has_value() ||
        grant->group_term_ != intent.group_term_ ||
        grant->last_authority_version_ != intent.authority_version_ ||
        grant->last_grant_revision_ != intent.grant_revision_ ||
        group->record_.owner_ != intent.former_owner_node_id_ ||
        group->record_.group_term_ != intent.group_term_ ||
        group->record_.authority_version_ != intent.authority_version_) {
      return Invalid(
          "failover anchors do not match the committed fenced group state");
    }
    return absl::OkStatus();
  }

  if (intent.authority_version_ == std::numeric_limits<std::uint64_t>::max() ||
      grant->fenced_ || !grant->grant_.has_value() ||
      grant->group_term_ != intent.group_term_ ||
      grant->last_authority_version_ != intent.authority_version_ + 1 ||
      grant->last_grant_revision_ <= intent.grant_revision_ ||
      grant->grant_->owner_ != intent.candidate_node_id_ ||
      grant->grant_->term_ != intent.group_term_ ||
      grant->grant_->authority_version_ != intent.authority_version_ + 1 ||
      grant->grant_->grant_revision_ != grant->last_grant_revision_ ||
      grant->grant_->spec_ != intent.old_grant_ ||
      group->record_.owner_ != intent.candidate_node_id_ ||
      group->record_.group_term_ != intent.group_term_ ||
      group->record_.authority_version_ != intent.authority_version_ + 1) {
    return Invalid(
        "failover anchors do not match the activated candidate authority");
  }
  return absl::OkStatus();
}

absl::Status ValidateCommittedAnchors(const FailoverIntent& intent,
                                      const MetaCommittedView& view,
                                      CommittedAnchorState expected,
                                      bool require_candidate_member = true) {
  return ValidateCommittedAnchors(intent, view.stores(), expected,
                                  require_candidate_member);
}

CommittedAnchorState AnchorStateFor(FailoverPhaseStage stage) {
  if (stage < FailoverPhaseStage::kOldAuthorityExcluded) {
    return CommittedAnchorState::kOldAuthorityActive;
  }
  if (stage < FailoverPhaseStage::kAuthorityActivated) {
    return CommittedAnchorState::kFenced;
  }
  return CommittedAnchorState::kActivated;
}

std::optional<MetaCandidateProgressObs> ExactCandidateProgress(
    const FailoverIntent& intent, const MetaCommittedView& view,
    const MetaObservationStore& observations) {
  const MetaStoresFacts facts(view.stores());
  const std::vector<MetaCandidateProgressObs> candidates =
      observations.LiveCandidateProgressFor(intent.group_id_, facts,
                                            UnixMillisNow());
  const auto exact = std::find_if(
      candidates.begin(), candidates.end(),
      [&](const MetaCandidateProgressObs& candidate) {
        return candidate.node_id_ == intent.candidate_node_id_ &&
               candidate.boot_incarnation_ == intent.candidate_boot_id_ &&
               candidate.assignment_id_ == intent.candidate_assignment_id_ &&
               candidate.group_id_ == intent.group_id_ &&
               candidate.group_term_ == intent.group_term_ &&
               candidate.population_manifest_revision_ ==
                   intent.population_manifest_revision_ &&
               candidate.population_manifest_digest_ ==
                   intent.population_manifest_digest_ &&
               candidate.partition_replication_epoch_ ==
                   intent.partition_replication_epoch_ &&
               candidate.source_node_id_ == intent.former_owner_node_id_ &&
               candidate.source_assignment_id_ ==
                   intent.former_owner_assignment_id_ &&
               candidate.source_boot_incarnation_ ==
                   intent.former_owner_boot_id_ &&
               candidate.source_replication_history_id_ ==
                   intent.parent_history_id_ &&
               candidate.applied_next_lsns_.size() == intent.flow_count_;
      });
  return exact == candidates.end() ? std::nullopt : std::optional(*exact);
}

absl::Status ValidateCandidateCaughtUp(
    const FailoverIntent& intent, const FailoverPhase& phase,
    const MetaCommittedView& view, const MetaObservationStore& observations) {
  const auto exact = ExactCandidateProgress(intent, view, observations);
  if (!exact.has_value()) {
    return Invalid(
        "candidate-caught-up requires fresh exact candidate progress");
  }
  for (std::size_t flow = 0; flow < intent.flow_count_; ++flow) {
    if (exact->applied_next_lsns_[flow] <
        phase.required_applied_next_lsns_[flow]) {
      return Invalid("candidate progress is behind the frozen frontier");
    }
  }
  return absl::OkStatus();
}

struct LiveFailoverContext {
  MetaOperationRecord operation;
  FailoverIntent intent;
  std::optional<FailoverPhase> phase;
};

absl::StatusOr<std::optional<LiveFailoverContext>> LiveFailoverForGroup(
    std::string_view group_id, const MetaOperationStore& operations) {
  std::optional<LiveFailoverContext> found;
  for (const MetaOperationRecord& operation : operations.LiveOperations()) {
    if (operation.kind_ != kFailoverOperationKind ||
        (operation.lifecycle_ != MetaOperationLifecycle::kSubmitted &&
         operation.lifecycle_ != MetaOperationLifecycle::kRunning)) {
      continue;
    }
    auto intent = DecodeFailoverIntent(operation.intent_);
    if (!intent.ok() ||
        operation.intent_hash_ != MetaSha256(operation.intent_) ||
        operation.replication_history_id_ != intent->parent_history_id_) {
      return Invalid("committed live failover contains invalid typed intent");
    }
    if (intent->group_id_ != group_id) continue;
    if (found.has_value()) {
      return Invalid("multiple live failovers target the same group");
    }
    LiveFailoverContext context{
        .operation = operation, .intent = *intent, .phase = std::nullopt};
    if (!operation.kind_phase_blob_.empty()) {
      auto phase = DecodeFailoverPhase(operation.kind_phase_blob_);
      if (!phase.ok()) {
        return Invalid("committed live failover contains invalid typed phase");
      }
      context.phase = *phase;
    }
    found = std::move(context);
  }
  return found;
}

absl::StatusOr<std::optional<LiveFailoverContext>> LiveFailoverForGroup(
    std::string_view group_id, const MetaCommittedView& view) {
  return LiveFailoverForGroup(group_id, view.operation());
}

bool RecoveryMatchesIntent(const MetaFailoverRecoveryRecord& recovery,
                           const FailoverIntent& intent);

absl::Status ValidatePendingRecoveryForAuthorityCut(
    const FailoverIntent& intent, const MetaStores& stores) {
  const auto recovery = stores.failover_recovery_.Find(intent.group_id_);
  if (!recovery.has_value() || !RecoveryMatchesIntent(*recovery, intent) ||
      !recovery->hold_required_ || recovery->recovery_required_ ||
      recovery->proof_state_ != MetaFailoverProofState::kPending ||
      recovery->frozen_proof_.has_value()) {
    return Invalid(
        "BeginGroupTerm requires the canonical pending source proof");
  }
  return absl::OkStatus();
}

absl::Status ValidateBeginGroupTerm(const BeginGroupTerm& command,
                                    const MetaCommittedView& view) {
  const bool has_operation_id = !IsZero(command.workflow_operation_id_);
  if (has_operation_id != (command.expected_operation_revision_ != 0)) {
    return Invalid("BeginGroupTerm has an incomplete workflow fence");
  }
  auto live = LiveFailoverForGroup(command.group_id_, view);
  if (!live.ok()) return live.status();
  if (!live->has_value()) {
    return has_operation_id
               ? Invalid("BeginGroupTerm workflow owner is no longer live")
               : absl::OkStatus();
  }
  const LiveFailoverContext& context = **live;
  if (!has_operation_id ||
      command.workflow_operation_id_ != context.operation.operation_id_ ||
      command.expected_operation_revision_ != context.operation.revision_ ||
      !context.phase.has_value() ||
      command.expected_term_ != context.intent.group_term_ - 1 ||
      command.new_term_ != context.intent.group_term_) {
    return Invalid("BeginGroupTerm does not match the live failover intent");
  }
  const auto grant = view.grant().GroupState(command.group_id_);
  const bool already_excluded =
      grant.has_value() && grant->fenced_ && !grant->grant_.has_value() &&
      grant->group_term_ == context.intent.group_term_;
  if (already_excluded) {
    if (context.phase->stage_ < FailoverPhaseStage::kOldAuthorityExcluding ||
        context.phase->stage_ > FailoverPhaseStage::kPromotionPrepared) {
      return Invalid("BeginGroupTerm replay is outside the exclusion window");
    }
    return ValidateCommittedAnchors(context.intent, view,
                                    CommittedAnchorState::kFenced);
  }
  if (context.phase->stage_ != FailoverPhaseStage::kOldAuthorityExcluding) {
    return Invalid("BeginGroupTerm is premature for the live failover phase");
  }
  if (absl::Status recovery =
          ValidatePendingRecoveryForAuthorityCut(context.intent, view.stores());
      !recovery.ok()) {
    return recovery;
  }
  return ValidateCommittedAnchors(context.intent, view,
                                  CommittedAnchorState::kOldAuthorityActive);
}

absl::Status ValidateActivateAuthority(const ActivateAuthority& command,
                                       const MetaCommittedView& view) {
  const bool has_operation_id = !IsZero(command.workflow_operation_id_);
  if (has_operation_id != (command.expected_operation_revision_ != 0)) {
    return Invalid("ActivateAuthority has an incomplete workflow fence");
  }
  auto live = LiveFailoverForGroup(command.group_id_, view);
  if (!live.ok()) return live.status();
  if (!live->has_value()) {
    return has_operation_id
               ? Invalid("ActivateAuthority workflow owner is no longer live")
               : absl::OkStatus();
  }
  const LiveFailoverContext& context = **live;
  if (!has_operation_id ||
      command.workflow_operation_id_ != context.operation.operation_id_ ||
      command.expected_operation_revision_ != context.operation.revision_ ||
      !context.phase.has_value() ||
      context.phase->stage_ < FailoverPhaseStage::kPromotionPrepared ||
      command.expected_term_ != context.intent.group_term_ ||
      command.new_owner_ != context.intent.candidate_node_id_ ||
      command.new_authority_version_ != context.intent.authority_version_ + 1 ||
      command.grant_ != context.intent.old_grant_) {
    return Invalid("ActivateAuthority does not match the live failover intent");
  }
  const auto grant = view.grant().GroupState(command.group_id_);
  const bool already_activated =
      grant.has_value() && !grant->fenced_ && grant->grant_.has_value() &&
      grant->grant_->owner_ == context.intent.candidate_node_id_;
  if (already_activated) {
    if (context.phase->stage_ > FailoverPhaseStage::kServing) {
      return Invalid("ActivateAuthority replay is outside the failover graph");
    }
    return ValidateCommittedAnchors(context.intent, view,
                                    CommittedAnchorState::kActivated);
  }
  if (context.phase->stage_ != FailoverPhaseStage::kPromotionPrepared) {
    return Invalid(
        "ActivateAuthority is premature for the live failover phase");
  }
  return ValidateCommittedAnchors(context.intent, view,
                                  CommittedAnchorState::kFenced);
}

absl::Status ValidateHoldingSourceDirective(const MetaDirectiveSpec& directive,
                                            const FailoverIntent& intent) {
  const auto request =
      cluster::control::DecodeRebuildRequest(directive.payload_);
  if (directive.kind_ != kMetaDirectiveAuthorizeSource ||
      directive.recipient_node_id_ != intent.former_owner_node_id_ ||
      directive.target_node_id_ != intent.candidate_node_id_ ||
      directive.target_boot_id_ != intent.candidate_boot_id_ ||
      directive.assignment_id_ != intent.candidate_assignment_id_ ||
      directive.source_node_id_ != intent.former_owner_node_id_ ||
      directive.source_assignment_id_ != intent.former_owner_assignment_id_ ||
      directive.source_boot_id_ != intent.former_owner_boot_id_ ||
      directive.source_replication_history_id_ != intent.parent_history_id_ ||
      directive.group_id_ != intent.group_id_ ||
      directive.group_term_ != intent.group_term_ - 1 ||
      directive.authority_version_ != intent.authority_version_ ||
      directive.grant_revision_ != intent.grant_revision_ ||
      directive.population_manifest_revision_ !=
          intent.population_manifest_revision_ ||
      directive.population_manifest_digest_ !=
          intent.population_manifest_digest_ ||
      directive.partition_replication_epoch_ !=
          intent.partition_replication_epoch_ ||
      !request.ok() || request->source_flow_count != intent.flow_count_ ||
      !directive.preconditions_.empty() || directive.storage_mutating_ ||
      directive.force_) {
    return Invalid("source-hold authorize directive changed failover anchors");
  }
  return absl::OkStatus();
}

absl::Status ValidateFrozenSourceDirective(const MetaDirectiveSpec& directive,
                                           const FailoverIntent& intent) {
  auto request =
      cluster::control::DecodeFrozenSourceRequest(directive.payload_);
  auto preconditions = cluster::control::DecodeFrozenSourcePreconditions(
      directive.preconditions_);
  if (directive.kind_ != kMetaDirectiveAuthorizeSource ||
      directive.recipient_node_id_ != intent.former_owner_node_id_ ||
      directive.target_node_id_ != intent.candidate_node_id_ ||
      directive.target_boot_id_ != intent.candidate_boot_id_ ||
      directive.assignment_id_ != intent.candidate_assignment_id_ ||
      directive.source_node_id_ != intent.former_owner_node_id_ ||
      directive.source_assignment_id_ != intent.former_owner_assignment_id_ ||
      directive.source_boot_id_ != intent.former_owner_boot_id_ ||
      directive.source_replication_history_id_ != intent.parent_history_id_ ||
      directive.group_id_ != intent.group_id_ ||
      directive.group_term_ != intent.group_term_ ||
      directive.authority_version_ != intent.authority_version_ ||
      directive.grant_revision_ != intent.grant_revision_ ||
      directive.population_manifest_revision_ !=
          intent.population_manifest_revision_ ||
      directive.population_manifest_digest_ !=
          intent.population_manifest_digest_ ||
      directive.partition_replication_epoch_ !=
          intent.partition_replication_epoch_ ||
      directive.storage_mutating_ || directive.force_ || !request.ok() ||
      !preconditions.ok() ||
      request->recovery_generation != intent.recovery_generation_ ||
      request->source_flow_count != intent.flow_count_ ||
      preconditions->excluded_group_term != intent.group_term_ - 1 ||
      preconditions->excluded_authority_version != intent.authority_version_ ||
      preconditions->excluded_grant_revision != intent.grant_revision_) {
    return Invalid("frozen-source directive changed failover anchors");
  }
  return absl::OkStatus();
}

absl::Status ValidatePreparingDirective(const MetaDirectiveSpec& directive,
                                        const FailoverIntent& intent,
                                        const FailoverPhase& phase) {
  if (directive.kind_ != "promotion-prepare" ||
      directive.recipient_node_id_ != intent.candidate_node_id_ ||
      directive.target_node_id_ != intent.candidate_node_id_ ||
      directive.target_boot_id_ != intent.candidate_boot_id_ ||
      directive.assignment_id_ != intent.candidate_assignment_id_ ||
      directive.source_node_id_ != intent.former_owner_node_id_ ||
      directive.source_assignment_id_ != intent.former_owner_assignment_id_ ||
      directive.source_boot_id_ != intent.former_owner_boot_id_ ||
      directive.source_replication_history_id_ != intent.parent_history_id_ ||
      directive.group_id_ != intent.group_id_ ||
      directive.group_term_ != intent.group_term_ ||
      directive.authority_version_ != intent.authority_version_ ||
      directive.grant_revision_ != intent.grant_revision_ ||
      directive.population_manifest_revision_ !=
          intent.population_manifest_revision_ ||
      directive.population_manifest_digest_ !=
          intent.population_manifest_digest_ ||
      directive.partition_replication_epoch_ !=
          intent.partition_replication_epoch_ ||
      !directive.storage_mutating_ || directive.force_) {
    return Invalid("promotion-prepare directive changed failover anchors");
  }
  auto request =
      cluster::control::DecodePromotionPrepareRequest(directive.payload_);
  auto preconditions = cluster::control::DecodePromotionPreparePreconditions(
      directive.preconditions_);
  if (!request.ok() || !preconditions.ok() ||
      request->parent_history_id != Hex(intent.parent_history_id_) ||
      request->required_applied_next_lsns !=
          phase.required_applied_next_lsns_ ||
      preconditions->excluded_group_term != intent.group_term_ ||
      preconditions->old_authority_exclusion_hash !=
          phase.old_authority_exclusion_hash_) {
    return Invalid("promotion-prepare payload or preconditions are stale");
  }
  return absl::OkStatus();
}

const MetaTerminalReceipt* FindCurrentReceipt(
    const MetaOperationRecord& operation,
    const MetaCurrentDirective& directive) {
  const auto found = std::find_if(
      operation.terminal_receipts_.begin(), operation.terminal_receipts_.end(),
      [&](const MetaTerminalReceipt& receipt) {
        return receipt.key_.operation_id_ == operation.operation_id_ &&
               receipt.key_.directive_id_ == directive.spec_.directive_id_ &&
               receipt.key_.attempt_id_ == directive.spec_.attempt_id_ &&
               receipt.key_.directive_revision_ ==
                   directive.directive_revision_;
      });
  return found == operation.terminal_receipts_.end() ? nullptr : &*found;
}

const MetaTerminalReceipt* FindSuccessfulReceipt(
    const MetaOperationRecord& operation,
    const MetaCurrentDirective& directive) {
  const MetaTerminalReceipt* receipt = FindCurrentReceipt(operation, directive);
  return receipt != nullptr &&
                 receipt->status_ == MetaDirectiveResultStatus::kSucceeded
             ? receipt
             : nullptr;
}

absl::Status ValidateRetainedFrozenSourceDirective(
    const TransitionOperationPhase& transition,
    const MetaOperationRecord& operation, const FailoverIntent& intent) {
  if (operation.current_directives_.size() > 1 ||
      transition.current_directives_.size() !=
          operation.current_directives_.size()) {
    return Invalid(
        "failover transition did not preserve its frozen-source directive");
  }
  if (operation.current_directives_.empty()) return absl::OkStatus();
  const MetaCurrentDirective& current = operation.current_directives_.front();
  if (transition.current_directives_.front() != current.spec_) {
    return Invalid("failover transition changed its frozen-source directive");
  }
  return ValidateFrozenSourceDirective(current.spec_, intent);
}

absl::Status ValidateSourceHeldTransition(
    const TransitionOperationPhase& transition,
    const MetaOperationRecord& operation, const FailoverIntent& intent) {
  if (!transition.current_directives_.empty() ||
      !transition.evidence_.empty() ||
      operation.current_directives_.size() != 1) {
    return Invalid("source-held requires one prior authorize-source directive");
  }
  const MetaCurrentDirective& current = operation.current_directives_.front();
  if (absl::Status status =
          ValidateHoldingSourceDirective(current.spec_, intent);
      !status.ok()) {
    return status;
  }
  const MetaTerminalReceipt* receipt =
      FindSuccessfulReceipt(operation, current);
  if (receipt == nullptr ||
      receipt->recipient_node_id_ != intent.former_owner_node_id_ ||
      receipt->recipient_boot_id_ != intent.former_owner_boot_id_ ||
      receipt->assignment_id_ != intent.candidate_assignment_id_) {
    return Invalid("source-held is missing its exact successful receipt");
  }
  return absl::OkStatus();
}

absl::Status ValidateFrozenProofTransition(
    const TransitionOperationPhase& transition,
    const MetaOperationRecord& operation, const FailoverIntent& intent,
    const FailoverPhase& phase, const MetaCommittedView& view,
    const MetaObservationStore& observations) {
  if (!transition.evidence_.empty()) {
    return Invalid("old-authority-excluded carries invalid frozen-source work");
  }
  if (absl::Status retained =
          ValidateRetainedFrozenSourceDirective(transition, operation, intent);
      !retained.ok()) {
    return retained;
  }
  const auto recovery = view.failover_recovery().Find(intent.group_id_);
  if (!recovery.has_value() ||
      recovery->recovery_generation_ != intent.recovery_generation_ ||
      recovery->old_source_node_id_ != intent.former_owner_node_id_ ||
      recovery->old_source_assignment_id_ !=
          intent.former_owner_assignment_id_ ||
      recovery->old_source_boot_incarnation_ != intent.former_owner_boot_id_ ||
      recovery->old_source_history_id_ != intent.parent_history_id_ ||
      recovery->excluded_authority_term_ != intent.group_term_ - 1 ||
      recovery->excluded_authority_version_ != intent.authority_version_ ||
      recovery->excluded_grant_revision_ != intent.grant_revision_ ||
      recovery->population_manifest_revision_ !=
          intent.population_manifest_revision_ ||
      recovery->population_manifest_digest_ !=
          intent.population_manifest_digest_ ||
      recovery->partition_replication_epoch_ !=
          intent.partition_replication_epoch_) {
    return Invalid(
        "old-authority-excluded recovery anchors do not match its intent");
  }

  const MetaCurrentDirective* current =
      operation.current_directives_.empty()
          ? nullptr
          : &operation.current_directives_.front();
  if (current != nullptr) {
    if (absl::Status status =
            ValidateFrozenSourceDirective(current->spec_, intent);
        !status.ok()) {
      return status;
    }
  }
  const MetaTerminalReceipt* receipt =
      current == nullptr ? nullptr : FindCurrentReceipt(operation, *current);
  const auto exact_recipient = [&] {
    return receipt != nullptr &&
           receipt->recipient_node_id_ == intent.former_owner_node_id_ &&
           receipt->recipient_boot_id_ == intent.former_owner_boot_id_ &&
           receipt->assignment_id_ == intent.candidate_assignment_id_;
  };
  const auto historical_exact_proof_is_valid = [&] {
    if (current == nullptr || !recovery->frozen_proof_.has_value() ||
        !exact_recipient() ||
        receipt->status_ != MetaDirectiveResultStatus::kSucceeded ||
        MetaSha256(receipt->result_) != receipt->result_hash_) {
      return false;
    }
    auto evidence =
        cluster::control::DecodeFrozenSourceEvidence(receipt->result_);
    return evidence.ok() &&
           evidence->recovery_generation == intent.recovery_generation_ &&
           evidence->source_history_id == Hex(intent.parent_history_id_) &&
           evidence->final_next_lsns ==
               recovery->frozen_proof_->final_next_lsns_ &&
           evidence->proof_hash == recovery->frozen_proof_->proof_hash_;
  };
  const auto phase_uses_historical_exact_proof = [&] {
    return recovery->frozen_proof_.has_value() &&
           recovery->frozen_proof_->final_next_lsns_ ==
               phase.required_applied_next_lsns_ &&
           recovery->frozen_proof_->proof_hash_ ==
               phase.old_authority_exclusion_hash_;
  };
  if (recovery->proof_state_ == MetaFailoverProofState::kExact) {
    if (!historical_exact_proof_is_valid() ||
        !phase_uses_historical_exact_proof()) {
      return Invalid(
          "old-authority-excluded is missing its exact durable frozen proof");
    }
    return absl::OkStatus();
  }

  if (recovery->proof_state_ != MetaFailoverProofState::kUnavailable) {
    return Invalid("old-authority-excluded proof is still pending");
  }
  if (recovery->frozen_proof_.has_value()) {
    if (!historical_exact_proof_is_valid()) {
      return Invalid(
          "unavailable source lost its historical exact proof anchors");
    }
    if (phase_uses_historical_exact_proof()) {
      return ValidateCandidateCaughtUp(intent, phase, view, observations);
    }
  }
  // A recorded terminal response must be an exact failure. No directive is
  // also legal: the exact old-source incarnation may disappear after the
  // authority fence but before dispatch. In both cases the durable
  // kUnavailable record plus a fresh candidate-backed vector is the monotonic
  // downgrade; absence itself is never represented as an exact proof.
  if (!recovery->frozen_proof_.has_value() && receipt != nullptr &&
      (!exact_recipient() ||
       (receipt->status_ != MetaDirectiveResultStatus::kFailed &&
        receipt->status_ != MetaDirectiveResultStatus::kRejected))) {
    return Invalid("unavailable frozen proof contradicts its terminal receipt");
  }
  auto unavailable_hash = ComputeFailoverUnavailableProofHash(
      intent, phase.required_applied_next_lsns_);
  if (!unavailable_hash.ok() ||
      *unavailable_hash != phase.old_authority_exclusion_hash_) {
    return Invalid("unavailable frozen proof has a stale proof summary");
  }
  if (absl::Status progress =
          ValidateCandidateCaughtUp(intent, phase, view, observations);
      !progress.ok()) {
    return progress;
  }
  return absl::OkStatus();
}

absl::Status ValidateFrozenFrontierDowngrade(
    const TransitionOperationPhase& transition,
    const MetaOperationRecord& operation, const FailoverIntent& intent,
    const FailoverPhase& previous, const FailoverPhase& next,
    const MetaCommittedView& view, const MetaObservationStore& observations) {
  if (!transition.evidence_.empty()) {
    return Invalid("frozen frontier downgrade cannot carry evidence work");
  }
  if (absl::Status retained =
          ValidateRetainedFrozenSourceDirective(transition, operation, intent);
      !retained.ok()) {
    return retained;
  }
  const auto recovery = view.failover_recovery().Find(intent.group_id_);
  if (!recovery.has_value() ||
      recovery->recovery_generation_ != intent.recovery_generation_ ||
      recovery->proof_state_ != MetaFailoverProofState::kUnavailable ||
      !recovery->frozen_proof_.has_value() ||
      previous.required_applied_next_lsns_ !=
          recovery->frozen_proof_->final_next_lsns_ ||
      previous.old_authority_exclusion_hash_ !=
          recovery->frozen_proof_->proof_hash_) {
    return Invalid(
        "frozen frontier downgrade lacks its retained historical proof");
  }
  if (next.required_applied_next_lsns_.size() !=
      previous.required_applied_next_lsns_.size()) {
    return Invalid("frozen frontier downgrade changed flow cardinality");
  }
  bool strictly_lower = false;
  for (std::size_t flow = 0; flow < next.required_applied_next_lsns_.size();
       ++flow) {
    if (next.required_applied_next_lsns_[flow] >
        previous.required_applied_next_lsns_[flow]) {
      return Invalid("frozen frontier downgrade advanced beyond final source");
    }
    strictly_lower =
        strictly_lower || next.required_applied_next_lsns_[flow] <
                              previous.required_applied_next_lsns_[flow];
  }
  const auto candidate = ExactCandidateProgress(intent, view, observations);
  if (!strictly_lower || !candidate.has_value() ||
      candidate->applied_next_lsns_ != next.required_applied_next_lsns_) {
    return Invalid(
        "frozen frontier downgrade is not the current exact candidate cut");
  }
  auto expected_hash = ComputeFailoverUnavailableProofHash(
      intent, next.required_applied_next_lsns_);
  if (!expected_hash.ok() ||
      *expected_hash != next.old_authority_exclusion_hash_) {
    return Invalid("frozen frontier downgrade has an invalid proof summary");
  }
  return ValidateCommittedAnchors(intent, view, CommittedAnchorState::kFenced);
}

absl::Status ValidatePreparedTransition(
    const TransitionOperationPhase& transition,
    const MetaOperationRecord& operation, const FailoverIntent& intent,
    const FailoverPhase& phase, const MetaCommittedView& view,
    const MetaObservationStore& observations) {
  if (!transition.current_directives_.empty() ||
      transition.evidence_.size() != 1 ||
      operation.current_directives_.size() != 1) {
    return Invalid(
        "promotion-prepared requires one prior directive and one evidence");
  }
  const MetaCurrentDirective& current = operation.current_directives_.front();
  const MetaTerminalReceipt* receipt =
      FindSuccessfulReceipt(operation, current);
  if (receipt == nullptr) {
    return Invalid(
        "promotion-prepared is missing its exact successful receipt");
  }
  // A terminal receipt proves what was committed to the operation journal;
  // the current-session observation proves those bytes were actually reported
  // by this exact candidate boot. Requiring both prevents a proposer from
  // fabricating a matching summary from committed fields alone.
  auto evidence = ResolveFailoverPreparedEvidence(
      view, operation, intent, phase, observations, *receipt, UnixMillisNow());
  if (!evidence.ok()) return evidence.status();
  if (!evidence->has_value()) {
    return Invalid(
        "promotion-prepared is missing fresh exact candidate evidence");
  }
  if (receipt->result_hash_ != phase.prepared_result_hash_ ||
      transition.evidence_.front() != **evidence) {
    return Invalid("promotion-prepared evidence summary changed its anchors");
  }
  return absl::OkStatus();
}

bool RecoveryMatchesIntent(const MetaFailoverRecoveryRecord& recovery,
                           const FailoverIntent& intent) {
  return recovery.group_id_ == intent.group_id_ &&
         recovery.recovery_generation_ == intent.recovery_generation_ &&
         recovery.old_source_node_id_ == intent.former_owner_node_id_ &&
         recovery.old_source_assignment_id_ ==
             intent.former_owner_assignment_id_ &&
         recovery.old_source_boot_incarnation_ ==
             intent.former_owner_boot_id_ &&
         recovery.old_source_history_id_ == intent.parent_history_id_ &&
         recovery.excluded_authority_term_ == intent.group_term_ - 1 &&
         recovery.excluded_authority_version_ == intent.authority_version_ &&
         recovery.excluded_grant_revision_ == intent.grant_revision_ &&
         recovery.population_manifest_revision_ ==
             intent.population_manifest_revision_ &&
         recovery.population_manifest_digest_ ==
             intent.population_manifest_digest_ &&
         recovery.partition_replication_epoch_ ==
             intent.partition_replication_epoch_;
}

absl::StatusOr<LiveFailoverContext> WorkflowOwnerContext(
    const MetaOperationId& operation_id, std::string_view group_id,
    const MetaStores& stores) {
  const auto operation = stores.operation_.FindOperation(operation_id);
  if (!operation.has_value() || operation->kind_ != kFailoverOperationKind) {
    return Invalid("authority mutation has no matching failover owner");
  }
  auto intent = DecodeFailoverIntent(operation->intent_);
  if (!intent.ok() || intent->group_id_ != group_id ||
      operation->intent_hash_ != MetaSha256(operation->intent_) ||
      operation->replication_history_id_ != intent->parent_history_id_ ||
      operation->kind_phase_blob_.empty()) {
    return Invalid("authority mutation has invalid failover ownership");
  }
  auto phase = DecodeFailoverPhase(operation->kind_phase_blob_);
  if (!phase.ok()) {
    return Invalid("authority mutation owner has invalid durable phase");
  }
  return LiveFailoverContext{
      .operation = *operation, .intent = *intent, .phase = *phase};
}

absl::Status ValidateWorkflowFenceShape(
    const MetaOperationId& operation_id,
    std::uint64_t expected_operation_revision) {
  if (IsZero(operation_id) != (expected_operation_revision == 0)) {
    return Invalid("authority mutation has an incomplete workflow fence");
  }
  return absl::OkStatus();
}

absl::Status ValidateOrdinaryAuthorityMutation(std::string_view group_id,
                                               const MetaStores& stores) {
  auto live = LiveFailoverForGroup(group_id, stores.operation_);
  if (!live.ok()) return live.status();
  return live->has_value()
             ? Invalid(
                   "ordinary authority mutation conflicts with a live "
                   "controlled failover")
             : absl::OkStatus();
}

absl::Status ValidateFailoverWorkflowCommandImpl(const BeginGroupTerm& command,
                                                 const MetaStores& stores,
                                                 std::uint64_t log_index) {
  (void)log_index;
  if (absl::Status shape = ValidateWorkflowFenceShape(
          command.workflow_operation_id_, command.expected_operation_revision_);
      !shape.ok()) {
    return shape;
  }
  auto live = LiveFailoverForGroup(command.group_id_, stores.operation_);
  if (!live.ok()) return live.status();
  if (IsZero(command.workflow_operation_id_)) {
    return live->has_value()
               ? Invalid(
                     "live failover authority cut requires a workflow fence")
               : absl::OkStatus();
  }
  auto context = WorkflowOwnerContext(command.workflow_operation_id_,
                                      command.group_id_, stores);
  if (!context.ok()) return context.status();
  if (command.expected_term_ != context->intent.group_term_ - 1 ||
      command.new_term_ != context->intent.group_term_) {
    return Invalid("BeginGroupTerm changed its failover intent anchors");
  }

  // Once the exact fenced state is present this command is a harmless replay,
  // including after its owner terminalized. Before that effect exists, the
  // live phase/revision fence is mandatory.
  if (ValidateCommittedAnchors(context->intent, stores,
                               CommittedAnchorState::kFenced,
                               /*require_candidate_member=*/false)
          .ok()) {
    return absl::OkStatus();
  }
  if (context->operation.lifecycle_ != MetaOperationLifecycle::kSubmitted &&
      context->operation.lifecycle_ != MetaOperationLifecycle::kRunning) {
    return Invalid("BeginGroupTerm owner is already terminal");
  }
  if (context->operation.revision_ != command.expected_operation_revision_ ||
      !context->phase.has_value() ||
      context->phase->stage_ != FailoverPhaseStage::kOldAuthorityExcluding) {
    return Invalid("BeginGroupTerm workflow phase or revision is stale");
  }
  if (absl::Status recovery =
          ValidatePendingRecoveryForAuthorityCut(context->intent, stores);
      !recovery.ok()) {
    return recovery;
  }
  return ValidateCommittedAnchors(context->intent, stores,
                                  CommittedAnchorState::kOldAuthorityActive);
}

absl::Status ValidateFailoverWorkflowCommandImpl(
    const ActivateAuthority& command, const MetaStores& stores,
    std::uint64_t log_index) {
  if (absl::Status shape = ValidateWorkflowFenceShape(
          command.workflow_operation_id_, command.expected_operation_revision_);
      !shape.ok()) {
    return shape;
  }
  auto live = LiveFailoverForGroup(command.group_id_, stores.operation_);
  if (!live.ok()) return live.status();
  if (IsZero(command.workflow_operation_id_)) {
    return live->has_value()
               ? Invalid(
                     "live failover authority activation requires a workflow "
                     "fence")
               : absl::OkStatus();
  }
  auto context = WorkflowOwnerContext(command.workflow_operation_id_,
                                      command.group_id_, stores);
  if (!context.ok()) return context.status();
  if (command.expected_term_ != context->intent.group_term_ ||
      command.new_owner_ != context->intent.candidate_node_id_ ||
      command.new_authority_version_ !=
          context->intent.authority_version_ + 1 ||
      command.grant_ != context->intent.old_grant_) {
    return Invalid("ActivateAuthority changed its failover intent anchors");
  }

  const auto group = stores.topology_.FindGroup(command.group_id_);
  const auto grant = stores.grant_.GroupState(command.group_id_);
  const bool exact_replay =
      group.has_value() && grant.has_value() && !grant->fenced_ &&
      grant->grant_.has_value() && log_index != 0 &&
      grant->grant_->owner_ == command.new_owner_ &&
      grant->grant_->term_ == command.expected_term_ &&
      grant->grant_->authority_version_ == command.new_authority_version_ &&
      grant->grant_->grant_revision_ == log_index &&
      grant->grant_->spec_ == command.grant_ &&
      group->record_.owner_ == command.new_owner_ &&
      group->record_.group_term_ == command.expected_term_ &&
      group->record_.authority_version_ == command.new_authority_version_ &&
      group->config_epoch_ == command.new_config_epoch_ &&
      stores.topology_.TopologyEpoch() == command.new_topology_epoch_;
  if (exact_replay) return absl::OkStatus();

  if (context->operation.lifecycle_ != MetaOperationLifecycle::kSubmitted &&
      context->operation.lifecycle_ != MetaOperationLifecycle::kRunning) {
    return Invalid("ActivateAuthority owner is already terminal");
  }
  if (context->operation.revision_ != command.expected_operation_revision_ ||
      !context->phase.has_value() ||
      context->phase->stage_ != FailoverPhaseStage::kPromotionPrepared) {
    return Invalid("ActivateAuthority workflow phase or revision is stale");
  }
  return ValidateCommittedAnchors(context->intent, stores,
                                  CommittedAnchorState::kFenced);
}

template <typename Command>
absl::Status ValidateFailoverTerminalCommandImpl(const Command& command,
                                                 const MetaStores& stores) {
  constexpr bool kCompleting = std::is_same_v<Command, CompleteOperation>;
  static_assert(kCompleting || std::is_same_v<Command, AbortOperation>);
  const std::string& payload = [&]() -> const std::string& {
    if constexpr (kCompleting) {
      return command.result_;
    } else {
      return command.reason_;
    }
  }();
  const MetaOperationLifecycle expected_terminal =
      kCompleting ? MetaOperationLifecycle::kCompleted
                  : MetaOperationLifecycle::kAborted;

  const auto operation = stores.operation_.FindOperation(command.operation_id_);
  if (!operation.has_value() || operation->kind_ != kFailoverOperationKind) {
    return absl::OkStatus();
  }
  if (command.expected_revision_ == std::numeric_limits<std::uint64_t>::max()) {
    return Invalid("failover terminal expected_revision overflow");
  }
  if (operation->lifecycle_ == expected_terminal &&
      operation->revision_ == command.expected_revision_ + 1 &&
      operation->terminal_result_ == payload &&
      operation->data_loss_possible_ == command.data_loss_possible_) {
    return absl::OkStatus();
  }
  if (operation->lifecycle_ == MetaOperationLifecycle::kCompleted ||
      operation->lifecycle_ == MetaOperationLifecycle::kAborted ||
      operation->revision_ != command.expected_revision_) {
    return Invalid("failover terminal operation revision is stale");
  }

  auto intent = DecodeFailoverIntent(operation->intent_);
  auto phase = DecodeFailoverPhase(operation->kind_phase_blob_);
  auto outcome = DecodeControlledFailoverOutcome(payload);
  if (!intent.ok() || !phase.ok() || !outcome.ok() ||
      operation->intent_hash_ != MetaSha256(operation->intent_) ||
      operation->replication_history_id_ != intent->parent_history_id_) {
    return Invalid("failover terminal command has invalid typed state");
  }
  if (outcome->terminal_stage_ != phase->stage_ ||
      outcome->loss_ == FailoverLossClassification::kBounded ||
      command.data_loss_possible_ !=
          (outcome->loss_ != FailoverLossClassification::kExact)) {
    return Invalid("failover terminal outcome contradicts its durable phase");
  }

  const auto recovery = stores.failover_recovery_.Find(intent->group_id_);
  if (!recovery.has_value() || !RecoveryMatchesIntent(*recovery, *intent) ||
      !recovery->hold_required_ || recovery->recovery_required_) {
    return Invalid("failover terminal recovery handoff is stale");
  }

  const bool require_candidate_member =
      kCompleting || phase->stage_ >= FailoverPhaseStage::kAuthorityActivated;
  bool authority_cut = false;
  switch (phase->stage_) {
    case FailoverPhaseStage::kSourceHolding:
    case FailoverPhaseStage::kSourceHeld:
      if (absl::Status status = ValidateCommittedAnchors(
              *intent, stores, CommittedAnchorState::kOldAuthorityActive,
              require_candidate_member);
          !status.ok()) {
        return status;
      }
      break;
    case FailoverPhaseStage::kOldAuthorityExcluding: {
      const absl::Status active = ValidateCommittedAnchors(
          *intent, stores, CommittedAnchorState::kOldAuthorityActive,
          require_candidate_member);
      if (active.ok()) break;
      if (absl::Status fenced = ValidateCommittedAnchors(
              *intent, stores, CommittedAnchorState::kFenced,
              require_candidate_member);
          !fenced.ok()) {
        return Invalid(
            "failover terminal authority is neither active nor fenced");
      }
      authority_cut = true;
      break;
    }
    case FailoverPhaseStage::kOldAuthorityExcluded:
    case FailoverPhaseStage::kCandidateCaughtUp:
    case FailoverPhaseStage::kPromotionPreparing:
    case FailoverPhaseStage::kPromotionPrepared:
      if (absl::Status status = ValidateCommittedAnchors(
              *intent, stores, CommittedAnchorState::kFenced,
              require_candidate_member);
          !status.ok()) {
        return status;
      }
      authority_cut = true;
      break;
    case FailoverPhaseStage::kAuthorityActivated:
    case FailoverPhaseStage::kServing:
      if (absl::Status status = ValidateCommittedAnchors(
              *intent, stores, CommittedAnchorState::kActivated,
              require_candidate_member);
          !status.ok()) {
        return status;
      }
      authority_cut = true;
      break;
  }

  const bool phase_uses_exact_proof =
      recovery->frozen_proof_.has_value() &&
      phase->old_authority_exclusion_hash_ ==
          recovery->frozen_proof_->proof_hash_ &&
      phase->required_applied_next_lsns_ ==
          recovery->frozen_proof_->final_next_lsns_;
  const std::vector<std::uint64_t> expected_frontier =
      !phase->required_applied_next_lsns_.empty()
          ? phase->required_applied_next_lsns_
      : recovery->frozen_proof_.has_value()
          ? recovery->frozen_proof_->final_next_lsns_
          : std::vector<std::uint64_t>{};

  if constexpr (kCompleting) {
    const FailoverLossClassification expected_loss =
        phase_uses_exact_proof ? FailoverLossClassification::kExact
                               : FailoverLossClassification::kUnknown;
    if (!outcome->succeeded_ || phase->stage_ != FailoverPhaseStage::kServing ||
        outcome->recovery_required_ || outcome->loss_ != expected_loss ||
        outcome->proven_next_lsns_ != phase->required_applied_next_lsns_ ||
        recovery->proof_state_ == MetaFailoverProofState::kPending ||
        (recovery->proof_state_ == MetaFailoverProofState::kExact &&
         !phase_uses_exact_proof)) {
      return Invalid("failover completion is not justified by serving state");
    }
    return absl::OkStatus();
  }

  if (outcome->succeeded_ || outcome->proven_next_lsns_ != expected_frontier) {
    return Invalid("failover abort outcome is inconsistent");
  }
  if (!authority_cut) {
    if (outcome->recovery_required_) {
      if (outcome->loss_ != FailoverLossClassification::kUnknown ||
          recovery->proof_state_ != MetaFailoverProofState::kUnavailable) {
        return Invalid(
            "pre-cutover recovery handoff must durably record unavailable "
            "source proof before reporting unknown loss");
      }
    } else if (recovery->proof_state_ != MetaFailoverProofState::kPending ||
               outcome->loss_ != FailoverLossClassification::kExact ||
               !outcome->proven_next_lsns_.empty()) {
      return Invalid("pre-cutover cancellation is not proven lossless");
    }
    return absl::OkStatus();
  }
  if (!outcome->recovery_required_) {
    return Invalid("post-cutover abort lost its recovery handoff");
  }
  if (outcome->loss_ == FailoverLossClassification::kExact &&
      (recovery->proof_state_ != MetaFailoverProofState::kExact ||
       !recovery->frozen_proof_.has_value() ||
       (!phase->required_applied_next_lsns_.empty() &&
        !phase_uses_exact_proof))) {
    return Invalid("post-cutover exact abort lost its durable proof");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverArchiveCommandImpl(
    const ArchiveOperations& command, const MetaStores& stores) {
  for (const std::uint64_t operation_seq : command.operation_seqs_) {
    // Archive replay deliberately remains a no-op after the live typed state
    // has been reduced to its generic summary.
    if (stores.operation_.FindArchivedBySeq(operation_seq).has_value()) {
      continue;
    }
    const auto operation = stores.operation_.FindOperationBySeq(operation_seq);
    if (!operation.has_value() || operation->kind_ != kFailoverOperationKind) {
      continue;
    }
    const bool completed =
        operation->lifecycle_ == MetaOperationLifecycle::kCompleted;
    const bool aborted =
        operation->lifecycle_ == MetaOperationLifecycle::kAborted;
    if (!completed && !aborted) {
      // Preserve the generic store's established rejection detail.
      continue;
    }

    auto intent = DecodeFailoverIntent(operation->intent_);
    auto phase = DecodeFailoverPhase(operation->kind_phase_blob_);
    auto outcome = DecodeControlledFailoverOutcome(operation->terminal_result_);
    if (!intent.ok() || !phase.ok() || !outcome.ok() ||
        operation->intent_hash_ != MetaSha256(operation->intent_) ||
        operation->replication_history_id_ != intent->parent_history_id_ ||
        outcome->terminal_stage_ != phase->stage_ ||
        outcome->succeeded_ != completed ||
        operation->data_loss_possible_ !=
            (outcome->loss_ != FailoverLossClassification::kExact)) {
      return Invalid("failover archive has invalid typed terminal state");
    }

    const auto last_generation =
        stores.failover_recovery_.LastGeneration(intent->group_id_);
    if (!last_generation.has_value() ||
        *last_generation < intent->recovery_generation_) {
      return Invalid("failover archive lost its recovery generation cursor");
    }
    // A successor may cross this boundary only from an explicit handoff or a
    // cleared generation. Its monotonic cursor is therefore durable evidence
    // that this terminal owner's cleanup responsibility has ended.
    if (*last_generation > intent->recovery_generation_) continue;

    const auto recovery = stores.failover_recovery_.Find(intent->group_id_);
    if (outcome->recovery_required_) {
      if (!recovery.has_value() || !RecoveryMatchesIntent(*recovery, *intent) ||
          !recovery->hold_required_ || !recovery->recovery_required_) {
        return Invalid(
            "failover archive precedes its durable recovery handoff");
      }
    } else if (recovery.has_value()) {
      return Invalid("failover archive precedes recovery cleanup");
    }
  }
  return absl::OkStatus();
}

struct TerminalCleanupContext {
  FailoverIntent intent;
  ControlledFailoverOutcome outcome;
};

absl::StatusOr<std::optional<TerminalCleanupContext>>
MaybeFindTerminalCleanupOwner(const MetaFailoverRecoveryRecord& recovery,
                              const MetaStores& stores) {
  std::optional<TerminalCleanupContext> found;
  for (const MetaOperationRecord& operation :
       stores.operation_.LiveOperations()) {
    if (operation.kind_ != kFailoverOperationKind) continue;
    auto intent = DecodeFailoverIntent(operation.intent_);
    if (!intent.ok()) {
      return Invalid("failover cleanup encountered corrupt typed intent");
    }
    if (intent->group_id_ != recovery.group_id_ ||
        intent->recovery_generation_ != recovery.recovery_generation_) {
      continue;
    }
    if (found.has_value()) {
      return Invalid("failover cleanup has multiple generation owners");
    }
    const bool completed =
        operation.lifecycle_ == MetaOperationLifecycle::kCompleted;
    const bool aborted =
        operation.lifecycle_ == MetaOperationLifecycle::kAborted;
    auto phase = DecodeFailoverPhase(operation.kind_phase_blob_);
    auto outcome = DecodeControlledFailoverOutcome(operation.terminal_result_);
    if ((!completed && !aborted) || !phase.ok() || !outcome.ok() ||
        !RecoveryMatchesIntent(recovery, *intent) ||
        operation.intent_hash_ != MetaSha256(operation.intent_) ||
        operation.replication_history_id_ != intent->parent_history_id_ ||
        outcome->terminal_stage_ != phase->stage_ ||
        outcome->succeeded_ != completed ||
        operation.data_loss_possible_ !=
            (outcome->loss_ != FailoverLossClassification::kExact)) {
      return Invalid("failover cleanup owner has invalid terminal state");
    }
    found = TerminalCleanupContext{.intent = std::move(*intent),
                                   .outcome = std::move(*outcome)};
  }
  return found;
}

absl::StatusOr<TerminalCleanupContext> FindTerminalCleanupOwner(
    const MetaFailoverRecoveryRecord& recovery, const MetaStores& stores) {
  auto found = MaybeFindTerminalCleanupOwner(recovery, stores);
  if (!found.ok()) return found.status();
  if (!found->has_value()) {
    return Invalid("failover cleanup has no matching terminal owner");
  }
  return std::move(**found);
}

bool RecoveryCommandMatchesAnchors(const SetFailoverRecovery& command,
                                   const MetaFailoverRecoveryRecord& recovery) {
  return command.group_id_ == recovery.group_id_ &&
         command.recovery_generation_ == recovery.recovery_generation_ &&
         command.old_source_node_id_ == recovery.old_source_node_id_ &&
         command.old_source_assignment_id_ ==
             recovery.old_source_assignment_id_ &&
         command.old_source_boot_incarnation_ ==
             recovery.old_source_boot_incarnation_ &&
         command.old_source_history_id_ == recovery.old_source_history_id_ &&
         command.excluded_authority_term_ ==
             recovery.excluded_authority_term_ &&
         command.excluded_authority_version_ ==
             recovery.excluded_authority_version_ &&
         command.excluded_grant_revision_ ==
             recovery.excluded_grant_revision_ &&
         command.population_manifest_revision_ ==
             recovery.population_manifest_revision_ &&
         command.population_manifest_digest_ ==
             recovery.population_manifest_digest_ &&
         command.partition_replication_epoch_ ==
             recovery.partition_replication_epoch_;
}

bool RecoveryCommandPreservesAnchors(
    const SetFailoverRecovery& command,
    const MetaFailoverRecoveryRecord& recovery) {
  return command.expected_revision_ == recovery.revision_ &&
         RecoveryCommandMatchesAnchors(command, recovery);
}

bool RecoveryCommandPreservesState(const SetFailoverRecovery& command,
                                   const MetaFailoverRecoveryRecord& recovery) {
  return RecoveryCommandPreservesAnchors(command, recovery) &&
         command.proof_state_ == recovery.proof_state_ &&
         command.frozen_proof_ == recovery.frozen_proof_;
}

bool RecoveryCommandDowngradesAvailability(
    const SetFailoverRecovery& command,
    const MetaFailoverRecoveryRecord& recovery) {
  if (!RecoveryCommandPreservesAnchors(command, recovery) ||
      command.hold_required_ != recovery.hold_required_ ||
      command.recovery_required_ != recovery.recovery_required_ ||
      command.proof_state_ != MetaFailoverProofState::kUnavailable ||
      recovery.proof_state_ == MetaFailoverProofState::kUnavailable) {
    return false;
  }
  if (recovery.proof_state_ == MetaFailoverProofState::kPending) {
    return !command.frozen_proof_.has_value();
  }
  return recovery.proof_state_ == MetaFailoverProofState::kExact &&
         command.frozen_proof_ == recovery.frozen_proof_;
}

bool RecoveryCommandEffectPresent(const SetFailoverRecovery& command,
                                  const MetaFailoverRecoveryRecord& recovery,
                                  std::uint64_t log_index) {
  return recovery.revision_ == log_index &&
         RecoveryCommandMatchesAnchors(command, recovery) &&
         command.hold_required_ == recovery.hold_required_ &&
         command.recovery_required_ == recovery.recovery_required_ &&
         command.proof_state_ == recovery.proof_state_ &&
         command.frozen_proof_ == recovery.frozen_proof_;
}

bool RecoveryCommandMatchesIntent(const SetFailoverRecovery& command,
                                  const FailoverIntent& intent) {
  return command.group_id_ == intent.group_id_ &&
         command.recovery_generation_ == intent.recovery_generation_ &&
         command.old_source_node_id_ == intent.former_owner_node_id_ &&
         command.old_source_assignment_id_ ==
             intent.former_owner_assignment_id_ &&
         command.old_source_boot_incarnation_ == intent.former_owner_boot_id_ &&
         command.old_source_history_id_ == intent.parent_history_id_ &&
         command.excluded_authority_term_ == intent.group_term_ - 1 &&
         command.excluded_authority_version_ == intent.authority_version_ &&
         command.excluded_grant_revision_ == intent.grant_revision_ &&
         command.population_manifest_revision_ ==
             intent.population_manifest_revision_ &&
         command.population_manifest_digest_ ==
             intent.population_manifest_digest_ &&
         command.partition_replication_epoch_ ==
             intent.partition_replication_epoch_;
}

absl::Status ValidateFailoverFailSafeCheckpointImpl(
    const SetFailoverRecovery& command, const MetaStores& stores) {
  auto live = LiveFailoverForGroup(command.group_id_, stores.operation_);
  if (!live.ok()) return live.status();
  if (!live->has_value() ||
      !RecoveryCommandMatchesIntent(command, (**live).intent)) {
    return Invalid("fail-safe recovery checkpoint has no exact live owner");
  }
  const LiveFailoverContext& owner = **live;
  const auto recovery = stores.failover_recovery_.Find(command.group_id_);
  if (!recovery.has_value()) {
    const std::uint64_t last_generation =
        stores.failover_recovery_.LastGeneration(command.group_id_).value_or(0);
    const std::uint64_t last_revision =
        stores.failover_recovery_.LastRevision(command.group_id_).value_or(0);
    if (last_generation == std::numeric_limits<std::uint64_t>::max() ||
        command.recovery_generation_ != last_generation + 1 ||
        command.expected_revision_ != last_revision ||
        owner.phase.has_value() ||
        !owner.operation.current_directives_.empty() ||
        command.proof_state_ != MetaFailoverProofState::kPending ||
        command.frozen_proof_.has_value() || !command.hold_required_ ||
        command.recovery_required_) {
      return Invalid("fail-safe recovery bootstrap is not canonical");
    }
    return ValidateCommittedAnchors(owner.intent, stores,
                                    CommittedAnchorState::kOldAuthorityActive);
  }

  if (!owner.phase.has_value() ||
      !RecoveryMatchesIntent(*recovery, owner.intent) ||
      !RecoveryCommandPreservesAnchors(command, *recovery) ||
      !recovery->hold_required_ || recovery->recovery_required_ ||
      command.hold_required_ != recovery->hold_required_ ||
      command.recovery_required_ != recovery->recovery_required_) {
    return Invalid("fail-safe proof checkpoint changed recovery ownership");
  }
  const absl::Status old_authority_active = ValidateCommittedAnchors(
      owner.intent, stores, CommittedAnchorState::kOldAuthorityActive,
      /*require_candidate_member=*/false);
  const absl::Status fenced = ValidateCommittedAnchors(
      owner.intent, stores, CommittedAnchorState::kFenced,
      /*require_candidate_member=*/false);
  const absl::Status activated = ValidateCommittedAnchors(
      owner.intent, stores, CommittedAnchorState::kActivated);

  if (RecoveryCommandDowngradesAvailability(command, *recovery) &&
      recovery->proof_state_ == MetaFailoverProofState::kPending) {
    const bool before_cut =
        (owner.phase->stage_ == FailoverPhaseStage::kSourceHolding ||
         owner.phase->stage_ == FailoverPhaseStage::kSourceHeld) &&
        old_authority_active.ok();
    const bool at_cut =
        owner.phase->stage_ == FailoverPhaseStage::kOldAuthorityExcluding &&
        (old_authority_active.ok() || fenced.ok());
    if (!before_cut && !at_cut) {
      return Invalid(
          "fail-safe pending proof downgrade has incompatible authority");
    }
    const MetaTerminalReceipt* receipt =
        owner.phase->stage_ == FailoverPhaseStage::kOldAuthorityExcluding &&
                owner.operation.current_directives_.size() == 1
            ? FindCurrentReceipt(owner.operation,
                                 owner.operation.current_directives_.front())
            : nullptr;
    if (receipt != nullptr &&
        receipt->status_ == MetaDirectiveResultStatus::kSucceeded) {
      return Invalid(
          "fail-safe unavailable proof discarded a successful source result");
    }
    return absl::OkStatus();
  }

  if (owner.phase->stage_ < FailoverPhaseStage::kOldAuthorityExcluding) {
    return Invalid("fail-safe proof checkpoint precedes the authority cut");
  }
  if (!fenced.ok() && !activated.ok()) {
    return Invalid("fail-safe proof checkpoint precedes the authority cut");
  }
  if (recovery->proof_state_ == MetaFailoverProofState::kExact &&
      owner.phase->stage_ >= FailoverPhaseStage::kOldAuthorityExcluded &&
      (!recovery->frozen_proof_.has_value() ||
       owner.phase->old_authority_exclusion_hash_ !=
           recovery->frozen_proof_->proof_hash_ ||
       owner.phase->required_applied_next_lsns_ !=
           recovery->frozen_proof_->final_next_lsns_)) {
    return Invalid("fail-safe proof checkpoint lost the exact phase frontier");
  }

  if (RecoveryCommandDowngradesAvailability(command, *recovery)) {
    return absl::OkStatus();
  }
  if (recovery->proof_state_ != MetaFailoverProofState::kPending ||
      owner.phase->stage_ != FailoverPhaseStage::kOldAuthorityExcluding) {
    return Invalid("fail-safe proof checkpoint is not a monotonic transition");
  }
  if (command.proof_state_ != MetaFailoverProofState::kExact ||
      !command.frozen_proof_.has_value() ||
      owner.operation.current_directives_.size() != 1) {
    return Invalid("fail-safe exact proof checkpoint has no frozen result");
  }
  const MetaCurrentDirective& current =
      owner.operation.current_directives_.front();
  if (absl::Status status =
          ValidateFrozenSourceDirective(current.spec_, owner.intent);
      !status.ok()) {
    return status;
  }
  const MetaTerminalReceipt* receipt =
      FindSuccessfulReceipt(owner.operation, current);
  if (receipt == nullptr ||
      receipt->recipient_node_id_ != owner.intent.former_owner_node_id_ ||
      receipt->recipient_boot_id_ != owner.intent.former_owner_boot_id_ ||
      receipt->assignment_id_ != owner.intent.candidate_assignment_id_ ||
      receipt->result_hash_ != MetaSha256(receipt->result_)) {
    return Invalid("fail-safe exact proof checkpoint has no exact receipt");
  }
  auto evidence =
      cluster::control::DecodeFrozenSourceEvidence(receipt->result_);
  if (!evidence.ok() ||
      evidence->recovery_generation != owner.intent.recovery_generation_ ||
      evidence->source_history_id != Hex(owner.intent.parent_history_id_) ||
      evidence->final_next_lsns.size() != owner.intent.flow_count_) {
    return Invalid("fail-safe exact proof checkpoint has invalid evidence");
  }
  auto proof_hash = cluster::control::ComputeFrozenSourceProofHash(*evidence);
  if (!proof_hash.ok() || *proof_hash != evidence->proof_hash ||
      command.frozen_proof_->final_next_lsns_ != evidence->final_next_lsns ||
      command.frozen_proof_->proof_hash_ != evidence->proof_hash) {
    return Invalid("fail-safe exact proof checkpoint changed the frozen proof");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverFailSafeCheckpointImpl(
    const TransitionOperationPhase& command, const MetaCommittedView& view,
    const MetaObservationStore& observations) {
  const auto operation = view.operation().FindOperation(command.operation_id_);
  if (!operation.has_value() || operation->kind_ != kFailoverOperationKind ||
      operation->lifecycle_ == MetaOperationLifecycle::kCompleted ||
      operation->lifecycle_ == MetaOperationLifecycle::kAborted ||
      operation->revision_ != command.expected_revision_ ||
      !command.evidence_.empty()) {
    return Invalid("fail-safe phase checkpoint has no exact live revision");
  }
  auto intent = DecodeFailoverIntent(operation->intent_);
  auto next = DecodeFailoverPhase(command.kind_phase_blob_);
  if (!intent.ok() || !next.ok() ||
      operation->intent_hash_ != MetaSha256(operation->intent_) ||
      operation->replication_history_id_ != intent->parent_history_id_) {
    return Invalid("fail-safe phase checkpoint has invalid typed state");
  }
  auto live = LiveFailoverForGroup(intent->group_id_, view.operation());
  if (!live.ok()) return live.status();
  if (!live->has_value() ||
      (**live).operation.operation_id_ != operation->operation_id_) {
    return Invalid("fail-safe phase checkpoint has no unique group owner");
  }

  std::optional<FailoverPhase> previous;
  if (!operation->kind_phase_blob_.empty()) {
    auto decoded = DecodeFailoverPhase(operation->kind_phase_blob_);
    if (!decoded.ok()) {
      return Invalid("fail-safe phase checkpoint has corrupt current phase");
    }
    previous = *decoded;
  }
  const bool bootstrap = !previous.has_value() &&
                         next->stage_ == FailoverPhaseStage::kSourceHolding;
  const bool rebases_unavailable_frontier =
      previous.has_value() &&
      previous->stage_ == FailoverPhaseStage::kOldAuthorityExcluded &&
      next->stage_ == FailoverPhaseStage::kOldAuthorityExcluded;
  const bool attributes_activation =
      previous.has_value() &&
      previous->stage_ == FailoverPhaseStage::kPromotionPrepared &&
      next->stage_ == FailoverPhaseStage::kAuthorityActivated;
  const bool attributes_serving =
      previous.has_value() &&
      previous->stage_ == FailoverPhaseStage::kAuthorityActivated &&
      next->stage_ == FailoverPhaseStage::kServing;
  if (!bootstrap && !rebases_unavailable_frontier && !attributes_activation &&
      !attributes_serving) {
    return Invalid("fail-safe rejects ordinary failover phase progression");
  }
  if (!rebases_unavailable_frontier && !command.current_directives_.empty()) {
    return Invalid("fail-safe phase checkpoint carries directive work");
  }
  if (bootstrap) {
    const auto recovery = view.failover_recovery().Find(intent->group_id_);
    if (!recovery.has_value() || !RecoveryMatchesIntent(*recovery, *intent) ||
        !recovery->hold_required_ || recovery->recovery_required_ ||
        recovery->proof_state_ != MetaFailoverProofState::kPending) {
      return Invalid("fail-safe initial phase has no recovery owner");
    }
  }
  return ValidateFailoverProposal(MetaCommand(command), view, observations);
}

absl::Status ValidateFailoverTerminalCleanupCommandImpl(
    const SetFailoverRecovery& command, const MetaStores& stores) {
  const auto recovery = stores.failover_recovery_.Find(command.group_id_);
  if (!recovery.has_value()) {
    return Invalid("failover terminal Set recovery has no matching record");
  }
  auto owner = FindTerminalCleanupOwner(*recovery, stores);
  if (!owner.ok()) return owner.status();
  if (owner->outcome.recovery_required_ && recovery->hold_required_ &&
      recovery->recovery_required_ &&
      RecoveryCommandDowngradesAvailability(command, *recovery)) {
    return absl::OkStatus();
  }
  if (!RecoveryCommandPreservesState(command, *recovery) ||
      !recovery->hold_required_ || recovery->recovery_required_) {
    return Invalid(
        "failover terminal Set recovery is not an exact hold-only cleanup");
  }
  if (owner->outcome.recovery_required_) {
    if (!command.hold_required_ || !command.recovery_required_) {
      return Invalid("failover terminal cleanup lost its recovery handoff");
    }
  } else if (command.hold_required_ || command.recovery_required_) {
    return Invalid("failover terminal cleanup did not release its hold");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverTerminalCleanupCommandImpl(
    const ClearFailoverRecovery& command, const MetaStores& stores) {
  const auto recovery = stores.failover_recovery_.Find(command.group_id_);
  if (!recovery.has_value() ||
      command.expected_revision_ != recovery->revision_ ||
      command.recovery_generation_ != recovery->recovery_generation_ ||
      recovery->hold_required_ || recovery->recovery_required_) {
    return Invalid(
        "failover terminal Clear is not an exact released tombstone");
  }
  auto owner = FindTerminalCleanupOwner(*recovery, stores);
  if (!owner.ok()) return owner.status();
  if (owner->outcome.recovery_required_) {
    return Invalid("failover recovery handoff cannot be cleared as cleanup");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverRecoveryMutationImpl(
    const SetFailoverRecovery& command, const MetaStores& stores,
    std::optional<std::uint64_t> replay_log_index) {
  const auto recovery = stores.failover_recovery_.Find(command.group_id_);
  if (replay_log_index.has_value() && recovery.has_value() &&
      RecoveryCommandEffectPresent(command, *recovery, *replay_log_index)) {
    return absl::OkStatus();
  }

  auto live = LiveFailoverForGroup(command.group_id_, stores.operation_);
  if (!live.ok()) return live.status();
  if (live->has_value()) {
    return ValidateFailoverFailSafeCheckpointImpl(command, stores);
  }
  if (!recovery.has_value()) return absl::OkStatus();

  auto terminal = MaybeFindTerminalCleanupOwner(*recovery, stores);
  if (!terminal.ok()) return terminal.status();
  if (!terminal->has_value()) {
    if (command.recovery_generation_ == recovery->recovery_generation_) {
      return ValidateFailoverRecoveryAvailabilityCommand(command, stores);
    }
    return absl::OkStatus();
  }
  // A later uncontrolled workflow owns validation of an atomic successor
  // generation. The terminal controlled owner continues to gate only its own
  // generation's handoff/release mutations.
  if (command.recovery_generation_ != recovery->recovery_generation_) {
    return absl::OkStatus();
  }
  return ValidateFailoverTerminalCleanupCommandImpl(command, stores);
}

absl::Status ValidateFailoverRecoveryMutationImpl(
    const ClearFailoverRecovery& command, const MetaStores& stores,
    std::optional<std::uint64_t> replay_log_index) {
  const auto recovery = stores.failover_recovery_.Find(command.group_id_);
  if (replay_log_index.has_value() && !recovery.has_value() &&
      stores.failover_recovery_.LastGeneration(command.group_id_) ==
          command.recovery_generation_ &&
      stores.failover_recovery_.LastRevision(command.group_id_) ==
          *replay_log_index) {
    // The store performs the final cleared_expected_revision check. This
    // branch only prevents the terminal-owner precondition from changing the
    // verdict of an already-applied exact log replay.
    return absl::OkStatus();
  }

  auto live = LiveFailoverForGroup(command.group_id_, stores.operation_);
  if (!live.ok()) return live.status();
  if (live->has_value()) {
    return Invalid("live controlled failover recovery cannot be cleared");
  }
  if (!recovery.has_value()) return absl::OkStatus();

  auto terminal = MaybeFindTerminalCleanupOwner(*recovery, stores);
  if (!terminal.ok()) return terminal.status();
  if (!terminal->has_value()) return absl::OkStatus();
  return ValidateFailoverTerminalCleanupCommandImpl(command, stores);
}

}  // namespace

absl::StatusOr<std::optional<MetaEvidenceSummary>>
ResolveFailoverPreparedEvidence(const MetaCommittedView& view,
                                const MetaOperationRecord& operation,
                                const FailoverIntent& intent,
                                const FailoverPhase& phase,
                                const MetaObservationStore& observations,
                                const MetaTerminalReceipt& receipt,
                                std::int64_t now_unix_ms) {
  if ((phase.stage_ != FailoverPhaseStage::kPromotionPreparing &&
       phase.stage_ != FailoverPhaseStage::kPromotionPrepared) ||
      operation.current_directives_.size() != 1) {
    return Invalid("promotion prepared evidence has no preparing phase");
  }
  const MetaCurrentDirective& current = operation.current_directives_.front();
  if (absl::Status status =
          ValidatePreparingDirective(current.spec_, intent, phase);
      !status.ok()) {
    return status;
  }
  const MetaTerminalReceipt* committed = FindCurrentReceipt(operation, current);
  if (committed == nullptr || *committed != receipt ||
      receipt.status_ != MetaDirectiveResultStatus::kSucceeded ||
      receipt.recipient_node_id_ != intent.candidate_node_id_ ||
      receipt.recipient_boot_id_ != intent.candidate_boot_id_ ||
      receipt.assignment_id_ != intent.candidate_assignment_id_ ||
      receipt.result_hash_ != MetaSha256(receipt.result_) ||
      (phase.stage_ == FailoverPhaseStage::kPromotionPrepared &&
       phase.prepared_result_hash_ != receipt.result_hash_)) {
    return Invalid(
        "promotion prepared evidence has no exact successful receipt");
  }

  auto prepared =
      cluster::control::DecodePromotionPreparedEvidence(receipt.result_);
  if (!prepared.ok()) {
    return Invalid(absl::StrCat("invalid promotion prepared evidence: ",
                                prepared.status().message()));
  }
  if (prepared->parent_history_id != Hex(intent.parent_history_id_) ||
      prepared->frozen_applied_next_lsns.size() !=
          phase.required_applied_next_lsns_.size()) {
    return Invalid(
        "invalid promotion prepared evidence: parent or flow count changed");
  }
  for (std::size_t flow = 0; flow < prepared->frozen_applied_next_lsns.size();
       ++flow) {
    if (prepared->frozen_applied_next_lsns[flow] <
        phase.required_applied_next_lsns_[flow]) {
      return Invalid(
          "invalid promotion prepared evidence: frontier is behind the "
          "required cut");
    }
  }

  const MetaStoresFacts facts(view.stores());
  const std::vector<MetaOperationEvidenceObs> observed =
      observations.EvidenceForOperation(operation.operation_id_, facts,
                                        now_unix_ms);
  for (const MetaOperationEvidenceObs& evidence : observed) {
    const bool from_exact_candidate_phase =
        evidence.kind_phase_ == "promotion-prepare:prepared" &&
        evidence.node_id_ == intent.candidate_node_id_ &&
        evidence.boot_incarnation_ == intent.candidate_boot_id_ &&
        evidence.operation_id_ == operation.operation_id_;
    if (!from_exact_candidate_phase) continue;
    if (evidence.assignment_id_ != intent.candidate_assignment_id_ ||
        evidence.evidence_hash_ != receipt.result_hash_ ||
        evidence.evidence_ != receipt.result_ ||
        evidence.group_id_ != intent.group_id_ ||
        evidence.group_term_ != intent.group_term_ ||
        evidence.population_manifest_revision_ !=
            intent.population_manifest_revision_ ||
        evidence.partition_replication_epoch_ !=
            intent.partition_replication_epoch_ ||
        evidence.replication_history_id_ != intent.parent_history_id_) {
      return Invalid(
          "current promotion prepared evidence conflicts with its terminal "
          "receipt");
    }
    return std::optional(SummarizeOperationEvidence(
        evidence, intent.population_manifest_digest_));
  }
  return std::optional<MetaEvidenceSummary>();
}

absl::Status ValidateFailoverTerminalCommand(const CompleteOperation& command,
                                             const MetaStores& stores) {
  return ValidateFailoverTerminalCommandImpl(command, stores);
}

absl::Status ValidateFailoverTerminalCommand(const AbortOperation& command,
                                             const MetaStores& stores) {
  return ValidateFailoverTerminalCommandImpl(command, stores);
}

absl::Status ValidateFailoverArchiveCommand(const ArchiveOperations& command,
                                            const MetaStores& stores) {
  return ValidateFailoverArchiveCommandImpl(command, stores);
}

absl::Status ValidateFailoverSubmissionCommand(const SubmitOperation& command,
                                               const MetaStores& stores) {
  if (command.kind_ != kFailoverOperationKind) return absl::OkStatus();
  auto intent = DecodeFailoverIntent(command.intent_);
  if (!intent.ok() || command.intent_hash_ != MetaSha256(command.intent_) ||
      command.replication_history_id_ != intent->parent_history_id_) {
    return Invalid("failover submit contains invalid typed intent anchors");
  }
  const std::vector<MetaPolicyReference> expected_policy_references = {
      {intent->old_grant_.policy_id_, intent->old_grant_.policy_version_}};
  if (command.policy_references_ != expected_policy_references) {
    return Invalid("failover submit does not exactly pin the old grant policy");
  }
  if (stores.failover_recovery_.Find(intent->group_id_).has_value()) {
    return Invalid("failover submit conflicts with active recovery state");
  }
  const std::uint64_t last_generation =
      stores.failover_recovery_.LastGeneration(intent->group_id_).value_or(0);
  if (last_generation == std::numeric_limits<std::uint64_t>::max() ||
      intent->recovery_generation_ != last_generation + 1) {
    return Invalid("failover submit recovery generation is stale");
  }
  return ValidateCommittedAnchors(*intent, stores,
                                  CommittedAnchorState::kOldAuthorityActive);
}

absl::Status ValidateFailoverTerminalCleanupCommand(
    const SetFailoverRecovery& command, const MetaStores& stores) {
  return ValidateFailoverTerminalCleanupCommandImpl(command, stores);
}

absl::Status ValidateFailoverTerminalCleanupCommand(
    const ClearFailoverRecovery& command, const MetaStores& stores) {
  return ValidateFailoverTerminalCleanupCommandImpl(command, stores);
}

absl::Status ValidateFailoverFailSafeCheckpoint(
    const SetFailoverRecovery& command, const MetaStores& stores) {
  return ValidateFailoverFailSafeCheckpointImpl(command, stores);
}

absl::Status ValidateFailoverFailSafeCheckpoint(
    const TransitionOperationPhase& command, const MetaCommittedView& view,
    const MetaObservationStore& observations) {
  return ValidateFailoverFailSafeCheckpointImpl(command, view, observations);
}

absl::Status ValidateFailoverRecoveryAvailabilityCommand(
    const SetFailoverRecovery& command, const MetaStores& stores) {
  const auto recovery = stores.failover_recovery_.Find(command.group_id_);
  if (!recovery.has_value() || !recovery->hold_required_ ||
      !recovery->recovery_required_ ||
      !RecoveryCommandDowngradesAvailability(command, *recovery)) {
    return Invalid(
        "recovery-owned mutation is not an exact availability downgrade");
  }
  return absl::OkStatus();
}

absl::Status ValidateFailoverRecoveryApplyCommand(
    const SetFailoverRecovery& command, const MetaStores& stores,
    std::uint64_t log_index) {
  return ValidateFailoverRecoveryMutationImpl(command, stores, log_index);
}

absl::Status ValidateFailoverRecoveryApplyCommand(
    const ClearFailoverRecovery& command, const MetaStores& stores,
    std::uint64_t log_index) {
  return ValidateFailoverRecoveryMutationImpl(command, stores, log_index);
}

absl::Status ValidateFailoverWorkflowCommand(const BeginGroupTerm& command,
                                             const MetaStores& stores,
                                             std::uint64_t log_index) {
  return ValidateFailoverWorkflowCommandImpl(command, stores, log_index);
}

absl::Status ValidateFailoverWorkflowCommand(const ActivateAuthority& command,
                                             const MetaStores& stores,
                                             std::uint64_t log_index) {
  return ValidateFailoverWorkflowCommandImpl(command, stores, log_index);
}

absl::Status ValidateFailoverWorkflowCommand(const GrantAuthority& command,
                                             const MetaStores& stores) {
  return ValidateOrdinaryAuthorityMutation(command.group_id_, stores);
}

absl::Status ValidateFailoverWorkflowCommand(const RevokeGrant& command,
                                             const MetaStores& stores) {
  return ValidateOrdinaryAuthorityMutation(command.group_id_, stores);
}

absl::Status ValidateFailoverWorkflowCommand(const FenceGroup& command,
                                             const MetaStores& stores) {
  return ValidateOrdinaryAuthorityMutation(command.group_id_, stores);
}

std::string_view FailoverLossClassificationName(
    FailoverLossClassification classification) noexcept {
  switch (classification) {
    case FailoverLossClassification::kExact:
      return "exact";
    case FailoverLossClassification::kBounded:
      return "bounded";
    case FailoverLossClassification::kUnknown:
      return "unknown";
  }
  return "invalid";
}

std::string_view FailoverPhaseStageName(FailoverPhaseStage stage) noexcept {
  switch (stage) {
    case FailoverPhaseStage::kSourceHolding:
      return "source-holding";
    case FailoverPhaseStage::kSourceHeld:
      return "source-held";
    case FailoverPhaseStage::kOldAuthorityExcluding:
      return "old-authority-excluding";
    case FailoverPhaseStage::kOldAuthorityExcluded:
      return "old-authority-excluded";
    case FailoverPhaseStage::kCandidateCaughtUp:
      return "candidate-caught-up";
    case FailoverPhaseStage::kPromotionPreparing:
      return "promotion-preparing";
    case FailoverPhaseStage::kPromotionPrepared:
      return "promotion-prepared";
    case FailoverPhaseStage::kAuthorityActivated:
      return "authority-activated";
    case FailoverPhaseStage::kServing:
      return "serving";
  }
  return "invalid";
}

absl::StatusOr<std::string> EncodeControlledFailoverOutcome(
    const ControlledFailoverOutcome& outcome) {
  if (absl::Status status = ValidateOutcome(outcome); !status.ok()) {
    return status;
  }
  MetaWriter writer;
  writer.WriteRaw(kOutcomeMagic);
  writer.WriteU16(kOutcomeVersion);
  writer.WriteBool(outcome.succeeded_);
  writer.WriteU8(static_cast<std::uint8_t>(outcome.terminal_stage_));
  writer.WriteU8(static_cast<std::uint8_t>(outcome.loss_));
  writer.WriteBool(outcome.recovery_required_);
  writer.WriteList(outcome.proven_next_lsns_,
                   [](MetaWriter& output, std::uint64_t cursor) {
                     output.WriteU64(cursor);
                   });
  writer.WriteString(outcome.reason_);
  return std::move(writer).TakeBuffer();
}

absl::StatusOr<ControlledFailoverOutcome> DecodeControlledFailoverOutcome(
    std::string_view encoded) {
  MetaReader reader(encoded);
  auto magic = reader.ReadRaw(kOutcomeMagic.size());
  if (!magic.ok()) return magic.status();
  if (*magic != kOutcomeMagic) {
    return MetaFailStopError("unknown controlled failover outcome magic");
  }
  auto version = reader.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kOutcomeVersion) {
    return MetaFailStopError("unknown controlled failover outcome version");
  }
  ControlledFailoverOutcome outcome;
  auto succeeded = reader.ReadBool("invalid failover success tag");
  auto stage = reader.ReadU8();
  auto loss = reader.ReadU8();
  auto recovery = reader.ReadBool("invalid failover recovery tag");
  if (!succeeded.ok()) return succeeded.status();
  if (!stage.ok()) return stage.status();
  if (!loss.ok()) return loss.status();
  if (!recovery.ok()) return recovery.status();
  outcome.succeeded_ = *succeeded;
  outcome.terminal_stage_ = static_cast<FailoverPhaseStage>(*stage);
  outcome.loss_ = static_cast<FailoverLossClassification>(*loss);
  outcome.recovery_required_ = *recovery;
  auto frontier = reader.ReadList<std::uint64_t>(
      kMaxMetaFailoverRecoveryFlows,
      [](MetaReader& input) { return input.ReadU64(); });
  if (!frontier.ok()) return frontier.status();
  outcome.proven_next_lsns_ = std::move(*frontier);
  auto reason = reader.ReadString(kMaxMetaAbortReasonBytes);
  if (!reason.ok()) return reason.status();
  outcome.reason_ = std::string(*reason);
  if (absl::Status status = reader.Finish(); !status.ok()) return status;
  if (absl::Status status = ValidateOutcome(outcome); !status.ok()) {
    return MetaFailStopError(status.message());
  }
  return outcome;
}

absl::StatusOr<std::string> EncodeFailoverIntent(const FailoverIntent& intent) {
  if (absl::Status status = ValidateIntent(intent); !status.ok()) return status;
  MetaWriter writer;
  WriteHeader(writer, kIntentMagic);
  writer.WriteString(intent.group_id_);
  writer.WriteU64(intent.recovery_generation_);
  writer.WriteU32(intent.attempt_timeout_ms_);
  writer.WriteString(intent.former_owner_node_id_);
  WriteFixedArray(writer, intent.former_owner_assignment_id_);
  WriteFixedArray(writer, intent.former_owner_boot_id_);
  writer.WriteString(intent.candidate_node_id_);
  WriteFixedArray(writer, intent.candidate_assignment_id_);
  WriteFixedArray(writer, intent.candidate_boot_id_);
  writer.WriteU64(intent.group_term_);
  writer.WriteU64(intent.authority_version_);
  writer.WriteU64(intent.grant_revision_);
  writer.WriteU64(intent.old_grant_.lease_duration_ms_);
  writer.WriteString(intent.old_grant_.policy_id_);
  writer.WriteU64(intent.old_grant_.policy_version_);
  writer.WriteU64(intent.population_manifest_revision_);
  WriteFixedArray(writer, intent.population_manifest_digest_);
  writer.WriteU64(intent.partition_replication_epoch_);
  WriteFixedArray(writer, intent.parent_history_id_);
  writer.WriteU32(intent.flow_count_);
  return std::move(writer).TakeBuffer();
}

absl::StatusOr<MetaHash256> ComputeFailoverUnavailableProofHash(
    const FailoverIntent& intent,
    const std::vector<std::uint64_t>& candidate_frontier) {
  auto encoded_intent = EncodeFailoverIntent(intent);
  if (!encoded_intent.ok()) return encoded_intent.status();
  if (candidate_frontier.size() != intent.flow_count_ ||
      std::any_of(candidate_frontier.begin(), candidate_frontier.end(),
                  [](std::uint64_t cursor) { return cursor == 0; })) {
    return Invalid("unavailable proof frontier does not match the intent");
  }
  MetaWriter writer;
  writer.WriteRaw(kUnavailableProofMagic);
  writer.WriteU16(kUnavailableProofVersion);
  writer.WriteString(*encoded_intent);
  writer.WriteList(candidate_frontier,
                   [](MetaWriter& output, std::uint64_t cursor) {
                     output.WriteU64(cursor);
                   });
  return MetaSha256(std::move(writer).TakeBuffer());
}

absl::StatusOr<FailoverIntent> DecodeFailoverIntent(std::string_view encoded) {
  MetaReader reader(encoded);
  if (absl::Status status = ReadHeader(reader, kIntentMagic); !status.ok()) {
    return status;
  }
  FailoverIntent intent;
  auto group = reader.ReadString(kMaxMetaGroupIdBytes);
  if (!group.ok()) return group.status();
  intent.group_id_ = *group;
  auto recovery_generation = reader.ReadU64();
  if (!recovery_generation.ok()) return recovery_generation.status();
  intent.recovery_generation_ = *recovery_generation;
  auto attempt_timeout = reader.ReadU32();
  if (!attempt_timeout.ok()) return attempt_timeout.status();
  intent.attempt_timeout_ms_ = *attempt_timeout;
  auto former = reader.ReadString(kMetaNodeIdBytes);
  if (!former.ok()) return former.status();
  intent.former_owner_node_id_ = *former;
  auto former_assignment = ReadFixedArray<16>(reader);
  if (!former_assignment.ok()) return former_assignment.status();
  intent.former_owner_assignment_id_ = *former_assignment;
  auto former_boot = ReadFixedArray<20>(reader);
  if (!former_boot.ok()) return former_boot.status();
  intent.former_owner_boot_id_ = *former_boot;
  auto candidate = reader.ReadString(kMetaNodeIdBytes);
  if (!candidate.ok()) return candidate.status();
  intent.candidate_node_id_ = *candidate;
  auto candidate_assignment = ReadFixedArray<16>(reader);
  if (!candidate_assignment.ok()) return candidate_assignment.status();
  intent.candidate_assignment_id_ = *candidate_assignment;
  auto candidate_boot = ReadFixedArray<20>(reader);
  if (!candidate_boot.ok()) return candidate_boot.status();
  intent.candidate_boot_id_ = *candidate_boot;
  auto term = reader.ReadU64();
  if (!term.ok()) return term.status();
  intent.group_term_ = *term;
  auto authority = reader.ReadU64();
  if (!authority.ok()) return authority.status();
  intent.authority_version_ = *authority;
  auto grant = reader.ReadU64();
  if (!grant.ok()) return grant.status();
  intent.grant_revision_ = *grant;
  auto lease_duration = reader.ReadU64();
  if (!lease_duration.ok()) return lease_duration.status();
  intent.old_grant_.lease_duration_ms_ = *lease_duration;
  auto policy_id = reader.ReadString(kMaxMetaPolicyIdBytes);
  if (!policy_id.ok()) return policy_id.status();
  intent.old_grant_.policy_id_ = std::move(*policy_id);
  auto policy_version = reader.ReadU64();
  if (!policy_version.ok()) return policy_version.status();
  intent.old_grant_.policy_version_ = *policy_version;
  auto manifest_revision = reader.ReadU64();
  if (!manifest_revision.ok()) return manifest_revision.status();
  intent.population_manifest_revision_ = *manifest_revision;
  auto manifest = ReadFixedArray<32>(reader);
  if (!manifest.ok()) return manifest.status();
  intent.population_manifest_digest_ = *manifest;
  auto population_epoch = reader.ReadU64();
  if (!population_epoch.ok()) return population_epoch.status();
  intent.partition_replication_epoch_ = *population_epoch;
  auto history = ReadFixedArray<20>(reader);
  if (!history.ok()) return history.status();
  intent.parent_history_id_ = *history;
  auto flow_count = reader.ReadU32();
  if (!flow_count.ok()) return flow_count.status();
  intent.flow_count_ = *flow_count;
  if (absl::Status status = reader.Finish(); !status.ok()) return status;
  if (absl::Status status = DecodeValidation(ValidateIntent(intent));
      !status.ok()) {
    return status;
  }
  return intent;
}

absl::StatusOr<std::string> EncodeFailoverPhase(const FailoverPhase& phase) {
  if (absl::Status status = ValidatePhase(phase); !status.ok()) return status;
  MetaWriter writer;
  WriteHeader(writer, kPhaseMagic);
  writer.WriteU8(static_cast<std::uint8_t>(phase.stage_));
  WriteFixedArray(writer, phase.old_authority_exclusion_hash_);
  writer.WriteList(phase.required_applied_next_lsns_,
                   [](MetaWriter& output, std::uint64_t cursor) {
                     output.WriteU64(cursor);
                   });
  WriteFixedArray(writer, phase.prepared_result_hash_);
  return std::move(writer).TakeBuffer();
}

absl::StatusOr<FailoverPhase> DecodeFailoverPhase(std::string_view encoded) {
  MetaReader reader(encoded);
  if (absl::Status status = ReadHeader(reader, kPhaseMagic); !status.ok()) {
    return status;
  }
  FailoverPhase phase;
  auto stage = reader.ReadU8();
  if (!stage.ok()) return stage.status();
  phase.stage_ = static_cast<FailoverPhaseStage>(*stage);
  auto exclusion = ReadFixedArray<32>(reader);
  if (!exclusion.ok()) return exclusion.status();
  phase.old_authority_exclusion_hash_ = *exclusion;
  auto frontier = reader.ReadList<std::uint64_t>(
      kMaxMetaFailoverRecoveryFlows,
      [](MetaReader& input) { return input.ReadU64(); });
  if (!frontier.ok()) return frontier.status();
  phase.required_applied_next_lsns_ = std::move(*frontier);
  auto prepared = ReadFixedArray<32>(reader);
  if (!prepared.ok()) return prepared.status();
  phase.prepared_result_hash_ = *prepared;
  if (absl::Status status = reader.Finish(); !status.ok()) return status;
  if (absl::Status status = DecodeValidation(ValidatePhase(phase));
      !status.ok()) {
    return status;
  }
  return phase;
}

absl::StatusOr<SubmitOperation> BuildControlledFailoverSubmission(
    std::string_view group_id, const MetaOperationId& operation_id,
    const MetaRequestId& request_id, const MetaCommittedView& view,
    const MetaObservationStore& observations, std::int64_t now_unix_ms,
    std::uint32_t attempt_timeout_ms) {
  if (group_id.empty() || group_id.size() > kMaxMetaGroupIdBytes ||
      IsZero(operation_id) || IsZero(request_id) || attempt_timeout_ms == 0 ||
      attempt_timeout_ms > kMaxControlledFailoverAttemptTimeoutMs) {
    return Invalid("controlled failover request identity is incomplete");
  }
  auto live = LiveFailoverForGroup(group_id, view);
  if (!live.ok()) return live.status();
  if (live->has_value()) {
    return Invalid("another failover workflow is active for this group");
  }
  if (view.failover_recovery().Find(group_id).has_value()) {
    return Invalid(
        "the group still has a recovery handoff; wait for cleanup or run "
        "uncontrolled recovery");
  }

  const auto group = view.topology().FindGroup(std::string(group_id));
  const auto grant = view.grant().GroupState(group_id);
  if (!group.has_value() || !grant.has_value()) {
    return Invalid("controlled failover group does not exist");
  }
  if (grant->fenced_ || !grant->grant_.has_value() || grant->group_term_ == 0 ||
      grant->group_term_ == std::numeric_limits<std::uint64_t>::max() ||
      group->record_.owner_ != grant->grant_->owner_ ||
      group->record_.group_term_ != grant->group_term_ ||
      group->record_.authority_version_ != grant->grant_->authority_version_ ||
      grant->last_authority_version_ != grant->grant_->authority_version_ ||
      grant->last_grant_revision_ != grant->grant_->grant_revision_) {
    return Invalid("controlled failover requires one consistent active owner");
  }
  if (grant->grant_->authority_version_ == 0 ||
      grant->grant_->authority_version_ ==
          std::numeric_limits<std::uint64_t>::max() ||
      grant->grant_->grant_revision_ == 0 ||
      grant->grant_->spec_.lease_duration_ms_ == 0 ||
      group->record_.population_manifest_revision_ == 0 ||
      IsZero(group->record_.population_manifest_digest_) ||
      group->record_.partition_replication_epoch_ == 0) {
    return Invalid("controlled failover active authority is incomplete");
  }

  const MetaStoresFacts facts(view.stores());
  CandidatePlan plan =
      CandidatePlanFor(group_id, facts, observations, now_unix_ms);
  switch (plan.disposition_) {
    case CandidatePlanDisposition::kGroupUnknown:
      return Invalid("controlled failover group is unknown to candidate plan");
    case CandidatePlanDisposition::kNoEligibleCandidates:
      return Invalid("controlled failover has no eligible candidate");
    case CandidatePlanDisposition::kMultipleCompatibilityDomains:
      return Invalid(
          "controlled failover candidates span compatibility domains");
    case CandidatePlanDisposition::kSelected:
      break;
  }
  if (!plan.selected_.has_value()) {
    return Invalid("controlled failover candidate plan is incomplete");
  }
  const MetaCandidateProgressObs& candidate = *plan.selected_;
  const auto former_member = std::find_if(
      group->members_.begin(), group->members_.end(), [&](const auto& member) {
        return member.node_id_ == group->record_.owner_ &&
               member.assignment_id_ == candidate.source_assignment_id_;
      });
  const auto candidate_member = std::find_if(
      group->members_.begin(), group->members_.end(), [&](const auto& member) {
        return member.node_id_ == candidate.node_id_ &&
               member.assignment_id_ == candidate.assignment_id_;
      });
  if (candidate.node_id_ == group->record_.owner_ ||
      candidate.source_node_id_ != group->record_.owner_ ||
      former_member == group->members_.end() ||
      former_member->role_ != MetaNodeRole::kPrimary ||
      candidate_member == group->members_.end() ||
      candidate_member->role_ != MetaNodeRole::kReplica ||
      candidate.group_term_ != grant->group_term_ ||
      candidate.population_manifest_revision_ !=
          group->record_.population_manifest_revision_ ||
      candidate.population_manifest_digest_ !=
          group->record_.population_manifest_digest_ ||
      candidate.partition_replication_epoch_ !=
          group->record_.partition_replication_epoch_ ||
      IsZero(candidate.source_boot_incarnation_) ||
      IsZero(candidate.source_replication_history_id_) ||
      candidate.applied_next_lsns_.empty() ||
      candidate.applied_next_lsns_.size() > kMaxMetaFailoverRecoveryFlows) {
    return Invalid(
        "selected candidate is not compatible with the current primary");
  }
  const std::uint64_t last_generation =
      view.failover_recovery().LastGeneration(group_id).value_or(0);
  if (last_generation == std::numeric_limits<std::uint64_t>::max()) {
    return Invalid("controlled failover recovery generation is exhausted");
  }

  FailoverIntent intent{
      .group_id_ = std::string(group_id),
      .recovery_generation_ = last_generation + 1,
      .attempt_timeout_ms_ = attempt_timeout_ms,
      .former_owner_node_id_ = group->record_.owner_,
      .former_owner_assignment_id_ = candidate.source_assignment_id_,
      .former_owner_boot_id_ = candidate.source_boot_incarnation_,
      .candidate_node_id_ = candidate.node_id_,
      .candidate_assignment_id_ = candidate.assignment_id_,
      .candidate_boot_id_ = candidate.boot_incarnation_,
      .group_term_ = grant->group_term_ + 1,
      .authority_version_ = grant->grant_->authority_version_,
      .grant_revision_ = grant->grant_->grant_revision_,
      .old_grant_ = grant->grant_->spec_,
      .population_manifest_revision_ =
          group->record_.population_manifest_revision_,
      .population_manifest_digest_ = group->record_.population_manifest_digest_,
      .partition_replication_epoch_ =
          group->record_.partition_replication_epoch_,
      .parent_history_id_ = candidate.source_replication_history_id_,
      .flow_count_ =
          static_cast<std::uint32_t>(candidate.applied_next_lsns_.size()),
  };
  auto encoded = EncodeFailoverIntent(intent);
  if (!encoded.ok()) return encoded.status();
  SubmitOperation submit;
  submit.request_id_ = request_id;
  submit.operation_id_ = operation_id;
  submit.kind_ = std::string(kFailoverOperationKind);
  submit.intent_ = std::move(*encoded);
  submit.intent_hash_ = MetaSha256(submit.intent_);
  submit.replication_history_id_ = intent.parent_history_id_;
  submit.policy_references_.push_back(
      {intent.old_grant_.policy_id_, intent.old_grant_.policy_version_});
  return submit;
}

absl::Status ValidateFailoverProposal(
    const MetaCommand& command, const MetaCommittedView& view,
    const MetaObservationStore& observations) {
  if (const auto* archive = std::get_if<ArchiveOperations>(&command)) {
    return ValidateFailoverArchiveCommand(*archive, view.stores());
  }
  if (const auto* submit = std::get_if<SubmitOperation>(&command)) {
    if (submit->kind_ != kFailoverOperationKind) return absl::OkStatus();
    auto intent = DecodeFailoverIntent(submit->intent_);
    if (!intent.ok() || submit->intent_hash_ != MetaSha256(submit->intent_) ||
        submit->replication_history_id_ != intent->parent_history_id_) {
      return Invalid("failover submit contains invalid typed intent anchors");
    }
    if (const auto existing =
            view.operation().FindOperation(submit->operation_id_);
        existing.has_value() && existing->intent_ == submit->intent_ &&
        existing->intent_hash_ == submit->intent_hash_) {
      return absl::OkStatus();
    }
    if (const auto archived =
            view.operation().FindArchived(submit->operation_id_);
        archived.has_value() &&
        archived->intent_hash_ == submit->intent_hash_) {
      return absl::OkStatus();
    }
    // The ctl entry performs candidate planning before it proposes, but the
    // proposal hook is the final leader-local safety boundary. Reconstructing
    // the canonical submission from the same committed/observation cut keeps
    // a well-formed caller from substituting another compatible-looking
    // replica, recovery generation, or policy pin after deterministic #39
    // selection.
    auto expected = BuildControlledFailoverSubmission(
        intent->group_id_, submit->operation_id_, submit->request_id_, view,
        observations, UnixMillisNow(), intent->attempt_timeout_ms_);
    if (!expected.ok()) return expected.status();
    if (expected->intent_ != submit->intent_ ||
        expected->intent_hash_ != submit->intent_hash_ ||
        expected->replication_history_id_ != submit->replication_history_id_ ||
        expected->policy_references_ != submit->policy_references_) {
      return Invalid(
          "failover submit does not match the current deterministic candidate "
          "plan");
    }
    for (const MetaOperationRecord& active :
         view.operation().LiveOperations()) {
      if (active.kind_ != kFailoverOperationKind ||
          active.lifecycle_ == MetaOperationLifecycle::kCompleted ||
          active.lifecycle_ == MetaOperationLifecycle::kAborted) {
        continue;
      }
      auto active_intent = DecodeFailoverIntent(active.intent_);
      if (!active_intent.ok()) {
        return Invalid("active failover has invalid durable typed intent");
      }
      if (active_intent->group_id_ == intent->group_id_) {
        return Invalid("another failover workflow is active for this group");
      }
    }
    const bool pins_old_grant_policy = std::any_of(
        submit->policy_references_.begin(), submit->policy_references_.end(),
        [&](const MetaPolicyReference& reference) {
          return reference.policy_id_ == intent->old_grant_.policy_id_ &&
                 reference.version_ == intent->old_grant_.policy_version_;
        });
    if (!pins_old_grant_policy) {
      return Invalid("failover submit does not pin the old grant policy");
    }
    return ValidateCommittedAnchors(*intent, view,
                                    CommittedAnchorState::kOldAuthorityActive);
  }

  if (const auto* begin = std::get_if<BeginGroupTerm>(&command)) {
    return ValidateBeginGroupTerm(*begin, view);
  }
  if (const auto* activate = std::get_if<ActivateAuthority>(&command)) {
    return ValidateActivateAuthority(*activate, view);
  }
  if (const auto* grant = std::get_if<GrantAuthority>(&command)) {
    return ValidateFailoverWorkflowCommand(*grant, view.stores());
  }
  if (const auto* revoke = std::get_if<RevokeGrant>(&command)) {
    return ValidateFailoverWorkflowCommand(*revoke, view.stores());
  }
  if (const auto* fence = std::get_if<FenceGroup>(&command)) {
    return ValidateFailoverWorkflowCommand(*fence, view.stores());
  }
  if (const auto* complete = std::get_if<CompleteOperation>(&command)) {
    return ValidateFailoverTerminalCommand(*complete, view.stores());
  }
  if (const auto* abort = std::get_if<AbortOperation>(&command)) {
    return ValidateFailoverTerminalCommand(*abort, view.stores());
  }
  if (const auto* set = std::get_if<SetFailoverRecovery>(&command)) {
    return ValidateFailoverRecoveryMutationImpl(*set, view.stores(),
                                                std::nullopt);
  }
  if (const auto* clear = std::get_if<ClearFailoverRecovery>(&command)) {
    return ValidateFailoverRecoveryMutationImpl(*clear, view.stores(),
                                                std::nullopt);
  }

  const auto* transition = std::get_if<TransitionOperationPhase>(&command);
  if (transition == nullptr) return absl::OkStatus();
  const auto operation =
      view.operation().FindOperation(transition->operation_id_);
  if (!operation.has_value()) {
    return absl::OkStatus();
  }
  const auto is_promotion_prepare = [](const auto& directive) {
    if constexpr (std::is_same_v<std::decay_t<decltype(directive)>,
                                 MetaCurrentDirective>) {
      return directive.spec_.kind_ == "promotion-prepare";
    } else {
      return directive.kind_ == "promotion-prepare";
    }
  };
  const bool owns_promotion_prepare =
      std::any_of(transition->current_directives_.begin(),
                  transition->current_directives_.end(),
                  is_promotion_prepare) ||
      std::any_of(operation->current_directives_.begin(),
                  operation->current_directives_.end(), is_promotion_prepare);
  if (operation->kind_ != kFailoverOperationKind) {
    return owns_promotion_prepare
               ? Invalid(
                     "promotion-prepare belongs only to a failover operation")
               : absl::OkStatus();
  }
  if (view.operation().TransitionAlreadyApplied(*transition)) {
    return absl::OkStatus();
  }
  if (operation->revision_ != transition->expected_revision_) {
    return Invalid("failover transition expected_revision mismatch");
  }
  auto intent = DecodeFailoverIntent(operation->intent_);
  auto next = DecodeFailoverPhase(transition->kind_phase_blob_);
  if (!intent.ok() || !next.ok() ||
      operation->intent_hash_ != MetaSha256(operation->intent_) ||
      operation->replication_history_id_ != intent->parent_history_id_) {
    return Invalid("committed failover operation contains invalid typed state");
  }
  std::optional<FailoverPhase> previous;
  if (!operation->kind_phase_blob_.empty()) {
    auto decoded = DecodeFailoverPhase(operation->kind_phase_blob_);
    if (!decoded.ok()) {
      return Invalid("committed failover phase is invalid");
    }
    previous = std::move(*decoded);
  }
  const bool dispatches_frozen_source =
      previous.has_value() &&
      previous->stage_ == FailoverPhaseStage::kOldAuthorityExcluding &&
      next->stage_ == FailoverPhaseStage::kOldAuthorityExcluding;
  const bool dispatches_source_hold =
      previous.has_value() &&
      previous->stage_ == FailoverPhaseStage::kSourceHolding &&
      next->stage_ == FailoverPhaseStage::kSourceHolding;
  if (dispatches_source_hold || dispatches_frozen_source) {
    if (!operation->current_directives_.empty() ||
        transition->current_directives_.size() != 1 ||
        !transition->evidence_.empty()) {
      return Invalid(
          "source authorization must be the only same-stage transition");
    }
    if (absl::Status anchors = ValidateCommittedAnchors(
            *intent, view,
            dispatches_source_hold ? CommittedAnchorState::kOldAuthorityActive
                                   : CommittedAnchorState::kFenced);
        !anchors.ok()) {
      return anchors;
    }
    return dispatches_source_hold
               ? ValidateHoldingSourceDirective(
                     transition->current_directives_.front(), *intent)
               : ValidateFrozenSourceDirective(
                     transition->current_directives_.front(), *intent);
  }
  const bool downgrades_frozen_frontier =
      previous.has_value() &&
      previous->stage_ == FailoverPhaseStage::kOldAuthorityExcluded &&
      next->stage_ == FailoverPhaseStage::kOldAuthorityExcluded;
  if (downgrades_frozen_frontier) {
    return ValidateFrozenFrontierDowngrade(
        *transition, *operation, *intent, *previous, *next, view, observations);
  }
  const std::optional<FailoverPhaseStage> expected_stage =
      previous.has_value() ? NextControlledStage(previous->stage_)
                           : std::optional(FailoverPhaseStage::kSourceHolding);
  if (!expected_stage.has_value() || next->stage_ != *expected_stage) {
    return Invalid("failover transition skipped or repeated a phase");
  }
  if (next->stage_ >= FailoverPhaseStage::kOldAuthorityExcluded &&
      next->required_applied_next_lsns_.size() != intent->flow_count_) {
    return Invalid("failover frozen frontier has the wrong flow count");
  }
  if (previous.has_value() &&
      previous->stage_ >= FailoverPhaseStage::kOldAuthorityExcluded &&
      (previous->old_authority_exclusion_hash_ !=
           next->old_authority_exclusion_hash_ ||
       previous->required_applied_next_lsns_ !=
           next->required_applied_next_lsns_)) {
    return Invalid("failover transition changed the frozen exclusion proof");
  }
  if (previous.has_value() &&
      previous->stage_ >= FailoverPhaseStage::kPromotionPrepared &&
      previous->prepared_result_hash_ != next->prepared_result_hash_) {
    return Invalid("failover transition changed the prepared result proof");
  }
  if (absl::Status anchors =
          ValidateCommittedAnchors(*intent, view, AnchorStateFor(next->stage_));
      !anchors.ok()) {
    return anchors;
  }

  switch (next->stage_) {
    case FailoverPhaseStage::kSourceHolding:
    case FailoverPhaseStage::kOldAuthorityExcluding:
      if (!transition->current_directives_.empty() ||
          !transition->evidence_.empty()) {
        return Invalid("directive-independent failover phase carries work");
      }
      return absl::OkStatus();
    case FailoverPhaseStage::kSourceHeld:
      return ValidateSourceHeldTransition(*transition, *operation, *intent);
    case FailoverPhaseStage::kOldAuthorityExcluded:
      return ValidateFrozenProofTransition(*transition, *operation, *intent,
                                           *next, view, observations);
    case FailoverPhaseStage::kCandidateCaughtUp:
      if (!transition->evidence_.empty()) {
        return Invalid("candidate-caught-up phase carries unrelated evidence");
      }
      if (absl::Status retained = ValidateRetainedFrozenSourceDirective(
              *transition, *operation, *intent);
          !retained.ok()) {
        return retained;
      }
      return ValidateCandidateCaughtUp(*intent, *next, view, observations);
    case FailoverPhaseStage::kPromotionPreparing:
      if (transition->current_directives_.size() != 1 ||
          !transition->evidence_.empty()) {
        return Invalid("promotion-preparing requires one current directive");
      }
      return ValidatePreparingDirective(transition->current_directives_.front(),
                                        *intent, *next);
    case FailoverPhaseStage::kPromotionPrepared:
      return ValidatePreparedTransition(*transition, *operation, *intent, *next,
                                        view, observations);
    case FailoverPhaseStage::kAuthorityActivated:
    case FailoverPhaseStage::kServing:
      if (!transition->current_directives_.empty() ||
          !transition->evidence_.empty()) {
        return Invalid("post-activation failover phase carries unrelated work");
      }
      return absl::OkStatus();
  }
  return Invalid("unknown failover phase");
}

}  // namespace keylane::meta
