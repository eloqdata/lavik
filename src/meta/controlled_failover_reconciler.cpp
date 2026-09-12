#include "keylane/meta/controlled_failover_reconciler.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "celer/runtime/worker.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/hash.h"
#include "spdlog/spdlog.h"

namespace keylane::meta {
namespace {

namespace control = keylane::cluster::control;

bool IsTerminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kCompleted ||
         lifecycle == MetaOperationLifecycle::kAborted;
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& value) {
  return absl::BytesToHexString(std::string_view(
      reinterpret_cast<const char*>(value.data()), value.size()));
}

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

MetaOperationId DerivedId(const MetaOperationId& root,
                          std::string_view purpose) {
  const MetaHash256 hash = MetaSha256(
      absl::StrCat("controlled-failover-v1/", Hex(root), "/", purpose));
  MetaOperationId id{};
  std::copy_n(hash.begin(), id.size(), id.begin());
  return id;
}

absl::Status Invalid(std::string_view reason) {
  return MetaDomainRejectError(reason);
}

using Plan = absl::StatusOr<std::optional<MetaCommand>>;

template <typename Command>
Plan Emit(Command command) {
  return std::optional<MetaCommand>(std::move(command));
}

const MetaTerminalReceipt* CurrentReceipt(
    const MetaOperationRecord& operation) {
  if (operation.current_directives_.size() != 1) return nullptr;
  const MetaCurrentDirective& current = operation.current_directives_.front();
  const auto found = std::find_if(
      operation.terminal_receipts_.begin(), operation.terminal_receipts_.end(),
      [&](const MetaTerminalReceipt& receipt) {
        return receipt.key_.operation_id_ == operation.operation_id_ &&
               receipt.key_.directive_id_ == current.spec_.directive_id_ &&
               receipt.key_.attempt_id_ == current.spec_.attempt_id_ &&
               receipt.key_.directive_revision_ == current.directive_revision_;
      });
  return found == operation.terminal_receipts_.end() ? nullptr : &*found;
}

bool RecoveryAnchorsMatch(const MetaFailoverRecoveryRecord& recovery,
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

SetFailoverRecovery RecoveryCommand(
    const MetaCommittedView& view, const FailoverIntent& intent,
    bool hold_required, bool recovery_required,
    MetaFailoverProofState proof_state = MetaFailoverProofState::kPending,
    std::optional<MetaFailoverFrozenProof> proof = std::nullopt) {
  SetFailoverRecovery command;
  command.group_id_ = intent.group_id_;
  command.expected_revision_ =
      view.failover_recovery().LastRevision(intent.group_id_).value_or(0);
  command.recovery_generation_ = intent.recovery_generation_;
  command.old_source_node_id_ = intent.former_owner_node_id_;
  command.old_source_assignment_id_ = intent.former_owner_assignment_id_;
  command.old_source_boot_incarnation_ = intent.former_owner_boot_id_;
  command.old_source_history_id_ = intent.parent_history_id_;
  command.excluded_authority_term_ = intent.group_term_ - 1;
  command.excluded_authority_version_ = intent.authority_version_;
  command.excluded_grant_revision_ = intent.grant_revision_;
  command.population_manifest_revision_ = intent.population_manifest_revision_;
  command.population_manifest_digest_ = intent.population_manifest_digest_;
  command.partition_replication_epoch_ = intent.partition_replication_epoch_;
  command.hold_required_ = hold_required;
  command.recovery_required_ = recovery_required;
  command.proof_state_ = proof_state;
  command.frozen_proof_ = std::move(proof);
  return command;
}

absl::StatusOr<MetaFailoverFrozenProof> FrozenProofFromReceipt(
    const MetaTerminalReceipt& receipt, const FailoverIntent& intent) {
  auto evidence = control::DecodeFrozenSourceEvidence(receipt.result_);
  if (!evidence.ok() ||
      evidence->recovery_generation != intent.recovery_generation_ ||
      evidence->source_history_id != Hex(intent.parent_history_id_) ||
      evidence->final_next_lsns.size() != intent.flow_count_) {
    return Invalid("frozen-source returned invalid exact evidence");
  }
  auto proof_hash = control::ComputeFrozenSourceProofHash(*evidence);
  if (!proof_hash.ok() || *proof_hash != evidence->proof_hash) {
    return Invalid("frozen-source returned an invalid proof hash");
  }
  return MetaFailoverFrozenProof{
      .final_next_lsns_ = evidence->final_next_lsns,
      .proof_hash_ = evidence->proof_hash,
  };
}

absl::StatusOr<FailoverPhase> DecodeOperationPhase(
    const MetaOperationRecord& operation) {
  if (operation.kind_phase_blob_.empty()) {
    return Invalid("controlled failover has no committed phase");
  }
  auto phase = DecodeFailoverPhase(operation.kind_phase_blob_);
  if (!phase.ok()) {
    return Invalid("controlled failover has corrupt committed phase");
  }
  return *phase;
}

Plan Transition(const MetaOperationRecord& operation, FailoverPhase phase,
                std::vector<MetaDirectiveSpec> directives = {},
                std::vector<MetaEvidenceSummary> evidence = {}) {
  auto encoded = EncodeFailoverPhase(phase);
  if (!encoded.ok()) return encoded.status();
  TransitionOperationPhase command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.kind_phase_blob_ = std::move(*encoded);
  command.current_directives_ = std::move(directives);
  command.evidence_ = std::move(evidence);
  return Emit(std::move(command));
}

std::vector<MetaDirectiveSpec> RetainedDirectiveSpecs(
    const MetaOperationRecord& operation) {
  std::vector<MetaDirectiveSpec> retained;
  retained.reserve(operation.current_directives_.size());
  for (const MetaCurrentDirective& current : operation.current_directives_) {
    retained.push_back(current.spec_);
  }
  return retained;
}

Plan Abort(const MetaOperationRecord& operation, FailoverPhaseStage stage,
           FailoverLossClassification loss, bool recovery_required,
           std::vector<std::uint64_t> frontier, std::string reason) {
  ControlledFailoverOutcome outcome{
      .succeeded_ = false,
      .terminal_stage_ = stage,
      .loss_ = loss,
      .recovery_required_ = recovery_required,
      .proven_next_lsns_ = std::move(frontier),
      .reason_ = std::move(reason),
  };
  auto encoded = EncodeControlledFailoverOutcome(outcome);
  if (!encoded.ok()) return encoded.status();
  AbortOperation command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.reason_ = std::move(*encoded);
  command.data_loss_possible_ = loss != FailoverLossClassification::kExact;
  return Emit(std::move(command));
}

Plan Complete(const MetaOperationRecord& operation, const FailoverPhase& phase,
              const MetaFailoverRecoveryRecord& recovery) {
  const bool exact = recovery.frozen_proof_.has_value() &&
                     phase.old_authority_exclusion_hash_ ==
                         recovery.frozen_proof_->proof_hash_ &&
                     phase.required_applied_next_lsns_ ==
                         recovery.frozen_proof_->final_next_lsns_;
  ControlledFailoverOutcome outcome{
      .succeeded_ = true,
      .terminal_stage_ = FailoverPhaseStage::kServing,
      .loss_ = exact ? FailoverLossClassification::kExact
                     : FailoverLossClassification::kUnknown,
      .recovery_required_ = false,
      .proven_next_lsns_ = phase.required_applied_next_lsns_,
      .reason_ = exact ? "candidate served the exact frozen source frontier"
                       : "candidate served the best available replica frontier",
  };
  auto encoded = EncodeControlledFailoverOutcome(outcome);
  if (!encoded.ok()) return encoded.status();
  CompleteOperation command;
  command.operation_id_ = operation.operation_id_;
  command.expected_revision_ = operation.revision_;
  command.result_ = std::move(*encoded);
  command.data_loss_possible_ = !exact;
  return Emit(std::move(command));
}

const MetaDataControlRuntimeNode* RuntimeNode(
    const MetaDataControlRuntimeSnapshot& runtime, std::string_view node_id,
    const MetaBootIncarnation& boot) {
  if (!runtime.leader_authority_eligible_) return nullptr;
  const std::string expected_boot = Hex(boot);
  const auto found = std::find_if(
      runtime.nodes_.begin(), runtime.nodes_.end(), [&](const auto& node) {
        return node.node_id_ == node_id && node.boot_id_ == expected_boot;
      });
  return found == runtime.nodes_.end() ? nullptr : &*found;
}

bool RuntimeHasReplacedBoot(const MetaDataControlRuntimeSnapshot& runtime,
                            std::string_view node_id,
                            const MetaBootIncarnation& boot) {
  const std::string expected_boot = Hex(boot);
  return std::any_of(
      runtime.nodes_.begin(), runtime.nodes_.end(), [&](const auto& node) {
        return node.node_id_ == node_id && node.boot_id_ != expected_boot;
      });
}

bool RuntimeHasReplacedHistory(const MetaDataControlRuntimeSnapshot& runtime,
                               std::string_view node_id,
                               const MetaBootIncarnation& boot,
                               const MetaReplicationHistoryId& history) {
  const MetaDataControlRuntimeNode* node = RuntimeNode(runtime, node_id, boot);
  return node != nullptr && node->replication_history_id_ != history;
}

const MetaDataControlRuntimeGroup* RuntimeGroup(
    const MetaDataControlRuntimeNode& node, const FailoverIntent& intent,
    const MetaAssignmentId& assignment, std::uint64_t term,
    std::uint64_t authority_version, std::uint64_t grant_revision) {
  const auto found = std::find_if(
      node.groups_.begin(), node.groups_.end(), [&](const auto& group) {
        return group.group_id_ == intent.group_id_ &&
               group.assignment_id_ == assignment &&
               group.group_term_ == term &&
               group.authority_version_ == authority_version &&
               group.grant_revision_ == grant_revision &&
               group.manifest_revision_ ==
                   intent.population_manifest_revision_ &&
               group.manifest_digest_ == intent.population_manifest_digest_ &&
               group.partition_replication_epoch_ ==
                   intent.partition_replication_epoch_;
      });
  return found == node.groups_.end() ? nullptr : &*found;
}

const MetaDataControlRuntimeGroup* RuntimeGroup(
    const MetaDataControlRuntimeNode& node, const FailoverIntent& intent,
    const MetaAssignmentId& assignment, std::uint64_t term) {
  return RuntimeGroup(node, intent, assignment, term, intent.authority_version_,
                      intent.grant_revision_);
}

bool RuntimeHasReplacedAssignment(const MetaDataControlRuntimeSnapshot& runtime,
                                  std::string_view node_id,
                                  const MetaBootIncarnation& boot,
                                  std::string_view group_id,
                                  const MetaAssignmentId& assignment) {
  const MetaDataControlRuntimeNode* node = RuntimeNode(runtime, node_id, boot);
  if (node == nullptr) return false;
  const auto group = std::find_if(node->groups_.begin(), node->groups_.end(),
                                  [&](const MetaDataControlRuntimeGroup& item) {
                                    return item.group_id_ == group_id;
                                  });
  // Runtime nodes are published only after a complete FDS was acknowledged;
  // an exact live session that omitted this group therefore proves assignment
  // removal rather than a partially installed projection.
  return group == node->groups_.end() || group->assignment_id_ != assignment;
}

bool HoldMatches(const control::WireSourceHistoryHold& hold,
                 const FailoverIntent& intent) {
  return hold.generation == intent.recovery_generation_ &&
         hold.source_assignment_id == intent.former_owner_assignment_id_ &&
         hold.source_boot_id == Hex(intent.former_owner_boot_id_) &&
         hold.source_replication_history_id == Hex(intent.parent_history_id_) &&
         hold.manifest_revision == intent.population_manifest_revision_ &&
         hold.manifest_digest == intent.population_manifest_digest_ &&
         hold.partition_replication_epoch ==
             intent.partition_replication_epoch_;
}

bool HoldMatches(const control::WireSourceHistoryHold& hold,
                 const MetaFailoverRecoveryRecord& recovery) {
  return hold.generation == recovery.recovery_generation_ &&
         hold.source_assignment_id == recovery.old_source_assignment_id_ &&
         hold.source_boot_id == Hex(recovery.old_source_boot_incarnation_) &&
         hold.source_replication_history_id ==
             Hex(recovery.old_source_history_id_) &&
         hold.manifest_revision == recovery.population_manifest_revision_ &&
         hold.manifest_digest == recovery.population_manifest_digest_ &&
         hold.partition_replication_epoch ==
             recovery.partition_replication_epoch_;
}

bool SourceRecoveryHoldMatches(const MetaDataControlRuntimeNode& source,
                               const FailoverIntent& intent) {
  return std::any_of(
      source.groups_.begin(), source.groups_.end(), [&](const auto& group) {
        return group.group_id_ == intent.group_id_ &&
               group.assignment_id_ == intent.former_owner_assignment_id_ &&
               group.manifest_revision_ ==
                   intent.population_manifest_revision_ &&
               group.manifest_digest_ == intent.population_manifest_digest_ &&
               group.partition_replication_epoch_ ==
                   intent.partition_replication_epoch_ &&
               group.source_history_hold_.has_value() &&
               HoldMatches(*group.source_history_hold_, intent);
      });
}

bool RuntimeNodeHealthy(const MetaDataControlRuntimeNode* node) {
  return node != nullptr && node->health_.has_value() &&
         node->health_->storage_ready && node->health_->population_ready &&
         !node->health_->draining;
}

bool SourceRecoveryHolderReady(const MetaDataControlRuntimeNode* source,
                               const FailoverIntent& intent) {
  // The projected hold proves desired state, while the session history proves
  // that the same physical backlog still backs it. Data tears down or changes
  // one of these observable facts on every path that clears the armed
  // hold; accepting the FDS projection alone could retain a stale exact proof
  // after an in-place replication-history reset.
  return RuntimeNodeHealthy(source) &&
         source->replication_history_id_ == intent.parent_history_id_ &&
         SourceRecoveryHoldMatches(*source, intent);
}

bool SourceRecoveryHolderReady(const MetaDataControlRuntimeNode* source,
                               const MetaFailoverRecoveryRecord& recovery) {
  if (!RuntimeNodeHealthy(source) ||
      source->replication_history_id_ != recovery.old_source_history_id_) {
    return false;
  }
  return std::any_of(
      source->groups_.begin(), source->groups_.end(), [&](const auto& group) {
        return group.group_id_ == recovery.group_id_ &&
               group.assignment_id_ == recovery.old_source_assignment_id_ &&
               group.manifest_revision_ ==
                   recovery.population_manifest_revision_ &&
               group.manifest_digest_ == recovery.population_manifest_digest_ &&
               group.partition_replication_epoch_ ==
                   recovery.partition_replication_epoch_ &&
               group.source_history_hold_.has_value() &&
               HoldMatches(*group.source_history_hold_, recovery);
      });
}

bool ConfirmedServingMatches(const MetaDataControlRuntimeNode* candidate,
                             const FailoverIntent& intent,
                             const MetaDataControlRuntimeSnapshot& runtime,
                             const std::optional<MetaGroupGrantState>& grant) {
  if (candidate == nullptr || !grant.has_value() ||
      !grant->grant_.has_value() ||
      !candidate->confirmed_serving_lease_.has_value()) {
    return false;
  }
  const auto* group = RuntimeGroup(
      *candidate, intent, intent.candidate_assignment_id_, intent.group_term_,
      intent.authority_version_ + 1, grant->grant_->grant_revision_);
  const control::LeaseGranted& lease = *candidate->confirmed_serving_lease_;
  return group != nullptr &&
         lease.data_boot_id == Hex(intent.candidate_boot_id_) &&
         lease.projection_hash == candidate->projection_hash_ &&
         lease.group_id == intent.group_id_ &&
         lease.assignment_id == intent.candidate_assignment_id_ &&
         lease.group_term == intent.group_term_ &&
         lease.authority_version == intent.authority_version_ + 1 &&
         lease.grant_revision == grant->grant_->grant_revision_ &&
         lease.leadership_generation == runtime.leadership_generation_ &&
         lease.granted_duration_ms != 0;
}

std::optional<MetaCandidateProgressObs> CurrentCandidate(
    const MetaCommittedView& view, const MetaObservationStore& observations,
    const FailoverIntent& intent, std::uint64_t term,
    std::int64_t now_unix_ms) {
  const MetaStoresFacts facts(view.stores());
  std::vector<MetaCandidateProgressObs> candidates =
      observations.LiveCandidateProgressFor(intent.group_id_, facts,
                                            now_unix_ms);
  const auto found = std::find_if(
      candidates.begin(), candidates.end(), [&](const auto& candidate) {
        return candidate.node_id_ == intent.candidate_node_id_ &&
               candidate.boot_incarnation_ == intent.candidate_boot_id_ &&
               candidate.assignment_id_ == intent.candidate_assignment_id_ &&
               candidate.group_term_ == term &&
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
  return found == candidates.end() ? std::nullopt : std::optional(*found);
}

MetaDirectiveSpec BaseDirective(const MetaOperationRecord& operation,
                                const FailoverIntent& intent,
                                std::string_view purpose,
                                std::string_view attempt_purpose) {
  MetaDirectiveSpec directive;
  directive.directive_id_ = DerivedId(operation.operation_id_, purpose);
  directive.attempt_id_ = DerivedId(operation.operation_id_, attempt_purpose);
  directive.target_node_id_ = intent.candidate_node_id_;
  directive.target_boot_id_ = intent.candidate_boot_id_;
  directive.assignment_id_ = intent.candidate_assignment_id_;
  directive.source_node_id_ = intent.former_owner_node_id_;
  directive.source_assignment_id_ = intent.former_owner_assignment_id_;
  directive.source_boot_id_ = intent.former_owner_boot_id_;
  directive.source_replication_history_id_ = intent.parent_history_id_;
  directive.group_id_ = intent.group_id_;
  directive.authority_version_ = intent.authority_version_;
  directive.grant_revision_ = intent.grant_revision_;
  directive.population_manifest_revision_ =
      intent.population_manifest_revision_;
  directive.population_manifest_digest_ = intent.population_manifest_digest_;
  directive.partition_replication_epoch_ = intent.partition_replication_epoch_;
  return directive;
}

absl::StatusOr<MetaDirectiveSpec> HoldingDirective(
    const MetaOperationRecord& operation, const FailoverIntent& intent) {
  MetaDirectiveSpec directive =
      BaseDirective(operation, intent, "source-hold", "source-hold-attempt");
  directive.recipient_node_id_ = intent.former_owner_node_id_;
  directive.group_term_ = intent.group_term_ - 1;
  directive.kind_ = std::string(kMetaDirectiveAuthorizeSource);
  auto request = control::EncodeRebuildRequest(
      {.source_flow_count = intent.flow_count_});
  if (!request.ok()) return request.status();
  directive.payload_ = std::move(*request);
  return directive;
}

absl::StatusOr<MetaDirectiveSpec> FrozenDirective(
    const MetaOperationRecord& operation, const FailoverIntent& intent) {
  MetaDirectiveSpec directive =
      BaseDirective(operation, intent, "frozen-source", "frozen-attempt");
  directive.recipient_node_id_ = intent.former_owner_node_id_;
  directive.group_term_ = intent.group_term_;
  directive.kind_ = std::string(kMetaDirectiveAuthorizeSource);
  auto request = control::EncodeFrozenSourceRequest(
      {.recovery_generation = intent.recovery_generation_,
       .source_flow_count = intent.flow_count_});
  auto preconditions = control::EncodeFrozenSourcePreconditions(
      {.excluded_group_term = intent.group_term_ - 1,
       .excluded_authority_version = intent.authority_version_,
       .excluded_grant_revision = intent.grant_revision_});
  if (!request.ok()) return request.status();
  if (!preconditions.ok()) return preconditions.status();
  directive.payload_ = std::move(*request);
  directive.preconditions_ = std::move(*preconditions);
  return directive;
}

absl::StatusOr<MetaDirectiveSpec> PromotionDirective(
    const MetaOperationRecord& operation, const FailoverIntent& intent,
    const FailoverPhase& phase) {
  MetaDirectiveSpec directive = BaseDirective(
      operation, intent, "promotion-prepare", "promotion-attempt");
  directive.recipient_node_id_ = intent.candidate_node_id_;
  directive.group_term_ = intent.group_term_;
  directive.kind_ = std::string(kMetaDirectivePromotionPrepare);
  directive.storage_mutating_ = true;
  auto request = control::EncodePromotionPrepareRequest(
      {.parent_history_id = Hex(intent.parent_history_id_),
       .required_applied_next_lsns = phase.required_applied_next_lsns_});
  auto preconditions = control::EncodePromotionPreparePreconditions(
      {.excluded_group_term = intent.group_term_,
       .old_authority_exclusion_hash = phase.old_authority_exclusion_hash_});
  if (!request.ok()) return request.status();
  if (!preconditions.ok()) return preconditions.status();
  directive.payload_ = std::move(*request);
  directive.preconditions_ = std::move(*preconditions);
  return directive;
}

bool ActivatedAuthorityMatches(const MetaCommittedView& view,
                               const FailoverIntent& intent) {
  const auto group = view.topology().FindGroup(intent.group_id_);
  const auto grant = view.grant().GroupState(intent.group_id_);
  return group.has_value() && grant.has_value() && !grant->fenced_ &&
         grant->grant_.has_value() &&
         group->record_.owner_ == intent.candidate_node_id_ &&
         group->record_.group_term_ == intent.group_term_ &&
         group->record_.authority_version_ == intent.authority_version_ + 1 &&
         grant->grant_->owner_ == intent.candidate_node_id_ &&
         grant->grant_->term_ == intent.group_term_ &&
         grant->grant_->authority_version_ == intent.authority_version_ + 1 &&
         grant->grant_->spec_ == intent.old_grant_;
}

bool FencedAuthorityMatches(const MetaCommittedView& view,
                            const FailoverIntent& intent) {
  const auto group = view.topology().FindGroup(intent.group_id_);
  const auto grant = view.grant().GroupState(intent.group_id_);
  return group.has_value() && grant.has_value() && grant->fenced_ &&
         !grant->grant_.has_value() &&
         group->record_.group_term_ == intent.group_term_ &&
         group->record_.authority_version_ == intent.authority_version_ &&
         grant->last_authority_version_ == intent.authority_version_ &&
         grant->last_grant_revision_ == intent.grant_revision_;
}

bool ActiveFormerAuthorityMatches(const MetaCommittedView& view,
                                  const FailoverIntent& intent) {
  const auto group = view.topology().FindGroup(intent.group_id_);
  const auto grant = view.grant().GroupState(intent.group_id_);
  return group.has_value() && grant.has_value() && !grant->fenced_ &&
         grant->grant_.has_value() &&
         group->record_.owner_ == intent.former_owner_node_id_ &&
         group->record_.group_term_ == intent.group_term_ - 1 &&
         group->record_.authority_version_ == intent.authority_version_ &&
         grant->grant_->owner_ == intent.former_owner_node_id_ &&
         grant->grant_->grant_revision_ == intent.grant_revision_;
}

FailoverLossClassification RecoveryLoss(
    const MetaFailoverRecoveryRecord& recovery) {
  return recovery.proof_state_ == MetaFailoverProofState::kExact &&
                 recovery.frozen_proof_.has_value()
             ? FailoverLossClassification::kExact
             : FailoverLossClassification::kUnknown;
}

std::vector<std::uint64_t> RecoveryFrontier(
    const MetaFailoverRecoveryRecord& recovery) {
  return recovery.frozen_proof_.has_value()
             ? recovery.frozen_proof_->final_next_lsns_
             : std::vector<std::uint64_t>{};
}

std::vector<std::uint64_t> FailureFrontier(
    const MetaFailoverRecoveryRecord& recovery, const FailoverPhase& phase) {
  return phase.required_applied_next_lsns_.empty()
             ? RecoveryFrontier(recovery)
             : phase.required_applied_next_lsns_;
}

Plan AbortExpiredBeforeCut(const MetaOperationRecord& operation,
                           const FailoverPhase& phase,
                           bool former_authority_available) {
  if (former_authority_available) {
    return Abort(
        operation, phase.stage_, FailoverLossClassification::kExact,
        /*recovery_required=*/false, {},
        "controlled failover attempt deadline expired before BeginGroupTerm; "
        "old primary remains authoritative");
  }
  return Abort(
      operation, phase.stage_, FailoverLossClassification::kUnknown,
      /*recovery_required=*/true, {},
      "controlled failover attempt deadline expired before BeginGroupTerm; "
      "old primary is not currently confirmed, so recovery handoff is "
      "retained");
}

Plan AbortExpiredAfterCut(const MetaOperationRecord& operation,
                          const FailoverPhase& phase,
                          const MetaFailoverRecoveryRecord& recovery) {
  const bool activated =
      phase.stage_ == FailoverPhaseStage::kAuthorityActivated;
  return Abort(
      operation, phase.stage_, RecoveryLoss(recovery),
      /*recovery_required=*/true, FailureFrontier(recovery, phase),
      activated
          ? "controlled failover attempt deadline expired after authority "
            "activation; a new recovery term is required"
          : "controlled failover attempt deadline expired after "
            "BeginGroupTerm; group remains fenced for independent recovery");
}

bool PhaseUsesHistoricalExactFrontier(
    const FailoverPhase& phase, const MetaFailoverRecoveryRecord& recovery) {
  return recovery.frozen_proof_.has_value() &&
         phase.old_authority_exclusion_hash_ ==
             recovery.frozen_proof_->proof_hash_ &&
         phase.required_applied_next_lsns_ ==
             recovery.frozen_proof_->final_next_lsns_;
}

std::string FrontierText(const std::vector<std::uint64_t>& frontier) {
  if (frontier.empty()) return "none";
  std::string result;
  for (const std::uint64_t cursor : frontier) {
    if (!result.empty()) result.push_back(',');
    absl::StrAppend(&result, cursor);
  }
  return result;
}

Plan CleanupTerminal(const MetaCommittedView& view,
                     const MetaOperationRecord& operation,
                     const FailoverIntent& intent,
                     const MetaDataControlRuntimeSnapshot& runtime,
                     detail::ControlledFailoverLiveness liveness) {
  auto outcome = DecodeControlledFailoverOutcome(operation.terminal_result_);
  if (!outcome.ok()) {
    return Invalid("terminal controlled failover has an invalid result");
  }
  const auto last_generation =
      view.failover_recovery().LastGeneration(intent.group_id_);
  if (!last_generation.has_value() ||
      *last_generation < intent.recovery_generation_) {
    return Invalid("terminal failover lost its recovery generation cursor");
  }
  // A higher generation can exist only after this one was either fully
  // cleared or explicitly handed off. Cleanup belongs to the successor now;
  // an old terminal owner must never rewrite or clear its record.
  if (*last_generation > intent.recovery_generation_) return std::nullopt;

  const auto recovery = view.failover_recovery().Find(intent.group_id_);
  if (!recovery.has_value()) {
    return outcome->recovery_required_
               ? Plan(Invalid("terminal failover lost its recovery handoff"))
               : Plan(std::nullopt);
  }
  if (!RecoveryAnchorsMatch(*recovery, intent)) {
    return Invalid("terminal failover recovery handoff changed identity");
  }
  if (outcome->recovery_required_) {
    // Publish the handoff first. A subsequent availability downgrade CASes
    // the handoff revision, so a stale pre-terminal downgrade can neither
    // erase nor race ahead of the durable ownership transfer.
    if (!recovery->recovery_required_ || !recovery->hold_required_) {
      return Emit(RecoveryCommand(view, intent, /*hold_required=*/true,
                                  /*recovery_required=*/true,
                                  recovery->proof_state_,
                                  recovery->frozen_proof_));
    }
    auto downgrade = detail::PlanFailoverRecoveryAvailabilityStep(
        *recovery, runtime, liveness.former_owner_unavailable_);
    return downgrade.has_value() ? Emit(MetaCommand(std::move(*downgrade)))
                                 : Plan(std::nullopt);
  }
  if (recovery->hold_required_ || recovery->recovery_required_) {
    return Emit(RecoveryCommand(view, intent, /*hold_required=*/false,
                                /*recovery_required=*/false,
                                recovery->proof_state_,
                                recovery->frozen_proof_));
  }

  // If the exact old boot is still connected, retain the tombstone until its
  // acknowledged FDS no longer contains this generation. This makes Clear a
  // record cleanup, not a race that can resurrect the hold from an older FDS.
  if (const auto* source = RuntimeNode(runtime, intent.former_owner_node_id_,
                                       intent.former_owner_boot_id_);
      source != nullptr) {
    const auto group = std::find_if(
        source->groups_.begin(), source->groups_.end(), [&](const auto& item) {
          return item.group_id_ == intent.group_id_ &&
                 item.assignment_id_ == intent.former_owner_assignment_id_;
        });
    if (group != source->groups_.end() &&
        group->source_history_hold_.has_value() &&
        group->source_history_hold_->generation ==
            intent.recovery_generation_) {
      return std::nullopt;
    }
  }
  ClearFailoverRecovery clear;
  clear.group_id_ = intent.group_id_;
  clear.expected_revision_ = recovery->revision_;
  clear.recovery_generation_ = intent.recovery_generation_;
  return Emit(std::move(clear));
}

}  // namespace

detail::ControlledFailoverReadiness detail::EvaluateControlledFailoverReadiness(
    const MetaCommittedView& view, const MetaObservationStore& observations,
    const MetaDataControlRuntimeSnapshot& runtime,
    const MetaOperationRecord& operation, const FailoverIntent& intent,
    const std::optional<FailoverPhase>& phase, std::int64_t now_unix_ms) {
  ControlledFailoverReadiness readiness;
  readiness.former_owner_replaced_ =
      RuntimeHasReplacedBoot(runtime, intent.former_owner_node_id_,
                             intent.former_owner_boot_id_) ||
      RuntimeHasReplacedHistory(runtime, intent.former_owner_node_id_,
                                intent.former_owner_boot_id_,
                                intent.parent_history_id_) ||
      RuntimeHasReplacedAssignment(
          runtime, intent.former_owner_node_id_, intent.former_owner_boot_id_,
          intent.group_id_, intent.former_owner_assignment_id_);
  readiness.candidate_replaced_ =
      RuntimeHasReplacedBoot(runtime, intent.candidate_node_id_,
                             intent.candidate_boot_id_) ||
      RuntimeHasReplacedAssignment(runtime, intent.candidate_node_id_,
                                   intent.candidate_boot_id_, intent.group_id_,
                                   intent.candidate_assignment_id_);

  const MetaDataControlRuntimeNode* source = RuntimeNode(
      runtime, intent.former_owner_node_id_, intent.former_owner_boot_id_);
  const MetaDataControlRuntimeNode* candidate = RuntimeNode(
      runtime, intent.candidate_node_id_, intent.candidate_boot_id_);
  const bool active = ActiveFormerAuthorityMatches(view, intent);
  const bool fenced = FencedAuthorityMatches(view, intent);
  const bool activated = ActivatedAuthorityMatches(view, intent);
  const auto grant = view.grant().GroupState(intent.group_id_);

  auto projected_group = [&](const MetaDataControlRuntimeNode* node,
                             const MetaAssignmentId& assignment)
      -> const MetaDataControlRuntimeGroup* {
    if (node == nullptr) return nullptr;
    if (active) {
      return RuntimeGroup(*node, intent, assignment, intent.group_term_ - 1);
    }
    if (fenced) {
      return RuntimeGroup(*node, intent, assignment, intent.group_term_);
    }
    if (activated && grant.has_value() && grant->grant_.has_value()) {
      return RuntimeGroup(*node, intent, assignment, intent.group_term_,
                          intent.authority_version_ + 1,
                          grant->grant_->grant_revision_);
    }
    return nullptr;
  };

  const MetaDataControlRuntimeGroup* source_group =
      projected_group(source, intent.former_owner_assignment_id_);
  readiness.former_owner_ready_ =
      source_group != nullptr && RuntimeNodeHealthy(source) &&
      source->replication_history_id_ == intent.parent_history_id_;
  if (readiness.former_owner_ready_) {
    const auto recovery = view.failover_recovery().Find(intent.group_id_);
    if (recovery.has_value() && recovery->hold_required_) {
      readiness.former_owner_ready_ =
          source_group->source_history_hold_.has_value() &&
          HoldMatches(*source_group->source_history_hold_, intent);
    }
  }

  const MetaDataControlRuntimeGroup* candidate_group =
      projected_group(candidate, intent.candidate_assignment_id_);
  const bool serving_already_confirmed =
      phase.has_value() &&
      phase->stage_ == FailoverPhaseStage::kAuthorityActivated &&
      ConfirmedServingMatches(candidate, intent, runtime, grant);
  readiness.candidate_ready_ =
      candidate_group != nullptr &&
      (RuntimeNodeHealthy(candidate) || serving_already_confirmed);
  const bool needs_candidate_progress =
      !phase.has_value() ||
      phase->stage_ <= FailoverPhaseStage::kCandidateCaughtUp;
  if (readiness.candidate_ready_ && needs_candidate_progress) {
    const std::uint64_t candidate_term =
        active ? intent.group_term_ - 1 : intent.group_term_;
    const auto progress = CurrentCandidate(view, observations, intent,
                                           candidate_term, now_unix_ms);
    readiness.candidate_ready_ =
        progress.has_value() && candidate != nullptr &&
        progress->session_generation_ == candidate->session_generation_;
  }
  if (readiness.candidate_ready_ && phase.has_value() &&
      phase->stage_ == FailoverPhaseStage::kPromotionPreparing) {
    const MetaTerminalReceipt* receipt = CurrentReceipt(operation);
    if (receipt != nullptr &&
        receipt->status_ == MetaDirectiveResultStatus::kSucceeded) {
      const auto observation_generation =
          observations.CurrentGeneration(intent.candidate_node_id_);
      const auto evidence = ResolveFailoverPreparedEvidence(
          view, operation, intent, *phase, observations, *receipt, now_unix_ms);
      readiness.candidate_ready_ =
          candidate != nullptr && observation_generation.has_value() &&
          *observation_generation == candidate->session_generation_ &&
          evidence.ok() && evidence->has_value();
    }
  }
  return readiness;
}

std::optional<SetFailoverRecovery> detail::PlanFailoverRecoveryAvailabilityStep(
    const MetaFailoverRecoveryRecord& recovery,
    const MetaDataControlRuntimeSnapshot& runtime,
    bool former_owner_unavailable) {
  if (!recovery.hold_required_ || !recovery.recovery_required_ ||
      recovery.proof_state_ == MetaFailoverProofState::kUnavailable) {
    return std::nullopt;
  }
  if (runtime.leader_authority_eligible_) {
    former_owner_unavailable |=
        RuntimeHasReplacedBoot(runtime, recovery.old_source_node_id_,
                               recovery.old_source_boot_incarnation_);
    former_owner_unavailable |= RuntimeHasReplacedHistory(
        runtime, recovery.old_source_node_id_,
        recovery.old_source_boot_incarnation_, recovery.old_source_history_id_);
    former_owner_unavailable |= RuntimeHasReplacedAssignment(
        runtime, recovery.old_source_node_id_,
        recovery.old_source_boot_incarnation_, recovery.group_id_,
        recovery.old_source_assignment_id_);
  }
  if (!former_owner_unavailable) return std::nullopt;

  SetFailoverRecovery command;
  command.group_id_ = recovery.group_id_;
  command.expected_revision_ = recovery.revision_;
  command.recovery_generation_ = recovery.recovery_generation_;
  command.old_source_node_id_ = recovery.old_source_node_id_;
  command.old_source_assignment_id_ = recovery.old_source_assignment_id_;
  command.old_source_boot_incarnation_ = recovery.old_source_boot_incarnation_;
  command.old_source_history_id_ = recovery.old_source_history_id_;
  command.excluded_authority_term_ = recovery.excluded_authority_term_;
  command.excluded_authority_version_ = recovery.excluded_authority_version_;
  command.excluded_grant_revision_ = recovery.excluded_grant_revision_;
  command.population_manifest_revision_ =
      recovery.population_manifest_revision_;
  command.population_manifest_digest_ = recovery.population_manifest_digest_;
  command.partition_replication_epoch_ = recovery.partition_replication_epoch_;
  command.hold_required_ = true;
  command.recovery_required_ = true;
  command.proof_state_ = MetaFailoverProofState::kUnavailable;
  command.frozen_proof_ = recovery.frozen_proof_;
  return command;
}

Plan detail::PlanControlledFailoverStep(
    const MetaCommittedView& view, const MetaOperationRecord& operation,
    const MetaObservationStore& observations,
    const MetaDataControlRuntimeSnapshot& runtime,
    ControlledFailoverLiveness liveness, std::int64_t now_unix_ms) {
  if (operation.kind_ != kFailoverOperationKind) return std::nullopt;
  auto decoded_intent = DecodeFailoverIntent(operation.intent_);
  if (!decoded_intent.ok() ||
      operation.intent_hash_ != MetaSha256(operation.intent_) ||
      operation.replication_history_id_ != decoded_intent->parent_history_id_) {
    return Invalid("controlled failover intent is not recoverable");
  }
  const FailoverIntent& intent = *decoded_intent;
  liveness.former_owner_unavailable_ |= RuntimeHasReplacedBoot(
      runtime, intent.former_owner_node_id_, intent.former_owner_boot_id_);
  liveness.candidate_unavailable_ |= RuntimeHasReplacedBoot(
      runtime, intent.candidate_node_id_, intent.candidate_boot_id_);

  if (IsTerminal(operation.lifecycle_)) {
    return CleanupTerminal(view, operation, intent, runtime, liveness);
  }
  const auto recovery = view.failover_recovery().Find(intent.group_id_);
  if (!recovery.has_value()) {
    if (view.failover_recovery().LastGeneration(intent.group_id_).value_or(0) >=
        intent.recovery_generation_) {
      return Invalid("controlled failover recovery generation was consumed");
    }
    return Emit(RecoveryCommand(view, intent, /*hold_required=*/true,
                                /*recovery_required=*/false));
  }
  if (!RecoveryAnchorsMatch(*recovery, intent)) {
    return Invalid("controlled failover recovery record conflicts with intent");
  }

  if (operation.kind_phase_blob_.empty()) {
    FailoverPhase holding;
    holding.stage_ = FailoverPhaseStage::kSourceHolding;
    return Transition(operation, std::move(holding));
  }
  auto decoded_phase = DecodeOperationPhase(operation);
  if (!decoded_phase.ok()) return decoded_phase.status();
  const FailoverPhase& phase = *decoded_phase;
  const MetaDataControlRuntimeNode* source = RuntimeNode(
      runtime, intent.former_owner_node_id_, intent.former_owner_boot_id_);
  const MetaDataControlRuntimeNode* candidate = RuntimeNode(
      runtime, intent.candidate_node_id_, intent.candidate_boot_id_);
  const ControlledFailoverReadiness readiness =
      EvaluateControlledFailoverReadiness(
          view, observations, runtime, operation, intent, phase, now_unix_ms);
  liveness.former_owner_unavailable_ |= readiness.former_owner_replaced_;
  liveness.candidate_unavailable_ |= readiness.candidate_replaced_;
  const bool former_authority_available =
      source != nullptr && RuntimeNodeHealthy(source) &&
      RuntimeGroup(*source, intent, intent.former_owner_assignment_id_,
                   intent.group_term_ - 1) != nullptr;

  // A frozen frontier remains a true historical upper bound after the source
  // boot disappears, but the held bytes are no longer known to exist. Record
  // that availability loss before any candidate-failure terminalization. The
  // proof itself is retained so a candidate that already reached it can still
  // complete exactly; a lagging candidate will take the explicit same-stage
  // frontier downgrade below.
  const bool cutover_committed =
      phase.stage_ > FailoverPhaseStage::kOldAuthorityExcluding ||
      (phase.stage_ == FailoverPhaseStage::kOldAuthorityExcluding &&
       FencedAuthorityMatches(view, intent));
  const bool pre_cut_source_loss =
      !cutover_committed &&
      (liveness.former_owner_unavailable_ ||
       (liveness.attempt_deadline_expired_ && !former_authority_available));
  if (pre_cut_source_loss &&
      recovery->proof_state_ == MetaFailoverProofState::kPending) {
    // Make the handoff self-describing before terminalization. In particular,
    // a queued Abort cannot strand a pending proof after the only source boot
    // has already been declared unusable.
    return Emit(RecoveryCommand(view, intent, /*hold_required=*/true,
                                /*recovery_required=*/false,
                                MetaFailoverProofState::kUnavailable));
  }
  if (!cutover_committed &&
      recovery->proof_state_ == MetaFailoverProofState::kUnavailable) {
    return Abort(
        operation, phase.stage_, FailoverLossClassification::kUnknown,
        /*recovery_required=*/true, {},
        "old primary availability was durably lost before BeginGroupTerm; "
        "controlled failover degraded to recovery handoff");
  }
  const bool candidate_failure_lost_exact_holder =
      liveness.candidate_unavailable_ &&
      !SourceRecoveryHolderReady(source, intent);
  const MetaTerminalReceipt* current_receipt = CurrentReceipt(operation);
  const bool terminal_failure_lost_exact_holder =
      current_receipt != nullptr &&
      current_receipt->status_ != MetaDirectiveResultStatus::kSucceeded &&
      !SourceRecoveryHolderReady(source, intent);
  const bool deadline_lost_exact_holder =
      liveness.attempt_deadline_expired_ &&
      !SourceRecoveryHolderReady(source, intent);
  const auto committed_grant = view.grant().GroupState(intent.group_id_);
  const bool activation_checkpoint_ready =
      phase.stage_ == FailoverPhaseStage::kPromotionPrepared &&
      ActivatedAuthorityMatches(view, intent);
  const bool serving_checkpoint_ready =
      phase.stage_ == FailoverPhaseStage::kAuthorityActivated &&
      ActivatedAuthorityMatches(view, intent) && readiness.candidate_ready_ &&
      ConfirmedServingMatches(candidate, intent, runtime, committed_grant);
  if (cutover_committed &&
      (liveness.former_owner_unavailable_ ||
       candidate_failure_lost_exact_holder ||
       terminal_failure_lost_exact_holder || deadline_lost_exact_holder) &&
      !activation_checkpoint_ready && !serving_checkpoint_ready &&
      recovery->proof_state_ == MetaFailoverProofState::kExact) {
    return Emit(RecoveryCommand(
        view, intent, recovery->hold_required_, recovery->recovery_required_,
        MetaFailoverProofState::kUnavailable, recovery->frozen_proof_));
  }

  switch (phase.stage_) {
    case FailoverPhaseStage::kSourceHolding: {
      if (liveness.former_owner_unavailable_) {
        return Abort(operation, phase.stage_,
                     FailoverLossClassification::kUnknown,
                     /*recovery_required=*/true, {},
                     "old primary became unavailable before BeginGroupTerm; "
                     "controlled failover degraded to recovery handoff");
      }
      if (liveness.candidate_unavailable_) {
        if (!former_authority_available) return std::nullopt;
        return Abort(operation, phase.stage_,
                     FailoverLossClassification::kExact,
                     /*recovery_required=*/false, {},
                     "candidate became unavailable before BeginGroupTerm; old "
                     "primary remains authoritative");
      }
      const MetaTerminalReceipt* receipt = CurrentReceipt(operation);
      if (receipt != nullptr &&
          receipt->status_ != MetaDirectiveResultStatus::kSucceeded &&
          former_authority_available) {
        return Abort(operation, phase.stage_,
                     FailoverLossClassification::kExact,
                     /*recovery_required=*/false, {},
                     absl::StrCat("source hold authorization failed before "
                                  "cutover: ",
                                  receipt->result_));
      }
      if (liveness.attempt_deadline_expired_) {
        return AbortExpiredBeforeCut(operation, phase,
                                     former_authority_available);
      }
      if (!readiness.former_owner_ready_ || !readiness.candidate_ready_) {
        return std::nullopt;
      }
      if (!ActiveFormerAuthorityMatches(view, intent)) {
        return Invalid("source-holding lost the active former authority");
      }
      if (operation.current_directives_.empty()) {
        if (source == nullptr || candidate == nullptr ||
            !CurrentCandidate(view, observations, intent,
                              intent.group_term_ - 1, now_unix_ms)
                 .has_value()) {
          return std::nullopt;
        }
        const auto* group =
            RuntimeGroup(*source, intent, intent.former_owner_assignment_id_,
                         intent.group_term_ - 1);
        if (group == nullptr || !group->source_history_hold_.has_value() ||
            !HoldMatches(*group->source_history_hold_, intent)) {
          return std::nullopt;
        }
        auto directive = HoldingDirective(operation, intent);
        if (!directive.ok()) return directive.status();
        return Transition(operation, phase, {std::move(*directive)});
      }
      if (receipt == nullptr) return std::nullopt;
      const auto* group = source == nullptr
                              ? nullptr
                              : RuntimeGroup(*source, intent,
                                             intent.former_owner_assignment_id_,
                                             intent.group_term_ - 1);
      if (group == nullptr || !group->source_history_hold_.has_value() ||
          !HoldMatches(*group->source_history_hold_, intent)) {
        return std::nullopt;
      }
      FailoverPhase held;
      held.stage_ = FailoverPhaseStage::kSourceHeld;
      return Transition(operation, std::move(held));
    }

    case FailoverPhaseStage::kSourceHeld: {
      if (liveness.former_owner_unavailable_) {
        return Abort(operation, phase.stage_,
                     FailoverLossClassification::kUnknown, true, {},
                     "old primary became unavailable before BeginGroupTerm; "
                     "controlled failover degraded to recovery handoff");
      }
      if (liveness.candidate_unavailable_) {
        if (!former_authority_available) return std::nullopt;
        return Abort(operation, phase.stage_,
                     FailoverLossClassification::kExact, false, {},
                     "candidate became unavailable before BeginGroupTerm; old "
                     "primary remains authoritative");
      }
      if (liveness.attempt_deadline_expired_) {
        return AbortExpiredBeforeCut(operation, phase,
                                     former_authority_available);
      }
      if (!readiness.former_owner_ready_ || !readiness.candidate_ready_) {
        return std::nullopt;
      }
      if (!ActiveFormerAuthorityMatches(view, intent)) {
        return Invalid("source-held lost the active former authority");
      }
      FailoverPhase excluding;
      excluding.stage_ = FailoverPhaseStage::kOldAuthorityExcluding;
      return Transition(operation, std::move(excluding));
    }

    case FailoverPhaseStage::kOldAuthorityExcluding: {
      if (ActiveFormerAuthorityMatches(view, intent)) {
        if (liveness.former_owner_unavailable_) {
          return Abort(operation, phase.stage_,
                       FailoverLossClassification::kUnknown, true, {},
                       "old primary became unavailable before BeginGroupTerm; "
                       "controlled failover degraded to recovery handoff");
        }
        if (liveness.candidate_unavailable_) {
          if (!former_authority_available) return std::nullopt;
          return Abort(operation, phase.stage_,
                       FailoverLossClassification::kExact, false, {},
                       "candidate became unavailable before BeginGroupTerm; "
                       "old primary remains authoritative");
        }
        if (liveness.attempt_deadline_expired_) {
          return AbortExpiredBeforeCut(operation, phase,
                                       former_authority_available);
        }
        if (!readiness.former_owner_ready_ || !readiness.candidate_ready_) {
          return std::nullopt;
        }
        BeginGroupTerm begin;
        begin.group_id_ = intent.group_id_;
        begin.expected_term_ = intent.group_term_ - 1;
        begin.new_term_ = intent.group_term_;
        begin.workflow_operation_id_ = operation.operation_id_;
        begin.expected_operation_revision_ = operation.revision_;
        return Emit(std::move(begin));
      }
      if (!FencedAuthorityMatches(view, intent)) {
        return Invalid("old-authority-excluding has incompatible authority");
      }
      const MetaTerminalReceipt* receipt = CurrentReceipt(operation);
      if (receipt != nullptr &&
          receipt->status_ != MetaDirectiveResultStatus::kSucceeded) {
        return Abort(
            operation, phase.stage_, FailoverLossClassification::kUnknown,
            /*recovery_required=*/true, {},
            absl::StrCat("frozen-source terminal failure: ", receipt->result_));
      }
      if (recovery->proof_state_ == MetaFailoverProofState::kPending &&
          receipt != nullptr &&
          receipt->status_ == MetaDirectiveResultStatus::kSucceeded) {
        auto proof = FrozenProofFromReceipt(*receipt, intent);
        if (!proof.ok()) {
          return Abort(operation, phase.stage_,
                       FailoverLossClassification::kUnknown,
                       /*recovery_required=*/true, {},
                       absl::StrCat("invalid frozen-source evidence: ",
                                    proof.status().message()));
        }
        return Emit(RecoveryCommand(view, intent, /*hold_required=*/true,
                                    /*recovery_required=*/false,
                                    MetaFailoverProofState::kExact,
                                    std::move(*proof)));
      }
      if (liveness.candidate_unavailable_) {
        return Abort(operation, phase.stage_, RecoveryLoss(*recovery), true,
                     FailureFrontier(*recovery, phase),
                     "candidate became unavailable after BeginGroupTerm; group "
                     "remains fenced for an independent recovery operation");
      }
      auto available_candidate = CurrentCandidate(
          view, observations, intent, intent.group_term_, now_unix_ms);
      if (recovery->proof_state_ != MetaFailoverProofState::kPending) {
        if (liveness.attempt_deadline_expired_) {
          return AbortExpiredAfterCut(operation, phase, *recovery);
        }
        if (!readiness.candidate_ready_) return std::nullopt;
        FailoverPhase excluded;
        excluded.stage_ = FailoverPhaseStage::kOldAuthorityExcluded;
        if (recovery->proof_state_ == MetaFailoverProofState::kExact &&
            recovery->frozen_proof_.has_value()) {
          excluded.old_authority_exclusion_hash_ =
              recovery->frozen_proof_->proof_hash_;
          excluded.required_applied_next_lsns_ =
              recovery->frozen_proof_->final_next_lsns_;
        } else if (recovery->proof_state_ ==
                       MetaFailoverProofState::kUnavailable &&
                   available_candidate.has_value()) {
          const bool candidate_has_historical_frontier =
              recovery->frozen_proof_.has_value() &&
              std::equal(recovery->frozen_proof_->final_next_lsns_.begin(),
                         recovery->frozen_proof_->final_next_lsns_.end(),
                         available_candidate->applied_next_lsns_.begin(),
                         available_candidate->applied_next_lsns_.end(),
                         std::less_equal<>());
          if (candidate_has_historical_frontier) {
            excluded.old_authority_exclusion_hash_ =
                recovery->frozen_proof_->proof_hash_;
            excluded.required_applied_next_lsns_ =
                recovery->frozen_proof_->final_next_lsns_;
          } else {
            excluded.required_applied_next_lsns_ =
                available_candidate->applied_next_lsns_;
            auto hash = ComputeFailoverUnavailableProofHash(
                intent, excluded.required_applied_next_lsns_);
            if (!hash.ok()) return hash.status();
            excluded.old_authority_exclusion_hash_ = *hash;
          }
        } else {
          return std::nullopt;
        }
        // The frozen authorization is the candidate's capability to consume
        // the retained source history. Keeping the exact spec also keeps its
        // original directive revision, so FDS replacement replays rather than
        // minting a new authorization while the candidate catches up.
        return Transition(operation, std::move(excluded),
                          RetainedDirectiveSpecs(operation));
      }
      if (liveness.attempt_deadline_expired_) {
        return AbortExpiredAfterCut(operation, phase, *recovery);
      }
      if (!readiness.candidate_ready_) return std::nullopt;
      if (operation.current_directives_.empty()) {
        if (liveness.former_owner_unavailable_) {
          if (!available_candidate.has_value()) return std::nullopt;
          return Emit(RecoveryCommand(view, intent, /*hold_required=*/true,
                                      /*recovery_required=*/false,
                                      MetaFailoverProofState::kUnavailable));
        }
        if (!readiness.former_owner_ready_) {
          // A leader change or short control reconnect clears volatile runtime
          // and current-FDS evidence even though the exact old boot may still
          // return. Do not discard the exact-proof opportunity until the
          // leader owner has observed the full revalidation grace (or a
          // replacement boot/assignment).
          return std::nullopt;
        }
        const auto* group =
            RuntimeGroup(*source, intent, intent.former_owner_assignment_id_,
                         intent.group_term_);
        if (group == nullptr || !group->source_history_hold_.has_value() ||
            !HoldMatches(*group->source_history_hold_, intent)) {
          return std::nullopt;
        }
        auto directive = FrozenDirective(operation, intent);
        if (!directive.ok()) return directive.status();
        return Transition(operation, phase, {std::move(*directive)});
      }

      if (receipt == nullptr && !liveness.former_owner_unavailable_) {
        return std::nullopt;
      }
      if (!available_candidate.has_value()) return std::nullopt;
      return Emit(RecoveryCommand(view, intent, /*hold_required=*/true,
                                  /*recovery_required=*/false,
                                  MetaFailoverProofState::kUnavailable));
    }

    case FailoverPhaseStage::kOldAuthorityExcluded: {
      if (liveness.candidate_unavailable_) {
        return Abort(operation, phase.stage_, RecoveryLoss(*recovery), true,
                     FailureFrontier(*recovery, phase),
                     "candidate became unavailable after authority exclusion; "
                     "group remains fenced for independent recovery");
      }
      auto progress = CurrentCandidate(view, observations, intent,
                                       intent.group_term_, now_unix_ms);
      if (readiness.candidate_ready_ && progress.has_value() &&
          recovery->proof_state_ == MetaFailoverProofState::kUnavailable &&
          PhaseUsesHistoricalExactFrontier(phase, *recovery)) {
        bool behind = false;
        for (std::size_t flow = 0;
             flow < phase.required_applied_next_lsns_.size(); ++flow) {
          behind = behind || progress->applied_next_lsns_[flow] <
                                 phase.required_applied_next_lsns_[flow];
        }
        if (behind) {
          FailoverPhase downgraded = phase;
          downgraded.required_applied_next_lsns_ = progress->applied_next_lsns_;
          auto hash = ComputeFailoverUnavailableProofHash(
              intent, downgraded.required_applied_next_lsns_);
          if (!hash.ok()) return hash.status();
          downgraded.old_authority_exclusion_hash_ = *hash;
          return Transition(operation, std::move(downgraded),
                            RetainedDirectiveSpecs(operation));
        }
      }
      if (liveness.attempt_deadline_expired_) {
        return AbortExpiredAfterCut(operation, phase, *recovery);
      }
      if (!readiness.candidate_ready_ || !progress.has_value()) {
        return std::nullopt;
      }
      for (std::size_t flow = 0;
           flow < phase.required_applied_next_lsns_.size(); ++flow) {
        if (progress->applied_next_lsns_[flow] <
            phase.required_applied_next_lsns_[flow]) {
          return std::nullopt;
        }
      }
      FailoverPhase caught_up = phase;
      caught_up.stage_ = FailoverPhaseStage::kCandidateCaughtUp;
      return Transition(operation, std::move(caught_up),
                        RetainedDirectiveSpecs(operation));
    }

    case FailoverPhaseStage::kCandidateCaughtUp: {
      if (liveness.candidate_unavailable_) {
        return Abort(operation, phase.stage_, RecoveryLoss(*recovery), true,
                     FailureFrontier(*recovery, phase),
                     "candidate became unavailable before promotion prepare; "
                     "group remains fenced for independent recovery");
      }
      if (liveness.attempt_deadline_expired_) {
        return AbortExpiredAfterCut(operation, phase, *recovery);
      }
      if (!readiness.candidate_ready_) return std::nullopt;
      auto progress = CurrentCandidate(view, observations, intent,
                                       intent.group_term_, now_unix_ms);
      if (!progress.has_value()) return std::nullopt;
      for (std::size_t flow = 0;
           flow < phase.required_applied_next_lsns_.size(); ++flow) {
        if (progress->applied_next_lsns_[flow] <
            phase.required_applied_next_lsns_[flow]) {
          return std::nullopt;
        }
      }
      auto directive = PromotionDirective(operation, intent, phase);
      if (!directive.ok()) return directive.status();
      FailoverPhase preparing = phase;
      preparing.stage_ = FailoverPhaseStage::kPromotionPreparing;
      return Transition(operation, std::move(preparing),
                        {std::move(*directive)});
    }

    case FailoverPhaseStage::kPromotionPreparing: {
      const MetaTerminalReceipt* receipt = CurrentReceipt(operation);
      if (receipt != nullptr &&
          receipt->status_ != MetaDirectiveResultStatus::kSucceeded) {
        return Abort(
            operation, phase.stage_, RecoveryLoss(*recovery), true,
            FailureFrontier(*recovery, phase),
            absl::StrCat("promotion prepare failed: ", receipt->result_));
      }
      if (liveness.candidate_unavailable_) {
        return Abort(operation, phase.stage_, RecoveryLoss(*recovery), true,
                     FailureFrontier(*recovery, phase),
                     "candidate became unavailable during promotion prepare; "
                     "group remains fenced for independent recovery");
      }
      std::optional<MetaEvidenceSummary> prepared_evidence;
      if (receipt != nullptr) {
        auto resolved = ResolveFailoverPreparedEvidence(view, operation, intent,
                                                        phase, observations,
                                                        *receipt, now_unix_ms);
        if (!resolved.ok()) {
          return Abort(operation, phase.stage_, RecoveryLoss(*recovery), true,
                       FailureFrontier(*recovery, phase),
                       std::string(resolved.status().message()));
        }
        prepared_evidence = std::move(*resolved);
      }
      if (liveness.attempt_deadline_expired_) {
        return AbortExpiredAfterCut(operation, phase, *recovery);
      }
      if (!readiness.candidate_ready_) return std::nullopt;
      if (receipt == nullptr) return std::nullopt;
      if (!prepared_evidence.has_value()) return std::nullopt;
      FailoverPhase prepared = phase;
      prepared.stage_ = FailoverPhaseStage::kPromotionPrepared;
      prepared.prepared_result_hash_ = receipt->result_hash_;
      return Transition(operation, std::move(prepared), {},
                        {std::move(*prepared_evidence)});
    }

    case FailoverPhaseStage::kPromotionPrepared: {
      // The authority commit is the durable cut. It must win attribution over
      // a concurrent disconnect: once committed, recovery may not silently
      // replace the candidate in this term and the terminal result must say a
      // new term is required.
      if (ActivatedAuthorityMatches(view, intent)) {
        FailoverPhase activated = phase;
        activated.stage_ = FailoverPhaseStage::kAuthorityActivated;
        return Transition(operation, std::move(activated));
      }
      if (liveness.candidate_unavailable_) {
        return Abort(operation, phase.stage_, RecoveryLoss(*recovery), true,
                     FailureFrontier(*recovery, phase),
                     "candidate became unavailable after promotion prepare; "
                     "group remains fenced for independent recovery");
      }
      if (!FencedAuthorityMatches(view, intent)) {
        return Invalid("promotion-prepared authority anchors changed");
      }
      if (liveness.attempt_deadline_expired_) {
        return AbortExpiredAfterCut(operation, phase, *recovery);
      }
      if (!readiness.candidate_ready_) {
        // Prepared evidence is durable, but local activation is boot- and
        // FDS-scoped. A newly elected Meta leader must wait for the exact
        // candidate session to acknowledge the fenced successor projection;
        // committing authority while that revalidation is absent would turn a
        // harmless reconnect into a mandatory extra-term recovery.
        return std::nullopt;
      }
      const auto group = view.topology().FindGroup(intent.group_id_);
      if (!group.has_value() ||
          view.topology().TopologyEpoch() ==
              std::numeric_limits<std::uint64_t>::max() ||
          group->config_epoch_ == std::numeric_limits<std::uint64_t>::max()) {
        return Invalid("authority activation epoch is exhausted");
      }
      ActivateAuthority activate;
      activate.group_id_ = intent.group_id_;
      activate.expected_term_ = intent.group_term_;
      activate.new_owner_ = intent.candidate_node_id_;
      activate.grant_ = intent.old_grant_;
      activate.new_authority_version_ = intent.authority_version_ + 1;
      activate.new_topology_epoch_ = view.topology().TopologyEpoch() + 1;
      activate.new_config_epoch_ = group->config_epoch_ + 1;
      activate.workflow_operation_id_ = operation.operation_id_;
      activate.expected_operation_revision_ = operation.revision_;
      return Emit(std::move(activate));
    }

    case FailoverPhaseStage::kAuthorityActivated: {
      if (!ActivatedAuthorityMatches(view, intent)) {
        return Invalid(
            "authority-activated lost the committed candidate grant");
      }
      const auto grant = view.grant().GroupState(intent.group_id_);
      if (!grant.has_value() || !grant->grant_.has_value()) {
        return Invalid("activated candidate grant disappeared");
      }
      if (readiness.candidate_ready_ &&
          ConfirmedServingMatches(candidate, intent, runtime, grant)) {
        FailoverPhase serving = phase;
        serving.stage_ = FailoverPhaseStage::kServing;
        return Transition(operation, std::move(serving));
      }
      if (liveness.candidate_unavailable_) {
        return Abort(operation, phase.stage_, RecoveryLoss(*recovery), true,
                     FailureFrontier(*recovery, phase),
                     "activated candidate became unavailable before serving; "
                     "a new recovery term is required");
      }
      if (liveness.attempt_deadline_expired_) {
        return AbortExpiredAfterCut(operation, phase, *recovery);
      }
      return std::nullopt;
    }

    case FailoverPhaseStage::kServing:
      if (!ActivatedAuthorityMatches(view, intent)) {
        return Invalid("serving failover lost activated authority");
      }
      return Complete(operation, phase, *recovery);
  }
  return Invalid("unknown controlled failover phase");
}

struct MetaControlledFailoverReconciler::Core {
  celer::ForeignExecutor executor_;
  std::shared_ptr<MetaMembershipGate> membership_gate_;
  std::shared_ptr<MetaObservationStore> observations_;
  std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status_;
  std::uint32_t revalidation_grace_ms_ = 0;
  // Worker-owned except the atomic ingress/stop-completion flags below.
  bool running_ = false;
  bool cancelled_ = true;
  bool shutdown_ = false;
  std::vector<std::shared_ptr<std::promise<void>>> waiters_;
  std::atomic<bool> shutdown_complete_{false};
  std::atomic<bool> stopping_{false};
};

MetaControlledFailoverReconciler::MetaControlledFailoverReconciler(
    celer::ForeignExecutor executor,
    std::shared_ptr<MetaMembershipGate> membership_gate,
    std::shared_ptr<MetaObservationStore> observations,
    std::shared_ptr<MetaDataControlRuntimeStatus> runtime_status,
    std::uint32_t revalidation_grace_ms)
    : core_(std::make_shared<Core>()) {
  core_->executor_ = std::move(executor);
  core_->membership_gate_ = std::move(membership_gate);
  core_->observations_ = std::move(observations);
  core_->runtime_status_ = std::move(runtime_status);
  core_->revalidation_grace_ms_ = revalidation_grace_ms;
}

MetaControlledFailoverReconciler::~MetaControlledFailoverReconciler() {
  Shutdown();
}

void MetaControlledFailoverReconciler::Start(MetaLeaderContext& context) {
  const auto core = core_;
  if (!core->executor_.Notify([core, context = &context]() noexcept {
        if (core->shutdown_) return;
        if (core->running_) std::terminate();
        core->cancelled_ = false;
        core->running_ = true;
        celer::ThisWorker().self_->Spawn(Run(core, context));
      })) {
    std::terminate();
  }
}

void MetaControlledFailoverReconciler::Stop(bool permanent) {
  const auto core = core_;
  if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
  if (permanent) core->stopping_.store(true, std::memory_order_release);
  auto complete = std::make_shared<std::promise<void>>();
  auto done = complete->get_future();
  if (!core->executor_.Notify([core, complete, permanent]() noexcept {
        core->shutdown_ |= permanent;
        core->cancelled_ = true;
        if (core->running_) {
          core->waiters_.push_back(complete);
        } else {
          complete->set_value();
        }
      })) {
    if (core->shutdown_complete_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
  if (permanent) {
    core->shutdown_complete_.store(true, std::memory_order_release);
  }
}

void MetaControlledFailoverReconciler::CancelAndWait() { Stop(false); }
void MetaControlledFailoverReconciler::Shutdown() { Stop(true); }
bool MetaControlledFailoverReconciler::accepting() const {
  return !core_->stopping_.load(std::memory_order_acquire);
}

celer::Task<absl::Status> MetaControlledFailoverReconciler::Run(
    std::shared_ptr<Core> core, MetaLeaderContext* context) {
  using Clock = std::chrono::steady_clock;
  struct VolatileOperationState {
    std::optional<Clock::time_point> source_since_;
    std::optional<Clock::time_point> candidate_since_;
    std::optional<Clock::time_point> attempt_deadline_;
  };
  struct VolatileRecoveryState {
    std::uint64_t recovery_generation_ = 0;
    std::optional<Clock::time_point> source_since_;
  };
  std::map<MetaOperationId, VolatileOperationState> volatile_state;
  std::map<std::string, VolatileRecoveryState> volatile_recovery_state;
  std::map<MetaOperationId, std::string> last_cut;
  std::unique_ptr<MetaMembershipGate::Lease> membership_lease;
  auto changed = std::make_shared<std::atomic<bool>>(false);
  auto subscribe = [&] {
    return context->SubscribeCommitted([changed](const MetaCommitEvent&) {
      changed->store(true, std::memory_order_release);
    });
  };
  auto subscribed = subscribe();
  while (!core->cancelled_) {
    if (subscribed.subscription_->needs_resync()) subscribed = subscribe();
    if (changed->exchange(false, std::memory_order_acq_rel)) {
      subscribed.view_ = context->CommittedView();
    }
    const MetaCommittedView& view = subscribed.view_;
    const MetaDataControlRuntimeSnapshot runtime =
        core->runtime_status_->Snapshot();
    std::vector<MetaOperationRecord> operations;
    std::set<MetaOperationId> present_operation_ids;
    for (const auto& operation : view.operation().LiveOperations()) {
      if (operation.kind_ == kFailoverOperationKind) {
        operations.push_back(operation);
        present_operation_ids.insert(operation.operation_id_);
        if (!IsTerminal(operation.lifecycle_)) {
          auto intent = DecodeFailoverIntent(operation.intent_);
          if (intent.ok()) {
            auto& state = volatile_state[operation.operation_id_];
            if (!state.attempt_deadline_.has_value()) {
              state.attempt_deadline_ =
                  Clock::now() +
                  std::chrono::milliseconds(intent->attempt_timeout_ms_);
            }
          }
        }
      }
    }
    // Operation journal retention is independent of a leader tenure. Drop
    // volatile diagnostics and grace timers as soon as an operation is
    // archived so repeated maintenance cannot grow this owner without bound.
    std::erase_if(volatile_state, [&](const auto& item) {
      return !present_operation_ids.contains(item.first);
    });
    std::erase_if(last_cut, [&](const auto& item) {
      return !present_operation_ids.contains(item.first);
    });
    const bool has_non_terminal =
        std::any_of(operations.begin(), operations.end(),
                    [](const auto& op) { return !IsTerminal(op.lifecycle_); });
    if (operations.empty()) {
      membership_lease.reset();
      volatile_state.clear();
    } else {
      // Terminal cleanup only releases/retains the recovery handoff and must
      // not keep the topology gate forever. In particular, an aborted
      // post-cutover attempt deliberately hands the group to an independent
      // uncontrolled operation, which must be able to acquire this gate.
      if (!has_non_terminal) {
        membership_lease.reset();
      } else if (membership_lease == nullptr) {
        membership_lease = core->membership_gate_->TryAcquire();
      }
      if (!has_non_terminal || membership_lease != nullptr) {
        bool proposed = false;
        for (const MetaOperationRecord& operation : operations) {
          if (core->cancelled_) break;
          auto intent = DecodeFailoverIntent(operation.intent_);
          if (!intent.ok()) {
            spdlog::critical(
                "controlled-failover operation={} has invalid durable intent",
                Hex(operation.operation_id_));
            continue;
          }
          detail::ControlledFailoverLiveness liveness;
          std::optional<FailoverPhase> current_phase;
          if (!operation.kind_phase_blob_.empty()) {
            auto decoded = DecodeFailoverPhase(operation.kind_phase_blob_);
            if (decoded.ok()) current_phase = *decoded;
          }
          const std::int64_t now_unix_ms =
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch())
                  .count();
          if (!IsTerminal(operation.lifecycle_)) {
            VolatileOperationState& state =
                volatile_state[operation.operation_id_];
            const auto now = Clock::now();
            if (!state.attempt_deadline_.has_value()) {
              state.attempt_deadline_ =
                  now + std::chrono::milliseconds(intent->attempt_timeout_ms_);
            }
            liveness.attempt_deadline_expired_ =
                now >= *state.attempt_deadline_;
            const detail::ControlledFailoverReadiness readiness =
                detail::EvaluateControlledFailoverReadiness(
                    view, *core->observations_, runtime, operation, *intent,
                    current_phase, now_unix_ms);
            auto observe = [&](bool present,
                               std::optional<Clock::time_point>* since) {
              if (present || !runtime.leader_authority_eligible_) {
                since->reset();
                return false;
              }
              if (!since->has_value()) *since = now;
              return now - **since >=
                     std::chrono::milliseconds(core->revalidation_grace_ms_);
            };
            liveness.former_owner_unavailable_ =
                readiness.former_owner_replaced_ ||
                observe(readiness.former_owner_ready_, &state.source_since_);
            liveness.candidate_unavailable_ =
                readiness.candidate_replaced_ ||
                observe(readiness.candidate_ready_, &state.candidate_since_);
          } else {
            volatile_state.erase(operation.operation_id_);
          }

          auto phase =
              operation.kind_phase_blob_.empty()
                  ? std::string("planning")
                  : [&]() {
                      auto decoded =
                          DecodeFailoverPhase(operation.kind_phase_blob_);
                      return decoded.ok() ? std::string(FailoverPhaseStageName(
                                                decoded->stage_))
                                          : std::string("invalid");
                    }();
          if (IsTerminal(operation.lifecycle_)) phase = "terminal-cleanup";
          if (last_cut[operation.operation_id_] != phase) {
            if (IsTerminal(operation.lifecycle_)) {
              const auto outcome =
                  DecodeControlledFailoverOutcome(operation.terminal_result_);
              if (outcome.ok()) {
                spdlog::info(
                    "controlled-failover operation={} group={} terminal={} "
                    "phase={} candidate={} old_source={} loss={} "
                    "recovery_required={} frontier={} reason={}",
                    Hex(operation.operation_id_), intent->group_id_,
                    operation.lifecycle_ == MetaOperationLifecycle::kCompleted
                        ? "completed"
                        : "aborted",
                    FailoverPhaseStageName(outcome->terminal_stage_),
                    intent->candidate_node_id_, intent->former_owner_node_id_,
                    FailoverLossClassificationName(outcome->loss_),
                    outcome->recovery_required_,
                    FrontierText(outcome->proven_next_lsns_), outcome->reason_);
              } else {
                spdlog::critical(
                    "controlled-failover operation={} group={} has invalid "
                    "terminal outcome: {}",
                    Hex(operation.operation_id_), intent->group_id_,
                    outcome.status().message());
              }
            } else {
              spdlog::info(
                  "controlled-failover operation={} group={} phase={} "
                  "candidate={} old_source={}",
                  Hex(operation.operation_id_), intent->group_id_, phase,
                  intent->candidate_node_id_, intent->former_owner_node_id_);
            }
            last_cut[operation.operation_id_] = phase;
          }

          auto planned = detail::PlanControlledFailoverStep(
              view, operation, *core->observations_, runtime, liveness,
              now_unix_ms);
          if (!planned.ok()) {
            spdlog::error(
                "controlled-failover operation={} group={} phase={} "
                "planner_conflict={}",
                Hex(operation.operation_id_), intent->group_id_, phase,
                planned.status().message());
            continue;
          }
          if (!planned->has_value()) continue;
          MetaCommand command = std::move(**planned);
          auto request = control::GenerateId128();
          if (!request.ok()) std::terminate();
          std::visit([&](auto& value) { value.request_id_ = *request; },
                     command);
          const auto applied = co_await context->Propose(std::move(command));
          if (core->cancelled_) break;
          changed->store(true, std::memory_order_release);
          if (!applied.ok() ||
              applied->verdict_ != MetaAuditVerdict::kAccepted) {
            spdlog::warn(
                "controlled-failover operation={} group={} phase={} "
                "proposal_deferred={}",
                Hex(operation.operation_id_), intent->group_id_, phase,
                applied.ok() ? applied->detail_
                             : std::string(applied.status().message()));
          }
          proposed = true;
          break;
        }
        if (proposed) continue;
      }
    }

    // Recovery-required records outlive their originating operation. Keep
    // checking the exact held source incarnation even after the operation is
    // archived; otherwise an old boot could disappear while the durable
    // handoff continued to advertise an available exact frontier forever.
    std::set<std::string> observed_recovery_groups;
    bool recovery_proposed = false;
    for (const MetaFailoverRecoveryRecord& recovery :
         view.failover_recovery().Records()) {
      if (!recovery.hold_required_ || !recovery.recovery_required_ ||
          recovery.proof_state_ == MetaFailoverProofState::kUnavailable) {
        continue;
      }
      observed_recovery_groups.insert(recovery.group_id_);
      VolatileRecoveryState& state =
          volatile_recovery_state[recovery.group_id_];
      if (state.recovery_generation_ != recovery.recovery_generation_) {
        state = VolatileRecoveryState{
            .recovery_generation_ = recovery.recovery_generation_,
            .source_since_ = std::nullopt};
      }

      const auto now = Clock::now();
      bool grace_expired = false;
      const MetaDataControlRuntimeNode* source =
          RuntimeNode(runtime, recovery.old_source_node_id_,
                      recovery.old_source_boot_incarnation_);
      const bool source_ready = SourceRecoveryHolderReady(source, recovery);
      if (source_ready || !runtime.leader_authority_eligible_) {
        state.source_since_.reset();
      } else {
        if (!state.source_since_.has_value()) state.source_since_ = now;
        grace_expired = now - *state.source_since_ >=
                        std::chrono::milliseconds(core->revalidation_grace_ms_);
      }

      auto command = detail::PlanFailoverRecoveryAvailabilityStep(
          recovery, runtime, grace_expired);
      if (!command.has_value()) continue;
      auto request = control::GenerateId128();
      if (!request.ok()) std::terminate();
      command->request_id_ = *request;
      const auto applied =
          co_await context->Propose(MetaCommand(std::move(*command)));
      if (core->cancelled_) break;
      changed->store(true, std::memory_order_release);
      if (!applied.ok() || applied->verdict_ != MetaAuditVerdict::kAccepted) {
        spdlog::warn(
            "controlled-failover recovery group={} generation={} "
            "old_source={} availability_downgrade_deferred={}",
            recovery.group_id_, recovery.recovery_generation_,
            recovery.old_source_node_id_,
            applied.ok() ? applied->detail_
                         : std::string(applied.status().message()));
      } else {
        spdlog::warn(
            "controlled-failover recovery group={} generation={} "
            "old_source={} proof_state=unavailable "
            "historical_frontier_retained={}",
            recovery.group_id_, recovery.recovery_generation_,
            recovery.old_source_node_id_, recovery.frozen_proof_.has_value());
      }
      recovery_proposed = true;
      break;
    }
    std::erase_if(volatile_recovery_state, [&](const auto& item) {
      return !observed_recovery_groups.contains(item.first);
    });
    if (recovery_proposed) continue;

    const absl::Status slept = co_await celer::SleepFor(
        *celer::ThisWorker().self_, std::chrono::milliseconds(25));
    if (!slept.ok()) break;
  }
  membership_lease.reset();
  core->running_ = false;
  for (const auto& waiter : core->waiters_) waiter->set_value();
  core->waiters_.clear();
  co_return absl::OkStatus();
}

}  // namespace keylane::meta
