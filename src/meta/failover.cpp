#include "keylane/meta/failover.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/coordinator.h"
#include "keylane/meta/encoding.h"
#include "keylane/meta/hash.h"

namespace keylane::meta {
namespace {

constexpr std::string_view kIntentMagic = "KLFI";
constexpr std::string_view kPhaseMagic = "KLFP";
constexpr std::uint16_t kFailoverSchemaVersion = 1;
constexpr std::uint32_t kMaxFailoverFlowCount = 1024;

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
  if (intent.group_id_.empty() ||
      intent.group_id_.size() > kMaxMetaGroupIdBytes ||
      !cluster::control::IsCanonicalIdentity160(
          intent.former_owner_node_id_) ||
      !cluster::control::IsCanonicalIdentity160(intent.candidate_node_id_) ||
      intent.former_owner_node_id_ == intent.candidate_node_id_ ||
      IsZero(intent.former_owner_assignment_id_) ||
      IsZero(intent.former_owner_boot_id_) ||
      IsZero(intent.candidate_assignment_id_) ||
      IsZero(intent.candidate_boot_id_) || intent.group_term_ == 0 ||
      intent.authority_version_ == 0 || intent.grant_revision_ == 0 ||
      intent.population_manifest_revision_ == 0 ||
      IsZero(intent.population_manifest_digest_) ||
      intent.partition_replication_epoch_ == 0 ||
      IsZero(intent.parent_history_id_)) {
    return Invalid("failover intent identity is incomplete");
  }
  return absl::OkStatus();
}

absl::Status ValidatePhase(const FailoverPhase& phase) {
  const auto stage = static_cast<std::uint8_t>(phase.stage_);
  if (stage < 1 || stage > 6 ||
      IsZero(phase.old_authority_exclusion_hash_)) {
    return Invalid("failover phase identity is incomplete");
  }
  const bool has_frontier = !phase.required_applied_next_lsns_.empty();
  if (phase.required_applied_next_lsns_.size() > kMaxFailoverFlowCount ||
      std::any_of(phase.required_applied_next_lsns_.begin(),
                  phase.required_applied_next_lsns_.end(),
                  [](std::uint64_t cursor) { return cursor == 0; })) {
    return Invalid("failover phase frontier is invalid");
  }
  if (stage == 1 && (has_frontier || !IsZero(phase.prepared_result_hash_))) {
    return Invalid("old-authority-excluded phase carries future evidence");
  }
  if (stage >= 2 && !has_frontier) {
    return Invalid("failover phase is missing the catch-up frontier");
  }
  if ((stage < 4) != IsZero(phase.prepared_result_hash_)) {
    return Invalid("failover phase has an inconsistent prepared result hash");
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

absl::Status ValidateCommittedAnchors(const FailoverIntent& intent,
                                      const MetaCommittedView& view) {
  const auto group = view.topology().FindGroup(intent.group_id_);
  const auto grant = view.grant().GroupState(intent.group_id_);
  if (!group.has_value() || !grant.has_value() || !grant->fenced_ ||
      grant->grant_.has_value() ||
      grant->group_term_ != intent.group_term_ ||
      grant->last_authority_version_ != intent.authority_version_ ||
      grant->last_grant_revision_ != intent.grant_revision_ ||
      group->record_.owner_ != intent.former_owner_node_id_ ||
      group->record_.group_term_ != intent.group_term_ ||
      group->record_.authority_version_ != intent.authority_version_ ||
      group->record_.population_manifest_revision_ !=
          intent.population_manifest_revision_ ||
      group->record_.population_manifest_digest_ !=
          intent.population_manifest_digest_ ||
      group->record_.partition_replication_epoch_ !=
          intent.partition_replication_epoch_) {
    return Invalid(
        "failover anchors do not match the committed fenced group state");
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
      !member_matches(intent.candidate_node_id_,
                      intent.candidate_assignment_id_)) {
    return Invalid("failover member assignment is stale");
  }
  return absl::OkStatus();
}

absl::Status ValidatePreparingDirective(
    const MetaDirectiveSpec& directive, const FailoverIntent& intent,
    const FailoverPhase& phase) {
  if (directive.kind_ != "promotion-prepare" ||
      directive.recipient_node_id_ != intent.candidate_node_id_ ||
      directive.target_node_id_ != intent.candidate_node_id_ ||
      directive.target_boot_id_ != intent.candidate_boot_id_ ||
      directive.assignment_id_ != intent.candidate_assignment_id_ ||
      directive.source_node_id_ != intent.former_owner_node_id_ ||
      directive.source_assignment_id_ !=
          intent.former_owner_assignment_id_ ||
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
  auto request = cluster::control::DecodePromotionPrepareRequest(
      directive.payload_);
  auto preconditions =
      cluster::control::DecodePromotionPreparePreconditions(
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

const MetaTerminalReceipt* FindSuccessfulReceipt(
    const MetaOperationRecord& operation,
    const MetaCurrentDirective& directive) {
  const auto found = std::find_if(
      operation.terminal_receipts_.begin(), operation.terminal_receipts_.end(),
      [&](const MetaTerminalReceipt& receipt) {
        return receipt.key_.operation_id_ == operation.operation_id_ &&
               receipt.key_.directive_id_ == directive.spec_.directive_id_ &&
               receipt.key_.attempt_id_ == directive.spec_.attempt_id_ &&
               receipt.key_.directive_revision_ ==
                   directive.directive_revision_ &&
               receipt.status_ == MetaDirectiveResultStatus::kSucceeded;
      });
  return found == operation.terminal_receipts_.end() ? nullptr : &*found;
}

absl::Status ValidatePreparedTransition(
    const TransitionOperationPhase& transition,
    const MetaOperationRecord& operation, const FailoverIntent& intent,
    const FailoverPhase& phase) {
  if (!transition.current_directives_.empty() ||
      transition.evidence_.size() != 1 ||
      operation.current_directives_.size() != 1) {
    return Invalid(
        "promotion-prepared requires one prior directive and one evidence");
  }
  const MetaCurrentDirective& current = operation.current_directives_.front();
  const MetaTerminalReceipt* receipt =
      FindSuccessfulReceipt(operation, current);
  if (receipt == nullptr ||
      receipt->recipient_node_id_ != intent.candidate_node_id_ ||
      receipt->recipient_boot_id_ != intent.candidate_boot_id_ ||
      receipt->assignment_id_ != intent.candidate_assignment_id_ ||
      receipt->result_hash_ != phase.prepared_result_hash_) {
    return Invalid(
        "promotion-prepared is missing its exact successful receipt");
  }
  auto prepared =
      cluster::control::DecodePromotionPreparedEvidence(receipt->result_);
  if (!prepared.ok() ||
      prepared->parent_history_id != Hex(intent.parent_history_id_) ||
      prepared->frozen_applied_next_lsns.size() !=
          phase.required_applied_next_lsns_.size()) {
    return Invalid("promotion prepared result does not match its parent");
  }
  for (std::size_t flow = 0;
       flow < prepared->frozen_applied_next_lsns.size(); ++flow) {
    if (prepared->frozen_applied_next_lsns[flow] <
        phase.required_applied_next_lsns_[flow]) {
      return Invalid("promotion prepared frontier is behind its requirement");
    }
  }
  const MetaEvidenceSummary& evidence = transition.evidence_.front();
  if (evidence.node_id_ != intent.candidate_node_id_ ||
      evidence.boot_incarnation_ != intent.candidate_boot_id_ ||
      evidence.assignment_id_ != intent.candidate_assignment_id_ ||
      evidence.operation_id_ != operation.operation_id_ ||
      evidence.group_id_ != intent.group_id_ ||
      evidence.group_term_ != intent.group_term_ ||
      evidence.population_manifest_revision_ !=
          intent.population_manifest_revision_ ||
      evidence.population_manifest_digest_ !=
          intent.population_manifest_digest_ ||
      evidence.partition_replication_epoch_ !=
          intent.partition_replication_epoch_ ||
      evidence.replication_history_id_ != intent.parent_history_id_ ||
      evidence.kind_hash_ != receipt->result_hash_) {
    return Invalid("promotion-prepared evidence summary changed its anchors");
  }
  return absl::OkStatus();
}

}  // namespace

absl::StatusOr<std::string> EncodeFailoverIntent(const FailoverIntent& intent) {
  if (absl::Status status = ValidateIntent(intent); !status.ok()) return status;
  MetaWriter writer;
  WriteHeader(writer, kIntentMagic);
  writer.WriteString(intent.group_id_);
  writer.WriteString(intent.former_owner_node_id_);
  WriteFixedArray(writer, intent.former_owner_assignment_id_);
  WriteFixedArray(writer, intent.former_owner_boot_id_);
  writer.WriteString(intent.candidate_node_id_);
  WriteFixedArray(writer, intent.candidate_assignment_id_);
  WriteFixedArray(writer, intent.candidate_boot_id_);
  writer.WriteU64(intent.group_term_);
  writer.WriteU64(intent.authority_version_);
  writer.WriteU64(intent.grant_revision_);
  writer.WriteU64(intent.population_manifest_revision_);
  WriteFixedArray(writer, intent.population_manifest_digest_);
  writer.WriteU64(intent.partition_replication_epoch_);
  WriteFixedArray(writer, intent.parent_history_id_);
  return std::move(writer).TakeBuffer();
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
      kMaxFailoverFlowCount,
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

absl::Status ValidateFailoverProposal(
    const MetaCommand& command, const MetaCommittedView& view,
    const MetaObservationStore& observations) {
  (void)observations;
  if (const auto* submit = std::get_if<SubmitOperation>(&command)) {
    if (submit->kind_ != kFailoverOperationKind) return absl::OkStatus();
    auto intent = DecodeFailoverIntent(submit->intent_);
    if (!intent.ok() || submit->intent_hash_ != MetaSha256(submit->intent_) ||
        submit->replication_history_id_ != intent->parent_history_id_) {
      return Invalid("failover submit contains invalid typed intent anchors");
    }
    return absl::OkStatus();
  }

  const auto* transition =
      std::get_if<TransitionOperationPhase>(&command);
  if (transition == nullptr) return absl::OkStatus();
  const auto operation =
      view.operation().FindOperation(transition->operation_id_);
  if (!operation.has_value() || operation->kind_ != kFailoverOperationKind) {
    return absl::OkStatus();
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
  if (static_cast<std::uint8_t>(next->stage_) >
      static_cast<std::uint8_t>(FailoverPhaseStage::kPromotionPrepared)) {
    return Invalid("failover phase is reserved for issue 41");
  }

  std::optional<FailoverPhase> previous;
  if (!operation->kind_phase_blob_.empty()) {
    auto decoded = DecodeFailoverPhase(operation->kind_phase_blob_);
    if (!decoded.ok()) {
      return Invalid("committed failover phase is invalid");
    }
    previous = std::move(*decoded);
  }
  const auto next_stage = static_cast<std::uint8_t>(next->stage_);
  const auto expected_stage = previous.has_value()
                                  ? static_cast<std::uint8_t>(previous->stage_) + 1
                                  : static_cast<std::uint8_t>(
                                        FailoverPhaseStage::kOldAuthorityExcluded);
  if (next_stage != expected_stage) {
    return Invalid("failover transition skipped or repeated a phase");
  }
  if (previous.has_value() &&
      (previous->old_authority_exclusion_hash_ !=
           next->old_authority_exclusion_hash_ ||
       (static_cast<std::uint8_t>(previous->stage_) >=
            static_cast<std::uint8_t>(
                FailoverPhaseStage::kCandidateCaughtUp) &&
        previous->required_applied_next_lsns_ !=
            next->required_applied_next_lsns_))) {
    return Invalid("failover transition changed an earlier phase proof");
  }
  if (absl::Status anchors = ValidateCommittedAnchors(*intent, view);
      !anchors.ok()) {
    return anchors;
  }

  switch (next->stage_) {
    case FailoverPhaseStage::kOldAuthorityExcluded:
    case FailoverPhaseStage::kCandidateCaughtUp:
      if (!transition->current_directives_.empty() ||
          !transition->evidence_.empty()) {
        return Invalid("preparing-independent failover phase carries work");
      }
      return absl::OkStatus();
    case FailoverPhaseStage::kPromotionPreparing:
      if (transition->current_directives_.size() != 1 ||
          !transition->evidence_.empty()) {
        return Invalid("promotion-preparing requires one current directive");
      }
      return ValidatePreparingDirective(
          transition->current_directives_.front(), *intent, *next);
    case FailoverPhaseStage::kPromotionPrepared:
      return ValidatePreparedTransition(*transition, *operation, *intent,
                                        *next);
    case FailoverPhaseStage::kAuthorityActivated:
    case FailoverPhaseStage::kServing:
      return Invalid("failover phase is reserved for issue 41");
  }
  return Invalid("unknown failover phase");
}

}  // namespace keylane::meta
