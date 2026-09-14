#include "keylane/meta/operation_store.h"

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <set>
#include <tuple>

#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/hash.h"
#include "keylane/meta/value_codec.h"
#include "spdlog/spdlog.h"

namespace keylane::meta {
namespace {

// The apply caller lost the log index <-> operation correspondence (seq = the
// submit command's own raft log index must be nonzero and unique). Same
// fail-stop policy as the audit store (spdlog::critical + abort).
[[noreturn]] void FatalOperationContractViolation(std::string_view what,
                                                  std::uint64_t seq) {
  spdlog::critical(
      "meta operation store: {} (operation_seq {}); the apply layer violated "
      "the log-index/operation correspondence — aborting per fail-stop "
      "policy",
      what, seq);
  std::abort();
}

bool IsTerminal(MetaOperationLifecycle lifecycle) {
  return lifecycle == MetaOperationLifecycle::kCompleted ||
         lifecycle == MetaOperationLifecycle::kAborted;
}

// The replay check of TransitionOperationPhase: the command's evidence chunk
// is already the tail of the record's accumulated evidence.
bool EvidenceTailMatches(const MetaOperationRecord& record,
                         const std::vector<MetaEvidenceSummary>& evidence) {
  if (record.evidence_.size() < evidence.size()) return false;
  return std::equal(evidence.begin(), evidence.end(),
                    record.evidence_.end() - evidence.size());
}

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::all_of(value.begin(), value.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

bool DirectiveSpecsMatch(const std::vector<MetaCurrentDirective>& installed,
                         const std::vector<MetaDirectiveSpec>& desired) {
  if (installed.size() != desired.size()) return false;
  for (std::size_t i = 0; i < installed.size(); ++i) {
    if (installed[i].spec_ != desired[i]) return false;
  }
  return true;
}

bool RecipientMatchesKind(const MetaDirectiveSpec& directive) {
  if (IsMetaPopulationDirective(directive.kind_)) {
    return directive.recipient_node_id_ == directive.target_node_id_;
  }
  if (IsMetaSourceDirective(directive.kind_)) {
    return directive.recipient_node_id_ == directive.source_node_id_;
  }
  // Operation kinds are extensible at the durable layer. Unknown kinds still
  // have to bind execution to one of the two persisted incarnations; the
  // control projector decides whether it knows how to send them.
  return directive.recipient_node_id_ == directive.target_node_id_ ||
         directive.recipient_node_id_ == directive.source_node_id_;
}

const MetaBootIncarnation* RecipientBoot(const MetaDirectiveSpec& directive) {
  if (IsMetaPopulationDirective(directive.kind_) &&
      directive.recipient_node_id_ == directive.target_node_id_) {
    return &directive.target_boot_id_;
  }
  if (IsMetaSourceDirective(directive.kind_) &&
      directive.recipient_node_id_ == directive.source_node_id_) {
    return &directive.source_boot_id_;
  }
  if (IsKnownMetaDirective(directive.kind_)) {
    return nullptr;
  }
  if (directive.recipient_node_id_ == directive.target_node_id_ &&
      directive.recipient_node_id_ != directive.source_node_id_) {
    return &directive.target_boot_id_;
  }
  if (directive.recipient_node_id_ == directive.source_node_id_ &&
      directive.recipient_node_id_ != directive.target_node_id_) {
    return &directive.source_boot_id_;
  }
  // An extensible kind cannot choose an incarnation when both roles name the
  // same stable node but carry different boots. Refuse the ambiguous result
  // anchor instead of silently preferring one role.
  if (directive.recipient_node_id_ == directive.target_node_id_ &&
      directive.target_boot_id_ == directive.source_boot_id_) {
    return &directive.target_boot_id_;
  }
  return nullptr;
}

bool DirectiveWellFormed(const MetaDirectiveSpec& directive) {
  const bool zero_manifest = IsZero(directive.population_manifest_digest_);
  const bool initializes_empty =
      directive.kind_ == kMetaDirectiveInitializeEmptyPopulation;
  const bool rebuild = directive.kind_ == kMetaDirectiveRebuild ||
                       directive.kind_ == kMetaDirectiveAuthorizeSource;
  const bool absent_source =
      directive.source_node_id_ == std::string(kMetaNodeIdBytes, '0') &&
      IsZero(directive.source_assignment_id_) &&
      IsZero(directive.source_boot_id_) &&
      IsZero(directive.source_replication_history_id_);
  const bool source_valid =
      initializes_empty
          ? absent_source
          : directive.source_node_id_.size() == kMetaNodeIdBytes &&
                !IsZero(directive.source_assignment_id_) &&
                !IsZero(directive.source_boot_id_) &&
                !IsZero(directive.source_replication_history_id_);
  const bool payload_valid =
      initializes_empty
          ? directive.payload_.size() == 2 * kMetaReplicationHistoryIdBytes &&
                std::all_of(directive.payload_.begin(),
                            directive.payload_.end(),
                            [](unsigned char value) {
                              return (value >= '0' && value <= '9') ||
                                     (value >= 'a' && value <= 'f');
                            }) &&
                directive.preconditions_.empty()
      : rebuild
          ? cluster::control::DecodeRebuildRequest(directive.payload_).ok() &&
                directive.preconditions_.empty()
          : directive.payload_.empty() && directive.preconditions_.empty();
  return !IsZero(directive.directive_id_) && !IsZero(directive.attempt_id_) &&
         directive.recipient_node_id_.size() == kMetaNodeIdBytes &&
         directive.target_node_id_.size() == kMetaNodeIdBytes &&
         !IsZero(directive.target_boot_id_) &&
         !IsZero(directive.assignment_id_) && source_valid &&
         !directive.group_id_.empty() &&
         directive.group_id_.size() <= kMaxMetaGroupIdBytes &&
         directive.group_term_ != 0 && directive.authority_version_ != 0 &&
         directive.grant_revision_ != 0 &&
         ((directive.population_manifest_revision_ == 0) == zero_manifest) &&
         !directive.kind_.empty() &&
         directive.kind_.size() <= kMaxMetaDirectiveKindBytes &&
         directive.payload_.size() <= kMaxMetaPayloadBytes &&
         directive.preconditions_.size() <=
             kMaxMetaDirectivePreconditionsBytes &&
         payload_valid && !directive.force_ && RecipientMatchesKind(directive);
}

bool EvidenceSummaryWellFormed(const MetaEvidenceSummary& evidence) {
  const bool zero_manifest = IsZero(evidence.population_manifest_digest_);
  return evidence.node_id_.size() == kMetaNodeIdBytes &&
         !evidence.group_id_.empty() &&
         evidence.group_id_.size() <= kMaxMetaGroupIdBytes &&
         !IsZero(evidence.assignment_id_) &&
         !IsZero(evidence.boot_incarnation_) && evidence.group_term_ != 0 &&
         ((evidence.population_manifest_revision_ == 0) == zero_manifest) &&
         !IsZero(evidence.replication_history_id_) &&
         !IsZero(evidence.operation_id_);
}

bool ValidResultStatus(MetaDirectiveResultStatus status) {
  const auto tag = static_cast<std::uint8_t>(status);
  return tag >=
             static_cast<std::uint8_t>(MetaDirectiveResultStatus::kSucceeded) &&
         tag <= static_cast<std::uint8_t>(MetaDirectiveResultStatus::kRejected);
}

bool TerminalReceiptWellFormed(const MetaTerminalReceipt& receipt) {
  return !IsZero(receipt.key_.operation_id_) &&
         !IsZero(receipt.key_.directive_id_) &&
         !IsZero(receipt.key_.attempt_id_) &&
         receipt.key_.directive_revision_ != 0 &&
         receipt.recipient_node_id_.size() == kMetaNodeIdBytes &&
         !IsZero(receipt.recipient_boot_id_) &&
         !IsZero(receipt.assignment_id_) &&
         ValidResultStatus(receipt.status_) &&
         receipt.result_.size() <= kMaxMetaPayloadBytes &&
         MetaSha256(receipt.result_) == receipt.result_hash_ &&
         receipt.committed_index_ != 0;
}

const MetaTerminalReceipt* FindReceipt(
    const std::vector<MetaTerminalReceipt>& receipts,
    const MetaTerminalReceiptKey& key) {
  const auto it = std::find_if(receipts.begin(), receipts.end(),
                               [&key](const MetaTerminalReceipt& receipt) {
                                 return receipt.key_ == key;
                               });
  return it == receipts.end() ? nullptr : &*it;
}

bool ReceiptMatches(const MetaTerminalReceipt& receipt,
                    const CommitDirectiveResult& command) {
  return receipt.recipient_node_id_ == command.recipient_node_id_ &&
         receipt.recipient_boot_id_ == command.recipient_boot_id_ &&
         receipt.assignment_id_ == command.assignment_id_ &&
         receipt.status_ == command.status_ &&
         receipt.result_hash_ == command.result_hash_ &&
         receipt.result_ == command.result_;
}

}  // namespace

absl::StatusOr<MetaSubmitResult> MetaOperationStore::SubmitOperation(
    const keylane::meta::SubmitOperation& command,
    std::uint64_t operation_seq) {
  if (command.kind_.empty() ||
      command.kind_.size() > kMaxMetaOperationKindBytes) {
    return MetaDomainRejectError("operation kind is empty or exceeds its cap");
  }
  if (command.intent_.size() > kMaxMetaPayloadBytes) {
    return MetaDomainRejectError("operation intent exceeds its cap");
  }
  if (command.policy_references_.size() >
      kMaxMetaPolicyReferencesPerOperation) {
    return MetaDomainRejectError("operation policy reference cap exceeded");
  }
  std::set<std::pair<std::string, std::uint64_t>> references;
  for (const MetaPolicyReference& reference : command.policy_references_) {
    if (reference.policy_id_.empty() ||
        reference.policy_id_.size() > kMaxMetaPolicyIdBytes ||
        reference.version_ == 0) {
      return MetaDomainRejectError("invalid operation policy reference");
    }
    if (!references.emplace(reference.policy_id_, reference.version_).second) {
      return MetaDomainRejectError("duplicate operation policy reference");
    }
  }
  // Permanent idempotency on the client-provided id, across live records and
  // archive tombstones.
  if (const auto it = live_.find(command.operation_id_); it != live_.end()) {
    if (it->second.intent_hash_ != command.intent_hash_ ||
        it->second.intent_ != command.intent_) {
      return MetaDomainRejectError(
          "operation id reused with a different intent");
    }
    return MetaSubmitResult{/*.created_=*/false, /*.archived_=*/false};
  }
  if (const auto it = archived_.find(command.operation_id_);
      it != archived_.end()) {
    if (it->second.intent_hash_ != command.intent_hash_) {
      return MetaDomainRejectError(
          "operation id reused with a different intent");
    }
    return MetaSubmitResult{/*.created_=*/false, /*.archived_=*/true};
  }
  // New operation: the seq must be the unique log index of this submit.
  if (operation_seq == 0 || live_by_seq_.contains(operation_seq) ||
      archived_by_seq_.contains(operation_seq)) {
    FatalOperationContractViolation(
        "operation_seq is zero or already bound to another operation",
        operation_seq);
  }
  if (active_count_ >= max_active_) {
    return MetaDomainRejectError("max_active_operations reached");
  }
  // Terminal records stay live until archived, so the live set alone could
  // grow without bound; the joint bound keeps every collection bounded
  // and ArchiveOperations is the escape valve.
  if (live_.size() >= static_cast<std::uint64_t>(max_active_) + max_archived_) {
    return MetaDomainRejectError(
        "live operation record bound reached; archive terminal operations");
  }
  MetaOperationRecord record;
  record.operation_id_ = command.operation_id_;
  record.operation_seq_ = operation_seq;
  record.kind_ = command.kind_;
  record.intent_ = command.intent_;
  record.intent_hash_ = command.intent_hash_;
  record.replication_history_id_ = command.replication_history_id_;
  record.policy_references_ = command.policy_references_;
  record.actor_ = command.actor_;
  live_.emplace(command.operation_id_, std::move(record));
  live_by_seq_.emplace(operation_seq, command.operation_id_);
  ++active_count_;
  return MetaSubmitResult{/*.created_=*/true, /*.archived_=*/false};
}

std::optional<MetaOperationRecord> MetaOperationStore::FindOperation(
    const MetaOperationId& id) const {
  const auto it = live_.find(id);
  if (it == live_.end()) return std::nullopt;
  return it->second;
}

std::optional<MetaOperationRecord> MetaOperationStore::FindOperationBySeq(
    std::uint64_t seq) const {
  const auto it = live_by_seq_.find(seq);
  if (it == live_by_seq_.end()) return std::nullopt;
  return FindOperation(it->second);
}

std::optional<MetaOperationArchiveSummary> MetaOperationStore::FindArchived(
    const MetaOperationId& id) const {
  const auto it = archived_.find(id);
  if (it == archived_.end()) return std::nullopt;
  return it->second;
}

std::optional<MetaOperationArchiveSummary>
MetaOperationStore::FindArchivedBySeq(std::uint64_t seq) const {
  const auto it = archived_by_seq_.find(seq);
  if (it == archived_by_seq_.end()) return std::nullopt;
  return FindArchived(it->second);
}

std::vector<MetaOperationRecord> MetaOperationStore::LiveOperations() const {
  std::vector<MetaOperationRecord> result;
  result.reserve(live_.size());
  for (const auto& [id, record] : live_) {
    (void)id;
    result.push_back(record);
  }
  return result;
}

bool MetaOperationStore::HasActiveKind(std::string_view kind) const {
  return std::any_of(live_.begin(), live_.end(), [&](const auto& entry) {
    return entry.second.kind_ == kind && !IsTerminal(entry.second.lifecycle_);
  });
}

std::optional<MetaTerminalReceipt> MetaOperationStore::FindTerminalReceipt(
    const MetaTerminalReceiptKey& key) const {
  if (const auto live = live_.find(key.operation_id_); live != live_.end()) {
    if (const MetaTerminalReceipt* receipt =
            FindReceipt(live->second.terminal_receipts_, key);
        receipt != nullptr) {
      return *receipt;
    }
  }
  if (const auto archived = archived_.find(key.operation_id_);
      archived != archived_.end()) {
    if (const MetaTerminalReceipt* receipt =
            FindReceipt(archived->second.terminal_receipts_, key);
        receipt != nullptr) {
      return *receipt;
    }
  }
  return std::nullopt;
}

bool MetaOperationStore::TransitionAlreadyApplied(
    const keylane::meta::TransitionOperationPhase& command) const {
  if (command.expected_revision_ == std::numeric_limits<std::uint64_t>::max()) {
    return false;
  }
  const auto it = live_.find(command.operation_id_);
  if (it == live_.end()) return false;
  const MetaOperationRecord& record = it->second;
  return record.lifecycle_ == MetaOperationLifecycle::kRunning &&
         record.revision_ == command.expected_revision_ + 1 &&
         record.kind_phase_blob_ == command.kind_phase_blob_ &&
         DirectiveSpecsMatch(record.current_directives_,
                             command.current_directives_) &&
         EvidenceTailMatches(record, command.evidence_);
}

bool MetaOperationStore::PolicyInUse(std::string_view policy_id,
                                     std::uint64_t version) const {
  for (const auto& [id, record] : live_) {
    (void)id;
    if (IsTerminal(record.lifecycle_)) continue;
    for (const MetaPolicyReference& reference : record.policy_references_) {
      if (reference.policy_id_ == policy_id && reference.version_ == version) {
        return true;
      }
    }
  }
  return false;
}

bool MetaOperationStore::PopulationManifestInUse(
    const MetaHash256& digest) const {
  for (const auto& [id, record] : live_) {
    (void)id;
    if (IsTerminal(record.lifecycle_)) continue;
    if (std::any_of(record.evidence_.begin(), record.evidence_.end(),
                    [&digest](const MetaEvidenceSummary& evidence) {
                      return evidence.population_manifest_revision_ != 0 &&
                             evidence.population_manifest_digest_ == digest;
                    })) {
      return true;
    }
    if (std::any_of(
            record.current_directives_.begin(),
            record.current_directives_.end(),
            [&digest](const MetaCurrentDirective& directive) {
              return directive.spec_.population_manifest_revision_ != 0 &&
                     directive.spec_.population_manifest_digest_ == digest;
            })) {
      return true;
    }
  }
  return false;
}

absl::Status MetaOperationStore::TransitionOperationPhase(
    const keylane::meta::TransitionOperationPhase& command,
    std::uint64_t committed_index) {
  const auto it = live_.find(command.operation_id_);
  if (it == live_.end()) {
    if (archived_.contains(command.operation_id_)) {
      return MetaDomainRejectError("operation is archived (already terminal)");
    }
    return MetaDomainRejectError("unknown operation id");
  }
  MetaOperationRecord& record = it->second;
  if (command.expected_revision_ == std::numeric_limits<std::uint64_t>::max()) {
    return MetaDomainRejectError("expected_revision overflow");
  }
  // Replay: the post-effect is already present with identical content.
  if (record.lifecycle_ == MetaOperationLifecycle::kRunning &&
      record.revision_ == command.expected_revision_ + 1 &&
      record.kind_phase_blob_ == command.kind_phase_blob_ &&
      DirectiveSpecsMatch(record.current_directives_,
                          command.current_directives_) &&
      EvidenceTailMatches(record, command.evidence_)) {
    return absl::OkStatus();
  }
  if (IsTerminal(record.lifecycle_)) {
    return MetaDomainRejectError("operation is terminal");
  }
  if (record.revision_ != command.expected_revision_) {
    return MetaDomainRejectError("expected_revision mismatch");
  }
  if (command.kind_phase_blob_.size() > kMaxMetaPayloadBytes) {
    return MetaDomainRejectError("kind_phase_blob exceeds its cap");
  }
  if (committed_index == 0 ||
      command.current_directives_.size() > kMaxMetaDirectivesPerOperation) {
    return MetaDomainRejectError("invalid directive transition index/count");
  }
  std::set<MetaDirectiveId> directive_ids;
  std::set<MetaAttemptId> attempt_ids;
  std::set<std::pair<std::string, MetaAssignmentId>> mutating_targets;
  for (const MetaDirectiveSpec& directive : command.current_directives_) {
    if (!DirectiveWellFormed(directive) ||
        !directive_ids.insert(directive.directive_id_).second ||
        !attempt_ids.insert(directive.attempt_id_).second) {
      return MetaDomainRejectError("invalid or duplicate current directive");
    }
    if (directive.storage_mutating_ &&
        !mutating_targets
             .emplace(directive.target_node_id_, directive.assignment_id_)
             .second) {
      return MetaDomainRejectError(
          "multiple storage-mutating directives for one assignment");
    }
  }
  for (const auto& [other_id, other] : live_) {
    if (other_id == command.operation_id_ || IsTerminal(other.lifecycle_)) {
      continue;
    }
    for (const MetaCurrentDirective& installed : other.current_directives_) {
      const MetaDirectiveSpec& directive = installed.spec_;
      if (directive.storage_mutating_ &&
          mutating_targets.contains(
              {directive.target_node_id_, directive.assignment_id_})) {
        return MetaDomainRejectError(
            "storage-mutating directive conflicts with another operation");
      }
    }
  }
  if (record.evidence_.size() + command.evidence_.size() >
      kMaxMetaOperationEvidencePerRecord) {
    return MetaDomainRejectError("per-record evidence cap reached");
  }
  for (const MetaEvidenceSummary& evidence : command.evidence_) {
    if (!EvidenceSummaryWellFormed(evidence)) {
      return MetaDomainRejectError("invalid evidence summary anchors");
    }
    if (evidence.operation_id_ != command.operation_id_) {
      return MetaDomainRejectError("evidence references a different operation");
    }
    if (IsZero(record.replication_history_id_) ||
        evidence.replication_history_id_ != record.replication_history_id_) {
      return MetaDomainRejectError(
          "evidence replication history does not match the operation");
    }
  }
  record.lifecycle_ = MetaOperationLifecycle::kRunning;
  record.kind_phase_blob_ = command.kind_phase_blob_;
  std::vector<MetaCurrentDirective> next_directives;
  next_directives.reserve(command.current_directives_.size());
  for (const MetaDirectiveSpec& desired : command.current_directives_) {
    const auto old = std::find_if(
        record.current_directives_.begin(), record.current_directives_.end(),
        [&desired](const MetaCurrentDirective& installed) {
          return installed.spec_.directive_id_ == desired.directive_id_;
        });
    const std::uint64_t revision =
        old != record.current_directives_.end() && old->spec_ == desired
            ? old->directive_revision_
            : committed_index;
    next_directives.push_back(MetaCurrentDirective{desired, revision});
  }
  record.current_directives_ = std::move(next_directives);
  record.evidence_.insert(record.evidence_.end(), command.evidence_.begin(),
                          command.evidence_.end());
  record.revision_ = command.expected_revision_ + 1;
  return absl::OkStatus();
}

absl::Status MetaOperationStore::CompleteOperation(
    const keylane::meta::CompleteOperation& command) {
  const auto it = live_.find(command.operation_id_);
  if (it == live_.end()) {
    if (archived_.contains(command.operation_id_)) {
      return MetaDomainRejectError("operation is archived (already terminal)");
    }
    return MetaDomainRejectError("unknown operation id");
  }
  MetaOperationRecord& record = it->second;
  if (command.expected_revision_ == std::numeric_limits<std::uint64_t>::max()) {
    return MetaDomainRejectError("expected_revision overflow");
  }
  if (record.lifecycle_ == MetaOperationLifecycle::kCompleted &&
      record.revision_ == command.expected_revision_ + 1 &&
      record.terminal_result_ == command.result_ &&
      record.data_loss_possible_ == command.data_loss_possible_) {
    return absl::OkStatus();  // replay no-op
  }
  if (IsTerminal(record.lifecycle_)) {
    return MetaDomainRejectError("operation is terminal");
  }
  if (record.revision_ != command.expected_revision_) {
    return MetaDomainRejectError("expected_revision mismatch");
  }
  if (command.result_.size() > kMaxMetaPayloadBytes) {
    return MetaDomainRejectError("terminal result exceeds its cap");
  }
  record.lifecycle_ = MetaOperationLifecycle::kCompleted;
  record.current_directives_.clear();
  record.terminal_result_ = command.result_;
  record.data_loss_possible_ = command.data_loss_possible_;
  record.revision_ = command.expected_revision_ + 1;
  --active_count_;
  return absl::OkStatus();
}

absl::Status MetaOperationStore::AbortOperation(
    const keylane::meta::AbortOperation& command) {
  const auto it = live_.find(command.operation_id_);
  if (it == live_.end()) {
    if (archived_.contains(command.operation_id_)) {
      return MetaDomainRejectError("operation is archived (already terminal)");
    }
    return MetaDomainRejectError("unknown operation id");
  }
  MetaOperationRecord& record = it->second;
  if (command.expected_revision_ == std::numeric_limits<std::uint64_t>::max()) {
    return MetaDomainRejectError("expected_revision overflow");
  }
  if (record.lifecycle_ == MetaOperationLifecycle::kAborted &&
      record.revision_ == command.expected_revision_ + 1 &&
      record.terminal_result_ == command.reason_) {
    return absl::OkStatus();  // replay no-op
  }
  if (IsTerminal(record.lifecycle_)) {
    return MetaDomainRejectError("operation is terminal");
  }
  if (record.revision_ != command.expected_revision_) {
    return MetaDomainRejectError("expected_revision mismatch");
  }
  if (command.reason_.size() > kMaxMetaAbortReasonBytes) {
    return MetaDomainRejectError("abort reason exceeds its cap");
  }
  record.lifecycle_ = MetaOperationLifecycle::kAborted;
  record.current_directives_.clear();
  record.terminal_result_ = command.reason_;
  record.data_loss_possible_ = false;
  record.revision_ = command.expected_revision_ + 1;
  --active_count_;
  return absl::OkStatus();
}

absl::Status MetaOperationStore::CommitDirectiveResult(
    const keylane::meta::CommitDirectiveResult& command,
    std::uint64_t committed_index) {
  const MetaTerminalReceiptKey key{command.operation_id_, command.directive_id_,
                                   command.attempt_id_,
                                   command.directive_revision_};
  if (const auto existing = FindTerminalReceipt(key); existing.has_value()) {
    if (ReceiptMatches(*existing, command)) {
      return absl::OkStatus();
    }
    return MetaDomainRejectError(
        "directive result conflicts with committed terminal receipt");
  }

  if (committed_index == 0 || IsZero(command.operation_id_) ||
      IsZero(command.directive_id_) || IsZero(command.attempt_id_) ||
      command.directive_revision_ == 0 ||
      command.recipient_node_id_.size() != kMetaNodeIdBytes ||
      IsZero(command.recipient_boot_id_) || IsZero(command.assignment_id_) ||
      !ValidResultStatus(command.status_) ||
      command.result_.size() > kMaxMetaPayloadBytes ||
      MetaSha256(command.result_) != command.result_hash_) {
    return MetaDomainRejectError("invalid directive result");
  }

  const auto operation = live_.find(command.operation_id_);
  if (operation == live_.end() || IsTerminal(operation->second.lifecycle_)) {
    // This is deliberately one semantic for an unknown, terminal-without-a-
    // receipt, archived-without-a-receipt, or explicitly pruned attempt. The
    // control adapter maps it to ResultNoLongerTracked and the data node may
    // discard its local replay record.
    return MetaDomainRejectError("directive result is no longer tracked");
  }
  MetaOperationRecord& record = operation->second;
  const auto directive = std::find_if(
      record.current_directives_.begin(), record.current_directives_.end(),
      [&command](const MetaCurrentDirective& current) {
        return current.spec_.directive_id_ == command.directive_id_ &&
               current.spec_.attempt_id_ == command.attempt_id_ &&
               current.directive_revision_ == command.directive_revision_;
      });
  if (directive == record.current_directives_.end()) {
    return MetaDomainRejectError("directive result is no longer tracked");
  }
  const MetaBootIncarnation* expected_boot = RecipientBoot(directive->spec_);
  if (expected_boot == nullptr ||
      directive->spec_.recipient_node_id_ != command.recipient_node_id_ ||
      *expected_boot != command.recipient_boot_id_ ||
      directive->spec_.assignment_id_ != command.assignment_id_) {
    return MetaDomainRejectError("directive result recipient anchor mismatch");
  }
  if (record.terminal_receipts_.size() >=
      max_terminal_receipts_per_operation_) {
    return MetaDomainRejectError("terminal receipt cap reached");
  }
  if (record.revision_ == std::numeric_limits<std::uint64_t>::max()) {
    return MetaDomainRejectError("operation revision exhausted");
  }

  MetaTerminalReceipt receipt;
  receipt.key_ = key;
  receipt.recipient_node_id_ = command.recipient_node_id_;
  receipt.recipient_boot_id_ = command.recipient_boot_id_;
  receipt.assignment_id_ = command.assignment_id_;
  receipt.status_ = command.status_;
  receipt.result_hash_ = command.result_hash_;
  receipt.result_ = command.result_;
  receipt.committed_index_ = committed_index;
  record.terminal_receipts_.push_back(std::move(receipt));
  // The receipt is authoritative workflow input. Advancing the phase CAS
  // prevents a reconciler that read the operation before this result commit
  // from overwriting it with a stale transition or terminal decision. The
  // exact replay returned above preserves both the first index and revision.
  ++record.revision_;
  return absl::OkStatus();
}

void MetaOperationStore::InvalidateCurrentDirectives(
    const std::vector<MetaTerminalReceiptKey>& directive_keys) {
  std::map<MetaOperationId, std::vector<const MetaTerminalReceiptKey*>>
      keys_by_operation;
  for (const MetaTerminalReceiptKey& key : directive_keys) {
    keys_by_operation[key.operation_id_].push_back(&key);
  }

  for (const auto& [operation_id, keys] : keys_by_operation) {
    const auto operation = live_.find(operation_id);
    if (operation == live_.end() || IsTerminal(operation->second.lifecycle_)) {
      continue;
    }
    MetaOperationRecord& record = operation->second;
    const std::size_t prior_size = record.current_directives_.size();
    std::erase_if(record.current_directives_, [&keys](const auto& current) {
      return std::any_of(keys.begin(), keys.end(), [&current](const auto* key) {
        return current.spec_.directive_id_ == key->directive_id_ &&
               current.spec_.attempt_id_ == key->attempt_id_ &&
               current.directive_revision_ == key->directive_revision_;
      });
    });
    if (record.current_directives_.size() == prior_size) continue;

    // Never wrap the CAS token. A record at the terminal uint64 value cannot
    // accept another phase transition anyway, but its stale directive must
    // still be removed to fail closed.
    if (record.revision_ != std::numeric_limits<std::uint64_t>::max()) {
      ++record.revision_;
    }
  }
}

absl::Status MetaOperationStore::ArchiveOperations(
    const keylane::meta::ArchiveOperations& command) {
  // Set semantics: duplicates inside the command collapse.
  std::vector<std::uint64_t> seqs = command.operation_seqs_;
  std::sort(seqs.begin(), seqs.end());
  seqs.erase(std::unique(seqs.begin(), seqs.end()), seqs.end());

  // Validate the whole set before mutating anything (atomic command).
  std::vector<MetaOperationId> to_archive;
  for (const std::uint64_t seq : seqs) {
    if (archived_by_seq_.contains(seq)) {
      continue;  // replay/already archived: idempotent no-op
    }
    const auto live_it = live_by_seq_.find(seq);
    if (live_it == live_by_seq_.end()) {
      return MetaDomainRejectError(
          "archive references an unknown operation_seq");
    }
    const MetaOperationRecord& record = live_.at(live_it->second);
    if (!IsTerminal(record.lifecycle_)) {
      return MetaDomainRejectError(
          "non-terminal operations are not archivable");
    }
    to_archive.push_back(live_it->second);
  }
  if (archived_.size() + to_archive.size() > max_archived_) {
    // The operator must export (ctl) before more summaries fit.
    return MetaDomainRejectError("archive summary cap reached");
  }
  for (const MetaOperationId& id : to_archive) {
    const auto node = live_.extract(id);
    const MetaOperationRecord& record = node.mapped();
    MetaOperationArchiveSummary summary;
    summary.operation_id_ = record.operation_id_;
    summary.operation_seq_ = record.operation_seq_;
    summary.intent_hash_ = record.intent_hash_;
    summary.actor_ = record.actor_;
    summary.terminal_lifecycle_ = record.lifecycle_;
    summary.terminal_result_ = record.terminal_result_;
    summary.data_loss_possible_ = record.data_loss_possible_;
    summary.terminal_receipts_ = record.terminal_receipts_;
    live_by_seq_.erase(record.operation_seq_);
    archived_by_seq_.emplace(record.operation_seq_, id);
    archived_.emplace(id, std::move(summary));
  }
  return absl::OkStatus();
}

absl::Status MetaOperationStore::PruneArchive(
    const PruneOperationArchive& command) {
  std::vector<std::uint64_t> seqs = command.operation_seqs_;
  std::sort(seqs.begin(), seqs.end());
  seqs.erase(std::unique(seqs.begin(), seqs.end()), seqs.end());
  if (seqs.size() > kMaxMetaArchivedOperationSummaries) {
    return MetaDomainRejectError("too many archive seqs to prune");
  }
  // Missing seqs are deliberate idempotent no-ops: after a snapshot/replay
  // the exported tombstone may already have been removed.
  for (const std::uint64_t seq : seqs) {
    const auto by_seq = archived_by_seq_.find(seq);
    if (by_seq == archived_by_seq_.end()) continue;
    archived_.erase(by_seq->second);
    archived_by_seq_.erase(by_seq);
  }
  return absl::OkStatus();
}

absl::Status MetaOperationStore::PruneTerminalReceipts(
    const keylane::meta::PruneTerminalReceipts& command) {
  if (command.receipts_.size() > kMaxMetaTerminalReceiptPrunesPerCommand) {
    return MetaDomainRejectError("too many terminal receipts to prune");
  }
  for (const MetaTerminalReceiptKey& key : command.receipts_) {
    if (IsZero(key.operation_id_) || IsZero(key.directive_id_) ||
        IsZero(key.attempt_id_) || key.directive_revision_ == 0) {
      return MetaDomainRejectError("invalid terminal receipt key");
    }
    const auto live = live_.find(key.operation_id_);
    if (live != live_.end() &&
        FindReceipt(live->second.terminal_receipts_, key) != nullptr &&
        !IsTerminal(live->second.lifecycle_)) {
      return MetaDomainRejectError(
          "active operation terminal receipts cannot be pruned");
    }
  }

  const auto erase_key = [](std::vector<MetaTerminalReceipt>& receipts,
                            const MetaTerminalReceiptKey& key) {
    std::erase_if(receipts, [&key](const MetaTerminalReceipt& receipt) {
      return receipt.key_ == key;
    });
  };
  for (const MetaTerminalReceiptKey& key : command.receipts_) {
    if (auto live = live_.find(key.operation_id_); live != live_.end()) {
      erase_key(live->second.terminal_receipts_, key);
    }
    if (auto archived = archived_.find(key.operation_id_);
        archived != archived_.end()) {
      erase_key(archived->second.terminal_receipts_, key);
    }
  }
  return absl::OkStatus();
}

namespace {

// Wire codecs for the snapshot and archive-export blobs (versioned strict
// encoding; every decode failure is MetaFailureClass::kFailStop).

absl::Status ReadStoreSchemaVersion(MetaReader& r) {
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unsupported operation store schema version");
  }
  return absl::OkStatus();
}

absl::StatusOr<MetaPolicyReference> ReadStoredPolicyReference(MetaReader& r) {
  auto reference = ReadMetaPolicyReference(r);
  if (!reference.ok()) return reference.status();
  if (reference->policy_id_.empty() || reference->version_ == 0) {
    return MetaFailStopError("invalid operation policy reference");
  }
  return reference;
}

absl::Status ReadLifecycle(MetaReader& r, MetaOperationLifecycle& out) {
  auto tag = r.ReadU8();
  if (!tag.ok()) return tag.status();
  if (*tag < static_cast<std::uint8_t>(MetaOperationLifecycle::kSubmitted) ||
      *tag > static_cast<std::uint8_t>(MetaOperationLifecycle::kAborted)) {
    return MetaFailStopError("unknown operation lifecycle tag");
  }
  out = static_cast<MetaOperationLifecycle>(*tag);
  return absl::OkStatus();
}

void WriteTerminalReceipt(MetaWriter& w, const MetaTerminalReceipt& receipt) {
  WriteFixedArray(w, receipt.key_.operation_id_);
  WriteFixedArray(w, receipt.key_.directive_id_);
  WriteFixedArray(w, receipt.key_.attempt_id_);
  w.WriteU64(receipt.key_.directive_revision_);
  w.WriteString(receipt.recipient_node_id_);
  WriteFixedArray(w, receipt.recipient_boot_id_);
  WriteFixedArray(w, receipt.assignment_id_);
  w.WriteU8(static_cast<std::uint8_t>(receipt.status_));
  WriteFixedArray(w, receipt.result_hash_);
  w.WriteString(receipt.result_);
  w.WriteU64(receipt.committed_index_);
}

absl::StatusOr<MetaTerminalReceipt> ReadTerminalReceipt(MetaReader& r) {
  MetaTerminalReceipt receipt;
  auto operation_id = ReadFixedArray<16>(r);
  if (!operation_id.ok()) return operation_id.status();
  receipt.key_.operation_id_ = *operation_id;
  auto directive_id = ReadFixedArray<16>(r);
  if (!directive_id.ok()) return directive_id.status();
  receipt.key_.directive_id_ = *directive_id;
  auto attempt_id = ReadFixedArray<16>(r);
  if (!attempt_id.ok()) return attempt_id.status();
  receipt.key_.attempt_id_ = *attempt_id;
  auto directive_revision = r.ReadU64();
  if (!directive_revision.ok()) return directive_revision.status();
  receipt.key_.directive_revision_ = *directive_revision;
  auto recipient_node = r.ReadString(kMetaNodeIdBytes);
  if (!recipient_node.ok()) return recipient_node.status();
  receipt.recipient_node_id_ = std::string(*recipient_node);
  auto recipient_boot = ReadFixedArray<kMetaBootIncarnationBytes>(r);
  if (!recipient_boot.ok()) return recipient_boot.status();
  receipt.recipient_boot_id_ = *recipient_boot;
  auto assignment_id = ReadFixedArray<16>(r);
  if (!assignment_id.ok()) return assignment_id.status();
  receipt.assignment_id_ = *assignment_id;
  auto status = r.ReadU8();
  if (!status.ok()) return status.status();
  receipt.status_ = static_cast<MetaDirectiveResultStatus>(*status);
  if (!ValidResultStatus(receipt.status_)) {
    return MetaFailStopError("unknown terminal receipt status");
  }
  auto result_hash = ReadFixedArray<32>(r);
  if (!result_hash.ok()) return result_hash.status();
  receipt.result_hash_ = *result_hash;
  auto result = r.ReadString(kMaxMetaPayloadBytes);
  if (!result.ok()) return result.status();
  receipt.result_ = std::string(*result);
  auto committed_index = r.ReadU64();
  if (!committed_index.ok()) return committed_index.status();
  receipt.committed_index_ = *committed_index;
  return receipt;
}

void WriteRecord(MetaWriter& w, const MetaOperationRecord& record) {
  WriteFixedArray(w, record.operation_id_);
  w.WriteU64(record.operation_seq_);
  w.WriteString(record.kind_);
  w.WriteString(record.intent_);
  WriteFixedArray(w, record.intent_hash_);
  WriteFixedArray(w, record.replication_history_id_);
  w.WriteList(record.policy_references_, WriteMetaPolicyReference);
  w.WriteU8(static_cast<std::uint8_t>(record.lifecycle_));
  w.WriteString(record.kind_phase_blob_);
  w.WriteList(record.current_directives_,
              [](MetaWriter& writer, const MetaCurrentDirective& directive) {
                WriteMetaDirectiveSpec(writer, directive.spec_);
                writer.WriteU64(directive.directive_revision_);
              });
  w.WriteList(record.terminal_receipts_, WriteTerminalReceipt);
  w.WriteU64(record.revision_);
  w.WriteList(record.evidence_, WriteMetaEvidenceSummary);
  w.WriteString(record.terminal_result_);
  w.WriteBool(record.data_loss_possible_);
  WriteActorContext(w, record.actor_);
}

absl::StatusOr<MetaOperationRecord> ReadRecord(MetaReader& r) {
  MetaOperationRecord record;
  auto id = ReadFixedArray<16>(r);
  if (!id.ok()) return id.status();
  record.operation_id_ = *id;
  auto seq = r.ReadU64();
  if (!seq.ok()) return seq.status();
  record.operation_seq_ = *seq;
  auto kind = r.ReadString(kMaxMetaOperationKindBytes);
  if (!kind.ok()) return kind.status();
  record.kind_ = std::string(*kind);
  auto intent = r.ReadString(kMaxMetaPayloadBytes);
  if (!intent.ok()) return intent.status();
  record.intent_ = std::string(*intent);
  auto intent_hash = ReadFixedArray<32>(r);
  if (!intent_hash.ok()) return intent_hash.status();
  record.intent_hash_ = *intent_hash;
  auto replication_history = ReadFixedArray<kMetaReplicationHistoryIdBytes>(r);
  if (!replication_history.ok()) return replication_history.status();
  record.replication_history_id_ = *replication_history;
  auto policy_references = r.ReadList<MetaPolicyReference>(
      kMaxMetaPolicyReferencesPerOperation,
      [](MetaReader& rr) { return ReadStoredPolicyReference(rr); });
  if (!policy_references.ok()) return policy_references.status();
  record.policy_references_ = std::move(*policy_references);
  if (absl::Status status = ReadLifecycle(r, record.lifecycle_); !status.ok()) {
    return status;
  }
  auto blob = r.ReadString(kMaxMetaPayloadBytes);
  if (!blob.ok()) return blob.status();
  record.kind_phase_blob_ = std::string(*blob);
  auto directives = r.ReadList<MetaCurrentDirective>(
      kMaxMetaDirectivesPerOperation,
      [](MetaReader& reader) -> absl::StatusOr<MetaCurrentDirective> {
        auto spec = ReadMetaDirectiveSpec(reader);
        if (!spec.ok()) return spec.status();
        auto revision = reader.ReadU64();
        if (!revision.ok()) return revision.status();
        return MetaCurrentDirective{std::move(*spec), *revision};
      });
  if (!directives.ok()) return directives.status();
  record.current_directives_ = std::move(*directives);
  auto receipts = r.ReadList<MetaTerminalReceipt>(
      kMaxMetaTerminalReceiptsPerOperation,
      [](MetaReader& reader) { return ReadTerminalReceipt(reader); });
  if (!receipts.ok()) return receipts.status();
  record.terminal_receipts_ = std::move(*receipts);
  auto revision = r.ReadU64();
  if (!revision.ok()) return revision.status();
  record.revision_ = *revision;
  auto evidence = r.ReadList<MetaEvidenceSummary>(
      kMaxMetaOperationEvidencePerRecord,
      [](MetaReader& rr) { return ReadMetaEvidenceSummary(rr); });
  if (!evidence.ok()) return evidence.status();
  record.evidence_ = std::move(*evidence);
  auto result = r.ReadString(kMaxMetaPayloadBytes);
  if (!result.ok()) return result.status();
  record.terminal_result_ = std::string(*result);
  auto data_loss_possible = r.ReadBool("bool tag must be 0 or 1");
  if (!data_loss_possible.ok()) return data_loss_possible.status();
  record.data_loss_possible_ = *data_loss_possible;
  auto actor = ReadActorContext(r);
  if (!actor.ok()) return actor.status();
  record.actor_ = std::move(*actor);
  return record;
}

void WriteSummary(MetaWriter& w, const MetaOperationArchiveSummary& summary) {
  WriteFixedArray(w, summary.operation_id_);
  w.WriteU64(summary.operation_seq_);
  WriteFixedArray(w, summary.intent_hash_);
  WriteActorContext(w, summary.actor_);
  w.WriteU8(static_cast<std::uint8_t>(summary.terminal_lifecycle_));
  w.WriteString(summary.terminal_result_);
  w.WriteBool(summary.data_loss_possible_);
  w.WriteList(summary.terminal_receipts_, WriteTerminalReceipt);
}

absl::StatusOr<MetaOperationArchiveSummary> ReadSummary(MetaReader& r) {
  MetaOperationArchiveSummary summary;
  auto id = ReadFixedArray<16>(r);
  if (!id.ok()) return id.status();
  summary.operation_id_ = *id;
  auto seq = r.ReadU64();
  if (!seq.ok()) return seq.status();
  summary.operation_seq_ = *seq;
  auto intent_hash = ReadFixedArray<32>(r);
  if (!intent_hash.ok()) return intent_hash.status();
  summary.intent_hash_ = *intent_hash;
  auto actor = ReadActorContext(r);
  if (!actor.ok()) return actor.status();
  summary.actor_ = std::move(*actor);
  if (absl::Status status = ReadLifecycle(r, summary.terminal_lifecycle_);
      !status.ok()) {
    return status;
  }
  // Tombstones are terminal by construction.
  if (!IsTerminal(summary.terminal_lifecycle_)) {
    return MetaFailStopError("archive summary is not terminal");
  }
  auto result = r.ReadString(kMaxMetaPayloadBytes);
  if (!result.ok()) return result.status();
  summary.terminal_result_ = std::string(*result);
  auto data_loss_possible = r.ReadBool("bool tag must be 0 or 1");
  if (!data_loss_possible.ok()) return data_loss_possible.status();
  summary.data_loss_possible_ = *data_loss_possible;
  auto receipts = r.ReadList<MetaTerminalReceipt>(
      kMaxMetaTerminalReceiptsPerOperation,
      [](MetaReader& reader) { return ReadTerminalReceipt(reader); });
  if (!receipts.ok()) return receipts.status();
  summary.terminal_receipts_ = std::move(*receipts);
  std::set<std::tuple<MetaDirectiveId, MetaAttemptId, std::uint64_t>>
      receipt_keys;
  for (const MetaTerminalReceipt& receipt : summary.terminal_receipts_) {
    if (!TerminalReceiptWellFormed(receipt) ||
        receipt.key_.operation_id_ != summary.operation_id_ ||
        !receipt_keys
             .emplace(receipt.key_.directive_id_, receipt.key_.attempt_id_,
                      receipt.key_.directive_revision_)
             .second) {
      return MetaFailStopError("invalid archived terminal receipt");
    }
  }
  return summary;
}

}  // namespace

absl::StatusOr<std::string> MetaOperationStore::ExportArchive() const {
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  w.WriteCount(static_cast<std::uint32_t>(archived_.size()));
  for (const auto& [id, summary] : archived_) {
    WriteSummary(w, summary);
  }
  return w.TakeBuffer();
}

absl::StatusOr<std::string> MetaOperationStore::Serialize() const {
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  w.WriteCount(static_cast<std::uint32_t>(live_.size()));
  for (const auto& [id, record] : live_) {
    WriteRecord(w, record);
  }
  w.WriteCount(static_cast<std::uint32_t>(archived_.size()));
  for (const auto& [id, summary] : archived_) {
    WriteSummary(w, summary);
  }
  return w.TakeBuffer();
}

absl::StatusOr<MetaOperationStore> MetaOperationStore::Deserialize(
    std::string_view bytes, std::uint32_t max_active,
    std::uint32_t max_archived,
    std::uint32_t max_terminal_receipts_per_operation) {
  MetaReader r(bytes);
  if (absl::Status status = ReadStoreSchemaVersion(r); !status.ok()) {
    return status;
  }
  // The live set (terminal records included) never exceeds the joint bound
  // enforced at submit; the decode cap doubles as the allocation guard.
  const std::uint64_t max_live =
      static_cast<std::uint64_t>(max_active) + max_archived;
  auto live_records = r.ReadList<MetaOperationRecord>(
      static_cast<std::uint32_t>(max_live),
      [](MetaReader& rr) { return ReadRecord(rr); });
  if (!live_records.ok()) return live_records.status();
  auto summaries = r.ReadList<MetaOperationArchiveSummary>(
      max_archived, [](MetaReader& rr) { return ReadSummary(rr); });
  if (!summaries.ok()) return summaries.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;

  MetaOperationStore store(max_active, max_archived,
                           max_terminal_receipts_per_operation);
  std::set<std::pair<std::string, MetaAssignmentId>> mutating_targets;
  for (auto& record : *live_records) {
    // Identity invariants are part of the durable format: ids and seqs are
    // unique across the whole journal; a violation is corruption.
    if (store.live_by_seq_.contains(record.operation_seq_) ||
        store.archived_by_seq_.contains(record.operation_seq_)) {
      return MetaFailStopError("duplicate operation_seq in snapshot");
    }
    std::set<std::pair<std::string, std::uint64_t>> policy_references;
    for (const MetaPolicyReference& reference : record.policy_references_) {
      if (!policy_references.emplace(reference.policy_id_, reference.version_)
               .second) {
        return MetaFailStopError(
            "duplicate operation policy reference in snapshot");
      }
    }
    for (const MetaEvidenceSummary& evidence : record.evidence_) {
      if (!EvidenceSummaryWellFormed(evidence) ||
          evidence.operation_id_ != record.operation_id_ ||
          IsZero(record.replication_history_id_) ||
          evidence.replication_history_id_ != record.replication_history_id_) {
        return MetaFailStopError(
            "operation evidence does not match committed anchors");
      }
    }
    std::set<MetaDirectiveId> directive_ids;
    std::set<MetaAttemptId> attempt_ids;
    for (const MetaCurrentDirective& installed : record.current_directives_) {
      const MetaDirectiveSpec& directive = installed.spec_;
      if (installed.directive_revision_ == 0 ||
          !DirectiveWellFormed(directive) ||
          !directive_ids.insert(directive.directive_id_).second ||
          !attempt_ids.insert(directive.attempt_id_).second ||
          (directive.storage_mutating_ &&
           !mutating_targets
                .emplace(directive.target_node_id_, directive.assignment_id_)
                .second)) {
        return MetaFailStopError("invalid current directive in snapshot");
      }
    }
    if (record.terminal_receipts_.size() >
        max_terminal_receipts_per_operation) {
      return MetaFailStopError("terminal receipt cap exceeded in snapshot");
    }
    std::set<std::tuple<MetaDirectiveId, MetaAttemptId, std::uint64_t>>
        receipt_keys;
    for (const MetaTerminalReceipt& receipt : record.terminal_receipts_) {
      if (!TerminalReceiptWellFormed(receipt) ||
          receipt.key_.operation_id_ != record.operation_id_ ||
          !receipt_keys
               .emplace(receipt.key_.directive_id_, receipt.key_.attempt_id_,
                        receipt.key_.directive_revision_)
               .second) {
        return MetaFailStopError("invalid terminal receipt in snapshot");
      }
    }
    if (IsTerminal(record.lifecycle_) && !record.current_directives_.empty()) {
      return MetaFailStopError("terminal operation retains current directives");
    }
    if (!IsTerminal(record.lifecycle_)) ++store.active_count_;
    store.live_by_seq_.emplace(record.operation_seq_, record.operation_id_);
    if (!store.live_.emplace(record.operation_id_, std::move(record)).second) {
      return MetaFailStopError("duplicate operation_id in snapshot");
    }
  }
  for (auto& summary : *summaries) {
    if (store.live_.contains(summary.operation_id_) ||
        store.archived_by_seq_.contains(summary.operation_seq_) ||
        store.live_by_seq_.contains(summary.operation_seq_)) {
      return MetaFailStopError("archive identity collides with live records");
    }
    if (summary.terminal_receipts_.size() >
        max_terminal_receipts_per_operation) {
      return MetaFailStopError(
          "archived terminal receipt cap exceeded in snapshot");
    }
    std::set<std::tuple<MetaDirectiveId, MetaAttemptId, std::uint64_t>>
        receipt_keys;
    for (const MetaTerminalReceipt& receipt : summary.terminal_receipts_) {
      if (!TerminalReceiptWellFormed(receipt) ||
          receipt.key_.operation_id_ != summary.operation_id_ ||
          !receipt_keys
               .emplace(receipt.key_.directive_id_, receipt.key_.attempt_id_,
                        receipt.key_.directive_revision_)
               .second) {
        return MetaFailStopError(
            "invalid archived terminal receipt in snapshot");
      }
    }
    store.archived_by_seq_.emplace(summary.operation_seq_,
                                   summary.operation_id_);
    if (!store.archived_.emplace(summary.operation_id_, std::move(summary))
             .second) {
      return MetaFailStopError("duplicate archived operation_id in snapshot");
    }
  }
  if (store.active_count_ > max_active) {
    return MetaFailStopError("snapshot exceeds max_active_operations");
  }
  return store;
}

absl::StatusOr<MetaOperationArchiveExport> DecodeMetaOperationArchiveExport(
    std::string_view bytes) {
  MetaReader r(bytes);
  if (absl::Status status = ReadStoreSchemaVersion(r); !status.ok()) {
    return status;
  }
  auto summaries = r.ReadList<MetaOperationArchiveSummary>(
      kMaxMetaArchivedOperationSummaries,
      [](MetaReader& rr) { return ReadSummary(rr); });
  if (!summaries.ok()) return summaries.status();
  if (absl::Status status = r.Finish(); !status.ok()) return status;
  MetaOperationArchiveExport out;
  out.summaries_ = std::move(*summaries);
  return out;
}

}  // namespace keylane::meta
