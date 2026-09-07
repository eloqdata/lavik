#include "keylane/meta/operation_store.h"

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <set>

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

}  // namespace

absl::StatusOr<MetaSubmitResult> MetaOperationStore::SubmitOperation(
    const keylane::meta::SubmitOperation& command,
    std::uint64_t operation_seq) {
  if (command.kind_.empty() ||
      command.kind_.size() > kMaxMetaOperationKindBytes) {
    return MetaDomainRejectError("operation kind is empty or exceeds its cap");
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
    if (it->second.intent_hash_ != command.intent_hash_) {
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

absl::Status MetaOperationStore::TransitionOperationPhase(
    const keylane::meta::TransitionOperationPhase& command) {
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
  if (record.evidence_.size() + command.evidence_.size() >
      kMaxMetaOperationEvidencePerRecord) {
    return MetaDomainRejectError("per-record evidence cap reached");
  }
  for (const MetaEvidenceSummary& evidence : command.evidence_) {
    if (evidence.operation_id_ != command.operation_id_) {
      return MetaDomainRejectError("evidence references a different operation");
    }
    if (record.replication_history_id_ == 0 ||
        evidence.replication_history_id_ != record.replication_history_id_) {
      return MetaDomainRejectError(
          "evidence replication history does not match the operation");
    }
  }
  record.lifecycle_ = MetaOperationLifecycle::kRunning;
  record.kind_phase_blob_ = command.kind_phase_blob_;
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
  record.terminal_result_ = command.reason_;
  record.data_loss_possible_ = false;
  record.revision_ = command.expected_revision_ + 1;
  --active_count_;
  return absl::OkStatus();
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

void WriteRecord(MetaWriter& w, const MetaOperationRecord& record) {
  WriteFixedArray(w, record.operation_id_);
  w.WriteU64(record.operation_seq_);
  w.WriteString(record.kind_);
  WriteFixedArray(w, record.intent_hash_);
  w.WriteU64(record.replication_history_id_);
  w.WriteList(record.policy_references_, WriteMetaPolicyReference);
  w.WriteU8(static_cast<std::uint8_t>(record.lifecycle_));
  w.WriteString(record.kind_phase_blob_);
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
  auto intent_hash = ReadFixedArray<32>(r);
  if (!intent_hash.ok()) return intent_hash.status();
  record.intent_hash_ = *intent_hash;
  auto replication_history = r.ReadU64();
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
    std::uint32_t max_archived) {
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

  MetaOperationStore store(max_active, max_archived);
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
      if (evidence.operation_id_ != record.operation_id_ ||
          record.replication_history_id_ == 0 ||
          evidence.replication_history_id_ != record.replication_history_id_) {
        return MetaFailStopError(
            "operation evidence does not match committed anchors");
      }
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
