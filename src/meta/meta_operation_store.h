#pragma once

// MetaOperationStore is the metadata control plane's committed operation
// journal and generic lifecycle state machine.
//
// Identity and idempotency:
//   - operation_id is the CLIENT-PROVIDED stable UUID and the operation's
//     PERMANENT idempotency key. SubmitOperation resolves by id across both
//     the live records and the archive tombstones: same id + same intent_hash
//     returns the existing record (or archived summary) unchanged; same id +
//     different intent_hash is payload reuse and rejects.
//   - operation_seq is the raft log index of the SubmitOperation command,
//     handed in by the apply caller. This requires no separate counter and is
//     naturally unique, monotonic, and consistent across nodes. It is a pure
//     reference for ordering and archival. A seq collision between two
//     different ids means the caller lost the index correspondence — an
//     apply-layer bug, so the
//     store FAILS STOP (spdlog::critical + abort, the system_exit policy).
//
// Generic lifecycle machine (kind-specific phase-graph legality belongs to
// coordinator ValidateProposal plugins, never to apply):
//   Submitted -> Running -> Completed | Aborted (terminal states are
//   irreversible; Completed/Aborted are also reachable directly from
//   Submitted). kind and intent_hash are immutable after submit (the
//   transition commands do not even carry them). Mutation commands carry
//   expected_revision as the CAS token; on accept the revision becomes
//   expected_revision + 1 — the command schema has no new_revision field, so
//   the CAS pins the post-value deterministically.
//
// Replay idempotency: re-applying a command at the same log index
// reproduces the same verdict and state. Each mutation first checks whether
// its post-effect is already present with identical content and accepts as a
// no-op; only genuinely conflicting content rejects (kDomainReject).
//
// Non-contiguous archival: ArchiveOperations moves a SET
// of terminal operations to archive summaries, so a long-Running operation
// never blocks archival. The command is atomic: every seq must resolve to a
// live terminal record or an already-archived summary (idempotent no-op), or
// the whole command rejects. Tombstone summaries keep
// (operation_id, operation_seq, intent_hash, actor, terminal state,
// data_loss_possible) so a late duplicate submit deterministically resolves
// as "already done" during the retention window. Non-terminal operations are
// never archivable. References to unknown ids/seqs reject.
//
// Bounded state: live non-terminal operations are capped by
// max_active (SubmitOperation creating beyond it rejects; terminal records
// stay live-but-inactive until archived), the whole live set — including
// terminal records awaiting archival — is capped by max_active + max_archived
// (SubmitOperation rejects at the joint bound; ArchiveOperations is the
// escape valve, keeping every collection bounded), archive summaries
// by max_archived (ArchiveOperations rejects at the cap; the operator exports
// via ctl — ExportArchive drains the summaries as versioned bytes), and a
// record's accumulated evidence summaries by
// kMaxMetaOperationEvidencePerRecord.
//
// Apply is a pure in-memory function: no IO, no locks, NO CLOCK (the stored
// actor context is command-carried text), no observation access; evidence
// summaries are persisted, never observation references. Domain rejections
// return absl::Status of MetaFailureClass::kDomainReject. Snapshot
// serialization is the versioned strict encoding of meta_encoding.h; decode
// failures (unknown version, cap violation, broken identity invariants) are
// MetaFailureClass::kFailStop.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "meta/meta_commands.h"
#include "meta/meta_encoding.h"

namespace keylane::meta {

// Per-record accumulated evidence cap (v1 policy value; bounded state).
inline constexpr std::uint32_t kMaxMetaOperationEvidencePerRecord = 1024;

enum class MetaOperationLifecycle : std::uint8_t {
  kSubmitted = 1,
  kRunning = 2,
  kCompleted = 3,  // terminal
  kAborted = 4,    // terminal
};

// A live operation record. The full intent blob is deliberately NOT retained:
// the record carries the intent_hash only (see the task field list; the log
// holds the original command).
struct MetaOperationRecord {
  MetaOperationId operation_id_{};
  std::uint64_t operation_seq_ = 0;  // raft log index of the submit
  std::string kind_;
  MetaHash256 intent_hash_{};
  std::uint64_t replication_history_id_ = 0;
  std::vector<MetaPolicyReference> policy_references_;
  MetaOperationLifecycle lifecycle_ = MetaOperationLifecycle::kSubmitted;
  // Opaque to committed apply; operation-specific coordinators own the schema.
  std::string kind_phase_blob_;
  std::uint64_t revision_ = 0;   // CAS token; bumps on every accepted mutation
  std::vector<MetaEvidenceSummary> evidence_;  // persisted summaries, in order
  std::string terminal_result_;  // Completed: result; Aborted: reason
  bool data_loss_possible_ = false;
  ActorContext actor_;  // submitter, copied from the command
  bool operator==(const MetaOperationRecord&) const = default;
};

// Tombstone of an archived terminal operation. Kept
// for the retention window so late duplicate submissions resolve
// deterministically.
struct MetaOperationArchiveSummary {
  MetaOperationId operation_id_{};
  std::uint64_t operation_seq_ = 0;
  MetaHash256 intent_hash_{};
  ActorContext actor_;
  MetaOperationLifecycle terminal_lifecycle_ =
      MetaOperationLifecycle::kCompleted;  // kCompleted or kAborted only
  std::string terminal_result_;
  bool data_loss_possible_ = false;
  bool operator==(const MetaOperationArchiveSummary&) const = default;
};

// SubmitOperation outcome: whether a record was created, and whether an
// idempotent duplicate resolved via the archive tombstone index.
struct MetaSubmitResult {
  bool created_ = false;
  bool archived_ = false;
  bool operator==(const MetaSubmitResult&) const = default;
};

class MetaOperationStore {
 public:
  explicit MetaOperationStore(
      std::uint32_t max_active = kMaxMetaActiveOperations,
      std::uint32_t max_archived = kMaxMetaArchivedOperationSummaries)
      : max_active_(max_active), max_archived_(max_archived) {}

  // operation_seq is the raft log index of this very command, supplied by the
  // apply caller. See the file header for the idempotency and fail-stop
  // semantics.
  absl::StatusOr<MetaSubmitResult> SubmitOperation(
      const SubmitOperation& command, std::uint64_t operation_seq);
  absl::Status TransitionOperationPhase(
      const TransitionOperationPhase& command);
  absl::Status CompleteOperation(const CompleteOperation& command);
  absl::Status AbortOperation(const AbortOperation& command);
  absl::Status ArchiveOperations(const ArchiveOperations& command);
  absl::Status PruneArchive(const PruneOperationArchive& command);

  // Fact queries. Archived ids/seqs resolve to their terminal summary —
  // "already done" — while unknown ones return nullopt.
  std::optional<MetaOperationRecord> FindOperation(
      const MetaOperationId& id) const;
  std::optional<MetaOperationRecord> FindOperationBySeq(
      std::uint64_t seq) const;
  std::optional<MetaOperationArchiveSummary> FindArchived(
      const MetaOperationId& id) const;
  std::optional<MetaOperationArchiveSummary> FindArchivedBySeq(
      std::uint64_t seq) const;
  bool OperationKnown(const MetaOperationId& id) const {
    return live_.contains(id) || archived_.contains(id);
  }
  // True when the command's exact post-effect is already the record's current
  // state. Apply uses this to preserve replay after committed evidence anchors
  // have legitimately advanced.
  bool TransitionAlreadyApplied(
      const keylane::meta::TransitionOperationPhase& command) const;
  // True only for a live Submitted/Running operation. Terminal records no
  // longer block retirement even before archival.
  bool PolicyInUse(std::string_view policy_id, std::uint64_t version) const;
  std::size_t ActiveCount() const { return active_count_; }  // non-terminal
  std::size_t LiveCount() const { return live_.size(); }
  std::size_t ArchivedCount() const { return archived_.size(); }

  // Versioned byte drain of all archive summaries for ctl export. At the cap,
  // the operator must export before ArchiveOperations accepts.
  absl::StatusOr<std::string> ExportArchive() const;

  // Snapshot serialization: versioned strict encoding; decode enforces caps
  // and identity invariants (unique ids/seqs, terminal-only archive) with
  // MetaFailureClass::kFailStop on violation.
  absl::StatusOr<std::string> Serialize() const;
  static absl::StatusOr<MetaOperationStore> Deserialize(
      std::string_view bytes,
      std::uint32_t max_active = kMaxMetaActiveOperations,
      std::uint32_t max_archived = kMaxMetaArchivedOperationSummaries);

 private:
  std::uint32_t max_active_;
  std::uint32_t max_archived_;
  std::map<MetaOperationId, MetaOperationRecord> live_;
  std::map<std::uint64_t, MetaOperationId> live_by_seq_;
  std::map<MetaOperationId, MetaOperationArchiveSummary> archived_;
  std::map<std::uint64_t, MetaOperationId> archived_by_seq_;
  std::uint32_t active_count_ = 0;  // live_ entries in Submitted/Running
};

// Decoded archive export blob (see MetaOperationStore::ExportArchive).
struct MetaOperationArchiveExport {
  std::vector<MetaOperationArchiveSummary> summaries_;
  bool operator==(const MetaOperationArchiveExport&) const = default;
};

absl::StatusOr<MetaOperationArchiveExport> DecodeMetaOperationArchiveExport(
    std::string_view bytes);

}  // namespace keylane::meta
