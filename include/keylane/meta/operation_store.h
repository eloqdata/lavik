#pragma once

// MetaOperationStore is the metadata control plane's committed operation
// journal and generic lifecycle state machine.
//
// Identity and idempotency:
//   - operation_id is the CLIENT-PROVIDED stable UUID and the operation's
//     idempotency key while its live record or archive tombstone is retained.
//     SubmitOperation resolves by id across both sets: same id + same
//     intent_hash returns the existing record (or archived summary) unchanged;
//     same id + different intent_hash is payload reuse and rejects. Explicit
//     archive pruning ends that retry window and permits later id reuse.
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
//   transition commands do not even carry them). Phase/terminal mutation
//   commands carry expected_revision as the CAS token; on accept the revision
//   becomes expected_revision + 1 — the command schema has no new_revision
//   field, so the CAS pins the post-value deterministically. A first
//   CommitDirectiveResult has no client CAS: after validating the exact live
//   directive identity it appends the durable receipt and increments the
//   operation revision internally, preventing an older reconciler decision
//   from committing over newly authoritative workflow input.
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
// data_loss_possible, terminal receipts) so a late duplicate submit or
// directive-result replay deterministically resolves during the retention
// window. Non-terminal operations are never archivable. References to unknown
// ids/seqs reject.
//
// Bounded state: live non-terminal operations are capped by
// max_active (SubmitOperation creating beyond it rejects; terminal records
// stay live-but-inactive until archived), the whole live set — including
// terminal records awaiting archival — is capped by max_active + max_archived
// (SubmitOperation rejects at the joint bound; ArchiveOperations is the
// escape valve, keeping every collection bounded), archive summaries
// by max_archived (ArchiveOperations rejects at the cap; the operator exports
// a read-only versioned snapshot via ctl and then explicitly prunes the
// externally retained summaries), and a
// record's accumulated evidence summaries by
// kMaxMetaOperationEvidencePerRecord. Live records and archive summaries each
// retain at most max_terminal_receipts_per_operation exact result receipts.
//
// Apply is a pure in-memory function: no IO, no locks, NO CLOCK (the stored
// actor context is command-carried text), no observation access; evidence
// summaries are persisted, never observation references. Domain rejections
// return absl::Status of MetaFailureClass::kDomainReject. Snapshot
// serialization is the versioned strict encoding of encoding.h; decode
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
#include "keylane/meta/commands.h"
#include "keylane/meta/encoding.h"

namespace keylane::meta {

// Per-record accumulated evidence cap (deployment policy; bounded state).
inline constexpr std::uint32_t kMaxMetaOperationEvidencePerRecord = 1024;

enum class MetaOperationLifecycle : std::uint8_t {
  kSubmitted = 1,
  kRunning = 2,
  kCompleted = 3,  // terminal
  kAborted = 4,    // terminal
};

struct MetaCurrentDirective {
  MetaDirectiveSpec spec_;
  std::uint64_t directive_revision_ = 0;  // transition Raft apply index
  bool operator==(const MetaCurrentDirective&) const = default;
};

// Durable proof that Meta committed one exact data-node result. It is kept
// with the operation across live and archive state so a data node can replay
// after losing ResultCommitted and receive the original committed index.
struct MetaTerminalReceipt {
  MetaTerminalReceiptKey key_;
  std::string recipient_node_id_;
  MetaBootIncarnation recipient_boot_id_{};
  MetaAssignmentId assignment_id_{};
  MetaDirectiveResultStatus status_ = MetaDirectiveResultStatus::kSucceeded;
  std::string result_;
  std::uint64_t committed_index_ = 0;
  bool operator==(const MetaTerminalReceipt&) const = default;
};

// A live operation record. The canonical intent is retained because Raft log
// compaction removes the submit command; recovery and a new leader must still
// be able to reconstruct the operation without consulting old log entries.
struct MetaOperationRecord {
  MetaOperationId operation_id_{};
  std::uint64_t operation_seq_ = 0;  // raft log index of the submit
  std::string kind_;
  std::string intent_;
  MetaHash256 intent_hash_{};
  MetaReplicationHistoryId replication_history_id_{};
  MetaOperationLifecycle lifecycle_ = MetaOperationLifecycle::kSubmitted;
  // Opaque to committed apply; operation-specific coordinators own the schema.
  std::string kind_phase_blob_;
  std::vector<MetaCurrentDirective> current_directives_;
  std::vector<MetaTerminalReceipt> terminal_receipts_;
  // Phase CAS token; bumps on every accepted mutation, including the first
  // commit of each authoritative directive result (exact receipt replay does
  // not bump it again).
  std::uint64_t revision_ = 0;
  std::vector<MetaEvidenceSummary> evidence_;  // persisted summaries, in order
  std::string terminal_result_;  // Completed: result; Aborted: reason
  bool data_loss_possible_ = false;
  ActorContext actor_;  // submitter, copied from the command
  bool operator==(const MetaOperationRecord&) const = default;
};

// Tombstone of an archived terminal operation. Kept for the retention window
// so late duplicate submissions resolve deterministically and lost result
// acknowledgements can replay their original TerminalReceipt.
struct MetaOperationArchiveSummary {
  MetaOperationId operation_id_{};
  std::uint64_t operation_seq_ = 0;
  MetaHash256 intent_hash_{};
  ActorContext actor_;
  MetaOperationLifecycle terminal_lifecycle_ =
      MetaOperationLifecycle::kCompleted;  // kCompleted or kAborted only
  std::string terminal_result_;
  bool data_loss_possible_ = false;
  std::vector<MetaTerminalReceipt> terminal_receipts_;
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
      std::uint32_t max_archived = kMaxMetaArchivedOperationSummaries,
      std::uint32_t max_terminal_receipts_per_operation =
          kMaxMetaTerminalReceiptsPerOperation)
      : max_active_(max_active),
        max_archived_(max_archived),
        max_terminal_receipts_per_operation_(
            max_terminal_receipts_per_operation) {}

  // operation_seq is the raft log index of this very command, supplied by the
  // apply caller. See the file header for the idempotency and fail-stop
  // semantics.
  absl::StatusOr<MetaSubmitResult> SubmitOperation(
      const SubmitOperation& command, std::uint64_t operation_seq);
  // committed_index becomes each newly installed directive's
  // directive_revision; exact phase-transition replay preserves it.
  absl::Status TransitionOperationPhase(const TransitionOperationPhase& command,
                                        std::uint64_t committed_index = 1);
  absl::Status CompleteOperation(const CompleteOperation& command);
  absl::Status AbortOperation(const AbortOperation& command);
  // committed_index is retained in the first exact terminal receipt. Exact
  // result replay returns that receipt unchanged and does not bump revision.
  absl::Status CommitDirectiveResult(const CommitDirectiveResult& command,
                                     std::uint64_t committed_index);
  absl::Status ArchiveOperations(const ArchiveOperations& command);
  absl::Status PruneArchive(const PruneOperationArchive& command);
  absl::Status PruneTerminalReceipts(
      const keylane::meta::PruneTerminalReceipts& command);

  // Removes exact live directive attempts whose committed cross-store anchor
  // is no longer valid. The caller supplies full receipt keys so a later
  // attempt reusing a directive id cannot be removed accidentally. Every
  // affected operation advances its phase CAS revision once, regardless of
  // how many of its directives are cleared; replay with already-absent keys
  // is a deterministic no-op. Revision saturation never wraps.
  void InvalidateCurrentDirectives(
      const std::vector<MetaTerminalReceiptKey>& directive_keys);

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
  // Absence has the wire-level meaning ResultNoLongerTracked. A tracked
  // receipt always returns its first commit index, including after archival.
  std::optional<MetaTerminalReceipt> FindTerminalReceipt(
      const MetaTerminalReceiptKey& key) const;
  std::vector<MetaOperationRecord> LiveOperations() const;
  // True only for a non-terminal live record of the requested durable kind.
  // Status callers use this bounded fact instead of copying the operation
  // journal and its evidence payloads.
  bool HasActiveKind(std::string_view kind) const;
  bool OperationKnown(const MetaOperationId& id) const {
    return live_.contains(id) || archived_.contains(id);
  }
  // True when the command's exact post-effect is already the record's current
  // state. Apply uses this to preserve replay after committed evidence anchors
  // have legitimately advanced.
  bool TransitionAlreadyApplied(
      const keylane::meta::TransitionOperationPhase& command) const;
  // Active operation evidence/directives retain manifest documents needed to
  // validate or resume their current phase.
  bool PopulationManifestInUse(const MetaHash256& digest) const;
  std::size_t ActiveCount() const { return active_count_; }  // non-terminal
  std::size_t LiveCount() const { return live_.size(); }
  std::size_t ArchivedCount() const { return archived_.size(); }

  // Read-only versioned encoding of all archive summaries for ctl export. At
  // the cap, retain this result externally and then submit a replicated
  // PruneOperationArchive command; export alone does not remove summaries.
  absl::StatusOr<std::string> ExportArchive() const;

  // Snapshot serialization: versioned strict encoding; decode enforces caps
  // and identity invariants (unique ids/seqs, terminal-only archive) with
  // MetaFailureClass::kFailStop on violation.
  absl::StatusOr<std::string> Serialize() const;
  static absl::StatusOr<MetaOperationStore> Deserialize(
      std::string_view bytes,
      std::uint32_t max_active = kMaxMetaActiveOperations,
      std::uint32_t max_archived = kMaxMetaArchivedOperationSummaries,
      std::uint32_t max_terminal_receipts_per_operation =
          kMaxMetaTerminalReceiptsPerOperation);

 private:
  std::uint32_t max_active_;
  std::uint32_t max_archived_;
  std::uint32_t max_terminal_receipts_per_operation_;
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
