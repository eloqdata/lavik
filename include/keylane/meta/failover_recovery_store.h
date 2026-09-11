#pragma once

// MetaFailoverRecoveryStore owns the group-scoped durable handoff between
// independent controlled and uncontrolled failover operations. It contains
// only source/exclusion/population facts needed to preserve or conservatively
// downgrade recovery; candidate attempts remain in the operation journal.

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

struct MetaFailoverRecoveryRecord {
  std::string group_id_;
  // Raft apply index of the latest absolute replacement.
  std::uint64_t revision_ = 0;
  std::uint64_t recovery_generation_ = 0;
  std::string old_source_node_id_;
  MetaAssignmentId old_source_assignment_id_{};
  MetaBootIncarnation old_source_boot_incarnation_{};
  MetaReplicationHistoryId old_source_history_id_{};
  std::uint64_t excluded_authority_term_ = 0;
  std::uint64_t excluded_authority_version_ = 0;
  std::uint64_t excluded_grant_revision_ = 0;
  std::uint64_t population_manifest_revision_ = 0;
  MetaHash256 population_manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  bool hold_required_ = false;
  // Recovery handoff always retains the shared source history hold. Clearing
  // the hold is a terminal release state, never an alternate recovery mode.
  bool recovery_required_ = false;
  MetaFailoverProofState proof_state_ = MetaFailoverProofState::kPending;
  std::optional<MetaFailoverFrozenProof> frozen_proof_;
  bool operator==(const MetaFailoverRecoveryRecord&) const = default;
};

class MetaFailoverRecoveryStore {
 public:
  explicit MetaFailoverRecoveryStore(std::uint32_t max_groups = kMaxMetaGroups)
      : max_groups_(max_groups) {}

  // Creates or CAS-replaces the active record. Anchors are immutable within a
  // generation; a successor generation may replace them atomically only after
  // the current generation has entered explicit recovery handoff. Released
  // tombstones must be cleared before reuse. Proof availability and loss flags
  // move only in the conservative direction. A
  // terminal same-generation replacement may clear both flags while retaining
  // the identity as an FDS-release tombstone; only Clear removes that record
  // after the exact old boot acknowledges the hold's absence. An exact final
  // frontier may be retained for audit after source availability degrades to
  // kUnavailable; it can never be changed or used alone to claim lossless
  // recovery. Exact replay at the same committed index is an idempotent no-op.
  absl::Status Set(const SetFailoverRecovery& command,
                   std::uint64_t committed_index);
  // Clears only an exact active generation/revision whose hold and recovery
  // desired-state flags are both released, then retains a cursor for that
  // mutation. Replaying the same clear after removal is accepted.
  absl::Status Clear(const ClearFailoverRecovery& command,
                     std::uint64_t committed_index);

  // Returns only the active record. A cleared generation still contributes to
  // LastGeneration/LastRevision but is intentionally absent here.
  std::optional<MetaFailoverRecoveryRecord> Find(
      std::string_view group_id) const;
  // Highest generation ever created, including a cleared tombstone.
  std::optional<std::uint64_t> LastGeneration(std::string_view group_id) const;
  // Latest Set/Clear revision, including a cleared generation tombstone. A
  // successor generation CASes this cursor so there is no clear/create crash
  // gap or ambiguous delayed replay.
  std::optional<std::uint64_t> LastRevision(std::string_view group_id) const;
  // Active records only, in canonical group-id order.
  std::vector<MetaFailoverRecoveryRecord> Records() const;
  // True only when an active record pins the manifest.
  bool PopulationManifestInUse(const MetaHash256& digest) const;
  // True only when an active record pins this exact source assignment.
  bool SourceAssignmentInUse(std::string_view node_id,
                             const MetaAssignmentId& assignment_id) const;
  // Number of retained group entries, including cleared generation
  // tombstones; this is the capacity-accounted size.
  std::size_t Size() const;

  // Snapshot v1 stores every group generation floor plus its optional active
  // record. Keeping cleared tombstones is required for monotonic generations;
  // count and flow vectors are bounded and decode violations fail stop.
  std::string Serialize() const;
  static absl::StatusOr<MetaFailoverRecoveryStore> Deserialize(
      std::string_view bytes, std::uint32_t max_groups = kMaxMetaGroups);

 private:
  struct Entry {
    std::uint64_t generation_floor_ = 0;
    std::uint64_t revision_ = 0;
    // The active revision consumed by the latest Clear. It makes an exact
    // clear replay distinguishable from another command for the same floor.
    std::uint64_t cleared_expected_revision_ = 0;
    std::optional<MetaFailoverRecoveryRecord> record_;
  };

  std::uint32_t max_groups_;
  std::map<std::string, Entry> entries_;
};

}  // namespace keylane::meta
