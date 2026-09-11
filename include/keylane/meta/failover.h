#pragma once

// Typed state carried inside the generic MetaOperationRecord for one
// top-level failover. The journal remains operation-kind agnostic; this codec
// gives the failover reconciler a strict, replayable contract without adding
// nested promotion operations.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "keylane/meta/commands.h"

namespace keylane::meta {

class MetaCommittedView;
class MetaObservationStore;

inline constexpr std::string_view kFailoverOperationKind = "failover";

struct FailoverIntent {
  std::string group_id_;
  std::string former_owner_node_id_;
  MetaAssignmentId former_owner_assignment_id_{};
  MetaBootIncarnation former_owner_boot_id_{};
  std::string candidate_node_id_;
  MetaAssignmentId candidate_assignment_id_{};
  MetaBootIncarnation candidate_boot_id_{};
  std::uint64_t group_term_ = 0;
  std::uint64_t authority_version_ = 0;
  std::uint64_t grant_revision_ = 0;
  std::uint64_t population_manifest_revision_ = 0;
  MetaHash256 population_manifest_digest_{};
  std::uint64_t partition_replication_epoch_ = 0;
  MetaReplicationHistoryId parent_history_id_{};

  bool operator==(const FailoverIntent&) const = default;
};

enum class FailoverPhaseStage : std::uint8_t {
  kOldAuthorityExcluded = 1,
  kCandidateCaughtUp = 2,
  kPromotionPreparing = 3,
  kPromotionPrepared = 4,
  // Reserved for the authority-activation workflow. The preparation validator
  // executes only through Prepared.
  kAuthorityActivated = 5,
  kServing = 6,
};

struct FailoverPhase {
  FailoverPhaseStage stage_ = FailoverPhaseStage::kOldAuthorityExcluded;
  MetaHash256 old_authority_exclusion_hash_{};
  std::vector<std::uint64_t> required_applied_next_lsns_;
  MetaHash256 prepared_result_hash_{};

  bool operator==(const FailoverPhase&) const = default;
};

// Encoders reject invalid domain values before producing the canonical,
// versioned bytes stored in MetaOperationRecord. Decoders fail-stop on an
// unknown version, corrupt/trailing bytes, or decoded state that violates the
// same invariants: committed failover blobs are never repaired heuristically.
absl::StatusOr<std::string> EncodeFailoverIntent(const FailoverIntent& intent);
absl::StatusOr<FailoverIntent> DecodeFailoverIntent(std::string_view encoded);
absl::StatusOr<std::string> EncodeFailoverPhase(const FailoverPhase& phase);
absl::StatusOr<FailoverPhase> DecodeFailoverPhase(std::string_view encoded);

// Coordinator hook for the promotion-preparation portion of the failover phase
// graph. It is a no-op for unrelated operation kinds and rejects any failover
// transition that skips the committed authority-exclusion boundary, changes
// intent anchors, or claims prepared evidence without a TTL-fresh observation
// from the candidate's current session that exactly matches the successful
// directive receipt. Corrupt committed typed state is rejected conservatively;
// this hook never advances authority itself.
absl::Status ValidateFailoverProposal(
    const MetaCommand& command, const MetaCommittedView& view,
    const MetaObservationStore& observations);

}  // namespace keylane::meta
