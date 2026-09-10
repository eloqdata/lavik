#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "keylane/meta/observation_store.h"

namespace keylane::meta {

enum class CandidatePlanDisposition : std::uint8_t {
  kSelected,
  kGroupUnknown,
  kNoEligibleCandidates,
  kMultipleCompatibilityDomains,
};

enum class CandidateSelectionBasis : std::uint8_t {
  kUniqueGreatest,
  kEqualGreatestNodeTieBreak,
  kIncomparableEnvelopeDeficit,
};

// Immutable result of one internal failover-planning call. It is deliberately
// not an operation or RPC contract: future failover orchestration consumes the
// selected observation immediately and owns any later validation it needs.
struct CandidatePlan {
  CandidatePlanDisposition disposition_ =
      CandidatePlanDisposition::kNoEligibleCandidates;
  std::optional<CandidateSelectionBasis> selection_basis_;
  std::optional<MetaCandidateProgressObs> selected_;
  // Node-sorted maximal candidates retained for diagnostics and audit input.
  std::vector<std::string> maximal_node_ids_;
};

// Selects a completed replica population at one fixed receive-time cut. The
// selector depends only on committed group facts plus member-scoped semantic
// anchors and non-extendable observation TTLs; unrelated heartbeat traffic
// cannot invalidate or restart the calculation.
CandidatePlan CandidatePlanFor(std::string_view group_id,
                               const MetaCommittedFacts& facts,
                               const MetaObservationStore& observations,
                               std::int64_t now_unix_ms);

}  // namespace keylane::meta
