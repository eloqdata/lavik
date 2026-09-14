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
// not an operation or RPC contract: the committed failover reconciler consumes
// the selected observation immediately and owns every later revalidation.
struct CandidatePlan {
  CandidatePlanDisposition disposition_ =
      CandidatePlanDisposition::kNoEligibleCandidates;
  std::optional<CandidateSelectionBasis> selection_basis_;
  std::optional<MetaCandidateProgressObs> selected_;
  // Node-sorted maximal candidates retained for diagnostics and audit input.
  std::vector<std::string> maximal_node_ids_;
};

// Returns the exact lineage domain in which this candidate's per-flow LSNs
// are comparable. Callers must only pass a validated typed observation.
MetaFailoverCompatibilityDomain CandidateCompatibilityDomain(
    const MetaCandidateProgressObs& candidate);

// Selects a completed replica population at one fixed receive-time cut. The
// selector depends only on committed group facts plus member-scoped semantic
// anchors and non-extendable observation TTLs; unrelated heartbeat traffic
// cannot invalidate or restart the calculation.
CandidatePlan CandidatePlanFor(std::string_view group_id,
                               const MetaCommittedFacts& facts,
                               const MetaObservationStore& observations,
                               std::int64_t now_unix_ms);

// Controlled failover compares only replicas copied from the exact current
// source incarnation. Other live domains are ignored, never compared by LSN.
CandidatePlan CandidatePlanForDomain(
    std::string_view group_id,
    const MetaFailoverCompatibilityDomain& required_domain,
    const MetaCommittedFacts& facts, const MetaObservationStore& observations,
    std::int64_t now_unix_ms);

// Uncontrolled failover considers compatibility domains newest source term
// first and falls back one exact domain at a time. Domains at the same term
// use canonical identity order, so every Meta leader makes the same choice.
CandidatePlan UncontrolledCandidatePlanFor(
    std::string_view group_id, const MetaCommittedFacts& facts,
    const MetaObservationStore& observations, std::int64_t now_unix_ms,
    const std::optional<MetaFailoverCandidateAction>& excluded_action =
        std::nullopt);

}  // namespace keylane::meta
