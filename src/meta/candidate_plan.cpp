#include "keylane/meta/candidate_plan.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/numeric/int128.h"

namespace keylane::meta {
namespace {

struct CompatibilityDomain {
  std::uint64_t manifest_revision_ = 0;
  MetaHash256 manifest_digest_{};
  std::uint64_t partition_epoch_ = 0;
  std::string source_node_id_;
  MetaAssignmentId source_assignment_id_{};
  MetaBootIncarnation source_boot_incarnation_{};
  MetaReplicationHistoryId source_history_id_{};
  std::size_t flow_count_ = 0;

  friend bool operator<(const CompatibilityDomain& left,
                        const CompatibilityDomain& right) {
    return std::tie(left.manifest_revision_, left.manifest_digest_,
                    left.partition_epoch_, left.source_node_id_,
                    left.source_assignment_id_,
                    left.source_boot_incarnation_, left.source_history_id_,
                    left.flow_count_) <
           std::tie(right.manifest_revision_, right.manifest_digest_,
                    right.partition_epoch_, right.source_node_id_,
                    right.source_assignment_id_,
                    right.source_boot_incarnation_, right.source_history_id_,
                    right.flow_count_);
  }
};

CompatibilityDomain DomainOf(const MetaCandidateProgressObs& candidate) {
  return {
      .manifest_revision_ = candidate.population_manifest_revision_,
      .manifest_digest_ = candidate.population_manifest_digest_,
      .partition_epoch_ = candidate.partition_replication_epoch_,
      .source_node_id_ = candidate.source_node_id_,
      .source_assignment_id_ = candidate.source_assignment_id_,
      .source_boot_incarnation_ = candidate.source_boot_incarnation_,
      .source_history_id_ = candidate.source_replication_history_id_,
      .flow_count_ = candidate.applied_next_lsns_.size(),
  };
}

bool StrictlyDominates(const MetaCandidateProgressObs& left,
                       const MetaCandidateProgressObs& right) {
  bool greater = false;
  for (std::size_t flow = 0; flow < left.applied_next_lsns_.size(); ++flow) {
    if (left.applied_next_lsns_[flow] < right.applied_next_lsns_[flow]) {
      return false;
    }
    greater = greater ||
              left.applied_next_lsns_[flow] > right.applied_next_lsns_[flow];
  }
  return greater;
}

}  // namespace

CandidatePlan CandidatePlanFor(std::string_view group_id,
                               const MetaCommittedFacts& facts,
                               const MetaObservationStore& observations,
                               std::int64_t now_unix_ms) {
  CandidatePlan plan;
  if (facts.CurrentGroupTerm(group_id) == 0) {
    plan.disposition_ = CandidatePlanDisposition::kGroupUnknown;
    return plan;
  }

  std::vector<MetaCandidateProgressObs> candidates =
      observations.LiveCandidateProgressFor(group_id, facts, now_unix_ms);
  if (candidates.empty()) return plan;
  std::sort(candidates.begin(), candidates.end(),
            [](const auto& left, const auto& right) {
              return left.node_id_ < right.node_id_;
            });

  const CompatibilityDomain domain = DomainOf(candidates.front());
  if (std::any_of(candidates.begin() + 1, candidates.end(),
                  [&](const auto& candidate) {
                    const CompatibilityDomain other = DomainOf(candidate);
                    return domain < other || other < domain;
                  })) {
    plan.disposition_ =
        CandidatePlanDisposition::kMultipleCompatibilityDomains;
    return plan;
  }

  std::vector<std::size_t> maximal;
  for (std::size_t candidate = 0; candidate < candidates.size(); ++candidate) {
    bool dominated = false;
    for (std::size_t other = 0; other < candidates.size(); ++other) {
      if (candidate != other &&
          StrictlyDominates(candidates[other], candidates[candidate])) {
        dominated = true;
        break;
      }
    }
    if (!dominated) maximal.push_back(candidate);
  }
  for (std::size_t index : maximal) {
    plan.maximal_node_ids_.push_back(candidates[index].node_id_);
  }

  std::size_t selected = maximal.front();
  if (maximal.size() == 1) {
    plan.selection_basis_ = CandidateSelectionBasis::kUniqueGreatest;
  } else {
    const bool equal = std::all_of(
        maximal.begin() + 1, maximal.end(), [&](std::size_t index) {
          return candidates[index].applied_next_lsns_ ==
                 candidates[maximal.front()].applied_next_lsns_;
        });
    if (equal) {
      // candidates and maximal are node-sorted, so the first is deterministic.
      plan.selection_basis_ =
          CandidateSelectionBasis::kEqualGreatestNodeTieBreak;
    } else {
      std::vector<std::uint64_t> envelope(
          candidates[selected].applied_next_lsns_.size(), 0);
      for (std::size_t index : maximal) {
        for (std::size_t flow = 0; flow < envelope.size(); ++flow) {
          envelope[flow] = std::max(
              envelope[flow], candidates[index].applied_next_lsns_[flow]);
        }
      }
      auto deficit = [&](std::size_t index) {
        absl::uint128 sum = 0;
        std::uint64_t maximum = 0;
        for (std::size_t flow = 0; flow < envelope.size(); ++flow) {
          const std::uint64_t item =
              envelope[flow] - candidates[index].applied_next_lsns_[flow];
          sum += item;
          maximum = std::max(maximum, item);
        }
        return std::tuple{sum, maximum, candidates[index].node_id_};
      };
      for (std::size_t index : maximal) {
        if (deficit(index) < deficit(selected)) selected = index;
      }
      plan.selection_basis_ =
          CandidateSelectionBasis::kIncomparableEnvelopeDeficit;
    }
  }
  plan.disposition_ = CandidatePlanDisposition::kSelected;
  plan.selected_ = std::move(candidates[selected]);
  return plan;
}

}  // namespace keylane::meta
