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

bool DomainCanonicalLess(const MetaFailoverCompatibilityDomain& left,
                         const MetaFailoverCompatibilityDomain& right) {
  return std::tie(left.source_group_term_, left.source_node_id_,
                  left.source_assignment_id_, left.source_boot_id_,
                  left.source_history_id_, left.flow_count_) <
         std::tie(right.source_group_term_, right.source_node_id_,
                  right.source_assignment_id_, right.source_boot_id_,
                  right.source_history_id_, right.flow_count_);
}

bool NewestDomainFirst(const MetaFailoverCompatibilityDomain& left,
                       const MetaFailoverCompatibilityDomain& right) {
  if (left.source_group_term_ != right.source_group_term_) {
    return left.source_group_term_ > right.source_group_term_;
  }
  return DomainCanonicalLess(left, right);
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

CandidatePlan SelectWithinDomain(
    std::vector<MetaCandidateProgressObs> candidates) {
  CandidatePlan plan;
  if (candidates.empty()) return plan;
  std::sort(candidates.begin(), candidates.end(),
            [](const auto& left, const auto& right) {
              return left.node_id_ < right.node_id_;
            });

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
    const bool equal =
        std::all_of(maximal.begin() + 1, maximal.end(), [&](std::size_t index) {
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
          envelope[flow] = std::max(envelope[flow],
                                    candidates[index].applied_next_lsns_[flow]);
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

}  // namespace

MetaFailoverCompatibilityDomain CandidateCompatibilityDomain(
    const MetaCandidateProgressObs& candidate) {
  return {
      .source_group_term_ = candidate.source_group_term_,
      .source_node_id_ = candidate.source_node_id_,
      .source_assignment_id_ = candidate.source_assignment_id_,
      .source_boot_id_ = candidate.source_boot_incarnation_,
      .source_history_id_ = candidate.source_replication_history_id_,
      .flow_count_ =
          static_cast<std::uint32_t>(candidate.applied_next_lsns_.size()),
  };
}

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

  const MetaFailoverCompatibilityDomain domain =
      CandidateCompatibilityDomain(candidates.front());
  if (std::any_of(candidates.begin() + 1, candidates.end(),
                  [&](const auto& candidate) {
                    return CandidateCompatibilityDomain(candidate) != domain;
                  })) {
    plan.disposition_ = CandidatePlanDisposition::kMultipleCompatibilityDomains;
    return plan;
  }
  return SelectWithinDomain(std::move(candidates));
}

CandidatePlan CandidatePlanForDomain(
    std::string_view group_id,
    const MetaFailoverCompatibilityDomain& required_domain,
    const MetaCommittedFacts& facts, const MetaObservationStore& observations,
    std::int64_t now_unix_ms) {
  CandidatePlan plan;
  if (facts.CurrentGroupTerm(group_id) == 0) {
    plan.disposition_ = CandidatePlanDisposition::kGroupUnknown;
    return plan;
  }
  std::vector<MetaCandidateProgressObs> candidates =
      observations.LiveCandidateProgressFor(group_id, facts, now_unix_ms);
  std::erase_if(candidates, [&](const auto& candidate) {
    return CandidateCompatibilityDomain(candidate) != required_domain;
  });
  return SelectWithinDomain(std::move(candidates));
}

CandidatePlan UncontrolledCandidatePlanFor(
    std::string_view group_id, const MetaCommittedFacts& facts,
    const MetaObservationStore& observations, std::int64_t now_unix_ms,
    const std::optional<MetaFailoverCandidateAction>& excluded_action) {
  CandidatePlan plan;
  if (facts.CurrentGroupTerm(group_id) == 0) {
    plan.disposition_ = CandidatePlanDisposition::kGroupUnknown;
    return plan;
  }
  std::vector<MetaCandidateProgressObs> candidates =
      observations.LiveCandidateProgressFor(group_id, facts, now_unix_ms);
  if (excluded_action.has_value()) {
    std::erase_if(candidates, [&](const auto& candidate) {
      return candidate.node_id_ == excluded_action->candidate_.node_id_ &&
             candidate.assignment_id_ ==
                 excluded_action->candidate_.assignment_id_ &&
             candidate.boot_incarnation_ ==
                 excluded_action->candidate_.boot_id_ &&
             CandidateCompatibilityDomain(candidate) ==
                 excluded_action->domain_;
    });
  }
  if (candidates.empty()) return plan;

  std::vector<MetaFailoverCompatibilityDomain> domains;
  domains.reserve(candidates.size());
  for (const auto& candidate : candidates) {
    const auto domain = CandidateCompatibilityDomain(candidate);
    if (std::ranges::find(domains, domain) == domains.end()) {
      domains.push_back(domain);
    }
  }
  std::sort(domains.begin(), domains.end(), NewestDomainFirst);

  for (const auto& domain : domains) {
    std::vector<MetaCandidateProgressObs> exact;
    std::ranges::copy_if(
        candidates, std::back_inserter(exact), [&](const auto& candidate) {
          return CandidateCompatibilityDomain(candidate) == domain;
        });
    CandidatePlan selected = SelectWithinDomain(std::move(exact));
    if (selected.disposition_ == CandidatePlanDisposition::kSelected) {
      return selected;
    }
  }
  return plan;
}

}  // namespace keylane::meta
