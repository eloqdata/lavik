/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "lavik/meta/automatic_failover_detector.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "absl/status/status.h"

namespace lavik::meta {
namespace {

std::uint64_t SaturatingAdd(std::uint64_t lhs, std::uint64_t rhs) noexcept {
  if (rhs > std::numeric_limits<std::uint64_t>::max() - lhs) {
    return std::numeric_limits<std::uint64_t>::max();
  }
  return lhs + rhs;
}

bool IsExactFailure(MetaOwnerServiceabilityReason reason) noexcept {
  return reason >= MetaOwnerServiceabilityReason::kSessionMissing &&
         reason <= MetaOwnerServiceabilityReason::kPopulationUnready;
}

MetaAutomaticFailoverBlocker BlockerFor(
    const MetaOwnerServiceabilityDecision& decision) noexcept {
  if (decision.state_ == MetaOwnerServiceabilityState::kBlocked) {
    switch (decision.blocker_) {
      case MetaOwnerServiceabilityBlocker::kLeaderIneligible:
        return MetaAutomaticFailoverBlocker::kLeaderIneligible;
      case MetaOwnerServiceabilityBlocker::kLeadershipWarmup:
        return MetaAutomaticFailoverBlocker::kLeadershipWarmup;
      case MetaOwnerServiceabilityBlocker::kAuthorityHandoff:
        return MetaAutomaticFailoverBlocker::kAuthorityHandoff;
      case MetaOwnerServiceabilityBlocker::kFailoverTransition:
        return MetaAutomaticFailoverBlocker::kFailoverTransition;
      case MetaOwnerServiceabilityBlocker::kNone:
        return MetaAutomaticFailoverBlocker::kIndeterminateEvidence;
    }
  }
  if (decision.reason_ == MetaOwnerServiceabilityReason::kStaleOwnerAnchor) {
    return MetaAutomaticFailoverBlocker::kStaleOwnerAnchor;
  }
  if (decision.reason_ == MetaOwnerServiceabilityReason::kCausalLeasePending) {
    return MetaAutomaticFailoverBlocker::kCausalLeasePending;
  }
  return MetaAutomaticFailoverBlocker::kIndeterminateEvidence;
}

bool IsValidState(MetaAutomaticFailoverState state) noexcept {
  switch (state) {
    case MetaAutomaticFailoverState::kHealthy:
    case MetaAutomaticFailoverState::kSuspect:
    case MetaAutomaticFailoverState::kBlocked:
    case MetaAutomaticFailoverState::kTriggering:
      return true;
  }
  return false;
}

bool IsValidBlocker(MetaAutomaticFailoverBlocker blocker) noexcept {
  switch (blocker) {
    case MetaAutomaticFailoverBlocker::kNone:
    case MetaAutomaticFailoverBlocker::kLeaderIneligible:
    case MetaAutomaticFailoverBlocker::kLeadershipWarmup:
    case MetaAutomaticFailoverBlocker::kAuthorityHandoff:
    case MetaAutomaticFailoverBlocker::kFailoverTransition:
    case MetaAutomaticFailoverBlocker::kStaleOwnerAnchor:
    case MetaAutomaticFailoverBlocker::kCausalLeasePending:
    case MetaAutomaticFailoverBlocker::kIndeterminateEvidence:
      return true;
  }
  return false;
}

bool IsValidReason(MetaOwnerServiceabilityReason reason) noexcept {
  switch (reason) {
    case MetaOwnerServiceabilityReason::kNone:
    case MetaOwnerServiceabilityReason::kSessionMissing:
    case MetaOwnerServiceabilityReason::kHeartbeatExpired:
    case MetaOwnerServiceabilityReason::kDraining:
    case MetaOwnerServiceabilityReason::kStorageUnready:
    case MetaOwnerServiceabilityReason::kPopulationUnready:
    case MetaOwnerServiceabilityReason::kStaleOwnerAnchor:
    case MetaOwnerServiceabilityReason::kCausalLeasePending:
      return true;
  }
  return false;
}

bool IsValidDiagnostic(const MetaAutomaticFailoverStatus& status,
                       std::uint64_t leadership_generation,
                       std::uint64_t eligibility_revision) noexcept {
  return !status.anchor_.group_id_.empty() &&
         status.anchor_.group_id_.size() <= kMaxMetaGroupIdBytes &&
         status.anchor_.leadership_generation_ == leadership_generation &&
         status.anchor_.leader_authority_eligibility_revision_ ==
             eligibility_revision &&
         IsValidState(status.state_) && IsValidReason(status.current_reason_) &&
         IsValidBlocker(status.blocker_);
}

}  // namespace

void MetaAutomaticFailoverDiagnosticsRegistry::BeginLeadership(
    std::uint64_t leadership_generation) {
  if (leadership_generation == 0) return;
  std::lock_guard<std::mutex> lock(mutex_);
  if (leadership_generation <= latest_leadership_generation_) return;
  statuses_.clear();
  leader_authority_eligibility_revision_ = 0;
  evaluated_applied_index_ = 0;
  leadership_generation_ = leadership_generation;
  latest_leadership_generation_ = leadership_generation;
}

void MetaAutomaticFailoverDiagnosticsRegistry::Publish(
    std::uint64_t leadership_generation,
    std::uint64_t leader_authority_eligibility_revision,
    std::uint64_t evaluated_applied_index,
    std::vector<MetaAutomaticFailoverStatus> statuses) {
  if (leadership_generation == 0 || statuses.size() > kMaxMetaGroups) return;
  for (const MetaAutomaticFailoverStatus& status : statuses) {
    if (!IsValidDiagnostic(status, leadership_generation,
                           leader_authority_eligibility_revision)) {
      return;
    }
  }
  std::sort(statuses.begin(), statuses.end(),
            [](const auto& left, const auto& right) {
              return left.anchor_.group_id_ < right.anchor_.group_id_;
            });
  if (std::adjacent_find(statuses.begin(), statuses.end(),
                         [](const auto& left, const auto& right) {
                           return left.anchor_.group_id_ ==
                                  right.anchor_.group_id_;
                         }) != statuses.end()) {
    return;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (leadership_generation_ != leadership_generation ||
      leader_authority_eligibility_revision <
          leader_authority_eligibility_revision_ ||
      evaluated_applied_index < evaluated_applied_index_) {
    return;
  }
  leader_authority_eligibility_revision_ =
      leader_authority_eligibility_revision;
  evaluated_applied_index_ = evaluated_applied_index;
  statuses_ = std::move(statuses);
}

void MetaAutomaticFailoverDiagnosticsRegistry::EndLeadership(
    std::uint64_t leadership_generation) {
  if (leadership_generation == 0) return;
  std::lock_guard<std::mutex> lock(mutex_);
  if (leadership_generation_ != leadership_generation) return;
  statuses_.clear();
  leader_authority_eligibility_revision_ = 0;
  evaluated_applied_index_ = 0;
  leadership_generation_ = 0;
}

MetaAutomaticFailoverDiagnosticsSnapshot
MetaAutomaticFailoverDiagnosticsRegistry::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return {.leadership_generation_ = leadership_generation_,
          .leader_authority_eligibility_revision_ =
              leader_authority_eligibility_revision_,
          .evaluated_applied_index_ = evaluated_applied_index_,
          .statuses_ = statuses_};
}

MetaAutomaticFailoverStateMachine::MetaAutomaticFailoverStateMachine(
    std::size_t max_groups)
    : max_groups_(
          std::min(max_groups, static_cast<std::size_t>(kMaxMetaGroups))) {}

absl::StatusOr<MetaAutomaticFailoverStateMachine::AdvanceResult>
MetaAutomaticFailoverStateMachine::Advance(const Input& input,
                                           std::uint64_t now_steady_ms) {
  if (input.suspect_after_ms_ == 0) {
    return absl::InvalidArgumentError(
        "automatic failover suspect threshold must be nonzero");
  }
  auto found = groups_.find(input.anchor_.group_id_);
  if (found == groups_.end()) {
    if (groups_.size() >= max_groups_) {
      return absl::ResourceExhaustedError(
          "automatic failover detector group capacity exceeded");
    }
    found = groups_.try_emplace(input.anchor_.group_id_).first;
  }

  GroupRuntime& runtime = found->second;
  const auto clear_suspect_clock = [&runtime] {
    runtime.active_since_ms_.reset();
    runtime.frozen_suspect_ms_ = 0;
  };
  const bool input_changed =
      runtime.status_.anchor_ != input.anchor_ ||
      runtime.leader_authority_eligible_ != input.leader_authority_eligible_ ||
      runtime.suspect_after_ms_ != input.suspect_after_ms_;
  if (input_changed) {
    runtime.leader_authority_eligible_ = input.leader_authority_eligible_;
    runtime.suspect_after_ms_ = input.suspect_after_ms_;
    runtime.status_ = {};
    clear_suspect_clock();
  }
  if (runtime.status_.state_ == MetaAutomaticFailoverState::kTriggering) {
    return AdvanceResult{.status_ = runtime.status_};
  }
  if (now_steady_ms < runtime.last_now_ms_) {
    runtime.status_ = {};
    clear_suspect_clock();
  }

  runtime.status_ = {
      .anchor_ = input.anchor_,
      .state_ = MetaAutomaticFailoverState::kBlocked,
      .effective_threshold_ms_ = input.suspect_after_ms_,
  };
  runtime.last_now_ms_ = now_steady_ms;
  if (!input.leader_authority_eligible_) {
    runtime.status_.blocker_ = MetaAutomaticFailoverBlocker::kLeaderIneligible;
    clear_suspect_clock();
    return AdvanceResult{.status_ = runtime.status_};
  }
  if (input.owner_serviceability_.state_ ==
      MetaOwnerServiceabilityState::kServiceable) {
    runtime.status_.state_ = MetaAutomaticFailoverState::kHealthy;
    clear_suspect_clock();
    return AdvanceResult{.status_ = runtime.status_};
  }
  if (input.owner_serviceability_.state_ !=
          MetaOwnerServiceabilityState::kUnserviceable ||
      !IsExactFailure(input.owner_serviceability_.reason_)) {
    if (runtime.active_since_ms_.has_value()) {
      runtime.frozen_suspect_ms_ =
          SaturatingAdd(runtime.frozen_suspect_ms_,
                        now_steady_ms - *runtime.active_since_ms_);
      runtime.active_since_ms_.reset();
    }
    runtime.status_.blocker_ = BlockerFor(input.owner_serviceability_);
    runtime.status_.accumulated_suspect_ms_ = runtime.frozen_suspect_ms_;
    return AdvanceResult{.status_ = runtime.status_};
  }

  if (!runtime.active_since_ms_.has_value()) {
    runtime.active_since_ms_ = now_steady_ms;
  }
  const std::uint64_t elapsed = SaturatingAdd(
      runtime.frozen_suspect_ms_, now_steady_ms - *runtime.active_since_ms_);
  runtime.status_.state_ = MetaAutomaticFailoverState::kSuspect;
  runtime.status_.current_reason_ = input.owner_serviceability_.reason_;
  runtime.status_.accumulated_suspect_ms_ = elapsed;
  if (elapsed >= input.suspect_after_ms_) {
    runtime.status_.state_ = MetaAutomaticFailoverState::kTriggering;
    return AdvanceResult{.status_ = runtime.status_, .trigger_now_ = true};
  }
  return AdvanceResult{.status_ = runtime.status_};
}

void MetaAutomaticFailoverStateMachine::EraseGroup(std::string_view group_id) {
  groups_.erase(std::string(group_id));
}

void MetaAutomaticFailoverStateMachine::Clear() { groups_.clear(); }

std::vector<MetaAutomaticFailoverStatus>
MetaAutomaticFailoverStateMachine::Snapshot() const {
  std::vector<MetaAutomaticFailoverStatus> result;
  result.reserve(groups_.size());
  for (const auto& entry : groups_) {
    result.push_back(entry.second.status_);
  }
  return result;
}

std::string_view MetaAutomaticFailoverStateName(
    MetaAutomaticFailoverState state) noexcept {
  switch (state) {
    case MetaAutomaticFailoverState::kHealthy:
      return "healthy";
    case MetaAutomaticFailoverState::kSuspect:
      return "suspect";
    case MetaAutomaticFailoverState::kBlocked:
      return "blocked";
    case MetaAutomaticFailoverState::kTriggering:
      return "triggering";
  }
  return "unknown";
}

std::string_view MetaAutomaticFailoverBlockerName(
    MetaAutomaticFailoverBlocker blocker) noexcept {
  switch (blocker) {
    case MetaAutomaticFailoverBlocker::kNone:
      return "none";
    case MetaAutomaticFailoverBlocker::kLeaderIneligible:
      return "leader_ineligible";
    case MetaAutomaticFailoverBlocker::kLeadershipWarmup:
      return "leadership_warmup";
    case MetaAutomaticFailoverBlocker::kAuthorityHandoff:
      return "authority_handoff";
    case MetaAutomaticFailoverBlocker::kFailoverTransition:
      return "failover_transition";
    case MetaAutomaticFailoverBlocker::kStaleOwnerAnchor:
      return "stale_owner_anchor";
    case MetaAutomaticFailoverBlocker::kCausalLeasePending:
      return "causal_lease_pending";
    case MetaAutomaticFailoverBlocker::kIndeterminateEvidence:
      return "indeterminate_evidence";
  }
  return "unknown";
}

}  // namespace lavik::meta
