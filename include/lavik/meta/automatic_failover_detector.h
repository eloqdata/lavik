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

#pragma once

// Pure, leader-local Automatic Failover Detector state machine. Runtime
// integration supplies one monotonic clock cut and one current Owner
// Serviceability result per call; this module performs no I/O, schedules no
// timers, and submits no commands.

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/meta/owner_serviceability.h"

namespace lavik::meta {

// The complete identity of one detector clock. Policy contents are immutable
// at a version, so the two current versions are sufficient Policy anchors.
// The leader-authority revision makes a temporary eligibility loss observable
// even when false -> true occurs between two detector polls.
struct MetaAutomaticFailoverAnchor {
  std::string group_id_;
  std::uint64_t leadership_generation_ = 0;
  std::uint64_t leader_authority_eligibility_revision_ = 0;
  std::string owner_node_id_;
  MetaAssignmentId owner_assignment_id_{};
  std::uint64_t group_term_ = 0;
  std::uint64_t automatic_failover_policy_version_ = 0;
  std::uint64_t authority_lease_policy_version_ = 0;

  bool operator==(const MetaAutomaticFailoverAnchor&) const = default;
};

enum class MetaAutomaticFailoverState : std::uint8_t {
  kHealthy = 2,
  kSuspect = 3,
  kBlocked = 4,
  kTriggering = 5,
};

// Fixed detector diagnostics. The first four preserve explicit
// Owner-Serviceability blockers; the final three classify Indeterminate input
// without treating it as an exact failure.
enum class MetaAutomaticFailoverBlocker : std::uint8_t {
  kNone = 0,
  kLeaderIneligible = 1,
  kLeadershipWarmup = 2,
  kAuthorityHandoff = 3,
  kFailoverTransition = 4,
  kStaleOwnerAnchor = 5,
  kCausalLeasePending = 6,
  kIndeterminateEvidence = 7,
};

struct MetaAutomaticFailoverStatus {
  MetaAutomaticFailoverAnchor anchor_;
  MetaAutomaticFailoverState state_ = MetaAutomaticFailoverState::kBlocked;
  // Populated only while the current classification is exact Unserviceable or
  // after the corresponding trigger has latched.
  MetaOwnerServiceabilityReason current_reason_ =
      MetaOwnerServiceabilityReason::kNone;
  MetaAutomaticFailoverBlocker blocker_ = MetaAutomaticFailoverBlocker::kNone;
  std::uint64_t accumulated_suspect_ms_ = 0;
  std::uint64_t effective_threshold_ms_ = 0;

  bool operator==(const MetaAutomaticFailoverStatus&) const = default;
};

struct MetaAutomaticFailoverDiagnosticsSnapshot {
  std::uint64_t leadership_generation_ = 0;
  // Eligibility continuity cut used to produce every status in this batch.
  // This remains meaningful for an empty batch and lets status readers reject
  // a false -> true ABA within one leadership generation.
  std::uint64_t leader_authority_eligibility_revision_ = 0;
  // Applied index of the one compact committed view used to evaluate every
  // status in this publication. Consumers must equality-check this identity
  // before joining the leader-local diagnostics to committed state.
  std::uint64_t evaluated_applied_index_ = 0;
  std::vector<MetaAutomaticFailoverStatus> statuses_;

  bool operator==(const MetaAutomaticFailoverDiagnosticsSnapshot&) const =
      default;
};

// Thread-safe publication Seam between the leader-owned detector reconciler
// and cross-thread diagnostic readers such as cluster status. A publication
// replaces one complete generation cut; invalid batches and stale lifecycle
// callbacks are ignored atomically, so readers see neither partial nor
// cross-generation detector state. Storage is capped at kMaxMetaGroups.
class MetaAutomaticFailoverDiagnosticsRegistry {
 public:
  // Opens a strictly newer nonzero leadership bracket and clears the prior
  // cut. Repeating the current generation is an idempotent no-op rather than
  // erasing already published diagnostics; older or ended generations cannot
  // be reopened.
  void BeginLeadership(std::uint64_t leadership_generation);
  // Atomically replaces the current generation's complete Group cut. Input
  // order is immaterial; Snapshot always returns Group-id order. An invalid,
  // oversized, duplicate, stale, future, cross-eligibility, or
  // ended-generation batch is dropped without changing the last valid cut.
  void Publish(std::uint64_t leadership_generation,
               std::uint64_t leader_authority_eligibility_revision,
               std::uint64_t evaluated_applied_index,
               std::vector<MetaAutomaticFailoverStatus> statuses);
  // Closes and clears only the matching current bracket. A delayed teardown
  // from an older generation cannot erase a newer leader's diagnostics.
  void EndLeadership(std::uint64_t leadership_generation);
  // Copies one mutex-consistent cut. Generation zero means that no leadership
  // bracket is active and therefore statuses is empty.
  MetaAutomaticFailoverDiagnosticsSnapshot Snapshot() const;

 private:
  mutable std::mutex mutex_;
  std::uint64_t leadership_generation_ = 0;
  std::uint64_t leader_authority_eligibility_revision_ = 0;
  std::uint64_t evaluated_applied_index_ = 0;
  // Retained after EndLeadership so a delayed Begin for an already ended
  // generation cannot resurrect diagnostics.
  std::uint64_t latest_leadership_generation_ = 0;
  std::vector<MetaAutomaticFailoverStatus> statuses_;
};

// Bounded per-Group state. `now_steady_ms` is an injected monotonic-clock
// reading in milliseconds; it has no relationship to wall-clock time. The
// Module is caller-serialized and deliberately has no internal synchronization.
class MetaAutomaticFailoverStateMachine {
 public:
  // One complete current input cut for a Group. This type belongs to the
  // detector API; callers cannot persist or publish it independently.
  struct Input {
    MetaAutomaticFailoverAnchor anchor_;
    // Eligibility is repeated explicitly because losing it discards SUSPECT
    // time even before a formal leadership-generation change.
    bool leader_authority_eligible_ = false;
    std::uint64_t suspect_after_ms_ = 0;
    MetaOwnerServiceabilityDecision owner_serviceability_;
  };

  // Result of one Advance call. The edge is deliberately separate from the
  // retained diagnostic status because it is true only for the first call
  // that enters TRIGGERING.
  struct AdvanceResult {
    MetaAutomaticFailoverStatus status_;
    // The caller may create stable proposal identities only on this edge.
    bool trigger_now_ = false;
  };

  // Tests may choose a smaller capacity; larger values are clamped to the
  // committed topology's absolute Group bound.
  explicit MetaAutomaticFailoverStateMachine(
      std::size_t max_groups = kMaxMetaGroups);

  // Advances exactly one Group from one current input cut. A backwards clock
  // cut conservatively discards elapsed time. A zero threshold is invalid;
  // invalid input or capacity exhaustion leaves all existing Groups unchanged.
  // After `trigger_now_`, the same input anchor remains TRIGGERING so later
  // health cannot retract an in-flight Begin; a definitive non-commit may be
  // restarted with EraseGroup, while committed-anchor changes reset naturally.
  [[nodiscard]] absl::StatusOr<AdvanceResult> Advance(
      const Input& input, std::uint64_t now_steady_ms);

  // Explicit Group/lifecycle cleanup. Neither operation emits trigger edges.
  void EraseGroup(std::string_view group_id);
  void Clear();
  // Returns Group-id-sorted diagnostic state without altering timers.
  std::vector<MetaAutomaticFailoverStatus> Snapshot() const;

 private:
  struct GroupRuntime {
    bool leader_authority_eligible_ = false;
    std::uint64_t suspect_after_ms_ = 0;
    MetaAutomaticFailoverStatus status_;
    std::optional<std::uint64_t> active_since_ms_;
    std::uint64_t frozen_suspect_ms_ = 0;
    std::uint64_t last_now_ms_ = 0;
  };

  std::size_t max_groups_;
  std::map<std::string, GroupRuntime> groups_;
};

// Returns the stable lowercase diagnostic spelling used by cluster status and
// structured logs. Values outside the declared enum return "unknown".
std::string_view MetaAutomaticFailoverStateName(
    MetaAutomaticFailoverState state) noexcept;
// Returns the stable lowercase diagnostic spelling used by cluster status and
// structured logs. Values outside the declared enum return "unknown".
std::string_view MetaAutomaticFailoverBlockerName(
    MetaAutomaticFailoverBlocker blocker) noexcept;

}  // namespace lavik::meta
