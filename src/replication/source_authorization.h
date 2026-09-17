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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/replication_group.h"

namespace lavik::detail {

enum class SourceAuthorizationAction : std::uint8_t {
  kAuthorized,
  // A strictly newer directive cannot coexist with exports from an older
  // revision. The caller must cancel and join those sessions, call RevokeAll,
  // then retry authorization so replacement remains one ordered transition.
  kRevokeOlder,
};

enum class SourceAuthorizationDisposition : std::uint8_t {
  kNotAuthorized,
  kLeaseSuspended,
  kAuthorized,
};

// Capability ledger for cluster population exports. ReplicationGroup guards
// every access with its master mutex, including worker-zero control updates,
// so native LVPSYNC classification and MasterSession publication are atomic
// with capability replacement or revocation. A directive revision may
// authorize several targets concurrently, but all of them must describe one
// source/group/manifest scope. Committed revocation clears that active set and
// advances a rejection floor, so delayed grants cannot recreate an export
// from revoked authority. Transport-session cleanup clears the same
// process-local capabilities without advancing that floor: a newly
// authenticated Meta session may then replay its exact current FDS.
class SourceAuthorizationLedger {
 public:
  absl::StatusOr<SourceAuthorizationAction> Authorize(
      const RebuildDirective& directive) {
    const RebuildIdentity& identity = directive.identity_;
    const Version candidate{identity.term_, identity.directive_revision_};
    if (revoked_through_.has_value() && !Newer(candidate, *revoked_through_)) {
      return absl::FailedPreconditionError(
          "cluster source directive version was already revoked");
    }
    std::optional<Version> accepted;
    if (watermark_.has_value()) {
      accepted = Version{watermark_->identity_.term_,
                         watermark_->identity_.directive_revision_};
      if (Older(candidate, *accepted)) {
        return absl::FailedPreconditionError(
            "stale cluster source directive version");
      }
      if (candidate == *accepted) {
        if (!SameRevisionScope(*watermark_, directive)) {
          return absl::FailedPreconditionError(
              "cluster source directive conflicts with its accepted "
              "revision");
        }
      }
    }

    if (std::any_of(active_.begin(), active_.end(),
                    [&](const RebuildDirective& installed) {
                      return installed == directive;
                    })) {
      return SourceAuthorizationAction::kAuthorized;
    }
    if (accepted.has_value() && Newer(candidate, *accepted) &&
        !active_.empty()) {
      return SourceAuthorizationAction::kRevokeOlder;
    }

    if (!accepted.has_value() || Newer(candidate, *accepted)) {
      watermark_ = directive;
    }
    active_.push_back(directive);
    if (pending_replays_ != 0) --pending_replays_;
    return SourceAuthorizationAction::kAuthorized;
  }

  void RevokeAll() {
    if (watermark_.has_value()) {
      const Version accepted{watermark_->identity_.term_,
                             watermark_->identity_.directive_revision_};
      if (!revoked_through_.has_value() || Newer(accepted, *revoked_through_)) {
        revoked_through_ = accepted;
      }
    }
    active_.clear();
    pending_replays_ = 0;
    SuspendLeaseAdmission();
  }

  // A control-session loss also loses lease authority. Retain monotonic
  // conflict detection and any committed revoke floor, but keep replayed FDS
  // capabilities closed until a fresh lease grant has been installed.
  void ClearActiveForSessionReplacement() {
    // If disconnect interrupts an FDS replay, both the capabilities already
    // installed from that FDS and those still pending must be replayed by the
    // replacement session. FullDesiredState's bounded object size makes this
    // addition bounded well below size_t exhaustion.
    pending_replays_ += active_.size();
    active_.clear();
    SuspendLeaseAdmission();
  }

  // A live FullDesiredState replacement clears capabilities before its
  // level-triggered directives are replayed. Retaining the exact expected
  // count keeps that bounded gap fail-closed but retryable and prevents idle
  // history cleanup from invalidating the identities about to be replayed.
  // This is the only lifecycle clear that deliberately preserves the
  // independent lease gate.
  void ClearActiveForFdsReplacement(std::size_t expected_replays) {
    active_.clear();
    pending_replays_ = expected_replays;
  }

  bool RetainsSourceHistory() const noexcept {
    return !active_.empty() || pending_replays_ != 0;
  }

  bool IsAuthorized(const RebuildIdentity& identity) const {
    return std::any_of(active_.begin(), active_.end(),
                       [&](const RebuildDirective& installed) {
                         return installed.identity_ == identity;
                       });
  }

  // Lease admission is a separate, O(1) gate over current FDS capabilities.
  // Expiry can therefore stop new exports without destroying the exact
  // authorization that the same current projection will need after renewal.
  void EnableLeaseAdmissionUntil(
      std::chrono::nanoseconds deadline_since_boot) noexcept {
    lease_admission_deadline_ = deadline_since_boot;
  }
  // Read-only partial export uses the same current finite Owner lease gate.
  bool LeaseAdmissionOpen(
      std::chrono::nanoseconds now_since_boot) const noexcept {
    return now_since_boot < lease_admission_deadline_;
  }
  void SuspendLeaseAdmission() noexcept {
    lease_admission_deadline_ = std::chrono::nanoseconds::zero();
  }

  // An authorize-source command and its sibling rebuild command deliberately
  // have different delivery identities, revisions, and attempt lifecycles.
  // The later native target handshake is authorized by their exact shared
  // rebuild scope, never by pretending those two Meta directives are the same
  // command or by accepting same/older target work.
  SourceAuthorizationDisposition ClassifyAuthorizedRebuild(
      const RebuildIdentity& requested, std::uint32_t flow_count,
      bool safe_source_active, std::chrono::nanoseconds now_since_boot) const {
    const bool capability_matches = std::any_of(
        active_.begin(), active_.end(), [&](const RebuildDirective& installed) {
          const RebuildIdentity& authorized = installed.identity_;
          return installed.flow_count_ == flow_count &&
                 installed.safe_source_active_ == safe_source_active &&
                 authorized.group_id_ == requested.group_id_ &&
                 authorized.assignment_id_ == requested.assignment_id_ &&
                 authorized.term_ == requested.term_ &&
                 authorized.directive_revision_ <
                     requested.directive_revision_ &&
                 authorized.authority_id_ == requested.authority_id_ &&
                 authorized.source_node_id_ == requested.source_node_id_ &&
                 authorized.source_assignment_id_ ==
                     requested.source_assignment_id_ &&
                 authorized.source_boot_id_ == requested.source_boot_id_ &&
                 authorized.source_history_id_ ==
                     requested.source_history_id_ &&
                 authorized.target_node_id_ == requested.target_node_id_ &&
                 authorized.target_boot_id_ == requested.target_boot_id_ &&
                 authorized.operation_id_ == requested.operation_id_ &&
                 authorized.manifest_revision_ ==
                     requested.manifest_revision_ &&
                 authorized.manifest_id_ == requested.manifest_id_ &&
                 authorized.partition_replication_epoch_ ==
                     requested.partition_replication_epoch_;
        });
    // The authenticated FDS boundary supplies only the number of local
    // capabilities that its level-triggered directive lane must replay. Until
    // that bounded replay completes, an otherwise source-valid unknown scope
    // is conservatively transient rather than authorized: no bytes may leave,
    // and the target's fixed retry limit prevents an unbounded connection loop.
    if (!capability_matches)
      return pending_replays_ != 0
                 ? SourceAuthorizationDisposition::kLeaseSuspended
                 : SourceAuthorizationDisposition::kNotAuthorized;
    return now_since_boot < lease_admission_deadline_
               ? SourceAuthorizationDisposition::kAuthorized
               : SourceAuthorizationDisposition::kLeaseSuspended;
  }

  bool MatchesAuthorizedRebuild(const RebuildIdentity& requested,
                                std::uint32_t flow_count,
                                bool safe_source_active,
                                std::chrono::nanoseconds now_since_boot) const {
    return ClassifyAuthorizedRebuild(requested, flow_count, safe_source_active,
                                     now_since_boot) ==
           SourceAuthorizationDisposition::kAuthorized;
  }

 private:
  struct Version {
    std::uint64_t term_ = 0;
    std::uint64_t revision_ = 0;

    bool operator==(const Version&) const = default;
  };

  static bool Older(Version left, Version right) {
    return left.term_ < right.term_ ||
           (left.term_ == right.term_ && left.revision_ < right.revision_);
  }

  static bool Newer(Version left, Version right) { return Older(right, left); }

  static bool SameRevisionScope(const RebuildDirective& left,
                                const RebuildDirective& right) {
    const RebuildIdentity& a = left.identity_;
    const RebuildIdentity& b = right.identity_;
    // assignment_id and authority_id are target-scoped: a single committed
    // transition may authorize this source for several target membership
    // incarnations, each with its own full authority anchor. NodeControl has
    // already validated those anchors against one installed group view. The
    // ledger's cross-target consistency boundary is therefore the exact
    // source membership incarnation and population, not one target's
    // assignment.
    return a.group_id_ == b.group_id_ && a.term_ == b.term_ &&
           a.directive_revision_ == b.directive_revision_ &&
           a.source_node_id_ == b.source_node_id_ &&
           a.source_assignment_id_ == b.source_assignment_id_ &&
           a.source_boot_id_ == b.source_boot_id_ &&
           a.source_history_id_ == b.source_history_id_ &&
           a.manifest_revision_ == b.manifest_revision_ &&
           a.manifest_id_ == b.manifest_id_ &&
           a.partition_replication_epoch_ == b.partition_replication_epoch_ &&
           left.flow_count_ == right.flow_count_ &&
           left.safe_source_active_ == right.safe_source_active_;
  }

  std::optional<RebuildDirective> watermark_;
  std::optional<Version> revoked_through_;
  std::vector<RebuildDirective> active_;
  std::size_t pending_replays_ = 0;
  std::chrono::nanoseconds lease_admission_deadline_{};
};

}  // namespace lavik::detail
