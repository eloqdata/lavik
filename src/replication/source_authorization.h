#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/replication_group.h"

namespace keylane::detail {

enum class SourceAuthorizationAction : std::uint8_t {
  kAuthorized,
  // A strictly newer directive cannot coexist with exports from an older
  // revision. The caller must cancel and join those sessions, call RevokeAll,
  // then retry authorization so replacement remains one ordered transition.
  kRevokeOlder,
};

// Worker-zero-owned capability ledger for cluster population exports. A
// directive revision may authorize several targets concurrently, but all of
// them must describe one source/group/manifest scope. Committed revocation
// clears that active set and advances a rejection floor, so delayed grants
// cannot recreate an export from revoked authority. Transport-session cleanup
// clears the same process-local capabilities without advancing that floor: a
// newly authenticated Meta session may then replay its exact current FDS.
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
  }

  // Drops capabilities inherited from a disconnected control session while
  // retaining both monotonic conflict detection and any committed revoke
  // floor. Only an authenticated replacement session may drive the replay.
  void ClearActiveForSessionReplacement() { active_.clear(); }

  bool IsAuthorized(const RebuildIdentity& identity) const {
    return std::any_of(active_.begin(), active_.end(),
                       [&](const RebuildDirective& installed) {
                         return installed.identity_ == identity;
                       });
  }

  // An authorize-source command and its sibling rebuild command deliberately
  // have different delivery identities and attempt lifecycles. The native
  // target handshake is authorized by their shared rebuild scope, never by
  // pretending those two Meta directives are the same command.
  bool MatchesAuthorizedRebuild(const RebuildIdentity& requested,
                                std::uint32_t flow_count,
                                bool safe_source_active) const {
    const RebuildExportScope requested_scope =
        RebuildDirective{.identity_ = requested, .flow_count_ = flow_count}
            .ExportScope();
    return std::any_of(
        active_.begin(), active_.end(), [&](const RebuildDirective& installed) {
          // Authorize() still admits and versions the complete directive. This
          // narrower equality is only the continuity check for an already
          // authorized native export session.
          return installed.safe_source_active_ == safe_source_active &&
                 installed.ExportScope() == requested_scope;
        });
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
};

}  // namespace keylane::detail
