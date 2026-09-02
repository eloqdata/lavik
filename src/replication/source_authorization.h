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
// them must describe one source/group/manifest scope. Whole-session revocation
// clears that active set and fences its version, so delayed grants cannot
// recreate any export from the revoked authority revision.
class SourceAuthorizationLedger {
 public:
  absl::StatusOr<SourceAuthorizationAction> Authorize(
      const RebuildDirective& directive) {
    const RebuildIdentity& identity = directive.identity_;
    const Version candidate{identity.term_, identity.directive_revision_};
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
        if (active_.empty()) {
          return absl::FailedPreconditionError(
              "cluster source directive version was already revoked");
        }
      }
    }

    if (std::any_of(active_.begin(), active_.end(),
                    [&](const RebuildIdentity& installed) {
                      return installed == identity;
                    })) {
      return SourceAuthorizationAction::kAuthorized;
    }
    if (accepted.has_value() && Newer(candidate, *accepted) &&
        !active_.empty()) {
      return SourceAuthorizationAction::kRevokeOlder;
    }

    if (!watermark_.has_value() ||
        Newer(candidate, Version{watermark_->identity_.term_,
                                 watermark_->identity_.directive_revision_})) {
      watermark_ = directive;
    }
    active_.push_back(identity);
    return SourceAuthorizationAction::kAuthorized;
  }

  void RevokeAll() { active_.clear(); }

  bool IsAuthorized(const RebuildIdentity& identity) const {
    return std::any_of(active_.begin(), active_.end(),
                       [&](const RebuildIdentity& installed) {
                         return installed == identity;
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
    return a.group_id_ == b.group_id_ && a.assignment_id_ == b.assignment_id_ &&
           a.term_ == b.term_ &&
           a.directive_revision_ == b.directive_revision_ &&
           a.authority_id_ == b.authority_id_ &&
           a.source_node_id_ == b.source_node_id_ &&
           a.source_boot_id_ == b.source_boot_id_ &&
           a.source_history_id_ == b.source_history_id_ &&
           a.manifest_id_ == b.manifest_id_ &&
           left.flow_count_ == right.flow_count_ &&
           left.safe_source_active_ == right.safe_source_active_;
  }

  std::optional<RebuildDirective> watermark_;
  std::vector<RebuildIdentity> active_;
};

}  // namespace keylane::detail
