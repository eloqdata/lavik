#pragma once

// MetaGrantStore owns the metadata control plane's committed per-group
// term, grant, and fence state.
//
// Per group the store keeps: the current group_term, the current grant
// (owner, term, authority_version, lease parameters, policy reference), and
// the fenced flag. INVARIANT: fenced_ == (no grant). A group is created
// fenced and grantless; BeginGroupTerm(T) is the only term-advancing command
// and re-enters the fenced/grantless state; ActivateAuthority installs a grant
// under the CURRENT nonzero term (it deliberately carries no new term) and
// unfences; RevokeGrant/FenceGroup drop the grant and fence.
//
// authority_version strictly increases per group across activations and
// survives revocation (last_authority_version_ is kept when the grant is
// dropped), so a stale activation can never re-install an older authority.
//
// Command semantics (all absolute values, with CAS via expected_* fields):
//   - BeginGroupTerm(expected=T-1, new=T): T must be exactly expected+1 and
//     expected must equal the current term; promotes and fences.
//   - GrantAuthority: same-owner committed grant-spec update (distinct from
//     the heartbeat path's memory-only lease renewal). CAS on term and
//     authority_version; owner and term never change. A changed spec records
//     its committed log index as grant_revision; an exact replay preserves the
//     prior value. The policy reference's existence in the policy store is a
//     cross-store fact the apply dispatcher checks (this store records the
//     reference and answers PolicyInUse).
//   - ActivateAuthority: the failover/migration atomic commit point. Split
//     into ValidateActivate (pure, all rejections) and ApplyGrantPart (the
//     install) so the apply dispatcher can atomically write the topology-store
//     part (owner, topology_epoch, config_epoch live in the topology store)
//     between the two. expected_term must equal the current term; the term
//     does not move. ApplyGrantPart assumes a successful ValidateActivate and
//     FAILS STOP on a term mismatch (contract violation = apply-layer bug).
//   - RevokeGrant/FenceGroup: CAS on the current term; drop the grant, fence.
//
// Replay idempotency: re-applying a command at the same log index
// must reproduce the same verdict and state. Each command first checks
// whether its post-effect is already present with identical content and then
// accepts as a no-op; only genuinely conflicting content is rejected (a
// kDomainReject absl::Status). A semantic no-op never moves a CAS token.
// GrantAuthority records the current committed log index as grant_revision
// when its spec changes; an exact renewal replay preserves the first index.
// RevokeGrant/FenceGroup do not advance term or authority-version tokens;
// BeginGroupTerm and ActivateAuthority carry explicit already-applied checks.
//
// Cross-store invariants (grant vs topology owner/epochs, policy reference
// existence, principal-vs-grant) are NOT enforced here: the store exposes
// fact queries and the apply dispatcher orchestrates. Apply is a pure
// in-memory function: no IO, no locks, no clock, no observation access.
// Snapshot serialization is the versioned strict
// encoding of encoding.h; decode failures (including a violated
// fenced/no-grant invariant) are MetaFailureClass::kFailStop.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "keylane/meta/commands.h"
#include "keylane/meta/encoding.h"

namespace keylane::meta {

// The current grant of one group. term_ always equals the group's current
// term and authority_version_ the group's last authority version while the
// grant is installed (see the file header).
struct MetaGroupGrant {
  std::string owner_;  // node_id
  std::uint64_t term_ = 0;
  std::uint64_t authority_version_ = 0;
  // Raft apply index of the activation or last committed spec change.
  // Heartbeat lease renewal is ephemeral and never changes this value.
  std::uint64_t grant_revision_ = 0;
  // Set only by failover cutover. Data activation must match this committed
  // action to the boot-local prepared context; ordinary authority activation
  // clears it, while same-owner grant renewal preserves it.
  std::optional<MetaFailoverActionId> activation_action_id_;
  MetaGrantSpec spec_;  // lease parameters + committed policy reference
  bool operator==(const MetaGroupGrant&) const = default;
};

// Read-only view of one group's term/grant state (fact query result).
struct MetaGroupGrantState {
  std::uint64_t group_term_ = 0;
  // Survives revocation; strictly increases on each ActivateAuthority.
  std::uint64_t last_authority_version_ = 0;
  // Retained while fenced so any later activation must advance beyond the
  // rejected authority anchor.
  std::uint64_t last_grant_revision_ = 0;
  std::optional<MetaGroupGrant> grant_;  // absent == fenced (invariant)
  bool fenced_ = true;
  bool operator==(const MetaGroupGrantState&) const = default;
};

class MetaGrantStore {
 public:
  explicit MetaGrantStore(std::uint32_t max_groups = kMaxMetaGroups)
      : max_groups_(max_groups) {}

  // Group lifecycle primitives; the apply dispatcher orchestrates them with
  // the topology store's CreateGroup/group-removal path. AddGroup is
  // idempotent. RemoveGroup rejects while a grant exists (the group must be
  // revoked/fenced first) and is an idempotent no-op once the group is gone.
  absl::Status AddGroup(std::string_view group_id);
  absl::Status RemoveGroup(std::string_view group_id);

  absl::Status BeginGroupTerm(const BeginGroupTerm& command);
  // A changed grant spec records committed_index as grant_revision; an exact
  // replay is a no-op and preserves the original revision.
  absl::Status GrantAuthority(const GrantAuthority& command,
                              std::uint64_t committed_index);
  // Pure validation of ActivateAuthority; every rejection lives here.
  absl::Status ValidateActivate(const ActivateAuthority& command,
                                std::uint64_t committed_index,
                                std::optional<MetaFailoverActionId>
                                    activation_action_id = std::nullopt) const;
  // Installs the grant part of ActivateAuthority, using committed_index as
  // the new grant_revision. Caller must have run ValidateActivate successfully
  // for the same command and index against the current state; a term mismatch
  // here is an apply-layer bug and fails stop.
  absl::Status ApplyGrantPart(
      const ActivateAuthority& command, std::uint64_t committed_index,
      std::optional<MetaFailoverActionId> activation_action_id = std::nullopt);
  absl::Status RevokeGrant(const RevokeGrant& command);
  absl::Status FenceGroup(const FenceGroup& command);

  // Fact queries (observation freshness and the policy-retire guard).
  std::optional<MetaGroupGrantState> GroupState(
      std::string_view group_id) const;
  std::optional<std::uint64_t> CurrentGroupTerm(
      std::string_view group_id) const;
  bool PolicyInUse(std::string_view policy_id, std::uint64_t version) const;
  std::size_t GroupCount() const { return groups_.size(); }

  // Snapshot serialization: versioned strict encoding; decode enforces caps
  // and the fenced/no-grant invariant (a violation is corruption: fail-stop).
  absl::StatusOr<std::string> Serialize() const;
  static absl::StatusOr<MetaGrantStore> Deserialize(
      std::string_view bytes, std::uint32_t max_groups = kMaxMetaGroups);

 private:
  struct Entry {
    std::uint64_t group_term_ = 0;
    std::uint64_t last_authority_version_ = 0;
    std::uint64_t last_grant_revision_ = 0;
    std::optional<MetaGroupGrant> grant_;
    bool fenced_ = true;
  };

  // The already-applied check of ActivateAuthority: the installed grant is
  // exactly what the command asks for.
  static bool GrantMatches(
      const Entry& entry, const ActivateAuthority& command,
      std::uint64_t committed_index,
      const std::optional<MetaFailoverActionId>& activation_action_id);

  std::uint32_t max_groups_;
  std::map<std::string, Entry> groups_;
};

}  // namespace keylane::meta
