#pragma once

// MetaGrantStore owns the metadata control plane's committed per-group
// term, grant, and fence state.
//
// Per group the store keeps the current group_term and optional current grant
// (owner and optional activation action). A missing grant is the fenced
// state. A group is created fenced and grantless; BeginGroupTerm(T) is the
// only term-advancing store primitive and re-enters that state; typed failover
// and FenceGroup reuse it.
// ActivateAuthority installs the one grant permitted in the CURRENT nonzero
// term and unfences. A grant can only be removed by advancing the term, so an
// authority can never be reinstalled in a term that already carried one.
//
// Command semantics (all absolute values, with CAS via expected_* fields):
//   - BeginGroupTerm(expected=T-1, new=T): T must be exactly expected+1 and
//     expected must equal the current term; promotes and fences.
//   - ActivateAuthority: the authority-install kernel used directly and by
//     typed failover cutover. It is split into ValidateActivate (pure, all
//     rejections) and ApplyGrantPart (the install) so the apply dispatcher can
//     atomically write the topology-store part (owner, topology_epoch, and
//     config_epoch) between the two. expected_term must equal the current term;
//     the term does not move. A different active grant in the same term is
//     rejected. ApplyGrantPart assumes successful validation and FAILS STOP on
//     a contract violation.
//
// Replay idempotency: re-applying a command at the same log index
// must reproduce the same verdict and state. Each command first checks
// whether its post-effect is already present with identical content and then
// accepts as a no-op; only genuinely conflicting content is rejected (a
// kDomainReject absl::Status). A semantic no-op never moves a CAS token.
//
// Cross-store invariants (grant vs topology owner/epochs and
// principal-vs-grant) are NOT enforced here: the store exposes
// fact queries and the apply dispatcher orchestrates. Apply is a pure
// in-memory function: no IO, no locks, no clock, no observation access.
// Snapshot serialization is the versioned strict encoding of encoding.h;
// decode failures are MetaFailureClass::kFailStop.

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

// The current grant of one group. Its term is the containing state's
// group_term_; keeping another copy here would permit contradictory snapshots.
struct MetaGroupGrant {
  std::string owner_;  // node_id
  // Set only by failover cutover. Data activation must match this committed
  // action to the boot-local prepared context; ordinary authority activation
  // clears it.
  std::optional<MetaFailoverActionId> activation_action_id_;
  bool operator==(const MetaGroupGrant&) const = default;
};

// Read-only view of one group's term/grant state (fact query result).
struct MetaGroupGrantState {
  std::uint64_t group_term_ = 0;
  std::optional<MetaGroupGrant> grant_;  // absent == fenced
  bool operator==(const MetaGroupGrantState&) const = default;
};

class MetaGrantStore {
 public:
  explicit MetaGrantStore(std::uint32_t max_groups = kMaxMetaGroups)
      : max_groups_(max_groups) {}

  // Group lifecycle primitives; the apply dispatcher orchestrates them with
  // the topology store's CreateGroup/group-removal path. AddGroup is
  // idempotent. RemoveGroup rejects while a grant exists (the group must be
  // moved to a new grantless term first) and is an idempotent no-op once the
  // group is gone.
  absl::Status AddGroup(std::string_view group_id);
  absl::Status RemoveGroup(std::string_view group_id);

  absl::Status BeginGroupTerm(const BeginGroupTerm& command);
  // Pure validation of ActivateAuthority; every rejection lives here. A
  // present activation_action_id must be the nonzero failover action whose
  // prepared context authorizes this cutover. It participates in exact replay
  // matching; nullopt denotes an ordinary activation and clears the binding.
  absl::Status ValidateActivate(const ActivateAuthority& command,
                                std::optional<MetaFailoverActionId>
                                    activation_action_id = std::nullopt) const;
  // Installs the grant part of ActivateAuthority and persists
  // activation_action_id on the grant.
  // Caller must have run ValidateActivate successfully with the same command,
  // and optional action binding against the current state; a mismatch here is
  // an apply-layer bug and fails stop.
  absl::Status ApplyGrantPart(
      const ActivateAuthority& command,
      std::optional<MetaFailoverActionId> activation_action_id = std::nullopt);

  // Fact queries used by observation freshness and aggregate validation.
  std::optional<MetaGroupGrantState> GroupState(
      std::string_view group_id) const;
  std::optional<std::uint64_t> CurrentGroupTerm(
      std::string_view group_id) const;
  std::size_t GroupCount() const { return groups_.size(); }

  // Snapshot serialization: versioned strict encoding with bounded fields.
  absl::StatusOr<std::string> Serialize() const;
  static absl::StatusOr<MetaGrantStore> Deserialize(
      std::string_view bytes, std::uint32_t max_groups = kMaxMetaGroups);

 private:
  // The already-applied check of ActivateAuthority: the installed grant is
  // exactly what the command asks for.
  static bool GrantMatches(
      const MetaGroupGrantState& entry, const ActivateAuthority& command,
      const std::optional<MetaFailoverActionId>& activation_action_id);

  std::uint32_t max_groups_;
  std::map<std::string, MetaGroupGrantState> groups_;
};

}  // namespace keylane::meta
