#pragma once

// Certificate-principal and authorization policy for the metadata
// control plane. TLS verifies the certificate chain and validity period;
// this module supplies the second half of authentication: selecting one
// canonical Keylane URI SAN, binding Raft peers to their configured member
// identity, and applying the two-role operator/data-node RBAC policy.

#include <sys/types.h>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace keylane::meta {

enum class MetaPrincipalRole : std::uint8_t {
  kDataNode,
  kMetaMember,
  kOperator,
};

struct MetaPrincipalIdentity {
  std::string principal_;
  MetaPrincipalRole role_ = MetaPrincipalRole::kDataNode;
  // Data-node id for kDataNode and decimal NuRaft server id for kMetaMember.
  // Operators have no bound subject id.
  std::string subject_id_;
  bool operator==(const MetaPrincipalIdentity&) const = default;
};

// Parses one canonical principal. Supported v1 forms are:
//   keylane://node/<40 lowercase hex>
//   keylane://meta/<positive decimal server id, no leading zeroes>
//   keylane://operator/<non-empty URL-safe name>
absl::StatusOr<MetaPrincipalIdentity> ParseMetaPrincipal(
    std::string_view principal);

// Selects exactly one recognized Keylane URI SAN and parses it. Certificates
// with no Keylane principal or with several competing Keylane principals are
// rejected; unrelated URI SANs are ignored.
absl::StatusOr<MetaPrincipalIdentity> AuthenticateMetaUriSans(
    std::span<const std::string> uri_sans);

// Maps an AF_UNIX peer credential to the operator role only when its
// kernel-reported uid appears in the explicit listener allowlist.
absl::StatusOr<MetaPrincipalIdentity> AuthenticateLocalOperator(
    uid_t peer_uid, std::span<const uid_t> allowed_uids);

// Registration binding rule: a data node's certificate principal must be the
// canonical URI derived from the node id. This also rejects uppercase/non-hex
// node ids before they can enter committed state.
absl::Status ValidateDataNodePrincipal(std::string_view node_id,
                                       std::string_view principal);

// NuRaft persists this descriptor in srv_config::aux so an authenticated
// certificate can be bound to the Raft source id before message processing.
struct MetaMemberIdentity {
  std::int32_t server_id_ = 0;
  std::string principal_;

  std::string EncodeAux() const;
  static absl::StatusOr<MetaMemberIdentity> DecodeAux(std::string_view aux);
};

// Validates the authenticated certificate identity against the source id and
// persisted member descriptor on every Raft connection.
absl::Status VerifyRaftPeerIdentity(std::int32_t claimed_server_id,
                                    std::span<const std::string> uri_sans,
                                    std::string_view expected_member_aux);

// Two-role control-plane authorization. Operators may perform privileged
// mutations and diagnostics. Data nodes may only establish/report their own
// observation session. Typed directive responses are outside this boundary.
enum class MetaAccess : std::uint8_t {
  kStatus,
  kPrivileged,
  kObservationWrite,
};

absl::Status AuthorizeMetaAccess(const MetaPrincipalIdentity& identity,
                                 MetaAccess access,
                                 std::string_view target_node_id = {});

}  // namespace keylane::meta
