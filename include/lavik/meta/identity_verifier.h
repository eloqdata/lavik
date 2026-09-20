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

// Certificate-principal and authorization policy for the metadata
// control plane. TLS verifies the certificate chain and validity period;
// this module supplies the second half of authentication: selecting one
// canonical Lavik URI SAN, binding Raft peers to their configured member
// identity, and applying the two-role operator/data-node RBAC policy.

#include <sys/types.h>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace lavik::meta {

enum class MetaPrincipalRole : std::uint8_t {
  kDataNode,
  kMetaMember,
  kOperator,
};

struct MetaPrincipalIdentity {
  std::string principal_;
  MetaPrincipalRole role_ = MetaPrincipalRole::kDataNode;
  // Data-node id for kDataNode and decimal Raft server id for kMetaMember.
  // Operators have no bound subject id.
  std::string subject_id_;
  bool operator==(const MetaPrincipalIdentity&) const = default;
};

// Parses one canonical principal. Supported v1 forms are:
//   lavik://node/<40 lowercase hex>
//   lavik://meta/<positive decimal server id, no leading zeroes>
//   lavik://operator/<non-empty URL-safe name>
absl::StatusOr<MetaPrincipalIdentity> ParseMetaPrincipal(
    std::string_view principal);

// Selects exactly one recognized Lavik URI SAN and parses it. Certificates
// with no Lavik principal or with several competing Lavik principals are
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

// LMI1 is the C++ member/directory descriptor. The Go bridge carries its
// decoded fields in committed Raft configuration contexts and snapshot
// metadata. It is independent of the listener bind and must agree with the
// identity store.
struct MetaMemberIdentity {
  std::int32_t server_id_ = 0;
  std::string principal_;
  std::string data_control_endpoint_;
  std::string ctl_endpoint_;

  std::string EncodeAux() const;
  static absl::StatusOr<MetaMemberIdentity> DecodeAux(std::string_view aux);
  bool operator==(const MetaMemberIdentity&) const = default;
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

}  // namespace lavik::meta
