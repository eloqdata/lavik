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

// MetaIdentityStore is the metadata control plane's committed Data-node and
// Meta-member identity registry.
//
// Invariants:
//   - Principal binding is globally one-to-one:
//     a principal can be bound to at most one Data node or Meta member, ever.
//     Retired records keep their binding as tombstones, so a principal is never
//     rebound because rotation is unimplemented; re-registering a retired
//     node_id or server_id is likewise rejected.
//   - A Data node's revision_ is its CAS token: 1 at registration,
//     expected_revision+1 after each applied mutation. Those mutations carry
//     expected_revision as an absolute CAS token; a mismatch is a domain
//     rejection.
//   - Retired is terminal for both registries: no command reactivates a Data
//     node or Meta member, and UpdateNode on a retired Data node is rejected.
//   - Active Data-node client endpoints are one or two numeric addresses on
//     one host. Tagged tcp:// and tls:// forms cannot be mixed with the legacy
//     positional form, and a tagged transport cannot appear twice.
//   - Active Meta-member Data-control endpoints are numeric, unique after IP
//     normalization, and their complete directory must fit one protocol
//     ServerHello frame. These conditions are checked before mutation so a
//     committed binding cannot make all future Data sessions unpublishable.
//   - State is size-bounded independently by registry: Data-node records
//     (active + retired tombstones) never exceed kMaxMetaNodes, and Meta-member
//     records independently never exceed kMaxMetaNodes. Over-cap applies are
//     rejected, never silently truncated.
//
// Replay idempotency: re-applying a command
// whose exact post-effect is already present — same content, and for CAS
// commands the record sitting at the revision this command would produce —
// is an idempotent accept (no-op); the same record slot with conflicting
// content is a domain rejection. This is what makes duplicate commit()
// during recovery produce the same state and the same
// verdict.
//
// Failure classes: domain rejections return MetaDomainRejectError
// (kDomainReject); deserialization failures are fail-stop (kFailStop), the
// same bytes failing identically on every node.
//
// Scope: apply is a pure in-memory function of command + committed state —
// no IO, no locks (concurrency control lives above), never reads the local
// clock, never touches observation state. Cross-store rules (e.g. whether a
// node still holds membership/authority) are NOT enforced here; the store
// exposes fact queries and the apply dispatcher orchestrates.
//
// Serialization: u16 schema_version envelope (same convention as
// meta_commands), then Data-node records sorted by node_id, followed by
// Meta-member records sorted by server_id. Byte output is deterministic so
// equal states serialize to equal bytes.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "lavik/meta/commands.h"
#include "lavik/meta/encoding.h"

namespace lavik::meta {

inline constexpr std::uint16_t kMetaIdentityStoreFormatVersion = 1;

// One registered node. node_id_ is the map key, duplicated here so query
// results are self-contained. revision_ and retired_ are defined by the
// invariants above.
struct MetaNodeRecord {
  std::string node_id_;
  std::string principal_;  // canonical SAN principal, globally 1:1
  std::vector<std::string> endpoints_;

  MetaNodeRole role_ = MetaNodeRole::kPrimary;
  std::uint64_t revision_ = 0;  // 1 at registration, +1 per applied mutation
  bool retired_ = false;
  bool operator==(const MetaNodeRecord&) const = default;
};

// Durable first-stage identity for a Raft member. Retirement preserves the
// principal tombstone so a certificate identity is never rebound to another
// server id.
struct MetaMemberRecord {
  std::uint32_t server_id_ = 0;
  std::string principal_;
  std::string data_control_endpoint_;
  // Kept optional in the v1 store representation so old/malformed development
  // state can fail at the config-directory boundary. Active configured
  // members always populate it, and changing it requires member replacement.
  std::optional<std::string> ctl_endpoint_;
  bool retired_ = false;
  bool operator==(const MetaMemberRecord&) const = default;
};

class MetaIdentityStore {
 public:
  // Domain-validated apply of the identity commands. Each returns
  // absl::OkStatus() on apply or idempotent accept, and a kDomainReject
  // status otherwise; state is unchanged on rejection.
  absl::Status Apply(const RegisterNode& cmd);
  absl::Status Apply(const UpdateNode& cmd);
  absl::Status Apply(const RetireNode& cmd);
  absl::Status Apply(const BindMetaMember& cmd);
  absl::Status Apply(const RetireMetaMember& cmd);

  // Fact queries for the apply dispatcher and for reads. Retired tombstones are
  // visible through FindNode/FindNodeByPrincipal; IsActiveNode is false for
  // unknown and retired node_ids.
  std::optional<MetaNodeRecord> FindNode(const std::string& node_id) const;
  std::optional<MetaNodeRecord> FindNodeByPrincipal(
      const std::string& principal) const;
  bool IsActiveNode(const std::string& node_id) const;
  std::optional<MetaMemberRecord> FindMetaMember(std::uint32_t server_id) const;
  bool IsActiveMetaMember(std::uint32_t server_id,
                          std::string_view principal) const;
  std::vector<MetaNodeRecord> Nodes() const;
  std::vector<MetaMemberRecord> MetaMembers() const;
  // Registered records including retired tombstones (tombstones keep the
  // principal binding, so they occupy the cap).
  std::size_t NodeCount() const { return nodes_.size(); }

  // Snapshot support: u16 schema_version envelope + Data-node count/records
  // + Meta-member count/records. A Meta-member record is
  // (server_id, principal, Data-control endpoint, optional ctl endpoint,
  // retired flag).
  // Serialize cannot fail: the state is bounded and codec-valid by
  // construction (field caps are enforced at apply time). Deserialize is
  // strict and every failure is the fail-stop class, including invariant
  // violations inside the bytes (duplicate node/server id, duplicate
  // principal, Data-node revision 0) — a corrupt snapshot fails identically
  // on every node.
  std::string Serialize() const;
  // Exact durable size without allocating or copying snapshot bytes.
  std::uint64_t SerializedSize() const;
  static absl::StatusOr<MetaIdentityStore> Deserialize(std::string_view bytes);

 private:
  void WriteSnapshot(MetaWriter& writer) const;
  std::map<std::string, MetaNodeRecord> nodes_;  // by node_id, sorted
  // Registry-specific reverse indexes jointly enforce the global 1:1 binding.
  std::map<std::string, std::string> node_id_by_principal_;
  std::map<std::uint32_t, MetaMemberRecord> meta_members_;
  std::map<std::string, std::uint32_t> meta_server_id_by_principal_;
};

}  // namespace lavik::meta
