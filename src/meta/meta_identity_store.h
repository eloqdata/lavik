#pragma once

// MetaIdentityStore is the metadata control plane's committed node registry:
// node_id -> MetaNodeRecord.
//
// Invariants:
//   - Principal binding is globally one-to-one:
//     a principal can be bound to at most one node_id, ever. Retired nodes
//     keep their binding as tombstones, so a principal is never rebound
//     because rotation is unimplemented; re-registering a retired node_id is
//     likewise rejected.
//   - revision_ is the CAS token: 1 at registration, expected_revision+1 after
//     each applied mutation. Mutations carry expected_revision as an absolute
//     CAS token; a mismatch is a domain rejection.
//   - Retired is terminal: no command reactivates a node, and UpdateNode on a
//     retired node is rejected.
//   - State is size-bounded: total records (active + retired
//     tombstones) never exceed kMaxMetaNodes; over-cap applies are rejected,
//     never silently truncated.
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
// meta_commands), then the sorted registry; byte output is deterministic so
// equal states serialize to equal bytes.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "meta/meta_commands.h"

namespace keylane::meta {

// One registered node. node_id_ is the map key, duplicated here so query
// results are self-contained. revision_ and retired_ are defined by the
// invariants above.
struct MetaNodeRecord {
  std::string node_id_;
  std::string principal_;  // canonical SAN principal, globally 1:1
  std::vector<std::string> endpoints_;
  std::uint64_t capability_mask_ = 0;
  MetaNodeRole role_ = MetaNodeRole::kPrimary;
  std::uint64_t revision_ = 0;  // 1 at registration, +1 per applied mutation
  bool retired_ = false;
  bool operator==(const MetaNodeRecord&) const = default;
};

// Durable first-stage identity for a NuRaft member. Retirement preserves the
// principal tombstone so a certificate identity is never rebound to another
// server id.
struct MetaMemberRecord {
  std::uint32_t server_id_ = 0;
  std::string principal_;
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
  // Registered records including retired tombstones (tombstones keep the
  // principal binding, so they occupy the cap).
  std::size_t NodeCount() const { return nodes_.size(); }

  // Snapshot support: u16 schema_version envelope + sorted records.
  // Serialize cannot fail: the state is bounded and codec-valid by
  // construction (field caps are enforced at apply time). Deserialize is
  // strict and every failure is the fail-stop class, including invariant
  // violations inside the bytes (duplicate node_id, duplicate principal,
  // revision 0) — a corrupt snapshot fails identically on every node.
  std::string Serialize() const;
  static absl::StatusOr<MetaIdentityStore> Deserialize(std::string_view bytes);

 private:
  std::map<std::string, MetaNodeRecord> nodes_;  // by node_id, sorted
  // principal -> node_id reverse index enforcing the global 1:1 binding.
  std::map<std::string, std::string> node_id_by_principal_;
  std::map<std::uint32_t, MetaMemberRecord> meta_members_;
  std::map<std::string, std::uint32_t> meta_server_id_by_principal_;
};

}  // namespace keylane::meta
