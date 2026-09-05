#include "meta/meta_identity_store.h"

#include <limits>

#include "absl/strings/str_cat.h"
#include "meta/meta_identity_verifier.h"

namespace keylane::meta {
namespace {

// Field-cap re-validation at the store boundary: commands normally arrive via
// the strict decoder (which enforces caps), but the store keeps its own
// invariants self-contained so an in-memory constructed command cannot push
// state beyond its bounds. Over-limit input fails and is never silently
// truncated.
absl::Status CheckNodeFields(const std::string& node_id,
                             const std::vector<std::string>& endpoints) {
  if (node_id.empty() || node_id.size() > kMetaNodeIdBytes) {
    return MetaDomainRejectError("node_id empty or over cap");
  }
  if (endpoints.size() > kMaxMetaEndpointsPerNode) {
    return MetaDomainRejectError("too many endpoints");
  }
  for (const std::string& ep : endpoints) {
    if (ep.size() > kMaxMetaEndpointBytes) {
      return MetaDomainRejectError("endpoint over cap");
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status MetaIdentityStore::Apply(const RegisterNode& cmd) {
  if (auto st = CheckNodeFields(cmd.node_id_, cmd.endpoints_); !st.ok()) {
    return st;
  }
  if (auto st = ValidateDataNodePrincipal(cmd.node_id_, cmd.principal_);
      !st.ok()) {
    return st;
  }
  if (const auto existing = nodes_.find(cmd.node_id_);
      existing != nodes_.end()) {
    // Replay of the same log index: the exact post-effect (identical content,
    // active, never mutated) is already present -> idempotent accept.
    // Any other record under this node_id is a content conflict.
    const MetaNodeRecord& record = existing->second;
    const bool identical = !record.retired_ && record.revision_ == 1 &&
                           record.principal_ == cmd.principal_ &&
                           record.endpoints_ == cmd.endpoints_ &&
                           record.capability_mask_ == cmd.capability_mask_ &&
                           record.role_ == cmd.role_;
    if (identical) return absl::OkStatus();
    return MetaDomainRejectError(
        absl::StrCat("node_id ", cmd.node_id_, " already registered"));
  }
  // Global one-to-one principal binding; retired tombstones hold their
  // binding, so a principal is never rebound (rotation unimplemented).
  if (node_id_by_principal_.contains(cmd.principal_)) {
    return MetaDomainRejectError(
        absl::StrCat("principal already bound to another node_id"));
  }
  if (nodes_.size() >= kMaxMetaNodes) {
    return MetaDomainRejectError("registered node cap reached");
  }
  MetaNodeRecord record;
  record.node_id_ = cmd.node_id_;
  record.principal_ = cmd.principal_;
  record.endpoints_ = cmd.endpoints_;
  record.capability_mask_ = cmd.capability_mask_;
  record.role_ = cmd.role_;
  record.revision_ = 1;
  nodes_.emplace(cmd.node_id_, std::move(record));
  node_id_by_principal_.emplace(cmd.principal_, cmd.node_id_);
  return absl::OkStatus();
}

absl::Status MetaIdentityStore::Apply(const UpdateNode& cmd) {
  if (auto st = CheckNodeFields(cmd.node_id_, cmd.endpoints_); !st.ok()) {
    return st;
  }
  const auto it = nodes_.find(cmd.node_id_);
  if (it == nodes_.end()) {
    return MetaDomainRejectError(
        absl::StrCat("unknown node_id ", cmd.node_id_));
  }
  MetaNodeRecord& record = it->second;
  // Replay: the record already carries this command's post-effect (revision
  // expected+1, identical mutable content) -> idempotent accept.
  // UpdateNode cannot touch the principal binding: the schema has no
  // principal field; rotation unimplemented), so it is not compared.
  const bool is_replay = !record.retired_ &&
                         record.revision_ == cmd.expected_revision_ + 1 &&
                         record.endpoints_ == cmd.endpoints_ &&
                         record.capability_mask_ == cmd.capability_mask_;
  if (is_replay) return absl::OkStatus();
  if (record.retired_) {
    return MetaDomainRejectError(
        absl::StrCat("node ", cmd.node_id_, " is retired"));
  }
  if (record.revision_ != cmd.expected_revision_) {
    return MetaDomainRejectError(
        absl::StrCat("expected_revision CAS conflict on ", cmd.node_id_));
  }
  record.endpoints_ = cmd.endpoints_;
  record.capability_mask_ = cmd.capability_mask_;
  record.revision_ = cmd.expected_revision_ + 1;
  return absl::OkStatus();
}

absl::Status MetaIdentityStore::Apply(const RetireNode& cmd) {
  if (cmd.node_id_.empty() || cmd.node_id_.size() > kMetaNodeIdBytes) {
    return MetaDomainRejectError("node_id empty or over cap");
  }
  const auto it = nodes_.find(cmd.node_id_);
  if (it == nodes_.end()) {
    return MetaDomainRejectError(
        absl::StrCat("unknown node_id ", cmd.node_id_));
  }
  MetaNodeRecord& record = it->second;
  // Replay: already retired at the revision this command produces.
  if (record.retired_ && record.revision_ == cmd.expected_revision_ + 1) {
    return absl::OkStatus();
  }
  if (record.retired_) {
    return MetaDomainRejectError(
        absl::StrCat("node ", cmd.node_id_, " already retired"));
  }
  if (record.revision_ != cmd.expected_revision_) {
    return MetaDomainRejectError(
        absl::StrCat("expected_revision CAS conflict on ", cmd.node_id_));
  }
  // Retire is terminal. The principal binding is kept (tombstone): it is
  // never rebound because rotation is unimplemented.
  record.retired_ = true;
  record.revision_ = cmd.expected_revision_ + 1;
  return absl::OkStatus();
}

absl::Status MetaIdentityStore::Apply(const BindMetaMember& cmd) {
  if (cmd.server_id_ == 0 ||
      cmd.server_id_ >
          static_cast<std::uint32_t>(std::numeric_limits<int>::max())) {
    return MetaDomainRejectError("meta server_id must be a positive int");
  }
  const MetaMemberIdentity descriptor{static_cast<int>(cmd.server_id_),
                                      cmd.principal_, cmd.min_schema_,
                                      cmd.max_schema_};
  if (auto checked = MetaMemberIdentity::DecodeAux(descriptor.EncodeAux());
      !checked.ok()) {
    return MetaDomainRejectError(checked.status().message());
  }
  if (const auto existing = meta_members_.find(cmd.server_id_);
      existing != meta_members_.end()) {
    const MetaMemberRecord& record = existing->second;
    if (!record.retired_ && record.principal_ == cmd.principal_ &&
        record.min_schema_ == cmd.min_schema_ &&
        record.max_schema_ == cmd.max_schema_) {
      return absl::OkStatus();
    }
    return MetaDomainRejectError("meta server_id already bound");
  }
  if (node_id_by_principal_.contains(cmd.principal_) ||
      meta_server_id_by_principal_.contains(cmd.principal_)) {
    return MetaDomainRejectError("principal already bound");
  }
  if (meta_members_.size() >= kMaxMetaNodes) {
    return MetaDomainRejectError("meta member cap reached");
  }
  MetaMemberRecord record;
  record.server_id_ = cmd.server_id_;
  record.principal_ = cmd.principal_;
  record.min_schema_ = cmd.min_schema_;
  record.max_schema_ = cmd.max_schema_;
  meta_members_.emplace(cmd.server_id_, record);
  meta_server_id_by_principal_.emplace(cmd.principal_, cmd.server_id_);
  return absl::OkStatus();
}

absl::Status MetaIdentityStore::Apply(const RetireMetaMember& cmd) {
  const auto it = meta_members_.find(cmd.server_id_);
  if (it == meta_members_.end()) {
    return MetaDomainRejectError("unknown meta server_id");
  }
  if (it->second.retired_) return absl::OkStatus();
  it->second.retired_ = true;
  return absl::OkStatus();
}

std::optional<MetaNodeRecord> MetaIdentityStore::FindNode(
    const std::string& node_id) const {
  const auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return std::nullopt;
  return it->second;
}

std::optional<MetaNodeRecord> MetaIdentityStore::FindNodeByPrincipal(
    const std::string& principal) const {
  const auto it = node_id_by_principal_.find(principal);
  if (it == node_id_by_principal_.end()) return std::nullopt;
  return FindNode(it->second);
}

bool MetaIdentityStore::IsActiveNode(const std::string& node_id) const {
  const auto it = nodes_.find(node_id);
  return it != nodes_.end() && !it->second.retired_;
}

std::optional<MetaMemberRecord> MetaIdentityStore::FindMetaMember(
    std::uint32_t server_id) const {
  const auto it = meta_members_.find(server_id);
  if (it == meta_members_.end()) return std::nullopt;
  return it->second;
}

bool MetaIdentityStore::IsActiveMetaMember(std::uint32_t server_id,
                                           std::string_view principal) const {
  const auto it = meta_members_.find(server_id);
  return it != meta_members_.end() && !it->second.retired_ &&
         it->second.principal_ == principal;
}

// Envelope: schema_version u16 | node count u32 | sorted records. See the
// header for the convention and the strictness contract.
std::string MetaIdentityStore::Serialize() const {
  MetaWriter w;
  w.WriteU16(kMetaCurrentSchemaVersion);
  w.WriteCount(static_cast<std::uint32_t>(nodes_.size()));
  for (const auto& [node_id, record] : nodes_) {
    w.WriteString(node_id);
    w.WriteString(record.principal_);
    w.WriteList(record.endpoints_, [](MetaWriter& ww, const std::string& ep) {
      ww.WriteString(ep);
    });
    w.WriteU64(record.capability_mask_);
    w.WriteU8(static_cast<std::uint8_t>(record.role_));
    w.WriteU64(record.revision_);
    w.WriteBool(record.retired_);
  }
  w.WriteCount(static_cast<std::uint32_t>(meta_members_.size()));
  for (const auto& [server_id, record] : meta_members_) {
    w.WriteU32(server_id);
    w.WriteString(record.principal_);
    w.WriteU16(record.min_schema_);
    w.WriteU16(record.max_schema_);
    w.WriteBool(record.retired_);
  }
  return w.TakeBuffer();
}

absl::StatusOr<MetaIdentityStore> MetaIdentityStore::Deserialize(
    std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version < kMetaMinReadableSchemaVersion ||
      *version > kMetaCurrentSchemaVersion) {
    return MetaFailStopError("unknown schema_version");
  }
  auto count = r.ReadCount(kMaxMetaNodes);
  if (!count.ok()) return count.status();

  MetaIdentityStore store;
  for (std::uint32_t i = 0; i < *count; ++i) {
    auto node_id = r.ReadString(kMetaNodeIdBytes);
    if (!node_id.ok()) return node_id.status();
    auto principal = r.ReadString(kMaxMetaPrincipalBytes);
    if (!principal.ok()) return principal.status();
    auto endpoints = r.ReadList<std::string>(
        kMaxMetaEndpointsPerNode,
        [](MetaReader& rr) -> absl::StatusOr<std::string> {
          auto raw = rr.ReadString(kMaxMetaEndpointBytes);
          if (!raw.ok()) return raw.status();
          return std::string(*raw);
        });
    if (!endpoints.ok()) return endpoints.status();
    auto capability_mask = r.ReadU64();
    if (!capability_mask.ok()) return capability_mask.status();
    auto role = r.ReadU8();
    if (!role.ok()) return role.status();
    if (*role != static_cast<std::uint8_t>(MetaNodeRole::kPrimary) &&
        *role != static_cast<std::uint8_t>(MetaNodeRole::kReplica)) {
      return MetaFailStopError("unknown node role");
    }
    auto revision = r.ReadU64();
    if (!revision.ok()) return revision.status();
    auto retired = r.ReadBool("retired tag must be 0 or 1");
    if (!retired.ok()) return retired.status();

    // Invariant enforcement (fail-stop): a corrupt snapshot must fail
    // identically on every node.
    const std::string node_id_str(*node_id);
    const std::string principal_str(*principal);
    if (node_id_str.empty() || principal_str.empty()) {
      return MetaFailStopError("empty node_id or principal in snapshot");
    }
    if (auto principal_status =
            ValidateDataNodePrincipal(node_id_str, principal_str);
        !principal_status.ok()) {
      return MetaFailStopError("non-canonical node principal in snapshot");
    }
    if (*revision == 0) {
      return MetaFailStopError("revision 0 in snapshot");
    }
    if (store.nodes_.contains(node_id_str)) {
      return MetaFailStopError("duplicate node_id in snapshot");
    }
    if (store.node_id_by_principal_.contains(principal_str)) {
      return MetaFailStopError("duplicate principal in snapshot");
    }

    MetaNodeRecord record;
    record.node_id_ = node_id_str;
    record.principal_ = principal_str;
    record.capability_mask_ = *capability_mask;
    record.role_ = static_cast<MetaNodeRole>(*role);
    record.revision_ = *revision;
    record.retired_ = *retired;
    record.endpoints_ = std::move(*endpoints);
    store.node_id_by_principal_.emplace(record.principal_, record.node_id_);
    store.nodes_.emplace(record.node_id_, std::move(record));
  }
  auto member_count = r.ReadCount(kMaxMetaNodes);
  if (!member_count.ok()) return member_count.status();
  for (std::uint32_t i = 0; i < *member_count; ++i) {
    auto server_id = r.ReadU32();
    if (!server_id.ok()) return server_id.status();
    auto principal = r.ReadString(kMaxMetaPrincipalBytes);
    if (!principal.ok()) return principal.status();
    auto min_schema = r.ReadU16();
    if (!min_schema.ok()) return min_schema.status();
    auto max_schema = r.ReadU16();
    if (!max_schema.ok()) return max_schema.status();
    auto retired = r.ReadBool("invalid meta member in snapshot");
    if (!retired.ok()) return retired.status();
    if (*server_id == 0 || *server_id > static_cast<std::uint32_t>(
                                            std::numeric_limits<int>::max())) {
      return MetaFailStopError("invalid meta member in snapshot");
    }
    const MetaMemberIdentity descriptor{static_cast<int>(*server_id),
                                        std::string(*principal), *min_schema,
                                        *max_schema};
    if (!MetaMemberIdentity::DecodeAux(descriptor.EncodeAux()).ok()) {
      return MetaFailStopError("invalid meta member descriptor in snapshot");
    }
    if (store.meta_members_.contains(*server_id) ||
        store.node_id_by_principal_.contains(std::string(*principal)) ||
        store.meta_server_id_by_principal_.contains(std::string(*principal))) {
      return MetaFailStopError("duplicate meta member binding in snapshot");
    }
    MetaMemberRecord record{*server_id, std::string(*principal), *min_schema,
                            *max_schema, *retired};
    store.meta_server_id_by_principal_.emplace(record.principal_,
                                               record.server_id_);
    store.meta_members_.emplace(record.server_id_, std::move(record));
  }
  if (auto st = r.Finish(); !st.ok()) return st;
  return store;
}

}  // namespace keylane::meta
