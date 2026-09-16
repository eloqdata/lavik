#include "keylane/meta/identity_store.h"

#include <limits>
#include <set>

#include "absl/strings/str_cat.h"
#include "keylane/cluster/control_protocol.h"
#include "keylane/meta/identity_verifier.h"
#include "keylane/numeric_endpoint.h"

namespace keylane::meta {
namespace {

namespace control = keylane::cluster::control;

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

enum class DataEndpointKind { kLegacy, kTcp, kTls };

struct ParsedDataEndpoint {
  DataEndpointKind kind_ = DataEndpointKind::kLegacy;
  std::string host_;
  std::uint16_t port_ = 0;
};

absl::StatusOr<ParsedDataEndpoint> ParseDataEndpoint(std::string_view encoded) {
  ParsedDataEndpoint result;
  if (encoded.starts_with("tcp://")) {
    result.kind_ = DataEndpointKind::kTcp;
    encoded.remove_prefix(6);
  } else if (encoded.starts_with("tls://")) {
    result.kind_ = DataEndpointKind::kTls;
    encoded.remove_prefix(6);
  }

  auto endpoint = keylane::ParseNumericEndpoint(encoded);
  if (!endpoint.has_value()) {
    return absl::InvalidArgumentError(
        "Data endpoint must be numeric IPv4:port or [IPv6]:port");
  }
  result.host_ = std::move(endpoint->host_);
  result.port_ = endpoint->port_;
  return result;
}

absl::Status ValidateDataEndpoints(
    const std::vector<std::string>& encoded_endpoints) {
  if (encoded_endpoints.empty() ||
      encoded_endpoints.size() > kMaxMetaEndpointsPerNode) {
    return absl::InvalidArgumentError(
        "active Data node must have one or two client endpoints");
  }
  std::vector<ParsedDataEndpoint> endpoints;
  endpoints.reserve(encoded_endpoints.size());
  bool has_legacy = false;
  bool has_explicit = false;
  for (const std::string& encoded : encoded_endpoints) {
    auto endpoint = ParseDataEndpoint(encoded);
    if (!endpoint.ok()) return endpoint.status();
    has_legacy |= endpoint->kind_ == DataEndpointKind::kLegacy;
    has_explicit |= endpoint->kind_ != DataEndpointKind::kLegacy;
    endpoints.push_back(std::move(*endpoint));
  }
  if (has_legacy && has_explicit) {
    return absl::InvalidArgumentError(
        "Data endpoints cannot mix tagged and legacy forms");
  }
  if (std::any_of(endpoints.begin() + 1, endpoints.end(),
                  [&](const ParsedDataEndpoint& endpoint) {
                    return endpoint.host_ != endpoints.front().host_;
                  })) {
    return absl::InvalidArgumentError(
        "Data endpoints must use one numeric host");
  }
  if (has_explicit && endpoints.size() == 2 &&
      endpoints[0].kind_ == endpoints[1].kind_) {
    return absl::InvalidArgumentError(
        "Data endpoints repeat the same transport");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string> CanonicalMetaDataControlEndpoint(
    std::string_view encoded) {
  auto endpoint = keylane::ParseNumericEndpoint(encoded);
  if (!endpoint.has_value()) {
    return absl::InvalidArgumentError(
        "Meta data-control endpoint must be numeric IPv4:port or [IPv6]:port");
  }
  return keylane::FormatNumericEndpoint(*endpoint);
}

absl::StatusOr<std::string> CanonicalMetaAdminEndpoint(
    std::string_view encoded) {
  auto endpoint = keylane::ParseNumericEndpoint(encoded);
  if (!endpoint.has_value()) {
    return absl::InvalidArgumentError(
        "Meta ctl endpoint must be numeric IPv4:port or [IPv6]:port");
  }
  in_addr address4{};
  in6_addr address6{};
  if ((::inet_pton(AF_INET, endpoint->host_.c_str(), &address4) == 1 &&
       address4.s_addr == htonl(INADDR_ANY)) ||
      (::inet_pton(AF_INET6, endpoint->host_.c_str(), &address6) == 1 &&
       IN6_IS_ADDR_UNSPECIFIED(&address6))) {
    return absl::InvalidArgumentError(
        "Meta ctl endpoint must be a concrete routable address");
  }
  return keylane::FormatNumericEndpoint(*endpoint);
}

absl::StatusOr<control::WireMetaEndpoint> ParseMetaControlEndpoint(
    const MetaMemberRecord& member) {
  auto endpoint = keylane::ParseNumericEndpoint(member.data_control_endpoint_);
  if (!endpoint.has_value()) {
    return absl::InvalidArgumentError(
        "Meta data-control endpoint must be numeric IPv4:port or [IPv6]:port");
  }
  return control::WireMetaEndpoint{
      .server_id = member.server_id_,
      .host = std::move(endpoint->host_),
      .port = endpoint->port_,
      .principal = member.principal_,
  };
}

// ServerHello is intentionally a single frame. Keep that wire invariant in
// the durable state transition as well as at publication time, otherwise one
// legal Raft entry could permanently make every Data connection fail closed.
absl::Status ValidateActiveMetaDirectory(
    const std::vector<MetaMemberRecord>& members) {
  std::vector<control::WireMetaEndpoint> directory;
  directory.reserve(members.size());
  std::set<std::pair<std::string, std::uint16_t>> endpoints;
  std::set<std::string> ctl_endpoints;
  for (const MetaMemberRecord& member : members) {
    if (member.retired_) continue;
    auto endpoint = ParseMetaControlEndpoint(member);
    if (!endpoint.ok()) return endpoint.status();
    if (!endpoints.emplace(endpoint->host, endpoint->port).second) {
      return absl::InvalidArgumentError(
          "active Meta data-control endpoints must be unique");
    }
    directory.push_back(std::move(*endpoint));
    if (member.ctl_endpoint_.has_value() &&
        !ctl_endpoints.insert(*member.ctl_endpoint_).second) {
      return absl::InvalidArgumentError(
          "active Meta ctl endpoints must be unique");
    }
  }
  control::ServerHello probe{
      .disposition = control::ServerHelloDisposition::kAccepted,
      .negotiated_version = control::kProtocolVersion,
      .meta_server_id = 1,
      .raft_term = 1,
      .session_id = {},
      .session_generation = 1,
      .leader_id = 1,
      .directory = std::move(directory),
      .observation_ttl_ms = 1,
      .session_progress_timeout_ms = 1,
  };
  auto encoded = control::EncodeMessage(control::WireMessage(std::move(probe)));
  if (!encoded.ok() || encoded->size() > control::kMaxFramePayloadBytes) {
    return absl::ResourceExhaustedError(
        "active Meta directory cannot fit in one ServerHello frame");
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status MetaIdentityStore::Apply(const RegisterNode& cmd) {
  if (auto st = CheckNodeFields(cmd.node_id_, cmd.endpoints_); !st.ok()) {
    return st;
  }
  if (auto st = ValidateDataEndpoints(cmd.endpoints_); !st.ok()) {
    return MetaDomainRejectError(st.message());
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
  if (auto st = ValidateDataEndpoints(cmd.endpoints_); !st.ok()) {
    return MetaDomainRejectError(st.message());
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
                         record.endpoints_ == cmd.endpoints_;
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
  if (cmd.data_control_endpoint_.empty() ||
      cmd.data_control_endpoint_.size() > kMaxMetaEndpointBytes) {
    return MetaDomainRejectError(
        "data_control_endpoint is empty or exceeds its cap");
  }
  auto principal = ParseMetaPrincipal(cmd.principal_);
  if (!principal.ok() || principal->role_ != MetaPrincipalRole::kMetaMember ||
      principal->subject_id_ != std::to_string(cmd.server_id_)) {
    return MetaDomainRejectError(
        "Meta member principal does not match its server id");
  }
  auto canonical_endpoint =
      CanonicalMetaDataControlEndpoint(cmd.data_control_endpoint_);
  if (!canonical_endpoint.ok()) {
    return MetaDomainRejectError(canonical_endpoint.status().message());
  }
  std::optional<std::string> canonical_ctl_endpoint;
  if (cmd.ctl_endpoint_.has_value()) {
    auto parsed = CanonicalMetaAdminEndpoint(*cmd.ctl_endpoint_);
    if (!parsed.ok()) {
      return MetaDomainRejectError(parsed.status().message());
    }
    canonical_ctl_endpoint = std::move(*parsed);
  }
  if (const auto existing = meta_members_.find(cmd.server_id_);
      existing != meta_members_.end()) {
    MetaMemberRecord& record = existing->second;
    if (!record.retired_ && record.principal_ == cmd.principal_ &&
        record.data_control_endpoint_ == *canonical_endpoint) {
      if (record.ctl_endpoint_ == canonical_ctl_endpoint) {
        return absl::OkStatus();
      }
      if (!record.ctl_endpoint_.has_value() &&
          canonical_ctl_endpoint.has_value()) {
        std::vector<MetaMemberRecord> candidate = MetaMembers();
        for (MetaMemberRecord& member : candidate) {
          if (member.server_id_ == record.server_id_) {
            member.ctl_endpoint_ = canonical_ctl_endpoint;
          }
        }
        if (auto status = ValidateActiveMetaDirectory(candidate);
            !status.ok()) {
          return MetaDomainRejectError(status.message());
        }
        record.ctl_endpoint_ = std::move(canonical_ctl_endpoint);
        return absl::OkStatus();
      }
      return MetaDomainRejectError("meta ctl endpoint is immutable");
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
  MetaMemberRecord record{
      .server_id_ = cmd.server_id_,
      .principal_ = cmd.principal_,
      .data_control_endpoint_ = std::move(*canonical_endpoint),
      .ctl_endpoint_ = std::move(canonical_ctl_endpoint),
      .retired_ = false};
  std::vector<MetaMemberRecord> candidate = MetaMembers();
  candidate.push_back(record);
  if (auto status = ValidateActiveMetaDirectory(candidate); !status.ok()) {
    return MetaDomainRejectError(status.message());
  }
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

std::vector<MetaNodeRecord> MetaIdentityStore::Nodes() const {
  std::vector<MetaNodeRecord> result;
  result.reserve(nodes_.size());
  for (const auto& [node_id, record] : nodes_) {
    (void)node_id;
    result.push_back(record);
  }
  return result;
}

std::vector<MetaMemberRecord> MetaIdentityStore::MetaMembers() const {
  std::vector<MetaMemberRecord> result;
  result.reserve(meta_members_.size());
  for (const auto& [server_id, record] : meta_members_) {
    (void)server_id;
    result.push_back(record);
  }
  return result;
}

// Envelope: schema_version u16 | Data-node count u32 | sorted Data-node
// records | Meta-member count u32 | sorted (server_id, principal,
// data_control_endpoint, optional ctl_endpoint, retired) records. See the
// header for strictness.
void MetaIdentityStore::WriteSnapshot(MetaWriter& w) const {
  w.WriteU16(kMetaIdentityStoreFormatVersion);
  w.WriteCount(static_cast<std::uint32_t>(nodes_.size()));
  for (const auto& [node_id, record] : nodes_) {
    w.WriteString(node_id);
    w.WriteString(record.principal_);
    w.WriteList(record.endpoints_, [](MetaWriter& ww, const std::string& ep) {
      ww.WriteString(ep);
    });

    w.WriteU8(static_cast<std::uint8_t>(record.role_));
    w.WriteU64(record.revision_);
    w.WriteBool(record.retired_);
  }
  w.WriteCount(static_cast<std::uint32_t>(meta_members_.size()));
  for (const auto& [server_id, record] : meta_members_) {
    w.WriteU32(server_id);
    w.WriteString(record.principal_);
    w.WriteString(record.data_control_endpoint_);
    w.WriteBool(record.ctl_endpoint_.has_value());
    if (record.ctl_endpoint_.has_value()) w.WriteString(*record.ctl_endpoint_);
    w.WriteBool(record.retired_);
  }
}

std::string MetaIdentityStore::Serialize() const {
  MetaWriter writer;
  WriteSnapshot(writer);
  return writer.TakeBuffer();
}

std::uint64_t MetaIdentityStore::SerializedSize() const {
  MetaWriter counter(false);
  WriteSnapshot(counter);
  return counter.size();
}

absl::StatusOr<MetaIdentityStore> MetaIdentityStore::Deserialize(
    std::string_view bytes) {
  MetaReader r(bytes);
  auto version = r.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaIdentityStoreFormatVersion) {
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
    if (auto endpoint_status = ValidateDataEndpoints(*endpoints);
        !endpoint_status.ok()) {
      return MetaFailStopError(endpoint_status.message());
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
    auto data_control_endpoint = r.ReadString(kMaxMetaEndpointBytes);
    if (!data_control_endpoint.ok()) return data_control_endpoint.status();
    auto has_ctl_endpoint =
        r.ReadBool("invalid meta ctl endpoint presence tag");
    if (!has_ctl_endpoint.ok()) return has_ctl_endpoint.status();
    std::optional<std::string> ctl_endpoint;
    if (*has_ctl_endpoint) {
      auto decoded = r.ReadString(kMaxMetaEndpointBytes);
      if (!decoded.ok()) return decoded.status();
      auto canonical_ctl = CanonicalMetaAdminEndpoint(*decoded);
      if (!canonical_ctl.ok() || *canonical_ctl != *decoded) {
        return MetaFailStopError("non-canonical Meta ctl endpoint in snapshot");
      }
      ctl_endpoint = std::move(*canonical_ctl);
    }
    auto retired = r.ReadBool("invalid meta member in snapshot");
    if (!retired.ok()) return retired.status();
    if (*server_id == 0 ||
        *server_id >
            static_cast<std::uint32_t>(std::numeric_limits<int>::max()) ||
        data_control_endpoint->empty()) {
      return MetaFailStopError("invalid meta member in snapshot");
    }
    auto parsed_principal = ParseMetaPrincipal(*principal);
    if (!parsed_principal.ok() ||
        parsed_principal->role_ != MetaPrincipalRole::kMetaMember ||
        parsed_principal->subject_id_ != std::to_string(*server_id)) {
      return MetaFailStopError("invalid meta member descriptor in snapshot");
    }
    auto canonical_endpoint =
        CanonicalMetaDataControlEndpoint(*data_control_endpoint);
    if (!canonical_endpoint.ok() ||
        *canonical_endpoint != *data_control_endpoint) {
      return MetaFailStopError(
          "non-canonical Meta data-control endpoint in snapshot");
    }
    if (store.meta_members_.contains(*server_id) ||
        store.node_id_by_principal_.contains(std::string(*principal)) ||
        store.meta_server_id_by_principal_.contains(std::string(*principal))) {
      return MetaFailStopError("duplicate meta member binding in snapshot");
    }
    MetaMemberRecord record{
        .server_id_ = *server_id,
        .principal_ = std::string(*principal),
        .data_control_endpoint_ = std::move(*canonical_endpoint),
        .ctl_endpoint_ = std::move(ctl_endpoint),
        .retired_ = *retired};
    store.meta_server_id_by_principal_.emplace(record.principal_,
                                               record.server_id_);
    store.meta_members_.emplace(record.server_id_, std::move(record));
  }
  if (auto status = ValidateActiveMetaDirectory(store.MetaMembers());
      !status.ok()) {
    return MetaFailStopError(status.message());
  }
  if (auto st = r.Finish(); !st.ok()) return st;
  return store;
}

}  // namespace keylane::meta
