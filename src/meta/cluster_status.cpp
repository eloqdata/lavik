#include "keylane/meta/cluster_status.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "keylane/numeric_endpoint.h"

namespace keylane::meta {
namespace {

constexpr std::uint16_t kWireVersion = 1;
constexpr std::size_t kMaxItems = 65'536;
constexpr std::size_t kMaxString = 64 * 1024;
constexpr std::size_t kMaxWireReply = 256 * 1024 * 1024;
// Binary payloads are hex-wrapped. Reserving room for the textual envelope
// keeps both the shared server retained-byte budget and the client line cap
// authoritative for the complete response, not merely its decoded half.
constexpr std::size_t kMaxPayload = (kMaxWireReply - 64) / 2;

class Writer {
 public:
  void U8(std::uint8_t value) { bytes_.push_back(static_cast<char>(value)); }
  void Bool(bool value) { U8(value ? 1 : 0); }
  void U16(std::uint16_t value) {
    U8(static_cast<std::uint8_t>(value >> 8));
    U8(static_cast<std::uint8_t>(value));
  }
  void U32(std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
      U8(static_cast<std::uint8_t>(value >> shift));
    }
  }
  void U64(std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
      U8(static_cast<std::uint8_t>(value >> shift));
    }
  }
  absl::Status String(std::string_view value) {
    if (value.size() > kMaxString) {
      return absl::ResourceExhaustedError("cluster status string exceeds cap");
    }
    if (bytes_.size() > kMaxPayload - 4 ||
        value.size() > kMaxPayload - 4 - bytes_.size()) {
      return absl::ResourceExhaustedError("cluster status payload exceeds cap");
    }
    U32(static_cast<std::uint32_t>(value.size()));
    bytes_.append(value);
    return absl::OkStatus();
  }
  const std::string& bytes() const { return bytes_; }

 private:
  std::string bytes_;
};

class Reader {
 public:
  explicit Reader(std::string_view bytes) : bytes_(bytes) {}

  absl::StatusOr<std::uint8_t> U8() {
    if (offset_ == bytes_.size()) return Truncated();
    return static_cast<std::uint8_t>(bytes_[offset_++]);
  }
  absl::StatusOr<bool> Bool() {
    auto value = U8();
    if (!value.ok()) return value.status();
    if (*value > 1) return absl::DataLossError("invalid boolean");
    return *value == 1;
  }
  absl::StatusOr<std::uint16_t> U16() {
    auto high = U8();
    if (!high.ok()) return high.status();
    auto low = U8();
    if (!low.ok()) return low.status();
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(*high) << 8) |
                                      *low);
  }
  absl::StatusOr<std::uint32_t> U32() {
    std::uint32_t value = 0;
    for (int ii = 0; ii < 4; ++ii) {
      auto byte = U8();
      if (!byte.ok()) return byte.status();
      value = (value << 8) | *byte;
    }
    return value;
  }
  absl::StatusOr<std::uint64_t> U64() {
    std::uint64_t value = 0;
    for (int ii = 0; ii < 8; ++ii) {
      auto byte = U8();
      if (!byte.ok()) return byte.status();
      value = (value << 8) | *byte;
    }
    return value;
  }
  absl::StatusOr<std::string> String() {
    auto size = U32();
    if (!size.ok()) return size.status();
    if (*size > kMaxString || *size > bytes_.size() - offset_) {
      return absl::DataLossError("invalid cluster status string length");
    }
    std::string value(bytes_.substr(offset_, *size));
    offset_ += *size;
    return value;
  }
  bool done() const { return offset_ == bytes_.size(); }

 private:
  static absl::Status Truncated() {
    return absl::DataLossError("truncated cluster status payload");
  }

  std::string_view bytes_;
  std::size_t offset_ = 0;
};

absl::Status Count(Writer& writer, std::size_t count) {
  if (count > kMaxItems) {
    return absl::ResourceExhaustedError(
        "cluster status item count exceeds cap");
  }
  writer.U32(static_cast<std::uint32_t>(count));
  return absl::OkStatus();
}

absl::StatusOr<std::size_t> Count(Reader& reader) {
  auto count = reader.U32();
  if (!count.ok()) return count.status();
  if (*count > kMaxItems) {
    return absl::DataLossError("cluster status item count exceeds cap");
  }
  return static_cast<std::size_t>(*count);
}

absl::Status OptionalString(Writer& writer,
                            const std::optional<std::string>& value) {
  writer.Bool(value.has_value());
  return value.has_value() ? writer.String(*value) : absl::OkStatus();
}

absl::StatusOr<std::optional<std::string>> OptionalString(Reader& reader) {
  auto present = reader.Bool();
  if (!present.ok()) return present.status();
  if (!*present) return std::optional<std::string>{};
  auto value = reader.String();
  if (!value.ok()) return value.status();
  return std::optional<std::string>(std::move(*value));
}

void OptionalU64(Writer& writer, const std::optional<std::uint64_t>& value) {
  writer.Bool(value.has_value());
  if (value.has_value()) writer.U64(*value);
}

absl::StatusOr<std::optional<std::uint64_t>> OptionalU64(Reader& reader) {
  auto present = reader.Bool();
  if (!present.ok()) return present.status();
  if (!*present) return std::optional<std::uint64_t>{};
  auto value = reader.U64();
  if (!value.ok()) return value.status();
  return std::optional<std::uint64_t>(*value);
}

absl::Status WriteMember(Writer& writer,
                         const ClusterMetaMemberWireV1& member) {
  if (member.server_id_ == 0) {
    return absl::InvalidArgumentError("Meta member id must be nonzero");
  }
  writer.U32(member.server_id_);
  if (absl::Status status = OptionalString(writer, member.ctl_endpoint_);
      !status.ok()) {
    return status;
  }
  writer.Bool(member.is_leader_);
  return absl::OkStatus();
}

absl::StatusOr<ClusterMetaMemberWireV1> ReadMember(Reader& reader) {
  ClusterMetaMemberWireV1 member;
  auto id = reader.U32();
  if (!id.ok()) return id.status();
  if (*id == 0) return absl::DataLossError("Meta member id is zero");
  member.server_id_ = *id;
  auto endpoint = OptionalString(reader);
  if (!endpoint.ok()) return endpoint.status();
  member.ctl_endpoint_ = std::move(*endpoint);
  auto leader = reader.Bool();
  if (!leader.ok()) return leader.status();
  member.is_leader_ = *leader;
  return member;
}

std::string Hex(std::string_view bytes) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string encoded;
  encoded.reserve(bytes.size() * 2);
  for (unsigned char byte : bytes) {
    encoded.push_back(kDigits[byte >> 4]);
    encoded.push_back(kDigits[byte & 0xf]);
  }
  return encoded;
}

absl::StatusOr<std::string> Unhex(std::string_view hex) {
  if (hex.size() % 2 != 0 || hex.size() / 2 > kMaxPayload) {
    return absl::DataLossError("invalid cluster status hex length");
  }
  auto nibble = [](char ch) -> int {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
  };
  std::string bytes;
  bytes.reserve(hex.size() / 2);
  for (std::size_t ii = 0; ii < hex.size(); ii += 2) {
    const int high = nibble(hex[ii]);
    const int low = nibble(hex[ii + 1]);
    if (high < 0 || low < 0) {
      return absl::DataLossError("invalid cluster status hex digit");
    }
    bytes.push_back(static_cast<char>((high << 4) | low));
  }
  return bytes;
}

absl::StatusOr<std::string> Payload(std::string_view reply,
                                    std::string_view prefix) {
  if (!reply.starts_with(prefix) || reply.size() <= prefix.size() ||
      reply[prefix.size()] != ' ') {
    return absl::DataLossError("unexpected cluster status reply envelope");
  }
  return Unhex(reply.substr(prefix.size() + 1));
}

absl::Status ValidateDirectory(
    const std::vector<ClusterMetaMemberWireV1>& members) {
  if (members.empty()) return absl::DataLossError("empty Meta directory");
  std::vector<std::uint32_t> ids;
  std::vector<std::string> endpoints;
  std::size_t leaders = 0;
  for (const auto& member : members) {
    ids.push_back(member.server_id_);
    leaders += member.is_leader_ ? 1 : 0;
    if (member.ctl_endpoint_.has_value()) {
      auto parsed = keylane::ParseNumericEndpoint(*member.ctl_endpoint_);
      if (!parsed.has_value() ||
          keylane::FormatNumericEndpoint(*parsed) != *member.ctl_endpoint_ ||
          parsed->host_ == "0.0.0.0" || parsed->host_ == "::") {
        return absl::DataLossError("invalid committed ctl endpoint");
      }
      endpoints.push_back(*member.ctl_endpoint_);
    }
  }
  std::sort(ids.begin(), ids.end());
  if (std::adjacent_find(ids.begin(), ids.end()) != ids.end()) {
    return absl::DataLossError("duplicate Meta member id");
  }
  std::sort(endpoints.begin(), endpoints.end());
  if (std::adjacent_find(endpoints.begin(), endpoints.end()) !=
      endpoints.end()) {
    return absl::DataLossError("duplicate committed ctl endpoint");
  }
  if (leaders > 1) return absl::DataLossError("multiple leaders in directory");
  return absl::OkStatus();
}

absl::Status ValidateHeadTarget(const ClusterHeadWireV1& head,
                                const MetaAdminTarget& target) {
  const auto responder =
      std::find_if(head.meta_members_.begin(), head.meta_members_.end(),
                   [&](const ClusterMetaMemberWireV1& member) {
                     return member.server_id_ == head.responder_id_;
                   });
  if (responder == head.meta_members_.end()) {
    return absl::DataLossError("responder id is absent from Meta directory");
  }
  if (target.transport_ != MetaAdminTarget::Transport::kUnix &&
      (!responder->ctl_endpoint_.has_value() ||
       *responder->ctl_endpoint_ != target.endpoint_)) {
    return absl::DataLossError("clusterhead responder endpoint mismatch");
  }
  return absl::OkStatus();
}

absl::Status ValidateStatusIdentity(const ClusterStatusWireV1& status) {
  const auto responder =
      std::find_if(status.meta_members_.begin(), status.meta_members_.end(),
                   [&](const ClusterMetaMemberWireV1& member) {
                     return member.server_id_ == status.capture_.responder_id_;
                   });
  if (responder == status.meta_members_.end() || !responder->is_leader_) {
    return absl::InvalidArgumentError(
        "clusterstatus responder is not the directory leader");
  }
  std::vector<std::string> node_ids;
  std::vector<std::string> node_groups;
  node_ids.reserve(status.data_nodes_.size());
  for (const auto& node : status.data_nodes_) {
    if (node.node_id_.empty() ||
        static_cast<std::uint8_t>(node.role_) >
            static_cast<std::uint8_t>(ClusterDataNodeRole::kReplica)) {
      return absl::InvalidArgumentError("invalid clusterstatus data node");
    }
    if (node.population_current_ &&
        (!node.current_session_ || !node.projection_current_ ||
         !node.health_fresh_)) {
      return absl::InvalidArgumentError(
          "clusterstatus population lacks current runtime evidence");
    }
    if (node.lease_status_ == ClusterLeaseStatus::kRecentlyGranted &&
        (!node.group_id_.has_value() || !node.current_session_ ||
         !node.projection_current_ || !node.health_fresh_ ||
         !node.population_current_)) {
      return absl::InvalidArgumentError(
          "clusterstatus recent lease lacks current runtime evidence");
    }
    node_ids.push_back(node.node_id_);
    if (node.group_id_.has_value()) node_groups.push_back(*node.group_id_);
  }
  std::sort(node_ids.begin(), node_ids.end());
  if (std::adjacent_find(node_ids.begin(), node_ids.end()) != node_ids.end()) {
    return absl::InvalidArgumentError("duplicate clusterstatus data node");
  }
  std::vector<std::string> group_ids;
  group_ids.reserve(status.groups_.size());
  for (const auto& group : status.groups_) {
    if (group.group_id_.empty()) {
      return absl::InvalidArgumentError("empty clusterstatus group id");
    }
    if (group.owner_node_id_.has_value() &&
        !std::binary_search(node_ids.begin(), node_ids.end(),
                            *group.owner_node_id_)) {
      return absl::InvalidArgumentError(
          "clusterstatus group owner is absent from data nodes");
    }
    if (group.serving_ready_) {
      if (!group.owner_node_id_.has_value() ||
          !group.grant_revision_.has_value()) {
        return absl::InvalidArgumentError(
            "clusterstatus serving group lacks an owner grant");
      }
      const auto owner =
          std::find_if(status.data_nodes_.begin(), status.data_nodes_.end(),
                       [&](const ClusterDataNodeWireV1& node) {
                         return node.node_id_ == *group.owner_node_id_;
                       });
      if (owner == status.data_nodes_.end() || !owner->group_id_.has_value() ||
          *owner->group_id_ != group.group_id_ ||
          owner->lease_status_ != ClusterLeaseStatus::kRecentlyGranted) {
        return absl::InvalidArgumentError(
            "clusterstatus serving group lacks a ready granted owner");
      }
    }
    group_ids.push_back(group.group_id_);
  }
  std::sort(group_ids.begin(), group_ids.end());
  if (std::adjacent_find(group_ids.begin(), group_ids.end()) !=
      group_ids.end()) {
    return absl::InvalidArgumentError("duplicate clusterstatus group");
  }
  if (std::any_of(node_groups.begin(), node_groups.end(),
                  [&](const std::string& group_id) {
                    return !std::binary_search(group_ids.begin(),
                                               group_ids.end(), group_id);
                  })) {
    return absl::InvalidArgumentError(
        "clusterstatus data node references an unknown group");
  }
  auto ranges = status.slot_ranges_;
  std::sort(ranges.begin(), ranges.end(),
            [](const auto& left, const auto& right) {
              return std::tie(left.first_, left.last_, left.group_id_) <
                     std::tie(right.first_, right.last_, right.group_id_);
            });
  std::uint32_t previous_last = 0;
  std::uint32_t expected_first = 0;
  bool first_range = true;
  bool full_slot_coverage = !ranges.empty();
  for (const auto& range : ranges) {
    if (range.first_ > range.last_ || range.last_ >= 16'384 ||
        !std::binary_search(group_ids.begin(), group_ids.end(),
                            range.group_id_)) {
      return absl::InvalidArgumentError("invalid clusterstatus slot range");
    }
    if (!first_range && range.first_ <= previous_last) {
      return absl::InvalidArgumentError(
          "overlapping clusterstatus slot ranges");
    }
    if (range.first_ != expected_first) full_slot_coverage = false;
    expected_first = range.last_ + 1;
    first_range = false;
    previous_last = range.last_;
  }
  full_slot_coverage = full_slot_coverage && expected_first == 16'384;
  if (status.meta_membership_stable_ && status.meta_members_.size() > 1 &&
      std::any_of(status.meta_members_.begin(), status.meta_members_.end(),
                  [](const ClusterMetaMemberWireV1& member) {
                    return !member.ctl_endpoint_.has_value();
                  })) {
    return absl::InvalidArgumentError(
        "stable multi-voter Meta directory lacks ctl endpoints");
  }
  if (status.topology_converged_ &&
      std::any_of(status.groups_.begin(), status.groups_.end(),
                  [](const ClusterGroupWireV1& group) {
                    return !group.topology_converged_;
                  })) {
    return absl::InvalidArgumentError(
        "cluster topology flag disagrees with group convergence");
  }
  if (status.serving_ready_) {
    const bool every_slot_group_ready =
        std::all_of(ranges.begin(), ranges.end(), [&](const auto& range) {
          const auto group =
              std::find_if(status.groups_.begin(), status.groups_.end(),
                           [&](const ClusterGroupWireV1& item) {
                             return item.group_id_ == range.group_id_;
                           });
          return group != status.groups_.end() && group->serving_ready_;
        });
    if (!full_slot_coverage || status.groups_.empty() ||
        !every_slot_group_ready) {
      return absl::InvalidArgumentError(
          "serving-ready cluster lacks complete ready slot ownership");
    }
  }
  return absl::OkStatus();
}

bool IsTypedRetry(std::string_view reply) {
  static constexpr std::array<std::string_view, 5> kErrors = {
      "ERR not_leader", "ERR leader_unknown", "ERR leader_not_caught_up",
      "ERR cut_changed", "ERR busy"};
  return std::find(kErrors.begin(), kErrors.end(), reply) != kErrors.end();
}

bool IsFatalRoundTripStatus(const absl::Status& status) {
  return status.code() == absl::StatusCode::kInvalidArgument ||
         status.code() == absl::StatusCode::kPermissionDenied ||
         status.code() == absl::StatusCode::kDataLoss ||
         status.code() == absl::StatusCode::kResourceExhausted;
}

bool TlsComplete(const MetaAdminTlsOptions& tls) {
  return !tls.ca_file_.empty() && !tls.certificate_file_.empty() &&
         !tls.private_key_file_.empty();
}

bool TlsAny(const MetaAdminTlsOptions& tls) {
  return !tls.ca_file_.empty() || !tls.certificate_file_.empty() ||
         !tls.private_key_file_.empty() || !tls.server_name_.empty();
}

bool SameTlsIdentity(const MetaAdminTlsOptions& left,
                     const MetaAdminTlsOptions& right) {
  return left.ca_file_ == right.ca_file_ &&
         left.certificate_file_ == right.certificate_file_ &&
         left.private_key_file_ == right.private_key_file_;
}

absl::StatusOr<MetaAdminTarget> LearnedTarget(
    std::string endpoint, const ClusterStatusOptions& options) {
  MetaAdminTarget target;
  target.endpoint_ = std::move(endpoint);
  if (TlsComplete(options.tls_)) {
    target.transport_ = MetaAdminTarget::Transport::kTcpMtls;
    target.tls_ = options.tls_;
    target.tls_.server_name_.clear();
  } else if (options.allow_plaintext_admin_) {
    target.transport_ = MetaAdminTarget::Transport::kTcpPlaintext;
  } else {
    return absl::FailedPreconditionError(
        "following a TCP Meta ctl endpoint requires mTLS or "
        "--allow-plaintext-admin");
  }
  return target;
}

ClusterStatusOutcome Retry(std::string reason) {
  return ClusterStatusOutcome{.result_ = ClusterStatusResult::kRetryable,
                              .status_ = std::nullopt,
                              .retry_reason_ = std::move(reason)};
}

void RetryBackoff(MetaAdminDeadline deadline) {
  const auto now = std::chrono::steady_clock::now();
  if (now >= deadline) return;
  std::this_thread::sleep_until(
      std::min(deadline, now + std::chrono::milliseconds(5)));
}

std::string EscapeJson(std::string_view value) {
  std::string output;
  for (unsigned char ch : value) {
    switch (ch) {
      case '"':
        output += "\\\"";
        break;
      case '\\':
        output += "\\\\";
        break;
      case '\b':
        output += "\\b";
        break;
      case '\f':
        output += "\\f";
        break;
      case '\n':
        output += "\\n";
        break;
      case '\r':
        output += "\\r";
        break;
      case '\t':
        output += "\\t";
        break;
      default:
        if (ch < 0x20) {
          static constexpr char kDigits[] = "0123456789abcdef";
          output += "\\u00";
          output.push_back(kDigits[ch >> 4]);
          output.push_back(kDigits[ch & 0xf]);
        } else {
          output.push_back(static_cast<char>(ch));
        }
    }
  }
  return output;
}

std::string Quote(std::string_view value) {
  return "\"" + EscapeJson(value) + "\"";
}

std::string BoolJson(bool value) { return value ? "true" : "false"; }

std::string U64Json(std::uint64_t value) {
  return Quote(std::to_string(value));
}

std::string OptionalU64Json(const std::optional<std::uint64_t>& value) {
  return value.has_value() ? U64Json(*value) : "null";
}

std::string OptionalStringJson(const std::optional<std::string>& value) {
  return value.has_value() ? Quote(*value) : "null";
}

std::string ResultName(ClusterStatusResult result) {
  switch (result) {
    case ClusterStatusResult::kReady:
      return "ready";
    case ClusterStatusResult::kNotReady:
      return "not_ready";
    case ClusterStatusResult::kRetryable:
      return "retryable";
  }
  return "retryable";
}

std::string LeaseName(ClusterLeaseStatus status) {
  switch (status) {
    case ClusterLeaseStatus::kRecentlyGranted:
      return "recently_granted";
    case ClusterLeaseStatus::kDenied:
      return "denied";
    case ClusterLeaseStatus::kUnknown:
      return "unknown";
  }
  return "unknown";
}

std::string_view DataNodeRoleName(ClusterDataNodeRole role) {
  switch (role) {
    case ClusterDataNodeRole::kPrimary:
      return "primary";
    case ClusterDataNodeRole::kReplica:
      return "replica";
  }
  return "unknown";
}

}  // namespace

absl::StatusOr<std::string> EncodeClusterHeadReply(
    const ClusterHeadWireV1& head) {
  if (head.responder_id_ == 0) {
    return absl::InvalidArgumentError("responder id must be nonzero");
  }
  if (absl::Status status = ValidateDirectory(head.meta_members_);
      !status.ok()) {
    return status;
  }
  Writer writer;
  writer.U16(kWireVersion);
  writer.U32(head.responder_id_);
  writer.U8(static_cast<std::uint8_t>(head.role_));
  writer.U64(head.term_);
  writer.Bool(head.leader_id_.has_value());
  if (head.leader_id_.has_value()) writer.U32(*head.leader_id_);
  writer.U64(head.config_index_);
  if (absl::Status status = Count(writer, head.meta_members_.size());
      !status.ok()) {
    return status;
  }
  for (const auto& member : head.meta_members_) {
    if (absl::Status status = WriteMember(writer, member); !status.ok()) {
      return status;
    }
  }
  return "OK clusterhead 1 " + Hex(writer.bytes());
}

absl::StatusOr<ClusterHeadWireV1> DecodeClusterHeadReply(
    std::string_view reply) {
  auto payload = Payload(reply, "OK clusterhead 1");
  if (!payload.ok()) return payload.status();
  Reader reader(*payload);
  auto version = reader.U16();
  if (!version.ok()) return version.status();
  if (*version != kWireVersion) {
    return absl::DataLossError("unsupported clusterhead payload version");
  }
  ClusterHeadWireV1 head;
  auto responder = reader.U32();
  if (!responder.ok()) return responder.status();
  head.responder_id_ = *responder;
  auto role = reader.U8();
  if (!role.ok()) return role.status();
  if (*role > static_cast<std::uint8_t>(ClusterMetaRole::kLeader)) {
    return absl::DataLossError("invalid Meta role");
  }
  head.role_ = static_cast<ClusterMetaRole>(*role);
  auto term = reader.U64();
  if (!term.ok()) return term.status();
  head.term_ = *term;
  auto has_leader = reader.Bool();
  if (!has_leader.ok()) return has_leader.status();
  if (*has_leader) {
    auto leader = reader.U32();
    if (!leader.ok()) return leader.status();
    if (*leader == 0) return absl::DataLossError("leader id is zero");
    head.leader_id_ = *leader;
  }
  auto config = reader.U64();
  if (!config.ok()) return config.status();
  head.config_index_ = *config;
  auto count = Count(reader);
  if (!count.ok()) return count.status();
  head.meta_members_.reserve(*count);
  for (std::size_t ii = 0; ii < *count; ++ii) {
    auto member = ReadMember(reader);
    if (!member.ok()) return member.status();
    head.meta_members_.push_back(std::move(*member));
  }
  if (!reader.done()) return absl::DataLossError("trailing clusterhead data");
  if (head.responder_id_ == 0) {
    return absl::DataLossError("responder id is zero");
  }
  if (absl::Status status = ValidateDirectory(head.meta_members_);
      !status.ok()) {
    return status;
  }
  const auto responder_member =
      std::find_if(head.meta_members_.begin(), head.meta_members_.end(),
                   [&](const ClusterMetaMemberWireV1& member) {
                     return member.server_id_ == head.responder_id_;
                   });
  const auto marked_leader = std::find_if(
      head.meta_members_.begin(), head.meta_members_.end(),
      [](const ClusterMetaMemberWireV1& member) { return member.is_leader_; });
  if (responder_member == head.meta_members_.end() ||
      (head.leader_id_.has_value() &&
       (marked_leader == head.meta_members_.end() ||
        marked_leader->server_id_ != *head.leader_id_)) ||
      (!head.leader_id_.has_value() &&
       marked_leader != head.meta_members_.end()) ||
      ((head.role_ == ClusterMetaRole::kLeader) !=
       (head.leader_id_.has_value() &&
        *head.leader_id_ == head.responder_id_))) {
    return absl::DataLossError("inconsistent clusterhead identity");
  }
  return head;
}

absl::StatusOr<std::string> EncodeClusterStatusReply(
    const ClusterStatusWireV1& status) {
  if (status.capture_.responder_id_ == 0) {
    return absl::InvalidArgumentError("responder id must be nonzero");
  }
  if (status.cluster_ready_ !=
      (status.meta_available_ && status.meta_membership_stable_ &&
       status.topology_converged_ && status.serving_ready_)) {
    return absl::InvalidArgumentError("inconsistent cluster readiness");
  }
  if (absl::Status valid = ValidateDirectory(status.meta_members_);
      !valid.ok()) {
    return valid;
  }
  if (absl::Status valid = ValidateStatusIdentity(status); !valid.ok()) {
    return valid;
  }
  Writer writer;
  writer.U16(kWireVersion);
  writer.U32(status.capture_.responder_id_);
  writer.U64(status.capture_.term_);
  writer.U64(status.capture_.config_index_);
  writer.U64(status.capture_.committed_index_);
  writer.U64(status.capture_.topology_epoch_);
  writer.Bool(status.meta_available_);
  writer.Bool(status.meta_membership_stable_);
  writer.Bool(status.topology_converged_);
  writer.Bool(status.serving_ready_);
  writer.Bool(status.cluster_ready_);
  if (absl::Status counted = Count(writer, status.meta_members_.size());
      !counted.ok())
    return counted;
  for (const auto& member : status.meta_members_) {
    if (absl::Status wrote = WriteMember(writer, member); !wrote.ok())
      return wrote;
  }
  if (absl::Status counted = Count(writer, status.data_nodes_.size());
      !counted.ok())
    return counted;
  for (const auto& node : status.data_nodes_) {
    if (absl::Status wrote = writer.String(node.node_id_); !wrote.ok())
      return wrote;
    writer.U8(static_cast<std::uint8_t>(node.role_));
    writer.Bool(node.retired_);
    if (absl::Status wrote = OptionalString(writer, node.group_id_);
        !wrote.ok())
      return wrote;
    writer.Bool(node.current_session_);
    writer.Bool(node.projection_current_);
    writer.Bool(node.health_fresh_);
    writer.Bool(node.population_current_);
    writer.U8(static_cast<std::uint8_t>(node.lease_status_));
  }
  if (absl::Status counted = Count(writer, status.groups_.size());
      !counted.ok())
    return counted;
  for (const auto& group : status.groups_) {
    if (absl::Status wrote = writer.String(group.group_id_); !wrote.ok())
      return wrote;
    writer.U64(group.term_);
    if (absl::Status wrote = OptionalString(writer, group.owner_node_id_);
        !wrote.ok())
      return wrote;
    writer.U64(group.config_epoch_);
    OptionalU64(writer, group.grant_revision_);
    writer.Bool(group.serving_ready_);
    writer.Bool(group.topology_converged_);
  }
  if (absl::Status counted = Count(writer, status.slot_ranges_.size());
      !counted.ok())
    return counted;
  for (const auto& range : status.slot_ranges_) {
    writer.U32(range.first_);
    writer.U32(range.last_);
    if (absl::Status wrote = writer.String(range.group_id_); !wrote.ok())
      return wrote;
  }
  if (absl::Status counted = Count(writer, status.blockers_.size());
      !counted.ok())
    return counted;
  for (const auto& blocker : status.blockers_) {
    if (absl::Status wrote = writer.String(blocker.code_); !wrote.ok())
      return wrote;
    if (absl::Status wrote = writer.String(blocker.scope_); !wrote.ok())
      return wrote;
    if (absl::Status wrote = writer.String(blocker.detail_); !wrote.ok())
      return wrote;
  }
  if (writer.bytes().size() > kMaxPayload) {
    return absl::ResourceExhaustedError("cluster status payload exceeds cap");
  }
  return "OK clusterstatus 1 " + Hex(writer.bytes());
}

absl::StatusOr<ClusterStatusWireV1> DecodeClusterStatusReply(
    std::string_view reply) {
  auto payload = Payload(reply, "OK clusterstatus 1");
  if (!payload.ok()) return payload.status();
  Reader reader(*payload);
  auto version = reader.U16();
  if (!version.ok()) return version.status();
  if (*version != kWireVersion) {
    return absl::DataLossError("unsupported clusterstatus payload version");
  }
  ClusterStatusWireV1 status;
  auto responder = reader.U32();
  if (!responder.ok()) return responder.status();
  status.capture_.responder_id_ = *responder;
  auto term = reader.U64();
  if (!term.ok()) return term.status();
  status.capture_.term_ = *term;
  auto config = reader.U64();
  if (!config.ok()) return config.status();
  status.capture_.config_index_ = *config;
  auto committed = reader.U64();
  if (!committed.ok()) return committed.status();
  status.capture_.committed_index_ = *committed;
  auto topology = reader.U64();
  if (!topology.ok()) return topology.status();
  status.capture_.topology_epoch_ = *topology;
  auto read_bool = [&reader](bool* output) -> absl::Status {
    auto value = reader.Bool();
    if (!value.ok()) return value.status();
    *output = *value;
    return absl::OkStatus();
  };
  for (bool* value : {&status.meta_available_, &status.meta_membership_stable_,
                      &status.topology_converged_, &status.serving_ready_,
                      &status.cluster_ready_}) {
    if (absl::Status read = read_bool(value); !read.ok()) return read;
  }
  auto member_count = Count(reader);
  if (!member_count.ok()) return member_count.status();
  status.meta_members_.reserve(*member_count);
  for (std::size_t ii = 0; ii < *member_count; ++ii) {
    auto member = ReadMember(reader);
    if (!member.ok()) return member.status();
    status.meta_members_.push_back(std::move(*member));
  }
  auto node_count = Count(reader);
  if (!node_count.ok()) return node_count.status();
  status.data_nodes_.reserve(*node_count);
  for (std::size_t ii = 0; ii < *node_count; ++ii) {
    ClusterDataNodeWireV1 node;
    auto id = reader.String();
    if (!id.ok()) return id.status();
    node.node_id_ = std::move(*id);
    auto role = reader.U8();
    if (!role.ok()) return role.status();
    if (*role > static_cast<std::uint8_t>(ClusterDataNodeRole::kReplica)) {
      return absl::DataLossError("invalid data node role");
    }
    node.role_ = static_cast<ClusterDataNodeRole>(*role);
    if (absl::Status read = read_bool(&node.retired_); !read.ok()) return read;
    auto group = OptionalString(reader);
    if (!group.ok()) return group.status();
    node.group_id_ = std::move(*group);
    for (bool* value : {&node.current_session_, &node.projection_current_,
                        &node.health_fresh_, &node.population_current_}) {
      if (absl::Status read = read_bool(value); !read.ok()) return read;
    }
    auto lease = reader.U8();
    if (!lease.ok()) return lease.status();
    if (*lease > static_cast<std::uint8_t>(ClusterLeaseStatus::kUnknown)) {
      return absl::DataLossError("invalid lease status");
    }
    node.lease_status_ = static_cast<ClusterLeaseStatus>(*lease);
    status.data_nodes_.push_back(std::move(node));
  }
  auto group_count = Count(reader);
  if (!group_count.ok()) return group_count.status();
  status.groups_.reserve(*group_count);
  for (std::size_t ii = 0; ii < *group_count; ++ii) {
    ClusterGroupWireV1 group;
    auto id = reader.String();
    if (!id.ok()) return id.status();
    group.group_id_ = std::move(*id);
    auto group_term = reader.U64();
    if (!group_term.ok()) return group_term.status();
    group.term_ = *group_term;
    auto owner = OptionalString(reader);
    if (!owner.ok()) return owner.status();
    group.owner_node_id_ = std::move(*owner);
    auto config_epoch = reader.U64();
    if (!config_epoch.ok()) return config_epoch.status();
    group.config_epoch_ = *config_epoch;
    auto grant = OptionalU64(reader);
    if (!grant.ok()) return grant.status();
    group.grant_revision_ = *grant;
    if (absl::Status read = read_bool(&group.serving_ready_); !read.ok())
      return read;
    if (absl::Status read = read_bool(&group.topology_converged_); !read.ok())
      return read;
    status.groups_.push_back(std::move(group));
  }
  auto range_count = Count(reader);
  if (!range_count.ok()) return range_count.status();
  status.slot_ranges_.reserve(*range_count);
  for (std::size_t ii = 0; ii < *range_count; ++ii) {
    ClusterSlotRangeWireV1 range;
    auto first = reader.U32();
    if (!first.ok()) return first.status();
    auto last = reader.U32();
    if (!last.ok()) return last.status();
    range.first_ = *first;
    range.last_ = *last;
    if (range.first_ > range.last_ || range.last_ >= 16'384) {
      return absl::DataLossError("invalid slot range");
    }
    auto group = reader.String();
    if (!group.ok()) return group.status();
    range.group_id_ = std::move(*group);
    status.slot_ranges_.push_back(std::move(range));
  }
  auto blocker_count = Count(reader);
  if (!blocker_count.ok()) return blocker_count.status();
  status.blockers_.reserve(*blocker_count);
  for (std::size_t ii = 0; ii < *blocker_count; ++ii) {
    ClusterBlockerWireV1 blocker;
    auto code = reader.String();
    if (!code.ok()) return code.status();
    blocker.code_ = std::move(*code);
    auto scope = reader.String();
    if (!scope.ok()) return scope.status();
    blocker.scope_ = std::move(*scope);
    auto detail = reader.String();
    if (!detail.ok()) return detail.status();
    blocker.detail_ = std::move(*detail);
    status.blockers_.push_back(std::move(blocker));
  }
  if (!reader.done()) return absl::DataLossError("trailing clusterstatus data");
  if (status.capture_.responder_id_ == 0) {
    return absl::DataLossError("responder id is zero");
  }
  if (absl::Status valid = ValidateDirectory(status.meta_members_); !valid.ok())
    return valid;
  if (status.cluster_ready_ !=
      (status.meta_available_ && status.meta_membership_stable_ &&
       status.topology_converged_ && status.serving_ready_)) {
    return absl::DataLossError("inconsistent cluster readiness");
  }
  if (absl::Status valid = ValidateStatusIdentity(status); !valid.ok()) {
    return absl::DataLossError(valid.message());
  }
  return status;
}

ClusterOperator::ClusterOperator()
    : round_trip_([](const MetaAdminTarget& target, std::string_view command,
                     MetaAdminDeadline deadline) {
        return MetaAdminClient().RoundTrip(target, command, deadline);
      }) {}

ClusterOperator::ClusterOperator(MetaAdminRoundTrip round_trip)
    : round_trip_(std::move(round_trip)) {}

absl::StatusOr<ClusterStatusOutcome> ClusterOperator::Status(
    const MetaAdminTarget& seed, const ClusterStatusOptions& options) const {
  if (!round_trip_) return absl::FailedPreconditionError("missing transport");
  if (TlsAny(options.tls_) &&
      (!TlsComplete(options.tls_) || !options.tls_.server_name_.empty())) {
    return absl::InvalidArgumentError(
        "cluster TLS requires a complete certificate triple and IP SAN "
        "verification");
  }
  if (seed.transport_ == MetaAdminTarget::Transport::kTcpPlaintext &&
      !options.allow_plaintext_admin_) {
    return absl::FailedPreconditionError(
        "plaintext TCP admin requires --allow-plaintext-admin");
  }
  if (seed.transport_ == MetaAdminTarget::Transport::kTcpPlaintext &&
      TlsComplete(options.tls_)) {
    return absl::InvalidArgumentError(
        "a TCP seed must use mTLS when cluster TLS is configured");
  }
  if (seed.transport_ == MetaAdminTarget::Transport::kTcpMtls &&
      (!TlsComplete(seed.tls_) || !TlsComplete(options.tls_) ||
       !seed.tls_.server_name_.empty() ||
       !SameTlsIdentity(seed.tls_, options.tls_))) {
    return absl::InvalidArgumentError(
        "cluster mTLS seed and learned endpoints must share one complete "
        "certificate triple with IP SAN verification");
  }

  MetaAdminTarget discovery = seed;
  std::vector<MetaAdminTarget> known_targets;
  std::string last_retry = "leader unavailable";
  for (std::size_t attempt = 0;
       std::chrono::steady_clock::now() < options.deadline_; ++attempt) {
    auto head_reply =
        round_trip_(discovery, "clusterhead 1", options.deadline_);
    if (!head_reply.ok()) {
      if (IsFatalRoundTripStatus(head_reply.status())) {
        return head_reply.status();
      }
      last_retry = std::string(head_reply.status().message());
      if (!known_targets.empty()) {
        discovery = known_targets[attempt % known_targets.size()];
      }
      RetryBackoff(options.deadline_);
      continue;
    }
    if (IsTypedRetry(*head_reply)) {
      last_retry = *head_reply;
      if (!known_targets.empty()) {
        discovery = known_targets[attempt % known_targets.size()];
      }
      RetryBackoff(options.deadline_);
      continue;
    }
    auto head = DecodeClusterHeadReply(*head_reply);
    if (!head.ok()) return head.status();
    if (absl::Status identity = ValidateHeadTarget(*head, discovery);
        !identity.ok()) {
      return identity;
    }
    if (!head->leader_id_.has_value()) {
      last_retry = "leader_unknown";
      RetryBackoff(options.deadline_);
      continue;
    }
    const auto member =
        std::find_if(head->meta_members_.begin(), head->meta_members_.end(),
                     [&](const ClusterMetaMemberWireV1& item) {
                       return item.server_id_ == *head->leader_id_;
                     });
    if (member == head->meta_members_.end()) {
      return absl::DataLossError("leader id is absent from Meta directory");
    }
    MetaAdminTarget leader_target;
    if (*head->leader_id_ == head->responder_id_ &&
        head->role_ == ClusterMetaRole::kLeader) {
      leader_target = discovery;
    } else {
      if (!member->ctl_endpoint_.has_value()) {
        last_retry = "leader_unknown";
        RetryBackoff(options.deadline_);
        continue;
      }
      auto learned = LearnedTarget(*member->ctl_endpoint_, options);
      if (!learned.ok()) return learned.status();
      leader_target = std::move(*learned);
    }
    auto status_reply =
        round_trip_(leader_target, "clusterstatus 1", options.deadline_);
    if (!status_reply.ok()) {
      if (IsFatalRoundTripStatus(status_reply.status())) {
        return status_reply.status();
      }
      last_retry = std::string(status_reply.status().message());
    } else if (IsTypedRetry(*status_reply)) {
      last_retry = *status_reply;
    } else {
      auto status = DecodeClusterStatusReply(*status_reply);
      if (!status.ok()) return status.status();
      if (status->capture_.responder_id_ != *head->leader_id_) {
        return absl::DataLossError("clusterstatus responder id mismatch");
      }
      const auto status_responder = std::find_if(
          status->meta_members_.begin(), status->meta_members_.end(),
          [&](const ClusterMetaMemberWireV1& item) {
            return item.server_id_ == status->capture_.responder_id_;
          });
      if (leader_target.transport_ != MetaAdminTarget::Transport::kUnix &&
          (status_responder == status->meta_members_.end() ||
           !status_responder->ctl_endpoint_.has_value() ||
           *status_responder->ctl_endpoint_ != leader_target.endpoint_)) {
        return absl::DataLossError("clusterstatus responder endpoint mismatch");
      }
      ClusterStatusOutcome outcome;
      outcome.result_ = status->cluster_ready_ ? ClusterStatusResult::kReady
                                               : ClusterStatusResult::kNotReady;
      outcome.status_ = std::move(*status);
      return outcome;
    }
    known_targets.clear();
    // A published TCP directory does not require a local UDS query to use it.
    // Retry the existing UDS route without remote credentials; rediscovery
    // still enforces TCP authorization above if the leader actually moves.
    if (TlsComplete(options.tls_) || options.allow_plaintext_admin_) {
      for (const auto& known_member : head->meta_members_) {
        if (!known_member.ctl_endpoint_.has_value()) continue;
        auto target = LearnedTarget(*known_member.ctl_endpoint_, options);
        if (!target.ok()) return target.status();
        known_targets.push_back(std::move(*target));
      }
    }
    if (!known_targets.empty()) {
      discovery = known_targets[attempt % known_targets.size()];
    }
    RetryBackoff(options.deadline_);
  }
  return Retry(last_retry.empty() ? "deadline exceeded" : last_retry);
}

absl::StatusOr<std::string> RenderClusterStatusJson(
    const ClusterStatusOutcome& outcome) {
  std::string json =
      "{\"schema_version\":1,\"result\":" + Quote(ResultName(outcome.result_)) +
      ",\"readiness_basis\":\"meta_observed_v1\"";
  if (!outcome.status_.has_value()) {
    json += ",\"meta_available\":false,\"meta_membership_stable\":false";
    json += ",\"topology_converged\":false,\"serving_ready\":false";
    json += ",\"cluster_ready\":false,\"capture\":null";
    json += ",\"meta_members\":[],\"data_nodes\":[],\"groups\":[]";
    json += ",\"slot_ranges\":[],\"blockers\":[],\"retry\":{";
    json += "\"reason\":" + Quote(outcome.retry_reason_) + "}}";
    return json;
  }
  const auto& status = *outcome.status_;
  json += ",\"meta_available\":" + BoolJson(status.meta_available_);
  json +=
      ",\"meta_membership_stable\":" + BoolJson(status.meta_membership_stable_);
  json += ",\"topology_converged\":" + BoolJson(status.topology_converged_);
  json += ",\"serving_ready\":" + BoolJson(status.serving_ready_);
  json += ",\"cluster_ready\":" + BoolJson(status.cluster_ready_);
  json += ",\"capture\":{\"responder_id\":" +
          Quote(std::to_string(status.capture_.responder_id_));
  json += ",\"term\":" + U64Json(status.capture_.term_);
  json += ",\"config_index\":" + U64Json(status.capture_.config_index_);
  json += ",\"committed_index\":" + U64Json(status.capture_.committed_index_);
  json +=
      ",\"topology_epoch\":" + U64Json(status.capture_.topology_epoch_) + "}";

  auto members = status.meta_members_;
  std::sort(members.begin(), members.end(),
            [](const auto& left, const auto& right) {
              return left.server_id_ < right.server_id_;
            });
  json += ",\"meta_members\":[";
  for (std::size_t ii = 0; ii < members.size(); ++ii) {
    if (ii != 0) json += ',';
    json += "{\"server_id\":" + Quote(std::to_string(members[ii].server_id_));
    json += ",\"leader\":" + BoolJson(members[ii].is_leader_);
    json +=
        ",\"ctl_endpoint\":" + OptionalStringJson(members[ii].ctl_endpoint_) +
        "}";
  }
  json += ']';

  auto nodes = status.data_nodes_;
  std::sort(nodes.begin(), nodes.end(),
            [](const auto& left, const auto& right) {
              return left.node_id_ < right.node_id_;
            });
  json += ",\"data_nodes\":[";
  for (std::size_t ii = 0; ii < nodes.size(); ++ii) {
    if (ii != 0) json += ',';
    const auto& node = nodes[ii];
    json += "{\"node_id\":" + Quote(node.node_id_);
    json += ",\"role\":" + Quote(DataNodeRoleName(node.role_));
    json += ",\"retired\":" + BoolJson(node.retired_);
    json += ",\"group_id\":" + OptionalStringJson(node.group_id_);
    json += ",\"current_session\":" + BoolJson(node.current_session_);
    json += ",\"projection_current\":" + BoolJson(node.projection_current_);
    json += ",\"health_fresh\":" + BoolJson(node.health_fresh_);
    json += ",\"population_current\":" + BoolJson(node.population_current_);
    json += ",\"lease\":" + Quote(LeaseName(node.lease_status_)) + "}";
  }
  json += ']';

  auto groups = status.groups_;
  std::sort(groups.begin(), groups.end(),
            [](const auto& left, const auto& right) {
              return left.group_id_ < right.group_id_;
            });
  json += ",\"groups\":[";
  for (std::size_t ii = 0; ii < groups.size(); ++ii) {
    if (ii != 0) json += ',';
    const auto& group = groups[ii];
    json += "{\"group_id\":" + Quote(group.group_id_);
    json += ",\"term\":" + U64Json(group.term_);
    json += ",\"owner_node_id\":" + OptionalStringJson(group.owner_node_id_);
    json += ",\"config_epoch\":" + U64Json(group.config_epoch_);
    json += ",\"grant_revision\":" + OptionalU64Json(group.grant_revision_);
    json += ",\"serving_ready\":" + BoolJson(group.serving_ready_);
    json +=
        ",\"topology_converged\":" + BoolJson(group.topology_converged_) + "}";
  }
  json += ']';

  auto ranges = status.slot_ranges_;
  std::sort(ranges.begin(), ranges.end(),
            [](const auto& left, const auto& right) {
              return std::tie(left.first_, left.last_, left.group_id_) <
                     std::tie(right.first_, right.last_, right.group_id_);
            });
  json += ",\"slot_ranges\":[";
  for (std::size_t ii = 0; ii < ranges.size(); ++ii) {
    if (ii != 0) json += ',';
    json += "{\"first\":" + Quote(std::to_string(ranges[ii].first_));
    json += ",\"last\":" + Quote(std::to_string(ranges[ii].last_));
    json += ",\"group_id\":" + Quote(ranges[ii].group_id_) + "}";
  }
  json += ']';

  auto blockers = status.blockers_;
  std::sort(blockers.begin(), blockers.end(),
            [](const auto& left, const auto& right) {
              return std::tie(left.code_, left.scope_, left.detail_) <
                     std::tie(right.code_, right.scope_, right.detail_);
            });
  json += ",\"blockers\":[";
  for (std::size_t ii = 0; ii < blockers.size(); ++ii) {
    if (ii != 0) json += ',';
    json += "{\"code\":" + Quote(blockers[ii].code_);
    json += ",\"scope\":" + Quote(blockers[ii].scope_);
    json += ",\"detail\":" + Quote(blockers[ii].detail_) + "}";
  }
  json += "]";
  json += ",\"retry\":null}";
  return json;
}

absl::StatusOr<std::string> RenderClusterStatusText(
    const ClusterStatusOutcome& outcome) {
  std::string text;
  switch (outcome.result_) {
    case ClusterStatusResult::kReady:
      text = "READY\n";
      break;
    case ClusterStatusResult::kNotReady:
      text = "NOT READY\n";
      break;
    case ClusterStatusResult::kRetryable:
      text = "RETRYABLE\nreason=" + outcome.retry_reason_ + "\n";
      return text;
  }
  if (!outcome.status_.has_value()) {
    return absl::DataLossError("stable status outcome has no snapshot");
  }
  const auto& status = *outcome.status_;
  text +=
      "meta_available=" + std::string(status.meta_available_ ? "yes" : "no") +
      " membership_stable=" +
      std::string(status.meta_membership_stable_ ? "yes" : "no") +
      " topology_converged=" +
      std::string(status.topology_converged_ ? "yes" : "no") +
      " serving_ready=" + std::string(status.serving_ready_ ? "yes" : "no") +
      "\n";
  auto blockers = status.blockers_;
  std::sort(blockers.begin(), blockers.end(),
            [](const auto& left, const auto& right) {
              return std::tie(left.code_, left.scope_, left.detail_) <
                     std::tie(right.code_, right.scope_, right.detail_);
            });
  for (const auto& blocker : blockers) {
    text += "blocker " + blocker.code_ + " " + blocker.scope_ + " " +
            blocker.detail_ + "\n";
  }
  return text;
}

}  // namespace keylane::meta
