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

#include "lavik/meta/cluster_create.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "lavik/CLI11.hpp"
#include "lavik/meta/cluster_status.h"
#include "lavik/numeric_endpoint.h"
#include "openssl/rand.h"

namespace lavik::meta {
namespace {

constexpr std::size_t kMaxManifestBytes = 64 * 1024;
constexpr std::size_t kMaxAdminCommandBytes = 64 * 1024;
constexpr std::size_t kMaxWireString = 64 * 1024;
constexpr std::uint16_t kWireVersion = 1;
constexpr std::uint32_t kMaxManifestItems = 16'384;

absl::Status Invalid(std::string message) {
  return absl::InvalidArgumentError(std::move(message));
}

bool IsCanonicalNodeId(std::string_view value) {
  return value.size() == 40 &&
         std::all_of(value.begin(), value.end(), [](unsigned char ch) {
           return std::isdigit(ch) != 0 || (ch >= 'a' && ch <= 'f');
         });
}

bool IsCanonicalOperationId(std::string_view value) {
  return value.size() == 32 &&
         std::all_of(value.begin(), value.end(), [](unsigned char ch) {
           return std::isdigit(ch) != 0 || (ch >= 'a' && ch <= 'f');
         });
}

template <typename T>
absl::StatusOr<T> ParseUnsigned(const CLI::ConfigItem& item) {
  if (item.inputs.size() != 1) return Invalid("duplicate manifest field");
  std::uint64_t value = 0;
  const std::string& input = item.inputs.front();
  const auto parsed =
      std::from_chars(input.data(), input.data() + input.size(), value);
  if (parsed.ec != std::errc{} || parsed.ptr != input.data() + input.size() ||
      value > std::numeric_limits<T>::max() || input != std::to_string(value)) {
    return Invalid("manifest integer is out of range");
  }
  return static_cast<T>(value);
}

absl::StatusOr<std::string> ParseString(const CLI::ConfigItem& item) {
  if (item.inputs.size() != 1 || item.inputs.front().empty()) {
    return Invalid("manifest string must be a single non-empty value");
  }
  return item.inputs.front();
}

absl::StatusOr<std::vector<std::string>> ParseStringList(
    const CLI::ConfigItem& item) {
  std::vector<std::string> values;
  values.reserve(item.inputs.size());
  for (const std::string& input : item.inputs) {
    if (input.empty()) return Invalid("manifest list contains an empty value");
    values.push_back(input);
  }
  return values;
}

bool CanonicalEndpoint(std::string_view value,
                       std::string_view prefix = "tcp://") {
  if (!value.starts_with(prefix)) return false;
  value.remove_prefix(prefix.size());
  const auto endpoint = ParseNumericEndpoint(value);
  return endpoint.has_value() && FormatNumericEndpoint(*endpoint) == value;
}

absl::Status ValidateAndNormalize(ClusterCreateManifestV1* manifest) {
  if (manifest == nullptr || manifest->schema_version_ != 1 ||
      manifest->meta_members_.empty() || manifest->data_nodes_.empty() ||
      manifest->groups_.empty() ||
      manifest->meta_members_.size() > kMaxManifestItems ||
      manifest->data_nodes_.size() > kMaxManifestItems ||
      manifest->groups_.size() > kMaxManifestItems ||
      manifest->slot_ranges_.size() > kMaxManifestItems) {
    return Invalid("clustercreate manifest is not a supported v1 topology");
  }
  if (manifest->automatic_uncontrolled_failover_suspect_after_ms_ <
          kMinimumAutomaticFailoverSuspectAfterMs ||
      manifest->automatic_uncontrolled_failover_suspect_after_ms_ >
          kMaximumAutomaticFailoverSuspectAfterMs) {
    return Invalid(
        "automatic_uncontrolled_failover_suspect_after_ms is out of range");
  }
  if (manifest->authority_lease_duration_ms_ <
          kMinimumAuthorityLeaseDurationMs ||
      manifest->authority_lease_duration_ms_ >
          kMaximumAuthorityLeaseDurationMs) {
    return Invalid("authority_lease_duration_ms is out of range");
  }

  std::sort(manifest->meta_members_.begin(), manifest->meta_members_.end(),
            [](const auto& left, const auto& right) {
              return left.server_id_ < right.server_id_;
            });
  std::set<std::uint32_t> meta_ids;
  std::set<std::string> raft_endpoints;
  std::set<std::string> data_control_endpoints;
  std::set<std::string> ctl_endpoints;
  for (const auto& member : manifest->meta_members_) {
    if (member.server_id_ == 0 ||
        member.server_id_ > static_cast<std::uint32_t>(
                                std::numeric_limits<std::int32_t>::max())) {
      return Invalid(
          "Meta member id must fit a positive signed 32-bit integer");
    }
    if (!CanonicalEndpoint(member.raft_endpoint_) ||
        !CanonicalEndpoint(member.data_control_endpoint_) ||
        !CanonicalEndpoint(member.ctl_endpoint_)) {
      return Invalid(
          "Meta endpoints must be canonical numeric tcp:// endpoints");
    }
    if (!meta_ids.insert(member.server_id_).second) {
      return Invalid("duplicate Meta member id");
    }
    if (!raft_endpoints.insert(member.raft_endpoint_).second) {
      return Invalid("duplicate Meta Raft endpoint");
    }
    if (!data_control_endpoints.insert(member.data_control_endpoint_).second) {
      return Invalid("duplicate Meta Data-control endpoint");
    }
    if (!ctl_endpoints.insert(member.ctl_endpoint_).second) {
      return Invalid("duplicate Meta ctl endpoint");
    }
  }

  std::sort(manifest->data_nodes_.begin(), manifest->data_nodes_.end(),
            [](const auto& left, const auto& right) {
              return left.node_id_ < right.node_id_;
            });
  std::set<std::string> node_ids;
  std::set<std::string> endpoints;
  for (const auto& node : manifest->data_nodes_) {
    if (!IsCanonicalNodeId(node.node_id_)) {
      return Invalid("Data node id must be 40 lowercase hexadecimal bytes");
    }
    if (node.client_endpoint_.empty() && node.tls_endpoint_.empty()) {
      return Invalid("Data node requires a client_endpoint or tls_endpoint");
    }
    if (!node.client_endpoint_.empty() &&
        !CanonicalEndpoint(node.client_endpoint_)) {
      return Invalid(
          "client endpoint must be a canonical numeric tcp:// endpoint");
    }
    if (!node.tls_endpoint_.empty() &&
        !CanonicalEndpoint(node.tls_endpoint_, "tls://")) {
      return Invalid(
          "TLS endpoint must be a canonical numeric tls:// endpoint");
    }
    if (!node.client_endpoint_.empty() && !node.tls_endpoint_.empty() &&
        ParseNumericEndpoint(std::string_view(node.client_endpoint_).substr(6))
                ->host_ !=
            ParseNumericEndpoint(std::string_view(node.tls_endpoint_).substr(6))
                ->host_) {
      return Invalid("Data TCP and TLS endpoints must use the same host");
    }
    if (!node_ids.insert(node.node_id_).second) {
      return Invalid("duplicate Data node id");
    }
    // A listener cannot serve two nodes or both transports. Compare socket
    // addresses without the scheme so conflicts fail before Genesis commits.
    for (const auto* endpoint : {&node.client_endpoint_, &node.tls_endpoint_}) {
      if (!endpoint->empty() && !endpoints.insert(endpoint->substr(6)).second) {
        return Invalid("duplicate Data listener endpoint");
      }
    }
  }

  std::sort(manifest->groups_.begin(), manifest->groups_.end(),
            [](const auto& left, const auto& right) {
              return left.group_id_ < right.group_id_;
            });
  std::set<std::string> group_ids;
  std::set<std::string> assigned_nodes;
  for (auto& group : manifest->groups_) {
    if (group.group_id_.empty() || group.group_id_.size() > 64) {
      return Invalid("Group id must contain 1 through 64 bytes");
    }
    if (!group_ids.insert(group.group_id_).second) {
      return Invalid("duplicate Group id");
    }
    if (!node_ids.contains(group.primary_node_id_)) {
      return Invalid("Group primary references an unknown Data node");
    }
    if (!assigned_nodes.insert(group.primary_node_id_).second) {
      return Invalid("a Data node belongs to more than one Group");
    }
    std::sort(group.replica_node_ids_.begin(), group.replica_node_ids_.end());
    for (const std::string& replica : group.replica_node_ids_) {
      if (!node_ids.contains(replica)) {
        return Invalid("Group replica references an unknown Data node");
      }
      if (!assigned_nodes.insert(replica).second) {
        return Invalid("a Data node belongs to more than one Group");
      }
    }
  }
  if (assigned_nodes != node_ids) {
    return Invalid("every declared Data node must belong to exactly one Group");
  }

  if (manifest->slots_generated_) {
    std::vector<ClusterCreateManifestV1::SlotRange> generated;
    generated.reserve(manifest->groups_.size());
    const std::uint64_t count = manifest->groups_.size();
    for (std::uint64_t index = 0; index < count; ++index) {
      const std::uint64_t first = index * 16'384 / count;
      const std::uint64_t next = (index + 1) * 16'384 / count;
      if (first == next) {
        return Invalid("every Group must own at least one Slot");
      }
      generated.push_back(
          {static_cast<std::uint16_t>(first),
           static_cast<std::uint16_t>(next - 1),
           manifest->groups_[static_cast<std::size_t>(index)].group_id_});
    }
    if (!manifest->slot_ranges_.empty() &&
        manifest->slot_ranges_ != generated) {
      return Invalid("generated Slot ranges are not canonical");
    }
    manifest->slot_ranges_ = std::move(generated);
    return absl::OkStatus();
  }

  if (manifest->slot_ranges_.empty()) {
    return Invalid(
        "manifest must choose contiguous-even or provide explicit Slot ranges");
  }
  std::sort(manifest->slot_ranges_.begin(), manifest->slot_ranges_.end(),
            [](const auto& left, const auto& right) {
              return std::tie(left.first_, left.last_, left.group_id_) <
                     std::tie(right.first_, right.last_, right.group_id_);
            });
  std::uint32_t expected_first = 0;
  std::set<std::string> groups_with_slots;
  std::vector<ClusterCreateManifestV1::SlotRange> canonical;
  canonical.reserve(manifest->slot_ranges_.size());
  for (const auto& range : manifest->slot_ranges_) {
    if (!group_ids.contains(range.group_id_)) {
      return Invalid("Slot range references an unknown Group");
    }
    if (range.first_ > range.last_ || range.first_ != expected_first) {
      return Invalid("explicit Slot ranges must have no gaps or overlaps");
    }
    groups_with_slots.insert(range.group_id_);
    expected_first = static_cast<std::uint32_t>(range.last_) + 1;
    if (!canonical.empty() && canonical.back().group_id_ == range.group_id_ &&
        static_cast<std::uint32_t>(canonical.back().last_) + 1 ==
            range.first_) {
      canonical.back().last_ = range.last_;
    } else {
      canonical.push_back(range);
    }
  }
  if (expected_first != 16'384) {
    return Invalid("explicit Slot ranges must cover 0 through 16383");
  }
  if (groups_with_slots != group_ids) {
    return Invalid("every Group must own at least one Slot");
  }
  manifest->slot_ranges_ = std::move(canonical);
  return absl::OkStatus();
}

class Writer {
 public:
  void U16(std::uint16_t value) {
    bytes_.push_back(static_cast<char>(value >> 8));
    bytes_.push_back(static_cast<char>(value));
  }
  void U32(std::uint32_t value) {
    U16(static_cast<std::uint16_t>(value >> 16));
    U16(static_cast<std::uint16_t>(value));
  }
  void Raw(std::string_view value) { bytes_.append(value); }
  absl::Status String(std::string_view value) {
    if (value.size() > kMaxWireString) {
      return Invalid("clustercreate string exceeds cap");
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
  absl::StatusOr<std::uint16_t> U16() {
    if (bytes_.size() - offset_ < 2) return Invalid("truncated request");
    const auto first = static_cast<unsigned char>(bytes_[offset_++]);
    const auto second = static_cast<unsigned char>(bytes_[offset_++]);
    return static_cast<std::uint16_t>((first << 8) | second);
  }
  absl::StatusOr<std::uint32_t> U32() {
    auto high = U16();
    if (!high.ok()) return high.status();
    auto low = U16();
    if (!low.ok()) return low.status();
    return (static_cast<std::uint32_t>(*high) << 16) | *low;
  }
  absl::StatusOr<std::string_view> Raw(std::size_t size) {
    if (size > bytes_.size() - offset_) return Invalid("truncated request");
    const std::string_view result = bytes_.substr(offset_, size);
    offset_ += size;
    return result;
  }
  absl::StatusOr<std::string> String() {
    auto size = U32();
    if (!size.ok()) return size.status();
    if (*size > kMaxWireString || *size > bytes_.size() - offset_) {
      return Invalid("invalid clustercreate string length");
    }
    std::string result(bytes_.substr(offset_, *size));
    offset_ += *size;
    return result;
  }
  bool done() const { return offset_ == bytes_.size(); }

 private:
  std::string_view bytes_;
  std::size_t offset_ = 0;
};

std::string Hex(std::string_view bytes) {
  constexpr char kDigits[] = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (unsigned char byte : bytes) {
    result.push_back(kDigits[byte >> 4]);
    result.push_back(kDigits[byte & 0xf]);
  }
  return result;
}

absl::StatusOr<std::string> Unhex(std::string_view input) {
  if (input.size() % 2 != 0 || input.size() / 2 > kMaxManifestBytes) {
    return Invalid("invalid clustercreate hex payload");
  }
  auto nibble = [](char value) -> int {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    return -1;
  };
  std::string result(input.size() / 2, '\0');
  for (std::size_t index = 0; index < input.size(); index += 2) {
    const int high = nibble(input[index]);
    const int low = nibble(input[index + 1]);
    if (high < 0 || low < 0)
      return Invalid("invalid clustercreate hex payload");
    result[index / 2] = static_cast<char>((high << 4) | low);
  }
  return result;
}

absl::StatusOr<std::uint64_t> ParseU64(std::string_view text) {
  std::uint64_t result = 0;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), result);
  if (text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != text.data() + text.size() ||
      text != std::to_string(result)) {
    return Invalid("invalid clustercreate reply integer");
  }
  return result;
}

bool IsZero(const MetaOperationId& id) {
  return std::all_of(id.begin(), id.end(),
                     [](std::uint8_t byte) { return byte == 0; });
}

absl::StatusOr<MetaOperationId> GenerateOperationId() {
  MetaOperationId id{};
  if (RAND_bytes(id.data(), static_cast<int>(id.size())) != 1 || IsZero(id)) {
    return absl::InternalError(
        "failed to generate cluster-create operation id");
  }
  return id;
}

absl::Status ClusterCreateReplyError(std::string_view reply) {
  constexpr std::string_view kPrefix = "ERR clustercreate 1 ";
  if (!reply.starts_with(kPrefix)) {
    return Invalid("malformed clustercreate error reply");
  }
  reply.remove_prefix(kPrefix.size());
  const std::size_t stage_end = reply.find(' ');
  if (stage_end == std::string_view::npos) {
    return Invalid("malformed clustercreate stage error");
  }
  const std::size_t code_end = reply.find(' ', stage_end + 1);
  if (code_end == std::string_view::npos || code_end + 1 >= reply.size()) {
    return Invalid("malformed clustercreate code error");
  }
  const std::string_view code =
      reply.substr(stage_end + 1, code_end - stage_end - 1);
  const std::string detail(reply);
  if (code == "uncertain-outcome") return absl::AbortedError(detail);
  if (code == "already-created" || code == "non-pristine" ||
      code == "pre-commit-failed" || code == "bad-request") {
    return absl::FailedPreconditionError(detail);
  }
  return Invalid("unknown clustercreate error code");
}

absl::Status BeforeMutationFailure(absl::Status status) {
  if (status.code() == absl::StatusCode::kDeadlineExceeded ||
      status.code() == absl::StatusCode::kUnavailable ||
      status.code() == absl::StatusCode::kAborted) {
    return absl::CancelledError(
        "cluster-create stopped before sending a mutation: " +
        std::string(status.message()));
  }
  return status;
}

}  // namespace

absl::StatusOr<ClusterCreateManifestV1> ParseClusterCreateManifest(
    std::string_view toml) {
  if (toml.size() > kMaxManifestBytes) {
    return absl::ResourceExhaustedError("cluster manifest exceeds 64 KiB");
  }

  std::istringstream input{std::string(toml)};
  std::vector<CLI::ConfigItem> items;
  try {
    items = CLI::ConfigTOML{}.from_config(input);
  } catch (const CLI::Error& error) {
    return Invalid(std::string("invalid TOML: ") + error.what());
  }

  enum class Section { kNone, kBootstrapPolicy, kMeta, kData, kGroup, kSlot };
  ClusterCreateManifestV1 result;
  Section section = Section::kNone;
  std::set<std::string> section_fields;
  std::set<std::string> top_fields;
  bool saw_bootstrap_policy = false;

  const auto finish_section = [&]() -> absl::Status {
    switch (section) {
      case Section::kNone:
        return absl::OkStatus();
      case Section::kBootstrapPolicy:
        break;
      case Section::kMeta:
        if (section_fields != std::set<std::string>{"ctl_endpoint",
                                                    "data_control_endpoint",
                                                    "id", "raft_endpoint"}) {
          return Invalid(
              "meta_members requires exactly id, raft_endpoint, "
              "data_control_endpoint, and ctl_endpoint");
        }
        break;
      case Section::kData:
        if (!section_fields.contains("id") ||
            (!section_fields.contains("client_endpoint") &&
             !section_fields.contains("tls_endpoint"))) {
          return Invalid(
              "data_nodes requires id and client_endpoint or tls_endpoint");
        }
        break;
      case Section::kGroup:
        if (!section_fields.contains("id") ||
            !section_fields.contains("primary") || section_fields.size() > 3 ||
            (section_fields.size() == 3 &&
             !section_fields.contains("replicas"))) {
          return Invalid(
              "groups requires id and primary, with optional replicas");
        }
        break;
      case Section::kSlot:
        if (section_fields != std::set<std::string>{"first", "group", "last"})
          return Invalid("slot_ranges requires exactly first, last, and group");
        break;
    }
    return absl::OkStatus();
  };

  for (const CLI::ConfigItem& item : items) {
    if (item.name == "++") {
      if (section != Section::kNone || item.parents.size() != 1) {
        return Invalid("unknown nested manifest section");
      }
      section_fields.clear();
      const std::string& name = item.parents.front();
      if (name == "bootstrap_policy") {
        if (saw_bootstrap_policy) {
          return Invalid("duplicate bootstrap_policy section");
        }
        saw_bootstrap_policy = true;
        section = Section::kBootstrapPolicy;
      } else if (name == "meta_members") {
        section = Section::kMeta;
        result.meta_members_.emplace_back();
      } else if (name == "data_nodes") {
        section = Section::kData;
        result.data_nodes_.emplace_back();
      } else if (name == "groups") {
        section = Section::kGroup;
        result.groups_.emplace_back();
      } else if (name == "slot_ranges") {
        section = Section::kSlot;
        result.slot_ranges_.emplace_back();
      } else {
        return Invalid("unknown manifest section " + name);
      }
      continue;
    }
    if (item.name == "--") {
      if (absl::Status status = finish_section(); !status.ok()) return status;
      section = Section::kNone;
      section_fields.clear();
      continue;
    }

    if (item.parents.empty()) {
      if (section != Section::kNone || !top_fields.insert(item.name).second) {
        return Invalid("duplicate or misplaced manifest field " + item.name);
      }
      if (item.name == "schema_version") {
        auto value = ParseUnsigned<std::uint32_t>(item);
        if (!value.ok()) return value.status();
        result.schema_version_ = *value;
      } else if (item.name == "slot_strategy") {
        auto value = ParseString(item);
        if (!value.ok()) return value.status();
        if (*value != "contiguous-even") {
          return Invalid("unsupported Slot allocation strategy");
        }
        result.slots_generated_ = true;
      } else {
        return Invalid("unknown manifest field " + item.name);
      }
      continue;
    }
    if (item.parents.size() != 1 || section == Section::kNone ||
        !section_fields.insert(item.name).second) {
      return Invalid("unknown or duplicate manifest field " + item.fullname());
    }

    switch (section) {
      case Section::kBootstrapPolicy:
        if (item.name == "automatic_uncontrolled_failover_suspect_after_ms") {
          auto value = ParseUnsigned<std::uint64_t>(item);
          if (!value.ok()) return value.status();
          result.automatic_uncontrolled_failover_suspect_after_ms_ = *value;
        } else if (item.name == "authority_lease_duration_ms") {
          auto value = ParseUnsigned<std::uint64_t>(item);
          if (!value.ok()) return value.status();
          result.authority_lease_duration_ms_ = *value;
        } else {
          return Invalid("unknown bootstrap_policy field");
        }
        break;
      case Section::kMeta: {
        if (item.name == "id") {
          auto value = ParseUnsigned<std::uint32_t>(item);
          if (!value.ok()) return value.status();
          result.meta_members_.back().server_id_ = *value;
        } else if (item.name == "raft_endpoint") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.meta_members_.back().raft_endpoint_ = std::move(*value);
        } else if (item.name == "data_control_endpoint") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.meta_members_.back().data_control_endpoint_ =
              std::move(*value);
        } else if (item.name == "ctl_endpoint") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.meta_members_.back().ctl_endpoint_ = std::move(*value);
        } else {
          return Invalid("unknown meta_members field");
        }
        break;
      }
      case Section::kData:
        if (item.name == "id") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.data_nodes_.back().node_id_ = std::move(*value);
        } else if (item.name == "client_endpoint") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.data_nodes_.back().client_endpoint_ = std::move(*value);
        } else if (item.name == "tls_endpoint") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.data_nodes_.back().tls_endpoint_ = std::move(*value);
        } else {
          return Invalid("unknown data_nodes field");
        }
        break;
      case Section::kGroup:
        if (item.name == "id") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.groups_.back().group_id_ = std::move(*value);
        } else if (item.name == "primary") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.groups_.back().primary_node_id_ = std::move(*value);
        } else if (item.name == "replicas") {
          auto values = ParseStringList(item);
          if (!values.ok()) return values.status();
          result.groups_.back().replica_node_ids_ = std::move(*values);
        } else {
          return Invalid("unknown groups field");
        }
        break;
      case Section::kSlot:
        if (item.name == "first") {
          auto value = ParseUnsigned<std::uint16_t>(item);
          if (!value.ok()) return value.status();
          result.slot_ranges_.back().first_ = *value;
        } else if (item.name == "last") {
          auto value = ParseUnsigned<std::uint16_t>(item);
          if (!value.ok()) return value.status();
          result.slot_ranges_.back().last_ = *value;
        } else if (item.name == "group") {
          auto value = ParseString(item);
          if (!value.ok()) return value.status();
          result.slot_ranges_.back().group_id_ = std::move(*value);
        } else {
          return Invalid("unknown slot_ranges field");
        }
        break;
      case Section::kNone:
        return Invalid("manifest section state is invalid");
    }
  }
  if (section != Section::kNone) {
    if (absl::Status status = finish_section(); !status.ok()) return status;
  }
  if (!top_fields.contains("schema_version") || result.meta_members_.empty()) {
    return Invalid("manifest requires schema_version and Meta members");
  }
  if (result.slots_generated_ && !result.slot_ranges_.empty()) {
    return Invalid("slot_strategy and slot_ranges are mutually exclusive");
  }
  if (absl::Status status = ValidateAndNormalize(&result); !status.ok()) {
    return status;
  }
  return result;
}

absl::StatusOr<std::string> EncodeClusterCreateRequest(
    const ClusterCreateManifestV1& manifest,
    const MetaOperationId& root_operation_id) {
  ClusterCreateManifestV1 canonical = manifest;
  if (absl::Status status = ValidateAndNormalize(&canonical); !status.ok()) {
    return status;
  }
  if (canonical != manifest) {
    return Invalid("clustercreate request manifest is not normalized");
  }
  if (IsZero(root_operation_id))
    return Invalid("clustercreate root operation id is zero");

  Writer writer;
  writer.U16(kWireVersion);
  writer.Raw(
      std::string_view(reinterpret_cast<const char*>(root_operation_id.data()),
                       root_operation_id.size()));
  writer.U32(static_cast<std::uint32_t>(
      manifest.automatic_uncontrolled_failover_suspect_after_ms_));
  writer.U32(static_cast<std::uint32_t>(manifest.authority_lease_duration_ms_));
  writer.U32(static_cast<std::uint32_t>(manifest.meta_members_.size()));
  for (const auto& member : manifest.meta_members_) {
    writer.U32(member.server_id_);
    if (absl::Status status = writer.String(member.raft_endpoint_);
        !status.ok()) {
      return status;
    }
    if (absl::Status status = writer.String(member.data_control_endpoint_);
        !status.ok()) {
      return status;
    }
    if (absl::Status status = writer.String(member.ctl_endpoint_);
        !status.ok()) {
      return status;
    }
  }
  writer.U16(manifest.slots_generated_ ? 1 : 0);
  writer.U32(static_cast<std::uint32_t>(manifest.data_nodes_.size()));
  for (const auto& node : manifest.data_nodes_) {
    if (absl::Status status = writer.String(node.node_id_); !status.ok())
      return status;
    if (absl::Status status = writer.String(node.client_endpoint_);
        !status.ok())
      return status;
    if (absl::Status status = writer.String(node.tls_endpoint_); !status.ok())
      return status;
  }
  writer.U32(static_cast<std::uint32_t>(manifest.groups_.size()));
  for (const auto& group : manifest.groups_) {
    if (absl::Status status = writer.String(group.group_id_); !status.ok())
      return status;
    if (absl::Status status = writer.String(group.primary_node_id_);
        !status.ok())
      return status;
    writer.U32(static_cast<std::uint32_t>(group.replica_node_ids_.size()));
    for (const std::string& replica : group.replica_node_ids_) {
      if (absl::Status status = writer.String(replica); !status.ok())
        return status;
    }
  }
  writer.U32(static_cast<std::uint32_t>(manifest.slot_ranges_.size()));
  for (const auto& range : manifest.slot_ranges_) {
    writer.U16(range.first_);
    writer.U16(range.last_);
    if (absl::Status status = writer.String(range.group_id_); !status.ok())
      return status;
  }
  std::string request = "clustercreate 1 " + Hex(writer.bytes());
  if (request.size() + 1 > kMaxAdminCommandBytes) {
    return absl::ResourceExhaustedError(
        "clustercreate request exceeds 64 KiB limit");
  }
  return request;
}

absl::StatusOr<ClusterCreateManifestV1> DecodeClusterCreateRequest(
    std::string_view request, MetaOperationId* root_operation_id) {
  constexpr std::string_view kPrefix = "clustercreate 1 ";
  if (root_operation_id == nullptr || !request.starts_with(kPrefix) ||
      request.size() + 1 > kMaxAdminCommandBytes) {
    return Invalid("invalid clustercreate request envelope");
  }
  auto bytes = Unhex(request.substr(kPrefix.size()));
  if (!bytes.ok()) return bytes.status();
  Reader reader(*bytes);
  auto version = reader.U16();
  if (!version.ok()) return version.status();
  if (*version != kWireVersion)
    return Invalid("unsupported clustercreate version");
  auto root = reader.Raw(root_operation_id->size());
  if (!root.ok()) return root.status();
  std::copy(root->begin(), root->end(), root_operation_id->begin());
  if (IsZero(*root_operation_id))
    return Invalid("clustercreate root operation id is zero");

  ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  auto suspect_after_ms = reader.U32();
  auto authority_lease_duration_ms = reader.U32();
  if (!suspect_after_ms.ok() || !authority_lease_duration_ms.ok()) {
    return Invalid("invalid bootstrap Policy defaults");
  }
  manifest.automatic_uncontrolled_failover_suspect_after_ms_ =
      *suspect_after_ms;
  manifest.authority_lease_duration_ms_ = *authority_lease_duration_ms;
  auto meta_count = reader.U32();
  if (!meta_count.ok() || *meta_count == 0 || *meta_count > kMaxManifestItems) {
    return Invalid("invalid Meta member count");
  }
  manifest.meta_members_.reserve(*meta_count);
  for (std::uint32_t index = 0; index < *meta_count; ++index) {
    ClusterCreateManifestV1::MetaMember member;
    auto id = reader.U32();
    auto raft = reader.String();
    auto data_control = reader.String();
    auto ctl = reader.String();
    if (!id.ok() || !raft.ok() || !data_control.ok() || !ctl.ok()) {
      return Invalid("invalid Meta member descriptor");
    }
    member.server_id_ = *id;
    member.raft_endpoint_ = std::move(*raft);
    member.data_control_endpoint_ = std::move(*data_control);
    member.ctl_endpoint_ = std::move(*ctl);
    manifest.meta_members_.push_back(std::move(member));
  }
  auto generated = reader.U16();
  if (!generated.ok() || *generated > 1) {
    return Invalid("invalid clustercreate Slot allocation mode");
  }
  manifest.slots_generated_ = *generated == 1;

  auto node_count = reader.U32();
  if (!node_count.ok() || *node_count > kMaxManifestItems)
    return Invalid("invalid Data node count");
  manifest.data_nodes_.reserve(*node_count);
  for (std::uint32_t index = 0; index < *node_count; ++index) {
    ClusterCreateManifestV1::DataNode node;
    auto id = reader.String();
    if (!id.ok()) return id.status();
    node.node_id_ = std::move(*id);
    auto endpoint = reader.String();
    if (!endpoint.ok()) return endpoint.status();
    node.client_endpoint_ = std::move(*endpoint);
    auto tls_endpoint = reader.String();
    if (!tls_endpoint.ok()) return tls_endpoint.status();
    node.tls_endpoint_ = std::move(*tls_endpoint);
    manifest.data_nodes_.push_back(std::move(node));
  }

  auto group_count = reader.U32();
  if (!group_count.ok() || *group_count > kMaxManifestItems)
    return Invalid("invalid Group count");
  manifest.groups_.reserve(*group_count);
  for (std::uint32_t index = 0; index < *group_count; ++index) {
    ClusterCreateManifestV1::Group group;
    auto id = reader.String();
    if (!id.ok()) return id.status();
    group.group_id_ = std::move(*id);
    auto primary = reader.String();
    if (!primary.ok()) return primary.status();
    group.primary_node_id_ = std::move(*primary);
    auto replica_count = reader.U32();
    if (!replica_count.ok() || *replica_count > kMaxManifestItems)
      return Invalid("invalid replica count");
    group.replica_node_ids_.reserve(*replica_count);
    for (std::uint32_t replica = 0; replica < *replica_count; ++replica) {
      auto id_value = reader.String();
      if (!id_value.ok()) return id_value.status();
      group.replica_node_ids_.push_back(std::move(*id_value));
    }
    manifest.groups_.push_back(std::move(group));
  }

  auto range_count = reader.U32();
  if (!range_count.ok() || *range_count > kMaxManifestItems)
    return Invalid("invalid Slot range count");
  manifest.slot_ranges_.reserve(*range_count);
  for (std::uint32_t index = 0; index < *range_count; ++index) {
    ClusterCreateManifestV1::SlotRange range;
    auto first = reader.U16();
    if (!first.ok()) return first.status();
    range.first_ = *first;
    auto last = reader.U16();
    if (!last.ok()) return last.status();
    range.last_ = *last;
    auto group = reader.String();
    if (!group.ok()) return group.status();
    range.group_id_ = std::move(*group);
    manifest.slot_ranges_.push_back(std::move(range));
  }
  if (!reader.done()) return Invalid("trailing clustercreate request data");
  ClusterCreateManifestV1 canonical = manifest;
  if (absl::Status status = ValidateAndNormalize(&canonical); !status.ok())
    return status;
  if (canonical != manifest)
    return Invalid("clustercreate request is not canonical");
  return manifest;
}

absl::StatusOr<ClusterCreateOutcome> DecodeClusterCreateReply(
    std::string_view reply) {
  constexpr std::string_view kPrefix = "OK clustercreate 1 ";
  if (!reply.starts_with(kPrefix))
    return Invalid("invalid clustercreate reply");
  reply.remove_prefix(kPrefix.size());
  std::istringstream input{std::string(reply)};
  std::string genesis_index_text;
  ClusterCreateOutcome outcome;
  if (!(input >> genesis_index_text >> outcome.operation_id_))
    return Invalid("invalid clustercreate reply");
  auto genesis_index = ParseU64(genesis_index_text);
  if (!genesis_index.ok() || *genesis_index == 0 ||
      !IsCanonicalOperationId(outcome.operation_id_)) {
    return Invalid("invalid clustercreate reply");
  }
  outcome.genesis_commit_index_ = *genesis_index;
  std::string trailing;
  if (input >> trailing) return Invalid("invalid clustercreate reply");
  return outcome;
}

absl::StatusOr<ClusterCreateOutcome> ClusterOperator::Create(
    const MetaAdminTarget& seed, const ClusterCreateManifestV1& manifest,
    const ClusterStatusOptions& options) const {
  ClusterCreateManifestV1 canonical = manifest;
  if (absl::Status status = ValidateAndNormalize(&canonical); !status.ok())
    return status;
  if (canonical != manifest)
    return Invalid("clustercreate manifest is not normalized");

  MetaAdminTarget leader;
  auto initial = CaptureStatus(seed, options, &leader);
  if (!initial.ok()) return BeforeMutationFailure(initial.status());
  if (!initial->status_.has_value()) {
    return BeforeMutationFailure(
        absl::UnavailableError(initial->retry_reason_));
  }
  const ClusterStatusWireV1& status = *initial->status_;
  // Lifecycle admission survives Meta membership changes. Only a pristine,
  // uninitialized cluster compares the manifest with the current Meta set.
  if (status.cluster_state_ == ClusterStateWireV1::kNonPristine) {
    return absl::FailedPreconditionError(
        "cluster-create non-pristine: Uninitialized Meta contains "
        "Data-cluster artifacts");
  }
  if (status.cluster_state_ != ClusterStateWireV1::kUninitialized) {
    return absl::FailedPreconditionError(
        "cluster-create already-created: Meta already owns a Data cluster");
  }
  const bool meta_matches =
      status.meta_members_.size() == manifest.meta_members_.size() &&
      std::equal(status.meta_members_.begin(), status.meta_members_.end(),
                 manifest.meta_members_.begin(),
                 [](const auto& actual, const auto& expected) {
                   return actual.server_id_ == expected.server_id_;
                 });
  if (!meta_matches) {
    return absl::FailedPreconditionError(
        "cluster-create bad-request: manifest does not match the committed "
        "Meta set");
  }

  if (std::chrono::steady_clock::now() >= options.deadline_) {
    return BeforeMutationFailure(
        absl::DeadlineExceededError("cluster-create deadline expired"));
  }
  auto root_operation_id = GenerateOperationId();
  if (!root_operation_id.ok()) return root_operation_id.status();
  const std::string expected_id = Hex(
      std::string_view(reinterpret_cast<const char*>(root_operation_id->data()),
                       root_operation_id->size()));
  auto request = EncodeClusterCreateRequest(manifest, *root_operation_id);
  if (!request.ok()) return request.status();
  auto reply = round_trip_(leader, *request, options.deadline_);
  if (!reply.ok()) {
    if (MetaAdminRequestDefinitelyNotSent(reply.status())) {
      return BeforeMutationFailure(reply.status());
    }
    return absl::AbortedError(
        "cluster-create proposal outcome is uncertain; operation=" +
        expected_id + " detail=" + std::string(reply.status().message()));
  }
  if (reply->starts_with("ERR clustercreate ")) {
    absl::Status error = ClusterCreateReplyError(*reply);
    if (error.code() == absl::StatusCode::kFailedPrecondition) return error;
    return absl::AbortedError(
        "cluster-create response did not prove a pre-commit rejection; "
        "operation=" +
        expected_id + " detail=" + std::string(error.message()));
  }
  auto outcome = DecodeClusterCreateReply(*reply);
  if (!outcome.ok()) {
    return absl::AbortedError(
        "cluster-create response was not trustworthy; operation=" +
        expected_id + " detail=" + std::string(outcome.status().message()));
  }
  if (outcome->operation_id_ != expected_id) {
    return absl::AbortedError(
        "cluster-create response named another operation; operation=" +
        expected_id);
  }
  return *outcome;
}

}  // namespace lavik::meta
