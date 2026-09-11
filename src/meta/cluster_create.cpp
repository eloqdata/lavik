#include "keylane/meta/cluster_create.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "keylane/CLI11.hpp"
#include "keylane/meta/cluster_status.h"
#include "keylane/numeric_endpoint.h"

namespace keylane::meta {
namespace {

constexpr std::size_t kMaxManifestBytes = 64 * 1024;
constexpr std::size_t kMaxAdminCommandBytes = 64 * 1024;
constexpr std::size_t kMaxWireString = 64 * 1024;
constexpr std::uint16_t kWireVersion = 1;
constexpr std::uint32_t kMaxClusterCreateTimeoutMs = 3'600'000;
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
      value > std::numeric_limits<T>::max() ||
      input != std::to_string(value)) {
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

bool CanonicalEndpoint(std::string_view value) {
  constexpr std::string_view kTcpPrefix = "tcp://";
  if (!value.starts_with(kTcpPrefix)) return false;
  value.remove_prefix(kTcpPrefix.size());
  const auto endpoint = ParseNumericEndpoint(value);
  return endpoint.has_value() && FormatNumericEndpoint(*endpoint) == value;
}

absl::Status ValidateAndNormalize(ClusterCreateManifestV1* manifest) {
  if (manifest == nullptr || manifest->schema_version_ != 1 ||
      manifest->meta_member_id_ == 0 || manifest->data_nodes_.empty() ||
      manifest->groups_.empty() ||
      manifest->data_nodes_.size() > kMaxManifestItems ||
      manifest->groups_.size() > kMaxManifestItems ||
      manifest->slot_ranges_.size() > kMaxManifestItems) {
    return Invalid("clustercreate manifest is not a supported v1 topology");
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
    if (!CanonicalEndpoint(node.client_endpoint_)) {
      return Invalid(
          "client endpoint must be a canonical numeric tcp:// endpoint");
    }
    if (!node_ids.insert(node.node_id_).second) {
      return Invalid("duplicate Data node id");
    }
    if (!endpoints.insert(node.client_endpoint_).second) {
      return Invalid("duplicate Data client endpoint");
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
    std::sort(group.replica_node_ids_.begin(),
              group.replica_node_ids_.end());
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
    if (!canonical.empty() &&
        canonical.back().group_id_ == range.group_id_ &&
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
    if (high < 0 || low < 0) return Invalid("invalid clustercreate hex payload");
    result[index / 2] = static_cast<char>((high << 4) | low);
  }
  return result;
}

absl::StatusOr<std::uint64_t> ParseU64(std::string_view text) {
  std::uint64_t result = 0;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), result);
  if (text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != text.data() + text.size() || text != std::to_string(result)) {
    return Invalid("invalid clustercreate reply integer");
  }
  return result;
}

bool ReadyMatches(const ClusterStatusWireV1& status,
                  const ClusterCreateManifestV1& manifest) {
  if (!status.cluster_ready_ || status.meta_members_.size() != 1 ||
      status.meta_members_.front().server_id_ != manifest.meta_member_id_ ||
      status.data_nodes_.size() != manifest.data_nodes_.size() ||
      status.groups_.size() != manifest.groups_.size() ||
      status.slot_ranges_.size() != manifest.slot_ranges_.size()) {
    return false;
  }

  std::map<std::string, std::pair<std::string, ClusterDataNodeRole>> expected;
  for (const auto& group : manifest.groups_) {
    expected.emplace(group.primary_node_id_,
                     std::pair(group.group_id_, ClusterDataNodeRole::kPrimary));
    for (const std::string& replica : group.replica_node_ids_) {
      expected.emplace(replica,
                       std::pair(group.group_id_, ClusterDataNodeRole::kReplica));
    }
  }
  for (const auto& node : status.data_nodes_) {
    const auto found = expected.find(node.node_id_);
    if (found == expected.end() || node.retired_ ||
        node.group_id_ != std::optional<std::string>(found->second.first) ||
        node.role_ != found->second.second || !node.current_session_ ||
        !node.projection_current_ || !node.health_fresh_ ||
        !node.population_current_ ||
        (node.role_ == ClusterDataNodeRole::kPrimary &&
         node.lease_status_ != ClusterLeaseStatus::kRecentlyGranted)) {
      return false;
    }
  }
  for (const auto& expected_group : manifest.groups_) {
    const auto group = std::find_if(
        status.groups_.begin(), status.groups_.end(), [&](const auto& item) {
          return item.group_id_ == expected_group.group_id_;
        });
    if (group == status.groups_.end() || group->term_ != 1 ||
        group->owner_node_id_ !=
            std::optional<std::string>(expected_group.primary_node_id_) ||
        group->config_epoch_ != 1 || !group->grant_revision_.has_value() ||
        !group->serving_ready_ || !group->topology_converged_) {
      return false;
    }
  }
  for (std::size_t index = 0; index < manifest.slot_ranges_.size(); ++index) {
    const auto& actual = status.slot_ranges_[index];
    const auto& expected_range = manifest.slot_ranges_[index];
    if (std::tie(actual.first_, actual.last_, actual.group_id_) !=
        std::tie(expected_range.first_, expected_range.last_,
                 expected_range.group_id_)) {
      return false;
    }
  }
  return true;
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
  if (code == "not-leader") return absl::UnavailableError(detail);
  if (code == "bad-request") return Invalid(detail);
  if (code == "runtime-invalid" || code == "non-empty-cluster" ||
      code == "domain-rejected" || code == "data-rejected") {
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

  enum class Section { kNone, kMeta, kData, kGroup, kSlot };
  ClusterCreateManifestV1 result;
  Section section = Section::kNone;
  std::set<std::string> section_fields;
  std::set<std::string> top_fields;
  std::vector<std::uint32_t> meta_members;

  const auto finish_section = [&]() -> absl::Status {
    switch (section) {
      case Section::kNone:
        return absl::OkStatus();
      case Section::kMeta:
        if (section_fields != std::set<std::string>{"id"})
          return Invalid("meta_members requires exactly id");
        break;
      case Section::kData:
        if (section_fields !=
            std::set<std::string>{"client_endpoint", "id"})
          return Invalid("data_nodes requires exactly id and client_endpoint");
        break;
      case Section::kGroup:
        if (!section_fields.contains("id") ||
            !section_fields.contains("primary") ||
            section_fields.size() > 3 ||
            (section_fields.size() == 3 &&
             !section_fields.contains("replicas"))) {
          return Invalid("groups requires id and primary, with optional replicas");
        }
        break;
      case Section::kSlot:
        if (section_fields !=
            std::set<std::string>{"first", "group", "last"})
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
      if (name == "meta_members") {
        section = Section::kMeta;
        meta_members.push_back(0);
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
      case Section::kMeta: {
        if (item.name != "id") return Invalid("unknown meta_members field");
        auto value = ParseUnsigned<std::uint32_t>(item);
        if (!value.ok()) return value.status();
        meta_members.back() = *value;
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
  if (!top_fields.contains("schema_version") || meta_members.size() != 1 ||
      meta_members.front() == 0) {
    return Invalid("manifest requires schema_version and exactly one Meta member");
  }
  if (result.slots_generated_ && !result.slot_ranges_.empty()) {
    return Invalid("slot_strategy and slot_ranges are mutually exclusive");
  }
  result.meta_member_id_ = meta_members.front();
  if (absl::Status status = ValidateAndNormalize(&result); !status.ok()) {
    return status;
  }
  return result;
}

absl::StatusOr<std::string> EncodeClusterCreateRequest(
    const ClusterCreateManifestV1& manifest, std::uint32_t wait_timeout_ms) {
  ClusterCreateManifestV1 canonical = manifest;
  if (absl::Status status = ValidateAndNormalize(&canonical); !status.ok()) {
    return status;
  }
  if (canonical != manifest) {
    return Invalid("clustercreate request manifest is not normalized");
  }
  if (wait_timeout_ms == 0 || wait_timeout_ms > kMaxClusterCreateTimeoutMs) {
    return Invalid("clustercreate timeout is out of range");
  }

  Writer writer;
  writer.U16(kWireVersion);
  writer.U32(manifest.meta_member_id_);
  writer.U16(manifest.slots_generated_ ? 1 : 0);
  writer.U32(static_cast<std::uint32_t>(manifest.data_nodes_.size()));
  for (const auto& node : manifest.data_nodes_) {
    if (absl::Status status = writer.String(node.node_id_); !status.ok())
      return status;
    if (absl::Status status = writer.String(node.client_endpoint_); !status.ok())
      return status;
  }
  writer.U32(static_cast<std::uint32_t>(manifest.groups_.size()));
  for (const auto& group : manifest.groups_) {
    if (absl::Status status = writer.String(group.group_id_); !status.ok())
      return status;
    if (absl::Status status = writer.String(group.primary_node_id_); !status.ok())
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
  writer.U32(wait_timeout_ms);
  std::string request = "clustercreate 1 " + Hex(writer.bytes());
  if (request.size() + 1 > kMaxAdminCommandBytes) {
    return absl::ResourceExhaustedError(
        "clustercreate request exceeds 64 KiB limit");
  }
  return request;
}

absl::StatusOr<ClusterCreateManifestV1> DecodeClusterCreateRequest(
    std::string_view request, std::uint32_t* wait_timeout_ms) {
  constexpr std::string_view kPrefix = "clustercreate 1 ";
  if (wait_timeout_ms == nullptr || !request.starts_with(kPrefix) ||
      request.size() + 1 > kMaxAdminCommandBytes) {
    return Invalid("invalid clustercreate request envelope");
  }
  auto bytes = Unhex(request.substr(kPrefix.size()));
  if (!bytes.ok()) return bytes.status();
  Reader reader(*bytes);
  auto version = reader.U16();
  if (!version.ok()) return version.status();
  if (*version != kWireVersion) return Invalid("unsupported clustercreate version");

  ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  auto meta = reader.U32();
  if (!meta.ok()) return meta.status();
  manifest.meta_member_id_ = *meta;
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
  auto timeout = reader.U32();
  if (!timeout.ok()) return timeout.status();
  *wait_timeout_ms = *timeout;
  if (!reader.done()) return Invalid("trailing clustercreate request data");
  ClusterCreateManifestV1 canonical = manifest;
  if (absl::Status status = ValidateAndNormalize(&canonical); !status.ok())
    return status;
  if (canonical != manifest) return Invalid("clustercreate request is not canonical");
  if (*wait_timeout_ms == 0 || *wait_timeout_ms > kMaxClusterCreateTimeoutMs)
    return Invalid("clustercreate timeout is out of range");
  return manifest;
}

absl::StatusOr<ClusterCreateOutcome> DecodeClusterCreateReply(
    std::string_view reply) {
  constexpr std::string_view kPrefix = "OK clustercreate 1 ";
  if (!reply.starts_with(kPrefix)) return Invalid("invalid clustercreate reply");
  reply.remove_prefix(kPrefix.size());
  std::istringstream input{std::string(reply)};
  std::string final_index_text;
  std::string count_text;
  if (!(input >> final_index_text >> count_text))
    return Invalid("invalid clustercreate reply");
  auto final_index = ParseU64(final_index_text);
  auto count = ParseU64(count_text);
  if (!final_index.ok() || !count.ok() || *final_index == 0 ||
      *count == 0 || *count > kMaxManifestItems) {
    return Invalid("invalid clustercreate reply");
  }
  ClusterCreateOutcome outcome;
  outcome.committed_index_ = *final_index;
  outcome.groups_.reserve(static_cast<std::size_t>(*count));
  for (std::uint64_t index = 0; index < *count; ++index) {
    std::string group_hex;
    std::string committed_text;
    ClusterCreateOutcome::Group group;
    if (!(input >> group_hex >> committed_text >> group.operation_id_))
      return Invalid("invalid clustercreate reply");
    auto group_id = Unhex(group_hex);
    auto committed = ParseU64(committed_text);
    if (!group_id.ok() || group_id->empty() || group_id->size() > 64 ||
        !committed.ok() || *committed == 0 ||
        !IsCanonicalOperationId(group.operation_id_)) {
      return Invalid("invalid clustercreate reply");
    }
    group.group_id_ = std::move(*group_id);
    group.committed_index_ = *committed;
    outcome.groups_.push_back(std::move(group));
  }
  std::string trailing;
  if (input >> trailing) return Invalid("invalid clustercreate reply");
  if (std::any_of(outcome.groups_.begin(), outcome.groups_.end(),
                  [&](const auto& group) {
                    return group.committed_index_ > outcome.committed_index_;
                  }) ||
      !std::is_sorted(outcome.groups_.begin(), outcome.groups_.end(),
                      [](const auto& left, const auto& right) {
                        return left.group_id_ < right.group_id_;
                      })) {
    return Invalid("clustercreate reply is not canonical");
  }
  return outcome;
}

absl::StatusOr<ClusterCreateOutcome> ClusterOperator::Create(
    const MetaAdminTarget& seed, const ClusterCreateManifestV1& manifest,
    const ClusterStatusOptions& options) const {
  ClusterCreateManifestV1 canonical = manifest;
  if (absl::Status status = ValidateAndNormalize(&canonical); !status.ok())
    return status;
  if (canonical != manifest) return Invalid("clustercreate manifest is not normalized");

  MetaAdminTarget leader;
  auto initial = CaptureStatus(seed, options, &leader);
  if (!initial.ok()) return BeforeMutationFailure(initial.status());
  if (!initial->status_.has_value()) {
    return BeforeMutationFailure(absl::UnavailableError(initial->retry_reason_));
  }
  const ClusterStatusWireV1& status = *initial->status_;
  const bool create_active = std::any_of(
      status.blockers_.begin(), status.blockers_.end(),
      [](const ClusterBlockerWireV1& blocker) {
        return blocker.code_ == kClusterCreateActiveBlockerCode;
      });
  if (status.meta_members_.size() != 1 ||
      status.meta_members_.front().server_id_ != manifest.meta_member_id_ ||
      create_active || !status.data_nodes_.empty() || !status.groups_.empty() ||
      !status.slot_ranges_.empty()) {
    return absl::FailedPreconditionError(
        "cluster-create requires the named single Meta and an empty topology");
  }

  const auto now = std::chrono::steady_clock::now();
  if (now >= options.deadline_) {
    return BeforeMutationFailure(
        absl::DeadlineExceededError("cluster-create deadline expired"));
  }
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
      options.deadline_ - now);
  // Leave the Admin transport enough of the caller's deadline to deliver the
  // server's structured timeout, including its last node-level blocker.
  constexpr std::int64_t kReplyReserveMs = 250;
  const auto wait_ms = static_cast<std::uint32_t>(std::clamp<std::int64_t>(
      remaining.count() - kReplyReserveMs, 1,
      std::numeric_limits<std::uint32_t>::max()));
  auto request = EncodeClusterCreateRequest(manifest, wait_ms);
  if (!request.ok()) return request.status();
  auto reply = round_trip_(leader, *request, options.deadline_);
  if (!reply.ok()) return reply.status();
  if (reply->starts_with("ERR clustercreate "))
    return ClusterCreateReplyError(*reply);
  auto outcome = DecodeClusterCreateReply(*reply);
  if (!outcome.ok()) return outcome.status();

  std::string last_blocker;
  while (std::chrono::steady_clock::now() < options.deadline_) {
    auto observed = Status(seed, options);
    if (!observed.ok()) return observed.status();
    if (observed->status_.has_value()) {
      if (ReadyMatches(*observed->status_, manifest)) return *outcome;
      if (!observed->status_->blockers_.empty()) {
        const auto& blocker = observed->status_->blockers_.front();
        last_blocker = " last_blocker=" + blocker.code_ + " scope=" +
                       blocker.scope_ + " detail=" + blocker.detail_;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  return absl::DeadlineExceededError(
      "cluster-create committed but the requested topology did not become READY;" +
      last_blocker);
}

}  // namespace keylane::meta
