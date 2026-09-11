#include "keylane/meta/cluster_create.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cstdint>
#include <limits>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "keylane/CLI11.hpp"
#include "keylane/meta/cluster_status.h"
#include "keylane/numeric_endpoint.h"

namespace keylane::meta {
namespace {

constexpr std::size_t kMaxManifestBytes = 64 * 1024;
constexpr std::uint16_t kWireVersion = 1;
constexpr std::size_t kMaxWireString = 64 * 1024;
constexpr std::uint32_t kMaxClusterCreateTimeoutMs = 3'600'000;

absl::Status Invalid(std::string message) {
  return absl::InvalidArgumentError(std::move(message));
}

bool IsCanonicalNodeId(std::string_view value) {
  return value.size() == 40 &&
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
  for (std::size_t ii = 0; ii < input.size(); ii += 2) {
    const int high = nibble(input[ii]);
    const int low = nibble(input[ii + 1]);
    if (high < 0 || low < 0) return Invalid("invalid clustercreate hex payload");
    result[ii / 2] = static_cast<char>((high << 4) | low);
  }
  return result;
}

absl::Status ValidateManifest(const ClusterCreateManifestV1& manifest) {
  if (manifest.schema_version_ != 1 || manifest.meta_member_id_ == 0 ||
      !IsCanonicalNodeId(manifest.data_node_id_) ||
      manifest.primary_node_id_ != manifest.data_node_id_ ||
      manifest.group_id_.empty() || manifest.group_id_.size() > 64 ||
      manifest.first_slot_ != 0 || manifest.last_slot_ != 16383) {
    return Invalid("clustercreate manifest is not the supported v1 topology");
  }
  constexpr std::string_view kTcpPrefix = "tcp://";
  if (!manifest.client_endpoint_.starts_with(kTcpPrefix)) {
    return Invalid("client endpoint must be a numeric tcp:// endpoint");
  }
  const std::string_view encoded_endpoint =
      std::string_view(manifest.client_endpoint_).substr(kTcpPrefix.size());
  const auto endpoint = ParseNumericEndpoint(encoded_endpoint);
  if (!endpoint.has_value() ||
      FormatNumericEndpoint(*endpoint) != encoded_endpoint) {
    return Invalid("client endpoint must be a canonical numeric tcp:// endpoint");
  }
  return absl::OkStatus();
}

bool ReadyMatches(const ClusterStatusWireV1& status,
                  const ClusterCreateManifestV1& manifest) {
  return status.cluster_ready_ && status.meta_members_.size() == 1 &&
         status.meta_members_.front().server_id_ == manifest.meta_member_id_ &&
         status.data_nodes_.size() == 1 &&
         status.data_nodes_.front().node_id_ == manifest.data_node_id_ &&
         status.data_nodes_.front().role_ == ClusterDataNodeRole::kPrimary &&
         status.data_nodes_.front().group_id_ == manifest.group_id_ &&
         status.data_nodes_.front().current_session_ &&
         status.data_nodes_.front().projection_current_ &&
         status.data_nodes_.front().health_fresh_ &&
         status.data_nodes_.front().population_current_ &&
         status.data_nodes_.front().lease_status_ ==
             ClusterLeaseStatus::kRecentlyGranted &&
         status.groups_.size() == 1 &&
         status.groups_.front().group_id_ == manifest.group_id_ &&
         status.groups_.front().term_ == 1 &&
         status.groups_.front().owner_node_id_ == manifest.data_node_id_ &&
         status.groups_.front().config_epoch_ == 1 &&
         status.groups_.front().grant_revision_.has_value() &&
         status.groups_.front().serving_ready_ &&
         status.groups_.front().topology_converged_ &&
         status.slot_ranges_.size() == 1 &&
         status.slot_ranges_.front().first_ == manifest.first_slot_ &&
         status.slot_ranges_.front().last_ == manifest.last_slot_ &&
         status.slot_ranges_.front().group_id_ == manifest.group_id_;
}

absl::Status ClusterCreateReplyError(std::string_view reply) {
  constexpr std::string_view kPrefix = "ERR clustercreate 1 ";
  if (!reply.starts_with(kPrefix)) {
    return absl::InvalidArgumentError("malformed clustercreate error reply");
  }
  reply.remove_prefix(kPrefix.size());
  const std::size_t stage_end = reply.find(' ');
  if (stage_end == std::string_view::npos) {
    return absl::InvalidArgumentError("malformed clustercreate stage error");
  }
  const std::size_t code_end = reply.find(' ', stage_end + 1);
  if (code_end == std::string_view::npos || code_end + 1 >= reply.size()) {
    return absl::InvalidArgumentError("malformed clustercreate code error");
  }
  const std::string_view code =
      reply.substr(stage_end + 1, code_end - stage_end - 1);
  const std::string detail(reply);
  if (code == "uncertain-outcome") {
    return absl::AbortedError(detail);
  }
  if (code == "not-leader") {
    return absl::UnavailableError(detail);
  }
  if (code == "bad-request") {
    return absl::InvalidArgumentError(detail);
  }
  if (code == "runtime-invalid" || code == "non-empty-cluster" ||
      code == "domain-rejected" ||
      code == "data-rejected") {
    return absl::FailedPreconditionError(detail);
  }
  return absl::InvalidArgumentError("unknown clustercreate error code");
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

  ClusterCreateManifestV1 result;
  std::string slot_group;
  std::set<std::string> seen_fields;
  std::set<std::string> seen_sections;
  static const std::set<std::string> kAllowedSections = {
      "meta_members", "data_nodes", "groups", "slot_ranges"};
  std::string section;
  for (const CLI::ConfigItem& item : items) {
    if (item.name == "++") {
      if (item.parents.size() != 1 ||
          !kAllowedSections.contains(item.parents.front()) ||
          !seen_sections.insert(item.parents.front()).second) {
        return Invalid("unknown or duplicate manifest section");
      }
      section = item.parents.front();
      continue;
    }
    if (item.name == "--") {
      section.clear();
      continue;
    }
    if (item.parents.size() > 1 ||
        (!item.parents.empty() && item.parents.front() != section)) {
      return Invalid("unknown nested manifest section");
    }
    const std::string path =
        item.parents.empty() ? item.name : item.parents.front() + "." + item.name;
    if (!seen_fields.insert(path).second) {
      return Invalid("duplicate manifest field " + path);
    }

    if (path == "schema_version") {
      auto value = ParseUnsigned<std::uint32_t>(item);
      if (!value.ok()) return value.status();
      result.schema_version_ = *value;
    } else if (path == "meta_members.id") {
      auto value = ParseUnsigned<std::uint32_t>(item);
      if (!value.ok()) return value.status();
      result.meta_member_id_ = *value;
    } else if (path == "data_nodes.id") {
      auto value = ParseString(item);
      if (!value.ok()) return value.status();
      result.data_node_id_ = std::move(*value);
    } else if (path == "data_nodes.client_endpoint") {
      auto value = ParseString(item);
      if (!value.ok()) return value.status();
      result.client_endpoint_ = std::move(*value);
    } else if (path == "groups.id") {
      auto value = ParseString(item);
      if (!value.ok()) return value.status();
      result.group_id_ = std::move(*value);
    } else if (path == "groups.primary") {
      auto value = ParseString(item);
      if (!value.ok()) return value.status();
      result.primary_node_id_ = std::move(*value);
    } else if (path == "slot_ranges.first") {
      auto value = ParseUnsigned<std::uint16_t>(item);
      if (!value.ok()) return value.status();
      result.first_slot_ = *value;
    } else if (path == "slot_ranges.last") {
      auto value = ParseUnsigned<std::uint16_t>(item);
      if (!value.ok()) return value.status();
      result.last_slot_ = *value;
    } else if (path == "slot_ranges.group") {
      auto value = ParseString(item);
      if (!value.ok()) return value.status();
      slot_group = std::move(*value);
    } else {
      return Invalid("unknown manifest field " + path);
    }
  }

  static const std::set<std::string> kRequiredFields = {
      "schema_version",          "meta_members.id",
      "data_nodes.id",           "data_nodes.client_endpoint",
      "groups.id",               "groups.primary",
      "slot_ranges.first",       "slot_ranges.last",
      "slot_ranges.group",
  };
  if (seen_fields != kRequiredFields || result.schema_version_ != 1 ||
      result.meta_member_id_ == 0) {
    return Invalid("manifest must contain exactly the v1 topology fields");
  }
  if (slot_group != result.group_id_) {
    return Invalid("slot range references another group");
  }
  if (absl::Status valid = ValidateManifest(result); !valid.ok()) return valid;
  return result;
}

absl::StatusOr<std::string> EncodeClusterCreateRequest(
    const ClusterCreateManifestV1& manifest, std::uint32_t wait_timeout_ms) {
  if (absl::Status valid = ValidateManifest(manifest); !valid.ok()) return valid;
  if (wait_timeout_ms == 0 ||
      wait_timeout_ms > kMaxClusterCreateTimeoutMs) {
    return Invalid("clustercreate timeout is out of range");
  }
  Writer writer;
  writer.U16(kWireVersion);
  writer.U32(manifest.meta_member_id_);
  for (const std::string* value :
       {&manifest.data_node_id_, &manifest.client_endpoint_,
        &manifest.group_id_, &manifest.primary_node_id_}) {
    if (absl::Status wrote = writer.String(*value); !wrote.ok()) return wrote;
  }
  writer.U16(manifest.first_slot_);
  writer.U16(manifest.last_slot_);
  writer.U32(wait_timeout_ms);
  return "clustercreate 1 " + Hex(writer.bytes());
}

absl::StatusOr<ClusterCreateManifestV1> DecodeClusterCreateRequest(
    std::string_view request, std::uint32_t* wait_timeout_ms) {
  constexpr std::string_view kPrefix = "clustercreate 1 ";
  if (wait_timeout_ms == nullptr || !request.starts_with(kPrefix)) {
    return Invalid("invalid clustercreate request envelope");
  }
  auto bytes = Unhex(request.substr(kPrefix.size()));
  if (!bytes.ok()) return bytes.status();
  Reader reader(*bytes);
  auto version = reader.U16();
  if (!version.ok()) return version.status();
  if (*version != kWireVersion) {
    return Invalid("unsupported clustercreate version");
  }
  ClusterCreateManifestV1 manifest;
  manifest.schema_version_ = 1;
  auto meta_id = reader.U32();
  if (!meta_id.ok()) return meta_id.status();
  manifest.meta_member_id_ = *meta_id;
  for (std::string* value :
       {&manifest.data_node_id_, &manifest.client_endpoint_,
        &manifest.group_id_, &manifest.primary_node_id_}) {
    auto decoded = reader.String();
    if (!decoded.ok()) return decoded.status();
    *value = std::move(*decoded);
  }
  auto first = reader.U16();
  if (!first.ok()) return first.status();
  manifest.first_slot_ = *first;
  auto last = reader.U16();
  if (!last.ok()) return last.status();
  manifest.last_slot_ = *last;
  auto timeout = reader.U32();
  if (!timeout.ok()) return timeout.status();
  *wait_timeout_ms = *timeout;
  if (!reader.done()) return Invalid("trailing clustercreate request data");
  if (absl::Status valid = ValidateManifest(manifest); !valid.ok()) return valid;
  if (*wait_timeout_ms == 0 ||
      *wait_timeout_ms > kMaxClusterCreateTimeoutMs) {
    return Invalid("clustercreate timeout is out of range");
  }
  return manifest;
}

absl::StatusOr<ClusterCreateOutcome> DecodeClusterCreateReply(
    std::string_view reply) {
  constexpr std::string_view kPrefix = "OK clustercreate 1 ";
  if (!reply.starts_with(kPrefix)) return Invalid("invalid clustercreate reply");
  reply.remove_prefix(kPrefix.size());
  const std::size_t space = reply.find(' ');
  if (space == std::string_view::npos) {
    return Invalid("invalid clustercreate reply");
  }
  ClusterCreateOutcome outcome;
  const std::string_view index = reply.substr(0, space);
  const auto parsed = std::from_chars(index.data(), index.data() + index.size(),
                                      outcome.committed_index_);
  outcome.operation_id_ = std::string(reply.substr(space + 1));
  if (parsed.ec != std::errc{} || parsed.ptr != index.data() + index.size() ||
      outcome.committed_index_ == 0 || outcome.operation_id_.size() != 32 ||
      !std::all_of(outcome.operation_id_.begin(), outcome.operation_id_.end(),
                   [](unsigned char ch) {
                     return std::isdigit(ch) != 0 || (ch >= 'a' && ch <= 'f');
                   })) {
    return Invalid("invalid clustercreate reply");
  }
  return outcome;
}

absl::StatusOr<ClusterCreateOutcome> ClusterOperator::Create(
    const MetaAdminTarget& seed, const ClusterCreateManifestV1& manifest,
    const ClusterStatusOptions& options) const {
  if (absl::Status valid = ValidateManifest(manifest); !valid.ok()) return valid;
  MetaAdminTarget leader;
  auto initial = CaptureStatus(seed, options, &leader);
  if (!initial.ok()) return BeforeMutationFailure(initial.status());
  if (!initial->status_.has_value()) {
    return BeforeMutationFailure(
        absl::UnavailableError(initial->retry_reason_));
  }
  const ClusterStatusWireV1& status = *initial->status_;
  const bool create_active =
      std::any_of(status.blockers_.begin(), status.blockers_.end(),
                  [](const ClusterBlockerWireV1& blocker) {
                    return blocker.code_ == kClusterCreateActiveBlockerCode;
                  });
  if (status.meta_members_.size() != 1 ||
      status.meta_members_.front().server_id_ != manifest.meta_member_id_ ||
      create_active ||
      !status.data_nodes_.empty() || !status.groups_.empty() ||
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
  const auto wait_ms = static_cast<std::uint32_t>(std::clamp<std::int64_t>(
      remaining.count(), 1, std::numeric_limits<std::uint32_t>::max()));
  auto request = EncodeClusterCreateRequest(manifest, wait_ms);
  if (!request.ok()) return request.status();
  auto reply = round_trip_(leader, *request, options.deadline_);
  if (!reply.ok()) return reply.status();
  if (reply->starts_with("ERR clustercreate ")) {
    return ClusterCreateReplyError(*reply);
  }
  auto outcome = DecodeClusterCreateReply(*reply);
  if (!outcome.ok()) return outcome.status();

  while (std::chrono::steady_clock::now() < options.deadline_) {
    auto observed = Status(seed, options);
    if (!observed.ok()) return observed.status();
    if (observed->status_.has_value() &&
        ReadyMatches(*observed->status_, manifest)) {
      return *outcome;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  }
  return absl::DeadlineExceededError(
      "cluster-create committed but the requested topology did not become READY");
}

}  // namespace keylane::meta
