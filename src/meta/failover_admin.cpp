#include "keylane/meta/failover_admin.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "keylane/meta/admin_client.h"
#include "keylane/meta/cluster_status.h"
#include "openssl/rand.h"

namespace keylane::meta {
namespace {

constexpr std::string_view kRequestPrefix = "failover 1 ";
constexpr std::string_view kReplyPrefix = "OK failover 1 ";
constexpr std::size_t kMaxAdminCommandBytes = 64 * 1024;

absl::Status Invalid(std::string message) {
  return absl::InvalidArgumentError(std::move(message));
}

template <std::size_t N>
bool IsZero(const std::array<std::uint8_t, N>& value) {
  return std::ranges::all_of(value,
                             [](std::uint8_t byte) { return byte == 0; });
}

char HexDigit(std::uint8_t nibble) {
  constexpr std::string_view kHex = "0123456789abcdef";
  return kHex[nibble & 0x0f];
}

std::string Hex(std::string_view bytes) {
  std::string result(bytes.size() * 2, '\0');
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    const auto byte = static_cast<std::uint8_t>(bytes[index]);
    result[index * 2] = HexDigit(byte >> 4);
    result[index * 2 + 1] = HexDigit(byte);
  }
  return result;
}

template <std::size_t N>
std::string Hex(const std::array<std::uint8_t, N>& bytes) {
  return Hex(std::string_view(reinterpret_cast<const char*>(bytes.data()),
                              bytes.size()));
}

int HexValue(char value) {
  if (value >= '0' && value <= '9') return value - '0';
  if (value >= 'a' && value <= 'f') return value - 'a' + 10;
  return -1;
}

absl::StatusOr<std::string> Unhex(std::string_view text) {
  if (text.empty() || text.size() % 2 != 0) {
    return Invalid("failover request payload is not canonical hex");
  }
  std::string result(text.size() / 2, '\0');
  for (std::size_t index = 0; index < result.size(); ++index) {
    const int high = HexValue(text[index * 2]);
    const int low = HexValue(text[index * 2 + 1]);
    if (high < 0 || low < 0) {
      return Invalid("failover request payload is not canonical hex");
    }
    result[index] = static_cast<char>((high << 4) | low);
  }
  return result;
}

class Writer {
 public:
  void U16(std::uint16_t value) {
    bytes_.push_back(static_cast<char>(value >> 8));
    bytes_.push_back(static_cast<char>(value));
  }
  void U64(std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
      bytes_.push_back(static_cast<char>(value >> shift));
    }
  }
  void Raw(std::string_view value) { bytes_.append(value); }
  const std::string& bytes() const { return bytes_; }

 private:
  std::string bytes_;
};

class Reader {
 public:
  explicit Reader(std::string_view bytes) : bytes_(bytes) {}

  absl::StatusOr<std::uint16_t> U16() {
    if (remaining() < 2) return Invalid("truncated failover request");
    const auto first = static_cast<std::uint8_t>(bytes_[offset_++]);
    const auto second = static_cast<std::uint8_t>(bytes_[offset_++]);
    return static_cast<std::uint16_t>((first << 8) | second);
  }
  absl::StatusOr<std::uint64_t> U64() {
    if (remaining() < 8) return Invalid("truncated failover request");
    std::uint64_t result = 0;
    for (int index = 0; index < 8; ++index) {
      result = (result << 8) | static_cast<std::uint8_t>(bytes_[offset_++]);
    }
    return result;
  }
  absl::StatusOr<std::string_view> Raw(std::size_t size) {
    if (remaining() < size) return Invalid("truncated failover request");
    const std::string_view result = bytes_.substr(offset_, size);
    offset_ += size;
    return result;
  }
  std::size_t remaining() const { return bytes_.size() - offset_; }

 private:
  std::string_view bytes_;
  std::size_t offset_ = 0;
};

absl::Status Validate(const FailoverAdminRequestV1& request) {
  if (IsZero(request.operation_id_) || request.group_id_.empty() ||
      request.group_id_.size() > kMaxMetaGroupIdBytes ||
      request.absolute_deadline_unix_ms_ == 0 ||
      request.absolute_deadline_unix_ms_ >
          static_cast<std::uint64_t>(
              std::numeric_limits<std::int64_t>::max())) {
    return Invalid("failover request is invalid");
  }
  return absl::OkStatus();
}

absl::StatusOr<std::uint64_t> ParseU64(std::string_view text) {
  std::uint64_t result = 0;
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), result);
  if (text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != text.data() + text.size() ||
      text != std::to_string(result)) {
    return Invalid("failover reply integer is not canonical");
  }
  return result;
}

bool IsCanonicalOperationId(std::string_view id) {
  return id.size() == MetaOperationId{}.size() * 2 &&
         std::ranges::all_of(id,
                             [](char value) { return HexValue(value) >= 0; });
}

absl::StatusOr<MetaOperationId> GenerateOperationId() {
  MetaOperationId result{};
  if (RAND_bytes(result.data(), static_cast<int>(result.size())) != 1 ||
      IsZero(result)) {
    return absl::InternalError("failed to generate failover operation id");
  }
  return result;
}

absl::Status FailoverReplyError(std::string_view reply) {
  constexpr std::string_view kErrorPrefix = "ERR failover 1 ";
  if (!reply.starts_with(kErrorPrefix)) {
    return absl::DataLossError("malformed failover reply");
  }
  reply.remove_prefix(kErrorPrefix.size());
  if (reply.starts_with("decode ")) {
    return absl::InvalidArgumentError(std::string(reply));
  }
  if (reply.starts_with("preflight ") || reply == "proposal rejected") {
    return absl::FailedPreconditionError(std::string(reply));
  }
  if (reply == "proposal not-leader") {
    return absl::UnavailableError(std::string(reply));
  }
  return absl::AbortedError(std::string(reply));
}

}  // namespace

absl::StatusOr<std::string> EncodeFailoverAdminRequest(
    const FailoverAdminRequestV1& request) {
  if (absl::Status status = Validate(request); !status.ok()) return status;

  Writer writer;
  writer.Raw(std::string_view(
      reinterpret_cast<const char*>(request.operation_id_.data()),
      request.operation_id_.size()));
  writer.U16(static_cast<std::uint16_t>(request.group_id_.size()));
  writer.Raw(request.group_id_);
  writer.U64(request.absolute_deadline_unix_ms_);
  std::string result = std::string(kRequestPrefix) + Hex(writer.bytes());
  if (result.size() + 1 > kMaxAdminCommandBytes) {
    return Invalid("failover request exceeds Admin command limit");
  }
  return result;
}

absl::StatusOr<FailoverAdminRequestV1> DecodeFailoverAdminRequest(
    std::string_view request) {
  if (!request.starts_with(kRequestPrefix) ||
      request.size() <= kRequestPrefix.size()) {
    return Invalid("expected failover 1 <hex-payload>");
  }
  request.remove_prefix(kRequestPrefix.size());
  auto bytes = Unhex(request);
  if (!bytes.ok()) return bytes.status();

  Reader reader(*bytes);
  FailoverAdminRequestV1 result;
  auto operation = reader.Raw(result.operation_id_.size());
  if (!operation.ok()) return operation.status();
  std::copy(operation->begin(), operation->end(),
            reinterpret_cast<char*>(result.operation_id_.data()));
  auto group_size = reader.U16();
  if (!group_size.ok()) return group_size.status();
  if (*group_size > kMaxMetaGroupIdBytes) {
    return Invalid("failover group id exceeds cap");
  }
  auto group = reader.Raw(*group_size);
  if (!group.ok()) return group.status();
  result.group_id_ = std::string(*group);
  auto deadline = reader.U64();
  if (!deadline.ok()) return deadline.status();
  result.absolute_deadline_unix_ms_ = *deadline;
  if (reader.remaining() != 0) return Invalid("trailing failover request data");
  if (absl::Status status = Validate(result); !status.ok()) return status;
  return result;
}

absl::StatusOr<FailoverOutcome> DecodeFailoverAdminReply(
    std::string_view reply) {
  if (!reply.starts_with(kReplyPrefix)) {
    return Invalid("invalid failover reply");
  }
  reply.remove_prefix(kReplyPrefix.size());
  const std::size_t split = reply.find(' ');
  if (split == std::string_view::npos ||
      reply.find(' ', split + 1) != std::string_view::npos) {
    return Invalid("invalid failover reply");
  }
  auto index = ParseU64(reply.substr(0, split));
  const std::string_view operation_id = reply.substr(split + 1);
  if (!index.ok() || *index == 0 || !IsCanonicalOperationId(operation_id)) {
    return Invalid("invalid failover reply");
  }
  return FailoverOutcome{.submission_commit_index_ = *index,
                         .operation_id_ = std::string(operation_id)};
}

absl::StatusOr<FailoverOutcome> ClusterOperator::Failover(
    const MetaAdminTarget& seed, const FailoverRequestOptions& request,
    const ClusterStatusOptions& options) const {
  if (request.group_id_.empty() ||
      request.group_id_.size() > kMaxMetaGroupIdBytes ||
      request.transition_timeout_ <= std::chrono::milliseconds::zero() ||
      request.operation_id_.has_value() !=
          request.absolute_deadline_unix_ms_.has_value()) {
    return Invalid("invalid controlled failover request");
  }

  MetaAdminTarget leader;
  auto initial = CaptureStatus(seed, options, &leader);
  if (!initial.ok()) return initial.status();
  if (!initial->status_.has_value()) {
    return absl::UnavailableError(initial->retry_reason_);
  }
  const ClusterStatusWireV1& status = *initial->status_;
  if (status.cluster_state_ != ClusterStateWireV1::kCreated) {
    return absl::FailedPreconditionError(
        "controlled failover requires cluster state Created");
  }
  if (std::ranges::none_of(status.groups_, [&](const auto& group) {
        return group.group_id_ == request.group_id_;
      })) {
    return absl::FailedPreconditionError(
        "controlled failover group is not committed");
  }
  if (std::chrono::steady_clock::now() >= options.deadline_) {
    return absl::DeadlineExceededError(
        "controlled failover Admin deadline expired before submission");
  }

  MetaOperationId operation_id{};
  if (request.operation_id_.has_value()) {
    operation_id = *request.operation_id_;
    if (IsZero(operation_id)) return Invalid("failover operation id is zero");
  } else {
    auto generated = GenerateOperationId();
    if (!generated.ok()) return generated.status();
    operation_id = *generated;
  }
  const std::string expected_id = Hex(operation_id);

  std::uint64_t absolute_deadline = 0;
  if (request.absolute_deadline_unix_ms_.has_value()) {
    absolute_deadline = *request.absolute_deadline_unix_ms_;
    if (absolute_deadline == 0 ||
        absolute_deadline > static_cast<std::uint64_t>(
                                std::numeric_limits<std::int64_t>::max())) {
      return Invalid("controlled failover deadline is invalid");
    }
  } else {
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    if (now < 0 || request.transition_timeout_.count() >
                       std::numeric_limits<std::int64_t>::max() - now) {
      return Invalid("controlled failover deadline overflows");
    }
    absolute_deadline =
        static_cast<std::uint64_t>(now + request.transition_timeout_.count());
  }
  auto encoded = EncodeFailoverAdminRequest(
      {.operation_id_ = operation_id,
       .group_id_ = request.group_id_,
       .absolute_deadline_unix_ms_ = absolute_deadline});
  if (!encoded.ok()) return encoded.status();

  auto reply = round_trip_(leader, *encoded, options.deadline_);
  if (!reply.ok()) {
    if (MetaAdminRequestDefinitelyNotSent(reply.status())) {
      return reply.status();
    }
    return absl::AbortedError(
        "controlled failover outcome is uncertain; operation=" + expected_id +
        " detail=" + std::string(reply.status().message()));
  }
  if (reply->starts_with("ERR ")) {
    absl::Status error = FailoverReplyError(*reply);
    if (error.code() == absl::StatusCode::kDataLoss) {
      return absl::AbortedError(
          "controlled failover response is untrustworthy; operation=" +
          expected_id + " detail=" + std::string(error.message()));
    }
    return error;
  }
  auto outcome = DecodeFailoverAdminReply(*reply);
  if (!outcome.ok() || outcome->operation_id_ != expected_id) {
    const std::string detail = outcome.ok()
                                   ? "response named another operation"
                                   : std::string(outcome.status().message());
    return absl::AbortedError(
        "controlled failover response is untrustworthy; operation=" +
        expected_id + " detail=" + detail);
  }
  return *outcome;
}

}  // namespace keylane::meta
