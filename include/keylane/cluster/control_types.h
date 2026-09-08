#pragma once

// Domain identities shared by the Data control installer and the immutable
// serving model. Wire codecs translate to these types at the control-protocol
// seam; the cluster data plane does not depend on a particular wire version.

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace keylane::cluster {

// A strongly typed, canonical 128-bit incarnation identity. Assignment and
// session ids deliberately use different tags so an adapter cannot bind a
// message to the wrong lifecycle merely because both happen to be 16 bytes.
template <typename Tag>
class ControlId128 {
 public:
  static constexpr std::size_t kByteSize = 16;
  static constexpr std::size_t kHexSize = 2 * kByteSize;
  using Bytes = std::array<std::uint8_t, kByteSize>;

  ControlId128() = default;

  static ControlId128 FromBytes(Bytes bytes) noexcept {
    return ControlId128(bytes);
  }

  // Parses the one accepted wire spelling. Uppercase is rejected so logs,
  // control frames, and durable Meta records have one representation.
  static std::optional<ControlId128> Parse(std::string_view hex) noexcept {
    if (hex.size() != kHexSize) return std::nullopt;
    Bytes bytes{};
    for (std::size_t i = 0; i < bytes.size(); ++i) {
      const auto nibble = [](char c) -> std::optional<std::uint8_t> {
        if (c >= '0' && c <= '9') {
          return static_cast<std::uint8_t>(c - '0');
        }
        if (c >= 'a' && c <= 'f') {
          return static_cast<std::uint8_t>(c - 'a' + 10);
        }
        return std::nullopt;
      };
      const auto high = nibble(hex[2 * i]);
      const auto low = nibble(hex[2 * i + 1]);
      if (!high.has_value() || !low.has_value()) return std::nullopt;
      bytes[i] = static_cast<std::uint8_t>((*high << 4) | *low);
    }
    return FromBytes(bytes);
  }

  bool empty() const noexcept { return !present_; }
  const Bytes& bytes() const noexcept { return bytes_; }

  std::string ToHexString() const {
    if (empty()) return {};
    constexpr char kHexDigits[] = "0123456789abcdef";
    std::string result(kHexSize, '0');
    for (std::size_t i = 0; i < bytes_.size(); ++i) {
      result[2 * i] = kHexDigits[bytes_[i] >> 4];
      result[2 * i + 1] = kHexDigits[bytes_[i] & 0x0f];
    }
    return result;
  }

  friend bool operator==(const ControlId128&, const ControlId128&) = default;
  friend bool operator<(const ControlId128& left,
                        const ControlId128& right) noexcept {
    if (left.present_ != right.present_) return !left.present_;
    return left.bytes_ < right.bytes_;
  }

 private:
  explicit ControlId128(Bytes bytes) : bytes_(bytes), present_(true) {}

  Bytes bytes_{};
  bool present_ = false;
};

struct AssignmentIdTag;
struct SessionIdTag;
struct OperationIdTag;
struct DirectiveIdTag;
struct AttemptIdTag;
using AssignmentId = ControlId128<AssignmentIdTag>;
using SessionId = ControlId128<SessionIdTag>;
using OperationId = ControlId128<OperationIdTag>;
using DirectiveId = ControlId128<DirectiveIdTag>;
using AttemptId = ControlId128<AttemptIdTag>;

using Sha256Digest = std::array<std::uint8_t, 32>;

}  // namespace keylane::cluster
