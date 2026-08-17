#pragma once

#include <charconv>
#include <cmath>
#include <limits>
#include <string_view>

namespace keylane {

// Strict Redis numeric grammar: consume the complete token, reject whitespace
// and hexadecimal spellings, accept a leading '+', and handle the documented
// infinity spellings independently of the standard library implementation.
inline bool ParseRedisDouble(std::string_view input, double* value,
                             bool allow_infinity = false) noexcept {
  if (value == nullptr || input.empty()) return false;

  bool negative = false;
  if (input.front() == '+' || input.front() == '-') {
    negative = input.front() == '-';
    input.remove_prefix(1);
    // A Redis number has at most one leading sign. In particular, stripping
    // '+' and then handing "-5" to from_chars would otherwise silently accept
    // "+-5" and flip data written by ZADD/GEOADD.
    if (input.empty() || input.front() == '+' || input.front() == '-') {
      return false;
    }
  }

  auto equal_ci = [](std::string_view left, std::string_view right) {
    if (left.size() != right.size()) return false;
    for (std::size_t i = 0; i < left.size(); ++i) {
      unsigned char c = static_cast<unsigned char>(left[i]);
      if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
      if (c != static_cast<unsigned char>(right[i])) return false;
    }
    return true;
  };
  if (equal_ci(input, "inf") || equal_ci(input, "infinity")) {
    if (!allow_infinity) return false;
    *value = negative ? -std::numeric_limits<double>::infinity()
                      : std::numeric_limits<double>::infinity();
    return true;
  }

  // Put a '-' back for from_chars; '+' was intentionally stripped.
  const char* begin = input.data();
  if (negative) --begin;
  const char* end = input.data() + input.size();
  const auto parsed =
      std::from_chars(begin, end, *value, std::chars_format::general);
  return parsed.ec == std::errc{} && parsed.ptr == end &&
         !std::isnan(*value) && (allow_infinity || std::isfinite(*value));
}

}  // namespace keylane
