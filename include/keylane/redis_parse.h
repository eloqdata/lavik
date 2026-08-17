#pragma once

#include <cerrno>
#include <charconv>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace keylane {

inline bool RedisEqualsIgnoreCase(std::string_view left,
                                  std::string_view right) noexcept {
  if (left.size() != right.size()) return false;
  for (std::size_t i = 0; i < left.size(); ++i) {
    unsigned char a = static_cast<unsigned char>(left[i]);
    unsigned char b = static_cast<unsigned char>(right[i]);
    if (a >= 'A' && a <= 'Z') a += 'a' - 'A';
    if (b >= 'A' && b <= 'Z') b += 'a' - 'A';
    if (a != b) return false;
  }
  return true;
}

// Redis string2ll grammar: decimal only, no leading '+', no leading zeroes
// except for the single token "0", and no negative zero.
inline bool ParseRedisInt64(std::string_view input,
                            std::int64_t* value) noexcept {
  if (value == nullptr || input.empty()) return false;
  std::string_view magnitude = input;
  if (magnitude.front() == '-') magnitude.remove_prefix(1);
  if (magnitude.empty() || (magnitude.size() > 1 && magnitude.front() == '0') ||
      (input.front() == '-' && magnitude == "0") || magnitude.front() < '1' ||
      magnitude.front() > '9') {
    return input == "0" && ((*value = 0), true);
  }
  for (char digit : magnitude) {
    if (digit < '0' || digit > '9') return false;
  }
  const auto parsed =
      std::from_chars(input.data(), input.data() + input.size(), *value);
  return parsed.ec == std::errc{} && parsed.ptr == input.data() + input.size();
}

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

  if (RedisEqualsIgnoreCase(input, "inf") ||
      RedisEqualsIgnoreCase(input, "infinity")) {
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
  return parsed.ec == std::errc{} && parsed.ptr == end && !std::isnan(*value) &&
         (allow_infinity || std::isfinite(*value));
}

// Redis string2ld grammar. ERANGE is accepted only for a non-zero finite
// subnormal value, matching Valkey 7.2's handling of strtold().
inline bool ParseRedisLongDouble(std::string_view input,
                                 long double* value) {
  constexpr std::size_t kMaxLongDoubleChars = 5 * 1024;
  if (value == nullptr || input.empty() ||
      input.size() >= kMaxLongDoubleChars ||
      std::isspace(static_cast<unsigned char>(input.front())) != 0) {
    return false;
  }
  std::string terminated(input);
  char* end = nullptr;
  errno = 0;
  const long double parsed = std::strtold(terminated.c_str(), &end);
  if (end != terminated.data() + terminated.size() || errno == EINVAL ||
      std::isnan(parsed) ||
      (errno == ERANGE &&
       (std::isinf(parsed) || std::fpclassify(parsed) == FP_ZERO))) {
    return false;
  }
  *value = parsed;
  return true;
}

inline bool FormatRedisLongDouble(long double value, std::string* output) {
  if (output == nullptr) return false;
  char buffer[std::numeric_limits<long double>::max_exponent10 + 128];
  const int formatted = std::snprintf(buffer, sizeof(buffer), "%.17Lf", value);
  if (formatted < 0 || static_cast<std::size_t>(formatted) >= sizeof(buffer)) {
    return false;
  }
  std::size_t length = static_cast<std::size_t>(formatted);
  if (std::string_view(buffer, length).find('.') != std::string_view::npos) {
    while (length != 0 && buffer[length - 1] == '0') --length;
    if (length != 0 && buffer[length - 1] == '.') --length;
  }
  output->assign(buffer, length);
  if (*output == "-0") *output = "0";
  return true;
}

}  // namespace keylane
