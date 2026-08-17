#pragma once

#include <cstddef>
#include <string_view>
#include <utility>

namespace keylane {

inline bool MatchGlobCharacterClass(std::string_view pattern, std::size_t open,
                                    unsigned char value, std::size_t* next) {
  std::size_t i = open + 1;
  bool negate = false;
  if (i < pattern.size() && pattern[i] == '^') {
    negate = true;
    ++i;
  }
  bool matched = false;
  while (i < pattern.size() && pattern[i] != ']') {
    unsigned char first = static_cast<unsigned char>(pattern[i++]);
    bool escaped = false;
    if (first == '\\' && i < pattern.size()) {
      first = static_cast<unsigned char>(pattern[i++]);
      escaped = true;
    }
    if (!escaped && i + 1 < pattern.size() && pattern[i] == '-') {
      ++i;
      unsigned char last = static_cast<unsigned char>(pattern[i]);
      // Valkey consumes the range endpoint even when it is ']'. In that
      // trailing-hyphen case the apparent closing bracket is therefore not
      // available to close the class; the remaining pattern is consumed as
      // part of an unterminated class until another ']' or end-of-pattern.
      ++i;
      if (first > last) std::swap(first, last);
      matched = matched || (value >= first && value <= last);
    } else {
      matched = matched || value == first;
    }
  }
  // Valkey treats an unterminated class as extending through the end of the
  // pattern. It does not fall back to matching the opening '[' literally.
  *next = i < pattern.size() ? i + 1 : pattern.size();
  return negate ? !matched : matched;
}

inline bool RedisGlobMatch(std::string_view pattern, std::string_view text) {
  // Valkey's matcher enters its main loop only when both sides are nonempty;
  // a bare '*' therefore does not match an initially empty string. Trailing
  // stars are skipped only after at least one input byte was consumed.
  if (text.empty()) return pattern.empty();
  std::size_t p = 0;
  std::size_t t = 0;
  std::size_t star_pattern = std::string_view::npos;
  std::size_t star_text = 0;
  while (t < text.size()) {
    if (p < pattern.size() && pattern[p] == '*') {
      while (p < pattern.size() && pattern[p] == '*') ++p;
      if (p == pattern.size()) return true;
      star_pattern = p;
      star_text = t;
      continue;
    }
    bool matched = false;
    std::size_t next = p;
    if (p < pattern.size()) {
      if (pattern[p] == '?') {
        matched = true;
        next = p + 1;
      } else if (pattern[p] == '[') {
        matched = MatchGlobCharacterClass(
            pattern, p, static_cast<unsigned char>(text[t]), &next);
      } else {
        if (pattern[p] == '\\' && p + 1 < pattern.size()) ++p;
        matched = static_cast<unsigned char>(pattern[p]) ==
                  static_cast<unsigned char>(text[t]);
        next = p + 1;
      }
    }
    if (matched) {
      p = next;
      ++t;
      continue;
    }
    if (star_pattern != std::string_view::npos && star_text < text.size()) {
      p = star_pattern;
      t = ++star_text;
      continue;
    }
    return false;
  }
  while (p < pattern.size() && pattern[p] == '*') ++p;
  return p == pattern.size();
}

}  // namespace keylane
