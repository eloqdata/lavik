#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace keylane {

struct StringValue {
  std::string data;
  std::optional<std::int64_t> expire_at_ms;
};

struct TransparentHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }
  std::size_t operator()(const std::string& s) const noexcept {
    return std::hash<std::string>{}(s);
  }
};

class DbShard {
 public:
  void Set(std::string_view key, std::string_view value);
  const StringValue* Get(std::string_view key) const;
  bool Delete(std::string_view key);
  bool Exists(std::string_view key) const;
  bool Increment(std::string_view key, std::int64_t* value);

 private:
  std::unordered_map<std::string, StringValue, TransparentHash, std::equal_to<>> strings_;
};

}  // namespace keylane
