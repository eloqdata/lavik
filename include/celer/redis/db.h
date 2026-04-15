#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace celer::redis {

struct StringValue {
  std::string data;
  std::optional<std::int64_t> expire_at_ms;
};

class DbShard {
 public:
  void Set(std::string key, std::string value);
  const StringValue* Get(std::string_view key) const;
  bool Delete(std::string_view key);
  bool Exists(std::string_view key) const;
  bool Increment(std::string_view key, std::int64_t* value);

 private:
  std::unordered_map<std::string, StringValue> strings_;
};

}  // namespace celer::redis
