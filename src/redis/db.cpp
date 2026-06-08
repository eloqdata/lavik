#include "keylane/redis/db.h"

#include <charconv>
#include <limits>
#include <string>

namespace keylane {

void DbShard::Set(std::string_view key, std::string_view value) {
  strings_.insert_or_assign(std::string(key), StringValue{std::string(value), std::nullopt});
}

const StringValue* DbShard::Get(std::string_view key) const {
  auto it = strings_.find(key);
  if (it == strings_.end()) {
    return nullptr;
  }
  return &it->second;
}

bool DbShard::Delete(std::string_view key) {
  auto it = strings_.find(key);
  if (it == strings_.end()) {
    return false;
  }
  strings_.erase(it);
  return true;
}

bool DbShard::Exists(std::string_view key) const {
  return strings_.find(key) != strings_.end();
}

bool DbShard::Increment(std::string_view key, std::int64_t* value) {
  auto it = strings_.find(key);
  if (it == strings_.end()) {
    auto [new_it, inserted] =
        strings_.try_emplace(std::string(key), StringValue{"0", std::nullopt});
    (void)inserted;
    it = new_it;
  }

  std::int64_t current = 0;
  const std::string_view current_view = it->second.data;
  auto [ptr, ec] = std::from_chars(current_view.data(), current_view.data() + current_view.size(),
                                   current);
  if (ec != std::errc{} || ptr != current_view.data() + current_view.size()) {
    return false;
  }
  if (current == std::numeric_limits<std::int64_t>::max()) {
    return false;
  }

  ++current;
  it->second.data = std::to_string(current);
  if (value != nullptr) {
    *value = current;
  }
  return true;
}

}  // namespace keylane
