#include "keylane/command_table.h"

#include "absl/status/statusor.h"

namespace keylane {

namespace {

constexpr std::uint32_t kKeyedRead = kCmdReadOnly | kCmdUsesDbGate;
constexpr std::uint32_t kKeyedWrite = kCmdWrite | kCmdUsesDbGate;

constexpr CommandSpec kCommandTable[] = {
    {"ping", CommandKind::kPing, 1, 2, 0, 0, 1, kCmdNoKeys},
    {"echo", CommandKind::kEcho, 2, 2, 0, 0, 1, kCmdNoKeys},
    {"select", CommandKind::kSelect, 2, 2, 0, 0, 1, kCmdNoKeys},
    {"dbsize", CommandKind::kDbSize, 1, 1, 0, 0, 1,
     kCmdReadOnly | kCmdGlobal | kCmdUsesDbGate | kCmdNoKeys},
    {"scan", CommandKind::kScan, 2, 0, 0, 0, 1,
     kCmdReadOnly | kCmdGlobal | kCmdUsesDbGate | kCmdNoKeys},
    {"flushdb", CommandKind::kFlushDb, 1, 0, 0, 0, 1,
     kCmdWrite | kCmdGlobal | kCmdNoKeys},
    {"get", CommandKind::kGet, 2, 2, 1, 1, 1, kKeyedRead},
    {"set", CommandKind::kSet, 3, 0, 1, 1, 1, kKeyedWrite},
    {"strlen", CommandKind::kStrlen, 2, 2, 1, 1, 1, kKeyedRead},
    {"incr", CommandKind::kIncr, 2, 2, 1, 1, 1, kKeyedWrite},
    {"expire", CommandKind::kExpire, 3, 4, 1, 1, 1, kKeyedWrite},
    {"pexpire", CommandKind::kPExpire, 3, 4, 1, 1, 1, kKeyedWrite},
    {"persist", CommandKind::kPersist, 2, 2, 1, 1, 1, kKeyedWrite},
    {"ttl", CommandKind::kTtl, 2, 2, 1, 1, 1, kKeyedRead},
    {"pttl", CommandKind::kPttl, 2, 2, 1, 1, 1, kKeyedRead},
    {"del", CommandKind::kDel, 2, 0, 1, -1, 1, kKeyedWrite | kCmdMultiShard},
    {"exists", CommandKind::kExists, 2, 0, 1, -1, 1,
     kKeyedRead | kCmdMultiShard},
    {"mset", CommandKind::kMSet, 3, 0, 1, -1, 2, kKeyedWrite | kCmdMultiShard},
    {"mget", CommandKind::kMGet, 2, 0, 1, -1, 1, kKeyedRead | kCmdMultiShard},
    {"multi", CommandKind::kMulti, 1, 1, 0, 0, 1, kCmdNoKeys},
    {"exec", CommandKind::kExec, 1, 1, 0, 0, 1, kCmdNoKeys},
    {"discard", CommandKind::kDiscard, 1, 1, 0, 0, 1, kCmdNoKeys},
    {"watch", CommandKind::kWatch, 2, 0, 1, -1, 1, kCmdReadOnly},
    {"unwatch", CommandKind::kUnwatch, 1, 1, 0, 0, 1, kCmdNoKeys},
    {"info", CommandKind::kInfo, 1, 2, 0, 0, 1, kCmdNoKeys | kCmdReadOnly},
    {"keys", CommandKind::kKeys, 2, 2, 0, 0, 1,
     kCmdNoKeys | kCmdReadOnly | kCmdGlobal},
};

bool EqualsIgnoreCase(std::string_view name, std::string_view lower) {
  if (name.size() != lower.size()) {
    return false;
  }
  for (std::size_t i = 0; i < name.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(name[i]);
    if (c >= 'A' && c <= 'Z') {
      c += 'a' - 'A';
    }
    if (c != static_cast<unsigned char>(lower[i])) {
      return false;
    }
  }
  return true;
}

}  // namespace

const CommandSpec* FindCommand(std::string_view name) {
  for (const CommandSpec& spec : kCommandTable) {
    if (EqualsIgnoreCase(name, spec.name_)) {
      return &spec;
    }
  }
  return nullptr;
}

absl::StatusOr<KeyIndexView> DetermineKeys(const CommandSpec& spec,
                                           std::size_t argc) {
  if (argc < spec.min_args_ || (spec.max_args_ != 0 && argc > spec.max_args_)) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "wrong number of arguments for '" +
                            std::string(spec.name_) + "' command");
  }
  KeyIndexView view;
  if (spec.first_key_ == 0) {
    return view;
  }
  const std::int64_t last =
      spec.last_key_ >= 0 ? static_cast<std::int64_t>(spec.last_key_)
                          : static_cast<std::int64_t>(argc) + spec.last_key_;
  if (last < spec.first_key_ || last >= static_cast<std::int64_t>(argc)) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "wrong number of arguments for '" +
                            std::string(spec.name_) + "' command");
  }
  view.first_ = spec.first_key_;
  view.last_ = static_cast<std::uint16_t>(last);
  view.step_ = spec.key_step_;
  return view;
}

}  // namespace keylane
