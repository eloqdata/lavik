#include "keylane/command_table.h"

#include <charconv>
#include <limits>

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
    {"flushall", CommandKind::kFlushAll, 1, 0, 0, 0, 1,
     kCmdWrite | kCmdGlobal | kCmdNoKeys},
    {"get", CommandKind::kGet, 2, 2, 1, 1, 1, kKeyedRead},
    {"set", CommandKind::kSet, 3, 0, 1, 1, 1, kKeyedWrite},
    {"lpush", CommandKind::kLPush, 3, 0, 1, 1, 1, kKeyedWrite},
    {"lpushx", CommandKind::kLPushX, 3, 0, 1, 1, 1, kKeyedWrite},
    {"rpush", CommandKind::kRPush, 3, 0, 1, 1, 1, kKeyedWrite},
    {"rpushx", CommandKind::kRPushX, 3, 0, 1, 1, 1, kKeyedWrite},
    {"lpop", CommandKind::kLPop, 2, 3, 1, 1, 1, kKeyedWrite},
    {"rpop", CommandKind::kRPop, 2, 3, 1, 1, 1, kKeyedWrite},
    {"llen", CommandKind::kLLen, 2, 2, 1, 1, 1, kKeyedRead},
    {"lindex", CommandKind::kLIndex, 3, 3, 1, 1, 1, kKeyedRead},
    {"lrange", CommandKind::kLRange, 4, 4, 1, 1, 1, kKeyedRead},
    {"lset", CommandKind::kLSet, 4, 4, 1, 1, 1, kKeyedWrite},
    {"linsert", CommandKind::kLInsert, 5, 5, 1, 1, 1, kKeyedWrite},
    {"lrem", CommandKind::kLRem, 4, 4, 1, 1, 1, kKeyedWrite},
    {"ltrim", CommandKind::kLTrim, 4, 4, 1, 1, 1, kKeyedWrite},
    {"lpos", CommandKind::kLPos, 3, 0, 1, 1, 1, kKeyedRead},
    {"lmove", CommandKind::kLMove, 5, 5, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard},
    {"rpoplpush", CommandKind::kRPopLPush, 3, 3, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard},
    // LMPOP/BLMPOP have argument-dependent key ranges. Their handlers build
    // the concrete transaction key set after parsing numkeys.
    {"lmpop", CommandKind::kLMPop, 4, 0, 2, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
    {"blpop", CommandKind::kBLPop, 3, 0, 1, -2, 1,
     kKeyedWrite | kCmdMultiShard},
    {"brpop", CommandKind::kBRPop, 3, 0, 1, -2, 1,
     kKeyedWrite | kCmdMultiShard},
    {"blmove", CommandKind::kBLMove, 6, 6, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard},
    {"brpoplpush", CommandKind::kBRPopLPush, 4, 4, 1, 2, 1,
     kKeyedWrite | kCmdMultiShard},
    {"blmpop", CommandKind::kBLMPop, 5, 0, 3, 0, 1,
     kCmdWrite | kCmdUsesDbGate | kCmdMultiShard | kCmdMovableKeys},
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
    {"tombraider", CommandKind::kTombRaider, 2, 3, 0, 0, 1,
     kCmdNoKeys | kCmdGlobal},
    {"defrag", CommandKind::kDefrag, 2, 3, 0, 0, 1,
     kCmdNoKeys | kCmdGlobal},
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

absl::StatusOr<KeyIndexView> DetermineKeys(
    const CommandSpec& spec, std::span<const std::string> args) {
  if ((spec.flags_ & kCmdMovableKeys) == 0) {
    return DetermineKeys(spec, args.size());
  }
  if (args.size() < spec.min_args_ ||
      (spec.max_args_ != 0 && args.size() > spec.max_args_)) {
    return absl::InvalidArgumentError(
        "wrong number of arguments for '" + std::string(spec.name_) +
        "' command");
  }
  const std::size_t count_arg =
      spec.kind_ == CommandKind::kBLMPop ? 2 : 1;
  std::uint64_t count = 0;
  const std::string_view text = args[count_arg];
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), count);
  const std::size_t first = count_arg + 1;
  if (parsed.ec != std::errc{} || parsed.ptr != text.data() + text.size() ||
      count == 0 || count > std::numeric_limits<std::uint16_t>::max() ||
      count > args.size() - first || first + count >= args.size()) {
    return absl::InvalidArgumentError(
        "wrong number of arguments for '" + std::string(spec.name_) +
        "' command");
  }
  return KeyIndexView{
      .first_ = static_cast<std::uint16_t>(first),
      .last_ = static_cast<std::uint16_t>(first + count - 1),
      .step_ = 1,
  };
}

}  // namespace keylane
