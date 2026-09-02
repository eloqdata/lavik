#include "stream_command.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>

#include "absl/strings/str_cat.h"
#include "blocking_wait.h"
#include "celer/io/storage.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "keylane/resp.h"

namespace keylane {
namespace {

constexpr std::string_view kMagicV1 = "KXS1";
constexpr std::string_view kMagicV2 = "KXS2";
constexpr std::string_view kGroupStateMagic = "KXG1";
constexpr std::string_view kRestoreGroupSubcommand =
    "__keylane_restore_group_v1";
storage::StorageEngine* g_storage = nullptr;
std::atomic<std::uint32_t> g_stream_node_max_entries{100};

struct Id {
  std::uint64_t ms_ = 0;
  std::uint64_t seq_ = 0;
  friend auto operator<=>(const Id&, const Id&) = default;
};

struct Entry {
  Id id_;
  std::vector<std::string> fields_;
};

struct Pending {
  Id id_;
  std::string consumer_;
  std::uint64_t delivery_ms_ = 0;
  std::uint64_t deliveries_ = 1;
};

struct Consumer {
  std::string name_;
  std::uint64_t seen_ms_ = 0;
  std::uint64_t active_ms_ = 0;
};

struct Group {
  std::string name_;
  Id last_id_;
  std::int64_t entries_read_ = -1;
  std::vector<Consumer> consumers_;
  std::vector<Pending> pending_;
};

struct Stream {
  Id last_id_;
  Id max_deleted_id_;
  std::uint64_t entries_added_ = 0;
  std::vector<Entry> entries_;
  // Counts of live entries in logical Redis stream macro nodes. The sum is
  // entries_.size(); exact trims may leave a partial first node, while
  // approximate trims remove only complete nodes.
  std::vector<std::uint32_t> node_entries_;
  std::vector<Group> groups_;
};

void SynthesizeStreamNodes(Stream* stream) {
  stream->node_entries_.clear();
  const std::uint32_t maximum =
      g_stream_node_max_entries.load(std::memory_order_relaxed);
  std::size_t remaining = stream->entries_.size();
  while (remaining != 0) {
    const auto count = static_cast<std::uint32_t>(
        std::min<std::size_t>(remaining, maximum));
    stream->node_entries_.push_back(count);
    remaining -= count;
  }
}

bool ValidStreamNodes(const Stream& stream) {
  std::size_t total = 0;
  for (const std::uint32_t count : stream.node_entries_) {
    if (count == 0 || total > stream.entries_.size() ||
        count > stream.entries_.size() - total)
      return false;
    total += count;
  }
  return total == stream.entries_.size();
}

void AppendStreamEntry(Stream* stream, Entry entry) {
  const std::uint32_t maximum =
      g_stream_node_max_entries.load(std::memory_order_relaxed);
  if (stream->node_entries_.empty() ||
      stream->node_entries_.back() >= maximum) {
    stream->node_entries_.push_back(1);
  } else {
    ++stream->node_entries_.back();
  }
  stream->entries_.push_back(std::move(entry));
}

void EraseStreamFront(Stream* stream, std::size_t count) {
  stream->entries_.erase(stream->entries_.begin(),
                         stream->entries_.begin() + count);
  while (count != 0) {
    if (count >= stream->node_entries_.front()) {
      count -= stream->node_entries_.front();
      stream->node_entries_.erase(stream->node_entries_.begin());
    } else {
      stream->node_entries_.front() -= count;
      count = 0;
    }
  }
}

void EraseStreamEntry(Stream* stream, std::size_t index) {
  std::size_t node_begin = 0;
  for (auto node = stream->node_entries_.begin();
       node != stream->node_entries_.end(); ++node) {
    if (index < node_begin + *node) {
      if (--*node == 0) stream->node_entries_.erase(node);
      break;
    }
    node_begin += *node;
  }
  stream->entries_.erase(stream->entries_.begin() + index);
}

std::uint64_t DefaultApproximateTrimLimit() {
  const std::uint64_t maximum =
      g_stream_node_max_entries.load(std::memory_order_relaxed);
  return std::clamp<std::uint64_t>(maximum * 100, 1, 1'000'000);
}

std::size_t TrimStreamMaxLen(Stream* stream, std::uint64_t maxlen,
                             bool approximate, std::uint64_t limit) {
  if (!approximate) {
    const std::size_t remove =
        stream->entries_.size() > maxlen
            ? stream->entries_.size() - static_cast<std::size_t>(maxlen)
            : 0;
    if (remove != 0) EraseStreamFront(stream, remove);
    return remove;
  }

  std::size_t remove = 0;
  std::size_t remaining = stream->entries_.size();
  for (const std::uint32_t node_entries : stream->node_entries_) {
    if (remaining <= maxlen || remaining - node_entries < maxlen) break;
    if (limit != 0 && remove + node_entries > limit) break;
    remove += node_entries;
    remaining -= node_entries;
  }
  if (remove != 0) EraseStreamFront(stream, remove);
  return remove;
}

std::size_t TrimStreamMinId(Stream* stream, Id minid, bool approximate,
                            std::uint64_t limit) {
  if (!approximate) {
    const auto end = std::lower_bound(
        stream->entries_.begin(), stream->entries_.end(), minid,
        [](const Entry& entry, Id wanted) { return entry.id_ < wanted; });
    const std::size_t remove = end - stream->entries_.begin();
    if (remove != 0) EraseStreamFront(stream, remove);
    return remove;
  }

  std::size_t remove = 0;
  for (const std::uint32_t node_entries : stream->node_entries_) {
    if (!(stream->entries_[remove + node_entries - 1].id_ < minid)) break;
    if (limit != 0 && remove + node_entries > limit) break;
    remove += node_entries;
  }
  if (remove != 0) EraseStreamFront(stream, remove);
  return remove;
}

std::size_t XInfoLimitedCount(std::size_t available, std::uint64_t requested) {
  return requested == 0 ? available
                        : std::min<std::uint64_t>(available, requested);
}

struct SubcommandShape {
  std::string_view name_;
  std::size_t min_args_;
  std::size_t max_args_;
};

constexpr SubcommandShape kXGroupShapes[] = {
    {"create", 5, 8},         {"setid", 5, 7},       {"destroy", 4, 4},
    {"createconsumer", 5, 5}, {"delconsumer", 5, 5}, {"help", 2, 2},
};
constexpr SubcommandShape kXInfoShapes[] = {
    {"consumers", 4, 4},
    {"groups", 3, 3},
    {"stream", 3, 6},
    {"help", 2, 2},
};

CommandReply Built(std::string_view encoded) {
  CommandReply reply;
  reply.encoded_ = encoded;
  return reply;
}

bool EqualCi(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    unsigned char c = static_cast<unsigned char>(a[i]);
    if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    if (c != static_cast<unsigned char>(b[i])) return false;
  }
  return true;
}

const SubcommandShape* FindSubcommandShape(CommandKind kind,
                                           std::string_view name) {
  const std::span<const SubcommandShape> shapes =
      kind == CommandKind::kXGroup
          ? std::span<const SubcommandShape>(kXGroupShapes)
          : std::span<const SubcommandShape>(kXInfoShapes);
  auto found = std::find_if(shapes.begin(), shapes.end(), [&](const auto& s) {
    return EqualCi(name, s.name_);
  });
  return found == shapes.end() ? nullptr : &*found;
}

std::string SubcommandSyntaxError(const CommandRequest& request) {
  const std::string command =
      request.kind_ == CommandKind::kXGroup ? "XGROUP" : "XINFO";
  return "ERR unknown subcommand or wrong number of arguments for '" +
         request.args_[1].substr(0, 128) + "'. Try " + command + " HELP.";
}

template <class T>
bool ParseInt(std::string_view text, T* value) {
  const auto parsed =
      std::from_chars(text.data(), text.data() + text.size(), *value);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

std::uint64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string FormatId(Id id) {
  return std::to_string(id.ms_) + "-" + std::to_string(id.seq_);
}

absl::StatusOr<Id> ParseId(std::string_view text, bool end = false,
                           bool allow_special = false) {
  if (allow_special && text == "-") return Id{};
  if (allow_special && text == "+") return Id{UINT64_MAX, UINT64_MAX};
  const std::size_t dash = text.find('-');
  Id id;
  if (dash == std::string_view::npos) {
    if (!ParseInt(text, &id.ms_))
      return absl::InvalidArgumentError(
          "Invalid stream ID specified as stream command argument");
    id.seq_ = end ? UINT64_MAX : 0;
    return id;
  }
  if (!ParseInt(text.substr(0, dash), &id.ms_) ||
      !ParseInt(text.substr(dash + 1), &id.seq_)) {
    return absl::InvalidArgumentError(
        "Invalid stream ID specified as stream command argument");
  }
  return id;
}

void Put32(std::string* out, std::uint32_t value) {
  for (unsigned i = 0; i < 4; ++i)
    out->push_back(static_cast<char>(value >> (i * 8)));
}
void Put64(std::string* out, std::uint64_t value) {
  for (unsigned i = 0; i < 8; ++i)
    out->push_back(static_cast<char>(value >> (i * 8)));
}
bool Get32(std::string_view in, std::size_t* at, std::uint32_t* value) {
  if (*at > in.size() || in.size() - *at < 4) return false;
  *value = 0;
  for (unsigned i = 0; i < 4; ++i)
    *value |=
        static_cast<std::uint32_t>(static_cast<unsigned char>(in[*at + i]))
        << (i * 8);
  *at += 4;
  return true;
}
bool Get64(std::string_view in, std::size_t* at, std::uint64_t* value) {
  if (*at > in.size() || in.size() - *at < 8) return false;
  *value = 0;
  for (unsigned i = 0; i < 8; ++i)
    *value |=
        static_cast<std::uint64_t>(static_cast<unsigned char>(in[*at + i]))
        << (i * 8);
  *at += 8;
  return true;
}
void PutString(std::string* out, std::string_view value) {
  Put32(out, static_cast<std::uint32_t>(value.size()));
  out->append(value);
}
bool GetString(std::string_view in, std::size_t* at, std::string* value) {
  std::uint32_t size = 0;
  if (!Get32(in, at, &size) || *at > in.size() || size > in.size() - *at)
    return false;
  value->assign(in.substr(*at, size));
  *at += size;
  return true;
}
void PutId(std::string* out, Id id) {
  Put64(out, id.ms_);
  Put64(out, id.seq_);
}
bool GetId(std::string_view in, std::size_t* at, Id* id) {
  return Get64(in, at, &id->ms_) && Get64(in, at, &id->seq_);
}

absl::StatusOr<Stream> Decode(
    const std::optional<storage::CompactValueView>& value) {
  if (!value) return Stream{};
  const std::string_view in = value->encoded_;
  const bool version_two = in.starts_with(kMagicV2);
  if (!version_two && !in.starts_with(kMagicV1))
    return absl::InternalError("invalid persisted Stream");
  std::size_t at = kMagicV1.size();
  Stream stream;
  std::uint32_t entry_count = 0, group_count = 0;
  if (!GetId(in, &at, &stream.last_id_) ||
      !GetId(in, &at, &stream.max_deleted_id_) ||
      !Get64(in, &at, &stream.entries_added_) ||
      !Get32(in, &at, &entry_count) || entry_count != value->logical_size_)
    return absl::InternalError("invalid persisted Stream header");
  constexpr std::size_t kMinimumEntryBytes = 2 * sizeof(std::uint64_t) + 4;
  if (entry_count > (in.size() - at) / kMinimumEntryBytes)
    return absl::InternalError("invalid persisted Stream entry count");
  stream.entries_.reserve(entry_count);
  for (std::uint32_t i = 0; i < entry_count; ++i) {
    Entry entry;
    std::uint32_t fields = 0;
    if (!GetId(in, &at, &entry.id_) || !Get32(in, &at, &fields) || fields % 2 ||
        fields > (in.size() - at) / sizeof(std::uint32_t))
      return absl::InternalError("invalid persisted Stream entry");
    entry.fields_.reserve(fields);
    for (std::uint32_t f = 0; f < fields; ++f) {
      std::string item;
      if (!GetString(in, &at, &item))
        return absl::InternalError("truncated persisted Stream field");
      entry.fields_.push_back(std::move(item));
    }
    stream.entries_.push_back(std::move(entry));
  }
  if (version_two) {
    std::uint32_t node_count = 0;
    if (!Get32(in, &at, &node_count) ||
        node_count > stream.entries_.size()) {
      return absl::InternalError("invalid persisted Stream nodes");
    }
    stream.node_entries_.reserve(node_count);
    for (std::uint32_t i = 0; i < node_count; ++i) {
      std::uint32_t count = 0;
      if (!Get32(in, &at, &count))
        return absl::InternalError("truncated persisted Stream nodes");
      stream.node_entries_.push_back(count);
    }
    if (!ValidStreamNodes(stream))
      return absl::InternalError("invalid persisted Stream node counts");
  } else {
    SynthesizeStreamNodes(&stream);
  }
  if (!Get32(in, &at, &group_count))
    return absl::InternalError("truncated persisted Stream groups");
  constexpr std::size_t kMinimumGroupBytes =
      4 + 2 * sizeof(std::uint64_t) + sizeof(std::uint64_t) + 4 + 4;
  if (group_count > (in.size() - at) / kMinimumGroupBytes)
    return absl::InternalError("invalid persisted Stream group count");
  stream.groups_.reserve(group_count);
  for (std::uint32_t i = 0; i < group_count; ++i) {
    Group group;
    std::uint64_t entries_read = 0;
    std::uint32_t consumers = 0, pending = 0;
    if (!GetString(in, &at, &group.name_) || !GetId(in, &at, &group.last_id_) ||
        !Get64(in, &at, &entries_read) || !Get32(in, &at, &consumers))
      return absl::InternalError("invalid persisted Stream group");
    constexpr std::size_t kMinimumConsumerBytes = 4 + 2 * sizeof(std::uint64_t);
    if (consumers > (in.size() - at) / kMinimumConsumerBytes)
      return absl::InternalError("invalid persisted Stream consumer count");
    group.entries_read_ = std::bit_cast<std::int64_t>(entries_read);
    for (std::uint32_t c = 0; c < consumers; ++c) {
      Consumer consumer;
      if (!GetString(in, &at, &consumer.name_) ||
          !Get64(in, &at, &consumer.seen_ms_) ||
          !Get64(in, &at, &consumer.active_ms_))
        return absl::InternalError("invalid persisted Stream consumer");
      group.consumers_.push_back(std::move(consumer));
    }
    if (!Get32(in, &at, &pending))
      return absl::InternalError("invalid persisted Stream PEL");
    constexpr std::size_t kMinimumPendingBytes =
        2 * sizeof(std::uint64_t) + 4 + 2 * sizeof(std::uint64_t);
    if (pending > (in.size() - at) / kMinimumPendingBytes)
      return absl::InternalError("invalid persisted Stream pending count");
    for (std::uint32_t p = 0; p < pending; ++p) {
      Pending item;
      if (!GetId(in, &at, &item.id_) || !GetString(in, &at, &item.consumer_) ||
          !Get64(in, &at, &item.delivery_ms_) ||
          !Get64(in, &at, &item.deliveries_))
        return absl::InternalError("invalid persisted Stream pending entry");
      if (!group.pending_.empty() && !(group.pending_.back().id_ < item.id_))
        return absl::InternalError(
            "invalid persisted Stream pending entry order");
      group.pending_.push_back(std::move(item));
    }
    stream.groups_.push_back(std::move(group));
  }
  if (at != in.size())
    return absl::InternalError("trailing persisted Stream bytes");
  return stream;
}

absl::StatusOr<std::string> Encode(const Stream& stream) {
  auto fits32 = [](std::size_t size) { return size <= UINT32_MAX; };
  if (!fits32(stream.entries_.size()) ||
      !fits32(stream.node_entries_.size()) ||
      !fits32(stream.groups_.size()) || !ValidStreamNodes(stream))
    return absl::OutOfRangeError("Stream exceeds storage limits");
  std::uint64_t encoded_bytes = kMagicV2.size() + 2 * 16 + 8 + 4;
  auto add_bytes = [&](std::uint64_t bytes) {
    if (bytes > storage::kMaxStringBytes - encoded_bytes) return false;
    encoded_bytes += bytes;
    return true;
  };
  for (const Entry& entry : stream.entries_) {
    if (!fits32(entry.fields_.size()) || !add_bytes(16 + 4))
      return absl::OutOfRangeError("Stream entry is too large");
    for (const std::string& field : entry.fields_) {
      if (!fits32(field.size()) ||
          !add_bytes(4 + static_cast<std::uint64_t>(field.size())))
        return absl::OutOfRangeError("Stream field is too large");
      }
  }
  if (!add_bytes(4 + 4 * stream.node_entries_.size()))
    return absl::OutOfRangeError("Stream exceeds storage limits");
  if (!add_bytes(4))
    return absl::OutOfRangeError("Stream exceeds storage limits");
  for (const Group& group : stream.groups_) {
    if (!fits32(group.name_.size()) || !fits32(group.consumers_.size()) ||
        !fits32(group.pending_.size()) ||
        !add_bytes(4 + static_cast<std::uint64_t>(group.name_.size()) + 16 + 8 +
                   4))
      return absl::OutOfRangeError("Stream group is too large");
    for (const Consumer& consumer : group.consumers_) {
      if (!fits32(consumer.name_.size()) ||
          !add_bytes(4 + static_cast<std::uint64_t>(consumer.name_.size()) + 8 +
                     8))
        return absl::OutOfRangeError("Stream consumer is too large");
    }
    if (!add_bytes(4))
      return absl::OutOfRangeError("Stream group is too large");
    for (const Pending& pending : group.pending_) {
      if (!fits32(pending.consumer_.size()) ||
          !add_bytes(16 + 4 +
                     static_cast<std::uint64_t>(pending.consumer_.size()) + 8 +
                     8))
        return absl::OutOfRangeError("Stream pending entry is too large");
    }
  }
  std::string out;
  out.reserve(static_cast<std::size_t>(encoded_bytes));
  out.append(kMagicV2);
  PutId(&out, stream.last_id_);
  PutId(&out, stream.max_deleted_id_);
  Put64(&out, stream.entries_added_);
  Put32(&out, stream.entries_.size());
  for (const Entry& entry : stream.entries_) {
    if (!fits32(entry.fields_.size()))
      return absl::OutOfRangeError("Stream entry is too large");
    PutId(&out, entry.id_);
    Put32(&out, entry.fields_.size());
    for (const std::string& field : entry.fields_) {
      if (!fits32(field.size()))
        return absl::OutOfRangeError("Stream field is too large");
      PutString(&out, field);
    }
  }
  Put32(&out, stream.node_entries_.size());
  for (const std::uint32_t count : stream.node_entries_) Put32(&out, count);
  Put32(&out, stream.groups_.size());
  for (const Group& group : stream.groups_) {
    PutString(&out, group.name_);
    PutId(&out, group.last_id_);
    Put64(&out, std::bit_cast<std::uint64_t>(group.entries_read_));
    Put32(&out, group.consumers_.size());
    for (const Consumer& consumer : group.consumers_) {
      PutString(&out, consumer.name_);
      Put64(&out, consumer.seen_ms_);
      Put64(&out, consumer.active_ms_);
    }
    Put32(&out, group.pending_.size());
    for (const Pending& pending : group.pending_) {
      PutId(&out, pending.id_);
      PutString(&out, pending.consumer_);
      Put64(&out, pending.delivery_ms_);
      Put64(&out, pending.deliveries_);
    }
  }
  if (out.size() > storage::kMaxStringBytes)
    return absl::OutOfRangeError("Stream exceeds storage limits");
  return out;
}

absl::StatusOr<std::string> EncodeGroupState(const Group& group) {
  if (group.name_.size() > UINT32_MAX || group.consumers_.size() > UINT32_MAX ||
      group.pending_.size() > UINT32_MAX) {
    return absl::OutOfRangeError("Stream group exceeds storage limits");
  }
  std::string out(kGroupStateMagic);
  PutString(&out, group.name_);
  PutId(&out, group.last_id_);
  Put64(&out, std::bit_cast<std::uint64_t>(group.entries_read_));
  Put32(&out, group.consumers_.size());
  for (const Consumer& consumer : group.consumers_) {
    if (consumer.name_.size() > UINT32_MAX) {
      return absl::OutOfRangeError("Stream consumer exceeds storage limits");
    }
    PutString(&out, consumer.name_);
    Put64(&out, consumer.seen_ms_);
    Put64(&out, consumer.active_ms_);
  }
  Put32(&out, group.pending_.size());
  for (const Pending& pending : group.pending_) {
    if (pending.consumer_.size() > UINT32_MAX) {
      return absl::OutOfRangeError("Stream pending entry exceeds limits");
    }
    PutId(&out, pending.id_);
    PutString(&out, pending.consumer_);
    Put64(&out, pending.delivery_ms_);
    Put64(&out, pending.deliveries_);
  }
  if (out.size() > storage::kMaxStringBytes) {
    return absl::OutOfRangeError("Stream group exceeds storage limits");
  }
  return out;
}

absl::StatusOr<Group> DecodeGroupState(std::string_view in) {
  if (!in.starts_with(kGroupStateMagic)) {
    return absl::InvalidArgumentError("invalid replicated Stream group");
  }
  std::size_t at = kGroupStateMagic.size();
  Group group;
  std::uint64_t entries_read = 0;
  std::uint32_t consumers = 0;
  if (!GetString(in, &at, &group.name_) ||
      !GetId(in, &at, &group.last_id_) ||
      !Get64(in, &at, &entries_read) || !Get32(in, &at, &consumers)) {
    return absl::InvalidArgumentError("truncated replicated Stream group");
  }
  group.entries_read_ = std::bit_cast<std::int64_t>(entries_read);
  constexpr std::size_t kMinConsumerBytes = 4 + 2 * sizeof(std::uint64_t);
  if (consumers > (in.size() - at) / kMinConsumerBytes) {
    return absl::InvalidArgumentError("invalid replicated Stream consumers");
  }
  group.consumers_.reserve(consumers);
  for (std::uint32_t index = 0; index < consumers; ++index) {
    Consumer consumer;
    if (!GetString(in, &at, &consumer.name_) ||
        !Get64(in, &at, &consumer.seen_ms_) ||
        !Get64(in, &at, &consumer.active_ms_)) {
      return absl::InvalidArgumentError(
          "truncated replicated Stream consumer");
    }
    group.consumers_.push_back(std::move(consumer));
  }
  std::uint32_t pending = 0;
  if (!Get32(in, &at, &pending)) {
    return absl::InvalidArgumentError("truncated replicated Stream PEL");
  }
  constexpr std::size_t kMinPendingBytes =
      2 * sizeof(std::uint64_t) + 4 + 2 * sizeof(std::uint64_t);
  if (pending > (in.size() - at) / kMinPendingBytes) {
    return absl::InvalidArgumentError("invalid replicated Stream PEL");
  }
  group.pending_.reserve(pending);
  for (std::uint32_t index = 0; index < pending; ++index) {
    Pending item;
    if (!GetId(in, &at, &item.id_) ||
        !GetString(in, &at, &item.consumer_) ||
        !Get64(in, &at, &item.delivery_ms_) ||
        !Get64(in, &at, &item.deliveries_)) {
      return absl::InvalidArgumentError(
          "truncated replicated Stream pending entry");
    }
    if (!group.pending_.empty() &&
        !(group.pending_.back().id_ < item.id_)) {
      return absl::InvalidArgumentError(
          "invalid replicated Stream pending order");
    }
    group.pending_.push_back(std::move(item));
  }
  if (at != in.size()) {
    return absl::InvalidArgumentError("trailing replicated Stream group data");
  }
  return group;
}

absl::StatusOr<std::vector<std::string>> RestoreGroupArgs(
    std::string_view key, std::string_view group_name, const Group* group) {
  std::vector<std::string> args{
      "XGROUP", std::string(kRestoreGroupSubcommand), std::string(key),
      std::string(group_name), group == nullptr ? "0" : "1"};
  if (group != nullptr) {
    auto encoded = EncodeGroupState(*group);
    if (!encoded.ok()) return encoded.status();
    args.push_back(std::move(*encoded));
  }
  return args;
}

storage::CompactValueUpdate NoChange() { return {}; }
absl::StatusOr<storage::CompactValueUpdate> Changed(Stream stream) {
  auto encoded = Encode(stream);
  if (!encoded.ok()) return encoded.status();
  return storage::CompactValueUpdate{.changed_ = true,
                                     .encoded_ = std::move(*encoded),
                                     .logical_size_ = stream.entries_.size(),
                                     .expire_at_ms_ = std::nullopt};
}

Group* FindGroup(Stream* stream, std::string_view name) {
  auto it =
      std::find_if(stream->groups_.begin(), stream->groups_.end(),
                   [&](const Group& group) { return group.name_ == name; });
  return it == stream->groups_.end() ? nullptr : &*it;
}
Consumer* FindConsumer(Group* group, std::string_view name) {
  auto it = std::find_if(group->consumers_.begin(), group->consumers_.end(),
                         [&](const Consumer& c) { return c.name_ == name; });
  return it == group->consumers_.end() ? nullptr : &*it;
}
const Entry* FindEntry(const Stream& stream, Id id) {
  auto it = std::lower_bound(
      stream.entries_.begin(), stream.entries_.end(), id,
      [](const Entry& entry, Id wanted) { return entry.id_ < wanted; });
  return it != stream.entries_.end() && it->id_ == id ? &*it : nullptr;
}

std::int64_t EstimateEntriesRead(const Stream& stream, Id id) {
  if (stream.entries_added_ == 0) return 0;
  if (stream.entries_added_ >
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    return -1;
  if (stream.entries_.empty() && id <= stream.last_id_)
    return static_cast<std::int64_t>(stream.entries_added_);
  if (id == stream.last_id_)
    return static_cast<std::int64_t>(stream.entries_added_);
  if (id > stream.last_id_ || stream.entries_.empty()) return -1;
  const Id first = stream.entries_.front().id_;
  if (stream.max_deleted_id_ == Id{} || stream.max_deleted_id_ < first) {
    if (stream.entries_added_ < stream.entries_.size()) return -1;
    const std::uint64_t before_first =
        stream.entries_added_ - stream.entries_.size();
    if (id < first) return static_cast<std::int64_t>(before_first);
    if (id == first) return static_cast<std::int64_t>(before_first + 1);
  }
  return -1;
}

void AdvanceEntriesRead(const Stream& stream, Group* group, Id delivered) {
  if (group->entries_read_ >= 0 && stream.max_deleted_id_ < delivered &&
      group->entries_read_ < std::numeric_limits<std::int64_t>::max()) {
    ++group->entries_read_;
  } else {
    group->entries_read_ = EstimateEntriesRead(stream, delivered);
  }
}

std::optional<std::uint64_t> GroupLag(const Stream& stream,
                                      const Group& group) {
  if (stream.entries_added_ == 0) return 0;
  std::int64_t entries_read = -1;
  if (group.entries_read_ >= 0 && stream.max_deleted_id_ < group.last_id_) {
    entries_read = group.entries_read_;
  } else {
    entries_read = EstimateEntriesRead(stream, group.last_id_);
  }
  if (entries_read < 0 ||
      static_cast<std::uint64_t>(entries_read) > stream.entries_added_) {
    return std::nullopt;
  }
  return stream.entries_added_ - static_cast<std::uint64_t>(entries_read);
}

std::string_view StorageError(ReplyBuilder& builder,
                              const absl::Status& status) {
  return status.message().starts_with("WRONGTYPE ") ||
                 status.message().starts_with("BUSYGROUP ") ||
                 status.message().starts_with("NOGROUP ")
             ? builder.AppendError(status.message())
             : builder.AppendError("ERR " + std::string(status.message()));
}

std::size_t KeyIndex(CommandKind kind) {
  return kind == CommandKind::kXGroup || kind == CommandKind::kXInfo ? 2 : 1;
}

Task<absl::Status> RunCompact(const CommandRequest& request,
                              const storage::Digest* digest,
                              storage::TxShardWrites* tx, bool read_only,
                              const storage::CompactValueCallback& callback,
                              storage::ReplicationCommandAppend* replication) {
  const std::string_view key = request.args_[KeyIndex(request.kind_)];
  if (!digest)
    co_return co_await g_storage->ExecuteCompact(
        request.db_id_, key, storage::ValueType::kStream, read_only, callback,
        0, replication);
  co_return co_await g_storage->ExecuteCompactLocked(
      request.db_id_, key, *digest, storage::ValueType::kStream, read_only,
      callback, tx, 0, replication);
}

void AppendEntry(ReplyBuilder& builder, const Entry& entry) {
  builder.AppendArrayHeader(2);
  builder.AppendBulkString(FormatId(entry.id_));
  builder.AppendArrayHeader(entry.fields_.size());
  for (const std::string& field : entry.fields_)
    builder.AppendBulkString(field);
}

struct ReadOneResult {
  struct Item {
    Id id_;
    std::optional<Entry> entry_;
  };
  std::vector<Item> entries_;
  Id cursor_;
};

Task<absl::StatusOr<ReadOneResult>> ReadOneLocal(
    std::uint8_t db_id, std::string key, Id cursor, bool dollar,
    bool group_read, std::string group_name, std::string consumer_name,
    bool new_messages, bool noack, std::uint64_t count,
    const storage::Digest* locked_digest = nullptr,
    storage::TxShardWrites* tx = nullptr,
    const CommandRequest* request = nullptr) {
  ReadOneResult result{.entries_ = {}, .cursor_ = cursor};
  const std::uint64_t now = NowMs();
  std::optional<storage::ReplicationCommandAppend> replication;
  std::vector<std::string> captured_group_args;
  if (group_read && tx == nullptr && request != nullptr) {
    replication = PrepareReplicationCommand(*request);
  }
  auto callback = [&](std::optional<storage::CompactValueView> value)
      -> absl::StatusOr<storage::CompactValueUpdate> {
    auto decoded = Decode(value);
    if (!decoded.ok()) return decoded.status();
    Stream stream = std::move(*decoded);
    if (!value && group_read)
      return absl::NotFoundError("NOGROUP No such key or consumer group");
    if (dollar) {
      result.cursor_ = stream.last_id_;
      return NoChange();
    }
    if (!group_read) {
      for (const Entry& entry : stream.entries_) {
        if (entry.id_ > cursor) {
          result.entries_.push_back({entry.id_, entry});
          result.cursor_ = entry.id_;
          if (result.entries_.size() == count) break;
        }
      }
      return NoChange();
    }
    Group* group = FindGroup(&stream, group_name);
    if (!group) return absl::NotFoundError("NOGROUP No such consumer group");
    bool changed = false;
    bool successful_delivery = false;
    Consumer* consumer = FindConsumer(group, consumer_name);
    if (!consumer) {
      group->consumers_.push_back(Consumer{consumer_name, now, 0});
      consumer = &group->consumers_.back();
      changed = true;
    } else if (consumer->seen_ms_ != now) {
      consumer->seen_ms_ = now;
      changed = true;
    }
    if (new_messages) {
      if (group->entries_read_ < 0) {
        group->entries_read_ = EstimateEntriesRead(stream, group->last_id_);
      }
      for (const Entry& entry : stream.entries_) {
        if (entry.id_ <= group->last_id_) continue;
        AdvanceEntriesRead(stream, group, entry.id_);
        group->last_id_ = entry.id_;
        changed = true;
        auto pending = std::lower_bound(
            group->pending_.begin(), group->pending_.end(), entry.id_,
            [](const Pending& item, Id wanted) { return item.id_ < wanted; });
        result.entries_.push_back({entry.id_, entry});
        successful_delivery = true;
        if (!noack) {
          if (pending != group->pending_.end() && pending->id_ == entry.id_) {
            // XGROUP SETID may rewind behind an entry already in the PEL.
            // Redis treats this as a fresh group delivery.
            pending->consumer_ = consumer_name;
            pending->delivery_ms_ = now;
            pending->deliveries_ = 1;
          } else {
            group->pending_.insert(pending,
                                   Pending{entry.id_, consumer_name, now, 1});
          }
        }
        if (result.entries_.size() == count) break;
      }
    } else {
      for (Pending& pending : group->pending_) {
        if (pending.consumer_ != consumer_name || pending.id_ <= cursor)
          continue;
        if (const Entry* entry = FindEntry(stream, pending.id_)) {
          result.entries_.push_back({pending.id_, *entry});
          pending.delivery_ms_ = now;
          ++pending.deliveries_;
          successful_delivery = true;
          changed = true;
        } else {
          // Redis exposes a deleted entry still present in the PEL as
          // [id, nil], without touching its delivery timestamp or counter.
          result.entries_.push_back({pending.id_, std::nullopt});
        }
        if (result.entries_.size() == count) break;
      }
    }
    if (successful_delivery && consumer->active_ms_ != now) changed = true;
    if (successful_delivery) consumer->active_ms_ = now;
    if (!changed) return NoChange();
    auto canonical = RestoreGroupArgs(key, group_name, group);
    if (!canonical.ok()) return canonical.status();
    if (replication.has_value()) replication->args_ = *canonical;
    captured_group_args = std::move(*canonical);
    return Changed(std::move(stream));
  };
  absl::Status status;
  if (locked_digest == nullptr) {
    status = co_await g_storage->ExecuteCompact(
        db_id, key, storage::ValueType::kStream, !group_read, callback, 0,
        replication ? &*replication : nullptr);
  } else {
    status = co_await g_storage->ExecuteCompactLocked(
        db_id, key, *locked_digest, storage::ValueType::kStream, !group_read,
        callback, group_read ? tx : nullptr, 0,
        replication ? &*replication : nullptr);
  }
  if (!status.ok()) co_return status;
  if (request != nullptr && !captured_group_args.empty()) {
    CaptureReplicationCommand(*request, db_id,
                              std::move(captured_group_args));
  }
  co_return result;
}

Task<CommandReply> ExecuteRead(
    const CommandRequest& request, ReplyBuilder& builder,
    std::span<const StreamExecKey> locked_keys = {},
    std::vector<storage::TxShardWrites>* tx_writes = nullptr,
    std::uint64_t client_id = 0) {
  const auto& a = request.args_;
  const bool group_read = request.kind_ == CommandKind::kXReadGroup;
  if (group_read) MarkReplicationCommandHandled(request);
  std::string group_name, consumer_name;
  std::uint64_t count = UINT64_MAX;
  std::uint64_t block_ms = 0;
  bool block = false, noack = false;
  std::size_t i = 1;
  if (group_read) {
    if (a.size() < 4 || !EqualCi(a[1], "group"))
      co_return Built(builder.AppendError("ERR syntax error"));
    group_name = a[2];
    consumer_name = a[3];
    i = 4;
  }
  for (; i < a.size() && !EqualCi(a[i], "streams");) {
    if (EqualCi(a[i], "count") && i + 1 < a.size()) {
      std::int64_t parsed_count = 0;
      if (!ParseInt(a[i + 1], &parsed_count))
        co_return Built(
            builder.AppendError("ERR value is not an integer or out of range"));
      count = parsed_count <= 0 ? UINT64_MAX
                                : static_cast<std::uint64_t>(parsed_count);
      i += 2;
    } else if (EqualCi(a[i], "block") && i + 1 < a.size()) {
      std::int64_t parsed_block = 0;
      if (!ParseInt(a[i + 1], &parsed_block)) {
        co_return Built(builder.AppendError(
            "ERR timeout is not an integer or out of range"));
      }
      if (parsed_block < 0) {
        co_return Built(builder.AppendError("ERR timeout is negative"));
      }
      block_ms = static_cast<std::uint64_t>(parsed_block);
      block = true;
      i += 2;
    } else if (group_read && EqualCi(a[i], "noack")) {
      noack = true;
      ++i;
    } else {
      co_return Built(builder.AppendError("ERR syntax error"));
    }
  }
  if (i >= a.size()) co_return Built(builder.AppendError("ERR syntax error"));
  const std::size_t first_key = ++i;
  const std::size_t remaining = a.size() - first_key;
  if (remaining < 2 || remaining % 2)
    co_return Built(builder.AppendError(
        absl::StrCat("ERR Unbalanced '", group_read ? "xreadgroup" : "xread",
                     "' list of streams: for each stream key an ID or '",
                     group_read ? ">" : "$", "' must be specified.")));
  const std::size_t key_count = remaining / 2;
  std::vector<Id> cursors(key_count);
  std::vector<bool> dollar(key_count, false), new_messages(key_count, false);
  for (std::size_t k = 0; k < key_count; ++k) {
    const std::string_view text = a[first_key + key_count + k];
    if (!group_read && text == "$") {
      dollar[k] = true;
    } else if (group_read && text == ">") {
      new_messages[k] = true;
    } else {
      auto parsed = ParseId(text);
      if (!parsed.ok()) co_return Built(StorageError(builder, parsed.status()));
      cursors[k] = *parsed;
    }
  }
  const bool has_history =
      group_read && std::any_of(new_messages.begin(), new_messages.end(),
                                [](bool new_message) { return !new_message; });
  if (!locked_keys.empty()) block = false;
  const auto started = std::chrono::steady_clock::now();
  if (block && block_ms != 0) {
    const auto remaining =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::time_point::max() - started);
    if (block_ms > static_cast<std::uint64_t>(remaining.count())) {
      co_return Built(builder.AppendError("ERR timeout is out of range"));
    }
  }
  const std::optional<std::chrono::steady_clock::time_point> deadline =
      block && block_ms != 0
          ? std::optional(started + std::chrono::milliseconds(block_ms))
          : std::nullopt;
  std::unique_ptr<BlockingWaitHandle> wait_handle;
  CommandRequest attempt_request = request;
  bool initialized_dollars = false;
  struct AttemptDbGuard {
    explicit AttemptDbGuard(std::uint8_t db, bool active)
        : db_(db), active_(active) {}
    ~AttemptDbGuard() { Release(); }
    void Release() {
      if (!active_) return;
      EndCommandDbOperation(db_);
      active_ = false;
    }
    std::uint8_t db_;
    bool active_;
  };
  while (true) {
    BlockingWakeCascade* attempt_cascade = nullptr;
    if (wait_handle) {
      const BlockingWakeReason state = BlockingWaitState(*wait_handle);
      if (state == BlockingWakeReason::kTimeout) {
        co_return Built(builder.AppendNullArray());
      }
      if (state == BlockingWakeReason::kUnblockedError) {
        co_return Built(builder.AppendError(
            "UNBLOCKED client unblocked via CLIENT UNBLOCK"));
      }
      if (state == BlockingWakeReason::kCancelled) {
        co_return Built(
            builder.AppendError("ERR blocking Stream wait cancelled"));
      }
      if (state == BlockingWakeReason::kReady) {
        attempt_cascade = ResetBlockingReady(*wait_handle);
      }
    }
    struct CascadeCompletion {
      BlockingWakeCascade* cascade_ = nullptr;
      ~CascadeCompletion() { Finish(); }
      void Finish() noexcept {
        if (cascade_ == nullptr) return;
        cascade_->Done();
        cascade_ = nullptr;
      }
    } cascade_completion{attempt_cascade};
    attempt_request.blocking_wake_cascade_ = attempt_cascade;
    const bool owns_attempt_gate = locked_keys.empty();
    while (owns_attempt_gate && !TryBeginCommandDbOperation(request.db_id_)) {
      if (block && block_ms != 0 &&
          std::chrono::steady_clock::now() - started >=
              std::chrono::milliseconds(block_ms)) {
        co_return Built(builder.AppendNullArray());
      }
      absl::Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return Built(StorageError(builder, slept));
    }
    AttemptDbGuard db_guard(request.db_id_, owns_attempt_gate);
    if (group_read && owns_attempt_gate &&
        !CommandWriteAdmissionIsCurrent(request)) {
      co_return Built(builder.AppendError(
          "TRYAGAIN replication role changed; retry command"));
    }
    if (const auto error = CommandServingGenerationError(request);
        error.has_value()) {
      co_return Built(builder.AppendError(*error));
    }
    std::vector<std::pair<std::string, std::vector<ReadOneResult::Item>>> found;
    for (std::size_t k = 0; k < key_count; ++k) {
      std::string key = a[first_key + k];
      const StreamExecKey* locked_key = nullptr;
      if (!locked_keys.empty()) {
        for (const StreamExecKey& candidate : locked_keys) {
          if (candidate.arg_ == first_key + k) {
            locked_key = &candidate;
            break;
          }
        }
        if (locked_key == nullptr) {
          co_return Built(builder.AppendError("ERR Stream key is missing"));
        }
      }
      const unsigned owner = locked_key == nullptr ? g_storage->OwnerForKey(key)
                                                   : locked_key->owner_;
      const bool initialize = dollar[k] && !initialized_dollars;
      // The local/remote owner branches are exhaustive. Avoid allocating an
      // error message that is overwritten once for every stream key.
      absl::StatusOr<ReadOneResult> one;
      const std::optional<storage::Digest> locked_digest =
          locked_key == nullptr
              ? std::optional<storage::Digest>{}
              : std::optional<storage::Digest>{locked_key->digest_};
      storage::TxShardWrites* local_tx =
          locked_key == nullptr || tx_writes == nullptr ? nullptr
                                                        : &(*tx_writes)[owner];
      if (owner == celer::ThisWorker().id_) {
        one = co_await ReadOneLocal(
            request.db_id_, key, cursors[k], initialize, group_read, group_name,
            consumer_name, new_messages[k], noack, count,
            locked_digest ? &*locked_digest : nullptr, local_tx,
            &attempt_request);
      } else {
        one = co_await celer::SubmitTaskTo(
            owner,
            [db = request.db_id_, key = std::move(key), cursor = cursors[k],
             initialize, group_read, group_name, consumer_name,
             is_new = new_messages[k], noack, count, locked_digest,
             local_tx, request_ptr = &attempt_request]() mutable
            -> Task<absl::StatusOr<ReadOneResult>> {
              co_return co_await ReadOneLocal(
                  db, std::move(key), cursor, initialize, group_read,
                  std::move(group_name), std::move(consumer_name), is_new,
                  noack, count, locked_digest ? &*locked_digest : nullptr,
                  local_tx, request_ptr);
            });
      }
      if (!one.ok()) co_return Built(StorageError(builder, one.status()));
      cursors[k] = one->cursor_;
      if (!one->entries_.empty() || (group_read && !new_messages[k])) {
        found.emplace_back(a[first_key + k], std::move(one->entries_));
      }
    }
    initialized_dollars = true;
    if (!found.empty()) {
      if (builder.version() == RespVersion::k3)
        builder.AppendMapHeader(found.size());
      else
        builder.AppendArrayHeader(found.size());
      for (const auto& [key, entries] : found) {
        if (builder.version() == RespVersion::k2) builder.AppendArrayHeader(2);
        builder.AppendBulkString(key);
        builder.AppendArrayHeader(entries.size());
        for (const ReadOneResult::Item& item : entries) {
          if (item.entry_.has_value()) {
            AppendEntry(builder, *item.entry_);
          } else {
            builder.AppendArrayHeader(2);
            builder.AppendBulkString(FormatId(item.id_));
            builder.AppendNullArray();
          }
        }
      }
      co_return Built(builder.View());
    }
    if (has_history) co_return Built(builder.AppendNullArray());
    if (!block) co_return Built(builder.AppendNullArray());
    if (block_ms != 0 && std::chrono::steady_clock::now() - started >=
                             std::chrono::milliseconds(block_ms))
      co_return Built(builder.AppendNullArray());
    db_guard.Release();
    cascade_completion.Finish();
    if (!wait_handle) {
      const std::string lane =
          group_read
              ? "xreadgroup:" + group_name
              : "xread:" +
                    std::to_string(storage::StorageEngine::AllocateWriteTxid());
      std::vector<BlockingWaitSpec> specs;
      specs.reserve(key_count);
      for (std::size_t k = 0; k < key_count; ++k) {
        if (group_read && !new_messages[k]) continue;
        BlockingWaitSpec spec{
            .key_ = a[first_key + k],
            .lane_ = lane,
            .value_type_ = BlockingValueType::kStream,
            .policy_ = group_read ? BlockingQueuePolicy::kFifo
                                  : BlockingQueuePolicy::kBroadcast,
            .stream_after_ = std::nullopt,
        };
        if (!group_read) {
          spec.stream_after_ = std::pair{cursors[k].ms_, cursors[k].seq_};
        }
        specs.push_back(std::move(spec));
      }
      auto registered = co_await RegisterBlockingWait(
          request.db_id_, std::move(specs), client_id, deadline);
      if (!registered.ok()) {
        co_return Built(StorageError(builder, registered.status()));
      }
      wait_handle = std::move(*registered);
      // Close the empty-check/register race before suspending.
      continue;
    }
    const BlockingWakeReason woke = co_await WaitForBlockingReady(*wait_handle);
    if (woke == BlockingWakeReason::kTimeout) {
      co_return Built(builder.AppendNullArray());
    }
    if (woke == BlockingWakeReason::kUnblockedError) {
      co_return Built(builder.AppendError(
          "UNBLOCKED client unblocked via CLIENT UNBLOCK"));
    }
    if (woke == BlockingWakeReason::kCancelled) {
      co_return Built(
          builder.AppendError("ERR blocking Stream wait cancelled"));
    }
  }
}

Task<CommandReply> ExecuteImpl(const CommandRequest& request,
                               const storage::Digest* digest,
                               storage::TxShardWrites* tx,
                               ReplyBuilder& builder,
                               std::uint64_t client_id = 0) {
  const auto& a = request.args_;
  if (request.kind_ == CommandKind::kXGroup && a.size() >= 2 &&
      EqualCi(a[1], kRestoreGroupSubcommand)) {
    if (!request.replication_origin_ || (a.size() != 5 && a.size() != 6) ||
        (a[4] != "0" && a[4] != "1") ||
        (a[4] == "0" && a.size() != 5) ||
        (a[4] == "1" && a.size() != 6)) {
      co_return Built(builder.AppendError(
          "ERR invalid replicated Stream group state"));
    }
    std::optional<Group> restored;
    if (a[4] == "1") {
      auto decoded = DecodeGroupState(a[5]);
      if (!decoded.ok() || decoded->name_ != a[3]) {
        co_return Built(builder.AppendError(
            "ERR invalid replicated Stream group payload"));
      }
      restored = std::move(*decoded);
    }
    auto restore = [&](std::optional<storage::CompactValueView> value)
        -> absl::StatusOr<storage::CompactValueUpdate> {
      auto decoded = Decode(value);
      if (!decoded.ok()) return decoded.status();
      if (!value && !restored.has_value()) {
        return absl::NotFoundError(
            "NOGROUP No such key or consumer group");
      }
      Stream stream = std::move(*decoded);
      auto found = std::find_if(
          stream.groups_.begin(), stream.groups_.end(),
          [&](const Group& group) { return group.name_ == a[3]; });
      if (restored.has_value()) {
        if (found == stream.groups_.end()) {
          stream.groups_.push_back(*restored);
        } else {
          *found = *restored;
        }
      } else if (found != stream.groups_.end()) {
        stream.groups_.erase(found);
      }
      return Changed(std::move(stream));
    };
    absl::Status restored_status =
        co_await RunCompact(request, digest, tx, false, restore, nullptr);
    co_return restored_status.ok()
                  ? Built(builder.AppendSimpleString("OK"))
                  : Built(StorageError(builder, restored_status));
  }
  if (request.kind_ == CommandKind::kXGroup ||
      request.kind_ == CommandKind::kXInfo) {
    const SubcommandShape* shape = FindSubcommandShape(request.kind_, a[1]);
    if (shape == nullptr) {
      co_return Built(builder.AppendError(SubcommandSyntaxError(request)));
    }
    if (a.size() < shape->min_args_ || a.size() > shape->max_args_) {
      const std::string command =
          request.kind_ == CommandKind::kXGroup ? "xgroup|" : "xinfo|";
      co_return Built(
          builder.AppendError("ERR wrong number of arguments for '" + command +
                              std::string(shape->name_) + "' command"));
    }
  }
  if ((request.kind_ == CommandKind::kXGroup ||
       request.kind_ == CommandKind::kXInfo) &&
      a.size() == 2 && EqualCi(a[1], "help")) {
    static constexpr std::string_view kXGroupHelp[] = {
        "CREATE <key> <groupname> <id|$> [option]",
        "    Create a new consumer group. Options are:",
        "    * MKSTREAM",
        "      Create the empty stream if it does not exist.",
        "    * ENTRIESREAD entries_read",
        "      Set the group's entries_read counter (internal use).",
        "CREATECONSUMER <key> <groupname> <consumer>",
        "    Create a new consumer in the specified group.",
        "DELCONSUMER <key> <groupname> <consumer>",
        "    Remove the specified consumer.",
        "DESTROY <key> <groupname>",
        "    Remove the specified group.",
        "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
        "    Set the current group ID and entries_read counter.",
    };
    static constexpr std::string_view kXInfoHelp[] = {
        "CONSUMERS <key> <groupname>",
        "    Show consumers of <groupname>.",
        "GROUPS <key>",
        "    Show the stream consumer groups.",
        "STREAM <key> [FULL [COUNT <count>]",
        "    Show information about the stream.",
    };
    const std::span<const std::string_view> help =
        request.kind_ == CommandKind::kXGroup
            ? std::span<const std::string_view>(kXGroupHelp)
            : std::span<const std::string_view>(kXInfoHelp);
    builder.AppendArrayHeader(help.size() + 3);
    builder.AppendSimpleString(
        request.kind_ == CommandKind::kXGroup
            ? "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"
            : "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:");
    for (std::string_view line : help) builder.AppendSimpleString(line);
    builder.AppendSimpleString("HELP");
    builder.AppendSimpleString("    Print this help.");
    co_return Built(builder.View());
  }
  if (request.kind_ == CommandKind::kXRead ||
      request.kind_ == CommandKind::kXReadGroup) {
    co_return co_await ExecuteRead(request, builder, {}, nullptr, client_id);
  }
  bool read_only = request.kind_ == CommandKind::kXLen ||
                   request.kind_ == CommandKind::kXRange ||
                   request.kind_ == CommandKind::kXRevRange ||
                   request.kind_ == CommandKind::kXPending ||
                   request.kind_ == CommandKind::kXInfo;
  long long integer = 0;
  bool nil = false;
  std::string simple;
  std::optional<Id> appended_id;
  std::vector<Entry> entries;
  std::vector<Pending> pending_output;
  Stream info_stream;
  Group info_group;
  std::vector<Group> info_groups;
  std::vector<Consumer> info_consumers;
  std::vector<Id> deleted_claim_ids;
  Id next_id{};
  bool claim_justid = false;
  bool null_range = false;
  bool xinfo_full = false;
  std::uint64_t xinfo_count = 10;
  std::vector<std::string> captured_xadd;
  std::vector<std::string> captured_xtrim;
  std::vector<std::string> captured_group_args;
  const bool group_state_write =
      request.kind_ == CommandKind::kXGroup ||
      request.kind_ == CommandKind::kXAck ||
      request.kind_ == CommandKind::kXClaim ||
      request.kind_ == CommandKind::kXAutoClaim;
  if (request.kind_ == CommandKind::kXAdd || group_state_write) {
    MarkReplicationCommandHandled(request);
  }
  const bool directly_replayable_write =
      request.kind_ == CommandKind::kXAdd ||
      request.kind_ == CommandKind::kXDel ||
      request.kind_ == CommandKind::kXTrim ||
      request.kind_ == CommandKind::kXSetId ||
      group_state_write;
  auto replication = tx == nullptr && directly_replayable_write
                         ? PrepareReplicationCommand(request)
                         : std::nullopt;

  if (request.kind_ == CommandKind::kXInfo) {
    if (EqualCi(a[1], "stream")) {
      if (a.size() == 4 && EqualCi(a[3], "full")) {
        xinfo_full = true;
      } else if (a.size() == 6 && EqualCi(a[3], "full") &&
                 EqualCi(a[4], "count")) {
        std::int64_t parsed_count = 0;
        if (!ParseInt(a[5], &parsed_count))
          co_return Built(builder.AppendError(
              "ERR value is not an integer or out of range"));
        xinfo_full = true;
        xinfo_count = parsed_count < 0 ? 10 : parsed_count;
      } else if (a.size() != 3) {
        co_return Built(builder.AppendError("ERR syntax error"));
      }
    } else if (EqualCi(a[1], "groups")) {
      if (a.size() != 3)
        co_return Built(builder.AppendError("ERR syntax error"));
    } else if (EqualCi(a[1], "consumers")) {
      if (a.size() != 4)
        co_return Built(builder.AppendError("ERR syntax error"));
    }
  }

  auto callback = [&](std::optional<storage::CompactValueView> value)
      -> absl::StatusOr<storage::CompactValueUpdate> {
    auto decoded = Decode(value);
    if (!decoded.ok()) return decoded.status();
    Stream stream = std::move(*decoded);
    const std::uint64_t now = NowMs();

    switch (request.kind_) {
      case CommandKind::kXAdd: {
        bool nomkstream = false;
        enum class Trim { kNone, kMaxLen, kMinId } trim = Trim::kNone;
        std::uint64_t maxlen = 0;
        bool trim_approximate = false;
        std::optional<std::uint64_t> trim_limit;
        std::optional<std::size_t> trim_specifier_arg;
        std::optional<std::size_t> trim_threshold_arg;
        Id minid{};
        std::size_t i = 2;
        while (i < a.size()) {
          if (EqualCi(a[i], "nomkstream")) {
            nomkstream = true;
            ++i;
          } else if ((EqualCi(a[i], "maxlen") || EqualCi(a[i], "minid")) &&
                     i + 1 < a.size()) {
            if (trim != Trim::kNone) {
              return absl::InvalidArgumentError(
                  "syntax error, MAXLEN and MINID options at the same time "
                  "are not compatible");
            }
            trim = EqualCi(a[i], "maxlen") ? Trim::kMaxLen : Trim::kMinId;
            ++i;
            trim_approximate = i < a.size() && a[i] == "~";
            if (i < a.size() && (trim_approximate || a[i] == "=")) {
              trim_specifier_arg = i;
              ++i;
            }
            if (i >= a.size())
              return absl::InvalidArgumentError("syntax error");
            trim_threshold_arg = i;
            if (trim == Trim::kMaxLen) {
              if (!ParseInt(a[i], &maxlen))
                return absl::InvalidArgumentError(
                    "value is not an integer or out of range");
            } else {
              auto parsed = ParseId(a[i]);
              if (!parsed.ok()) return parsed.status();
              minid = *parsed;
            }
            ++i;
            if (i < a.size() && EqualCi(a[i], "limit")) {
              std::uint64_t parsed_limit = 0;
              if (!trim_approximate || i + 1 >= a.size() ||
                  !ParseInt(a[i + 1], &parsed_limit))
                return absl::InvalidArgumentError("syntax error");
              trim_limit = parsed_limit;
              i += 2;
            }
          } else
            break;
        }
        if (i >= a.size()) return absl::InvalidArgumentError("syntax error");
        const std::size_t id_arg = i;
        const std::string_view id_text = a[i++];
        if ((a.size() - i) == 0 || (a.size() - i) % 2)
          return absl::InvalidArgumentError(
              "wrong number of arguments for 'xadd' command");
        Id id;
        if (id_text == "*") {
          if (now > stream.last_id_.ms_) {
            id = Id{.ms_ = now, .seq_ = 0};
          } else if (stream.last_id_.seq_ !=
                     std::numeric_limits<std::uint64_t>::max()) {
            id = Id{.ms_ = stream.last_id_.ms_,
                    .seq_ = stream.last_id_.seq_ + 1};
          } else if (stream.last_id_.ms_ !=
                     std::numeric_limits<std::uint64_t>::max()) {
            id = Id{.ms_ = stream.last_id_.ms_ + 1, .seq_ = 0};
          } else {
            return absl::InvalidArgumentError(
                "The stream has exhausted the last possible ID, unable to "
                "add more items");
          }
        } else if (id_text.ends_with("-*")) {
          const std::string_view milliseconds =
              id_text.substr(0, id_text.size() - 2);
          if (!ParseInt(milliseconds, &id.ms_))
            return absl::InvalidArgumentError(
                "Invalid stream ID specified as stream command argument");
          if (id.ms_ < stream.last_id_.ms_)
            return absl::InvalidArgumentError(
                "The ID specified in XADD is equal or smaller than the target "
                "stream top item");
          if (id.ms_ == stream.last_id_.ms_) {
            if (stream.last_id_.seq_ ==
                std::numeric_limits<std::uint64_t>::max())
              return absl::InvalidArgumentError(
                  "The ID specified in XADD is equal or smaller than the "
                  "target stream top item");
            id.seq_ = stream.last_id_.seq_ + 1;
          } else {
            id.seq_ = 0;
          }
        } else if (id_text.find('-') == std::string_view::npos) {
          if (!ParseInt(id_text, &id.ms_))
            return absl::InvalidArgumentError(
                "Invalid stream ID specified as stream command argument");
          id.seq_ = 0;
          if (id == Id{})
            return absl::InvalidArgumentError(
                "The ID specified in XADD must be greater than 0-0");
          if (id <= stream.last_id_)
            return absl::InvalidArgumentError(
                "The ID specified in XADD is equal or smaller than the target "
                "stream top item");
        } else {
          auto parsed = ParseId(id_text);
          if (!parsed.ok()) return parsed.status();
          id = *parsed;
          if (id == Id{})
            return absl::InvalidArgumentError(
                "The ID specified in XADD must be greater than 0-0");
          if (id <= stream.last_id_)
            return absl::InvalidArgumentError(
                "The ID specified in XADD is equal or smaller than the target "
                "stream top item");
        }
        if (!value && nomkstream) {
          nil = true;
          return NoChange();
        }
        Entry entry{.id_ = id, .fields_ = {}};
        for (; i < a.size(); ++i) entry.fields_.push_back(a[i]);
        AppendStreamEntry(&stream, std::move(entry));
        stream.last_id_ = id;
        ++stream.entries_added_;
        simple = FormatId(id);
        appended_id = id;
        captured_xadd = request.args_;
        captured_xadd[id_arg] = FormatId(id);
        const std::uint64_t effective_limit =
            trim_approximate
                ? trim_limit.value_or(DefaultApproximateTrimLimit())
                : 0;
        if (trim == Trim::kMaxLen) {
          (void)TrimStreamMaxLen(&stream, maxlen, trim_approximate,
                                 effective_limit);
        } else if (trim == Trim::kMinId) {
          (void)TrimStreamMinId(&stream, minid, trim_approximate,
                                effective_limit);
        }
        // Approximate trimming depends on the local macro-node boundaries.
        // Propagate the exact resulting boundary so replicas and AOF replay do
        // not depend on their stream-node-max-entries setting.
        if (trim_approximate) {
          captured_xadd[*trim_specifier_arg] = "=";
          captured_xadd[*trim_threshold_arg] =
              trim == Trim::kMaxLen
                  ? std::to_string(stream.entries_.size())
                  : FormatId(stream.entries_.empty()
                                 ? Id{.ms_ = UINT64_MAX, .seq_ = UINT64_MAX}
                                 : stream.entries_.front().id_);
        }
        if (replication.has_value()) replication->args_ = captured_xadd;
        return Changed(std::move(stream));
      }
      case CommandKind::kXDel: {
        std::vector<Id> ids;
        for (std::size_t i = 2; i < a.size(); ++i) {
          auto id = ParseId(a[i]);
          if (!id.ok()) return id.status();
          ids.push_back(*id);
        }
        for (Id id : ids) {
          auto it = std::find_if(stream.entries_.begin(), stream.entries_.end(),
                                 [&](const Entry& e) { return e.id_ == id; });
          if (it != stream.entries_.end()) {
            EraseStreamEntry(&stream, it - stream.entries_.begin());
            ++integer;
            stream.max_deleted_id_ = std::max(stream.max_deleted_id_, id);
          }
        }
        return integer
                   ? Changed(std::move(stream))
                   : absl::StatusOr<storage::CompactValueUpdate>(NoChange());
      }
      case CommandKind::kXLen:
        integer = stream.entries_.size();
        return NoChange();
      case CommandKind::kXRange:
      case CommandKind::kXRevRange: {
        const bool reverse = request.kind_ == CommandKind::kXRevRange;
        std::string_view start_text = a[2], end_text = a[3];
        if (reverse) std::swap(start_text, end_text);
        bool start_exclusive = start_text.starts_with('(');
        bool end_exclusive = end_text.starts_with('(');
        if (start_exclusive) start_text.remove_prefix(1);
        if (end_exclusive) end_text.remove_prefix(1);
        if ((start_exclusive && (start_text == "-" || start_text == "+")) ||
            (end_exclusive && (end_text == "-" || end_text == "+"))) {
          return absl::InvalidArgumentError(
              "Invalid stream ID specified as stream command argument");
        }
        auto start = ParseId(start_text, false, true);
        auto end = ParseId(end_text, true, true);
        if (!start.ok()) return start.status();
        if (!end.ok()) return end.status();
        if ((start_exclusive &&
             *start == Id{.ms_ = UINT64_MAX, .seq_ = UINT64_MAX}) ||
            (end_exclusive && *end == Id{})) {
          return absl::InvalidArgumentError(
              "Invalid stream ID specified as stream command argument");
        }
        std::uint64_t count = UINT64_MAX;
        if (a.size() > 4) {
          std::int64_t parsed_count = 0;
          if (a.size() != 6 || !EqualCi(a[4], "count") ||
              !ParseInt(a[5], &parsed_count))
            return absl::InvalidArgumentError("syntax error");
          if (parsed_count <= 0) {
            null_range = true;
            return NoChange();
          }
          count = static_cast<std::uint64_t>(parsed_count);
        }
        for (const Entry& entry : stream.entries_) {
          if ((start_exclusive ? entry.id_ > *start : entry.id_ >= *start) &&
              (end_exclusive ? entry.id_ < *end : entry.id_ <= *end))
            entries.push_back(entry);
        }
        if (reverse) std::reverse(entries.begin(), entries.end());
        if (entries.size() > count) entries.resize(count);
        return NoChange();
      }
      case CommandKind::kXTrim: {
        std::size_t i = 2;
        bool maxlen_mode = EqualCi(a[i], "maxlen");
        bool minid_mode = EqualCi(a[i], "minid");
        if (!maxlen_mode && !minid_mode)
          return absl::InvalidArgumentError("syntax error");
        ++i;
        const bool approximate = i < a.size() && a[i] == "~";
        std::optional<std::size_t> trim_specifier_arg;
        if (i < a.size() && (approximate || a[i] == "=")) {
          trim_specifier_arg = i;
          ++i;
        }
        if (i >= a.size()) return absl::InvalidArgumentError("syntax error");
        const std::size_t trim_threshold_arg = i;
        std::uint64_t maxlen = 0;
        Id minid{};
        if (maxlen_mode) {
          if (!ParseInt(a[i], &maxlen))
            return absl::InvalidArgumentError(
                "value is not an integer or out of range");
        } else {
          auto parsed = ParseId(a[i]);
          if (!parsed.ok()) return parsed.status();
          minid = *parsed;
        }
        ++i;
        std::optional<std::uint64_t> requested_limit;
        if (i < a.size()) {
          std::uint64_t parsed_limit = 0;
          if (!approximate || i + 2 != a.size() || !EqualCi(a[i], "limit") ||
              !ParseInt(a[i + 1], &parsed_limit))
            return absl::InvalidArgumentError("syntax error");
          requested_limit = parsed_limit;
          i += 2;
        }
        if (i != a.size()) return absl::InvalidArgumentError("syntax error");
        const std::uint64_t limit =
            approximate
                ? requested_limit.value_or(DefaultApproximateTrimLimit())
                : 0;
        std::size_t removed = 0;
        if (maxlen_mode) {
          removed = TrimStreamMaxLen(&stream, maxlen, approximate, limit);
        } else {
          removed = TrimStreamMinId(&stream, minid, approximate, limit);
        }
        integer = removed;
        if (approximate && removed != 0) {
          captured_xtrim = request.args_;
          captured_xtrim[*trim_specifier_arg] = "=";
          captured_xtrim[trim_threshold_arg] =
              maxlen_mode
                  ? std::to_string(stream.entries_.size())
                  : FormatId(stream.entries_.empty()
                                 ? Id{.ms_ = UINT64_MAX, .seq_ = UINT64_MAX}
                                 : stream.entries_.front().id_);
          if (replication.has_value()) replication->args_ = captured_xtrim;
          MarkReplicationCommandHandled(request);
        }
        return integer
                   ? Changed(std::move(stream))
                   : absl::StatusOr<storage::CompactValueUpdate>(NoChange());
      }
      case CommandKind::kXSetId: {
        auto id = ParseId(a[2]);
        if (!id.ok()) return id.status();
        std::optional<std::uint64_t> entries_added;
        std::optional<Id> max_deleted_id;
        for (std::size_t i = 3; i < a.size(); i += 2) {
          if (i + 1 >= a.size())
            return absl::InvalidArgumentError("syntax error");
          if (EqualCi(a[i], "entriesadded")) {
            std::int64_t parsed_entries = 0;
            if (!ParseInt(a[i + 1], &parsed_entries))
              return absl::InvalidArgumentError(
                  "value is not an integer or out of range");
            if (parsed_entries < 0)
              return absl::InvalidArgumentError(
                  "entries_added must be positive");
            entries_added = static_cast<std::uint64_t>(parsed_entries);
          } else if (EqualCi(a[i], "maxdeletedid")) {
            auto parsed = ParseId(a[i + 1]);
            if (!parsed.ok()) return parsed.status();
            if (*id < *parsed)
              return absl::InvalidArgumentError(
                  "The ID specified in XSETID is smaller than the provided "
                  "max_deleted_entry_id");
            max_deleted_id = *parsed;
          } else
            return absl::InvalidArgumentError("syntax error");
        }
        if (!value) return absl::InvalidArgumentError("no such key");
        if (*id < stream.max_deleted_id_)
          return absl::InvalidArgumentError(
              "The ID specified in XSETID is smaller than current "
              "max_deleted_entry_id");
        if (!stream.entries_.empty() && *id < stream.entries_.back().id_)
          return absl::InvalidArgumentError(
              "The ID specified in XSETID is smaller than the target stream "
              "top item");
        if (entries_added.has_value() &&
            *entries_added < stream.entries_.size())
          return absl::InvalidArgumentError(
              "The entries_added specified in XSETID is smaller than the "
              "target stream length");
        stream.last_id_ = *id;
        if (entries_added.has_value()) stream.entries_added_ = *entries_added;
        if (max_deleted_id.has_value() && *max_deleted_id != Id{})
          stream.max_deleted_id_ = *max_deleted_id;
        simple = "OK";
        return Changed(std::move(stream));
      }
      case CommandKind::kXGroup: {
        const std::string_view sub = a[1];
        if (EqualCi(sub, "create")) {
          if (a.size() < 5) return absl::InvalidArgumentError("syntax error");
          if (FindGroup(&stream, a[3]))
            return absl::AlreadyExistsError(
                "BUSYGROUP Consumer Group name already exists");
          bool mkstream = false;
          std::int64_t entries_read = -1;
          for (std::size_t i = 5; i < a.size();) {
            if (EqualCi(a[i], "mkstream")) {
              mkstream = true;
              ++i;
            } else if (EqualCi(a[i], "entriesread") && i + 1 < a.size() &&
                       ParseInt(a[i + 1], &entries_read))
              i += 2;
            else
              return absl::InvalidArgumentError("syntax error");
          }
          if (!value && !mkstream)
            return absl::NotFoundError(
                "The XGROUP subcommand requires the key to exist");
          Id id = stream.last_id_;
          if (a[4] != "$") {
            auto parsed = ParseId(a[4]);
            if (!parsed.ok()) return parsed.status();
            id = *parsed;
          }
          stream.groups_.push_back(Group{.name_ = a[3],
                                         .last_id_ = id,
                                         .entries_read_ = entries_read,
                                         .consumers_ = {},
                                         .pending_ = {}});
          simple = "OK";
          return Changed(std::move(stream));
        }
        if (!value)
          return absl::InvalidArgumentError(
              "The XGROUP subcommand requires the key to exist. Note that for "
              "CREATE you may want to use the MKSTREAM option to create an "
              "empty stream automatically.");
        Group* group = FindGroup(&stream, a[3]);
        if (EqualCi(sub, "destroy")) {
          if (!group) {
            integer = 0;
            return NoChange();
          }
          std::erase_if(stream.groups_,
                        [&](const Group& g) { return g.name_ == a[3]; });
          integer = 1;
          return Changed(std::move(stream));
        }
        if (!group)
          return absl::NotFoundError("NOGROUP No such consumer group");
        if (EqualCi(sub, "setid")) {
          if (a.size() < 5) return absl::InvalidArgumentError("syntax error");
          if (a[4] == "$")
            group->last_id_ = stream.last_id_;
          else {
            auto id = ParseId(a[4]);
            if (!id.ok()) return id.status();
            group->last_id_ = *id;
          }
          if (a.size() == 7 && EqualCi(a[5], "entriesread") &&
              ParseInt(a[6], &group->entries_read_)) {
          } else if (a.size() == 5) {
            group->entries_read_ = -1;
          } else
            return absl::InvalidArgumentError("syntax error");
          simple = "OK";
          return Changed(std::move(stream));
        }
        if (EqualCi(sub, "createconsumer")) {
          if (a.size() != 5) return absl::InvalidArgumentError("syntax error");
          if (FindConsumer(group, a[4])) {
            integer = 0;
            return NoChange();
          }
          group->consumers_.push_back(Consumer{a[4], now, 0});
          integer = 1;
          return Changed(std::move(stream));
        }
        if (EqualCi(sub, "delconsumer")) {
          if (a.size() != 5) return absl::InvalidArgumentError("syntax error");
          Consumer* consumer = FindConsumer(group, a[4]);
          if (!consumer) {
            integer = 0;
            return NoChange();
          }
          integer = std::count_if(
              group->pending_.begin(), group->pending_.end(),
              [&](const Pending& p) { return p.consumer_ == a[4]; });
          std::erase_if(group->pending_,
                        [&](const Pending& p) { return p.consumer_ == a[4]; });
          std::erase_if(group->consumers_,
                        [&](const Consumer& c) { return c.name_ == a[4]; });
          return Changed(std::move(stream));
        }
        return absl::InvalidArgumentError("unknown subcommand");
      }
      case CommandKind::kXAck: {
        Group* group = FindGroup(&stream, a[2]);
        if (!group) return NoChange();
        for (std::size_t i = 3; i < a.size(); ++i) {
          auto id = ParseId(a[i]);
          if (!id.ok()) return id.status();
          const std::size_t old = group->pending_.size();
          std::erase_if(group->pending_,
                        [&](const Pending& p) { return p.id_ == *id; });
          integer += old - group->pending_.size();
        }
        return integer
                   ? Changed(std::move(stream))
                   : absl::StatusOr<storage::CompactValueUpdate>(NoChange());
      }
      case CommandKind::kXPending: {
        Group* group = FindGroup(&stream, a[2]);
        if (!group)
          return absl::NotFoundError("NOGROUP No such consumer group");
        if (a.size() == 3) {
          info_group = *group;
          return NoChange();
        }
        std::size_t option = 3;
        std::uint64_t min_idle = 0;
        if (option < a.size() && EqualCi(a[option], "idle")) {
          if (option + 1 >= a.size() || !ParseInt(a[option + 1], &min_idle)) {
            return absl::InvalidArgumentError(
                "value is not an integer or out of range");
          }
          option += 2;
        }
        if (a.size() - option != 3 && a.size() - option != 4)
          return absl::InvalidArgumentError("syntax error");
        std::string_view start_text = a[option];
        std::string_view end_text = a[option + 1];
        const bool start_exclusive = start_text.starts_with('(');
        const bool end_exclusive = end_text.starts_with('(');
        if (start_exclusive) start_text.remove_prefix(1);
        if (end_exclusive) end_text.remove_prefix(1);
        auto start = ParseId(start_text, false, true),
             end = ParseId(end_text, true, true);
        std::int64_t parsed_count = 0;
        if (!start.ok()) return start.status();
        if (!end.ok()) return end.status();
        if (!ParseInt(a[option + 2], &parsed_count))
          return absl::InvalidArgumentError(
              "value is not an integer or out of range");
        std::string_view consumer =
            a.size() - option == 4 ? a[option + 3] : std::string_view{};
        if (parsed_count <= 0) return NoChange();
        const std::uint64_t count = static_cast<std::uint64_t>(parsed_count);
        for (const Pending& p : group->pending_)
          if ((start_exclusive ? p.id_ > *start : p.id_ >= *start) &&
              (end_exclusive ? p.id_ < *end : p.id_ <= *end) &&
              now - std::min(now, p.delivery_ms_) >= min_idle &&
              (consumer.empty() || p.consumer_ == consumer)) {
            pending_output.push_back(p);
            if (pending_output.size() == count) break;
          }
        return NoChange();
      }
      case CommandKind::kXClaim:
      case CommandKind::kXAutoClaim: {
        Group* group = FindGroup(&stream, a[2]);
        if (!group)
          return absl::NotFoundError("NOGROUP No such consumer group");
        const std::string_view consumer_name = a[3];
        std::uint64_t min_idle = 0;
        if (!ParseInt(a[4], &min_idle))
          return absl::InvalidArgumentError("Invalid min-idle-time argument");
        bool changed = false;
        bool justid = false;
        bool force = false;
        std::optional<std::uint64_t> delivery_time;
        std::optional<std::uint64_t> retry_count;
        std::optional<Id> last_id;
        std::vector<Id> ids;
        if (request.kind_ == CommandKind::kXClaim) {
          std::size_t i = 5;
          while (i < a.size() && !EqualCi(a[i], "idle") &&
                 !EqualCi(a[i], "time") && !EqualCi(a[i], "retrycount") &&
                 !EqualCi(a[i], "force") && !EqualCi(a[i], "justid") &&
                 !EqualCi(a[i], "lastid")) {
            auto id = ParseId(a[i]);
            if (!id.ok()) return id.status();
            ids.push_back(*id);
            ++i;
          }
          if (ids.empty()) return absl::InvalidArgumentError("syntax error");
          for (; i < a.size();) {
            if (EqualCi(a[i], "justid")) {
              justid = true;
              ++i;
            } else if (EqualCi(a[i], "force")) {
              force = true;
              ++i;
            } else if (EqualCi(a[i], "idle") && i + 1 < a.size()) {
              std::uint64_t idle = 0;
              if (!ParseInt(a[i + 1], &idle))
                return absl::InvalidArgumentError(
                    "Invalid IDLE option argument for XCLAIM");
              delivery_time = idle > now ? now : now - idle;
              i += 2;
            } else if (EqualCi(a[i], "time") && i + 1 < a.size()) {
              std::uint64_t time = 0;
              if (!ParseInt(a[i + 1], &time))
                return absl::InvalidArgumentError(
                    "Invalid TIME option argument for XCLAIM");
              delivery_time = std::min(now, time);
              i += 2;
            } else if (EqualCi(a[i], "retrycount") && i + 1 < a.size()) {
              std::uint64_t retries = 0;
              if (!ParseInt(a[i + 1], &retries))
                return absl::InvalidArgumentError(
                    "Invalid RETRYCOUNT option argument for XCLAIM");
              retry_count = retries;
              i += 2;
            } else if (EqualCi(a[i], "lastid") && i + 1 < a.size()) {
              auto parsed = ParseId(a[i + 1]);
              if (!parsed.ok()) return parsed.status();
              last_id = *parsed;
              i += 2;
            } else {
              return absl::InvalidArgumentError("syntax error");
            }
          }
          if (last_id.has_value() && *last_id > group->last_id_) {
            group->last_id_ = *last_id;
            changed = true;
          }
        } else {
          std::string_view start_text = a[5];
          const bool start_exclusive = start_text.starts_with('(');
          if (start_exclusive) start_text.remove_prefix(1);
          if (start_exclusive && (start_text == "-" || start_text == "+")) {
            return absl::InvalidArgumentError(
                "Invalid stream ID specified as stream command argument");
          }
          auto start = ParseId(start_text, false, !start_exclusive);
          if (!start.ok()) return start.status();
          if (start_exclusive &&
              *start == Id{std::numeric_limits<std::uint64_t>::max(),
                           std::numeric_limits<std::uint64_t>::max()}) {
            return absl::InvalidArgumentError(
                "invalid start ID for the interval");
          }
          std::uint64_t count = 100;
          for (std::size_t i = 6; i < a.size();) {
            if (EqualCi(a[i], "count") && i + 1 < a.size() &&
                ParseInt(a[i + 1], &count)) {
              if (count == 0)
                return absl::InvalidArgumentError("COUNT must be > 0");
              i += 2;
            } else if (EqualCi(a[i], "justid")) {
              justid = true;
              ++i;
            } else
              return absl::InvalidArgumentError("syntax error");
          }
          auto pending = std::lower_bound(
              group->pending_.begin(), group->pending_.end(), *start,
              [](const Pending& item, Id wanted) { return item.id_ < wanted; });
          if (start_exclusive && pending != group->pending_.end() &&
              pending->id_ == *start) {
            ++pending;
          }
          std::size_t pending_index = pending - group->pending_.begin();
          std::uint64_t remaining = count;
          std::uint64_t attempts =
              count > std::numeric_limits<std::uint64_t>::max() / 10
                  ? std::numeric_limits<std::uint64_t>::max()
                  : count * 10;
          while (pending_index < group->pending_.size() && attempts != 0 &&
                 remaining != 0) {
            --attempts;
            const Id id = group->pending_[pending_index].id_;
            if (FindEntry(stream, id) == nullptr) {
              deleted_claim_ids.push_back(id);
              group->pending_.erase(group->pending_.begin() + pending_index);
              changed = true;
              --remaining;
              continue;
            }
            const Pending& item = group->pending_[pending_index];
            if (now - std::min(now, item.delivery_ms_) >= min_idle) {
              ids.push_back(id);
              --remaining;
            }
            ++pending_index;
          }
          next_id = pending_index < group->pending_.size()
                        ? group->pending_[pending_index].id_
                        : Id{};
        }
        Consumer* claim_consumer = FindConsumer(group, consumer_name);
        if (!claim_consumer) {
          group->consumers_.push_back(
              Consumer{std::string(consumer_name), now, 0});
          claim_consumer = &group->consumers_.back();
          changed = true;
        } else if (claim_consumer->seen_ms_ != now) {
          claim_consumer->seen_ms_ = now;
          changed = true;
        }
        for (Id id : ids) {
          auto p = std::find_if(group->pending_.begin(), group->pending_.end(),
                                [&](const Pending& x) { return x.id_ == id; });
          const Entry* entry = FindEntry(stream, id);
          if (entry == nullptr) {
            if (p != group->pending_.end()) {
              if (request.kind_ == CommandKind::kXAutoClaim)
                deleted_claim_ids.push_back(id);
              group->pending_.erase(p);
              changed = true;
            }
            continue;
          }
          if (p == group->pending_.end()) {
            if (!force || request.kind_ != CommandKind::kXClaim) continue;
            p = std::lower_bound(group->pending_.begin(), group->pending_.end(),
                                 id, [](const Pending& pending, Id wanted) {
                                   return pending.id_ < wanted;
                                 });
            p = group->pending_.insert(
                p, Pending{id, std::string(consumer_name), now, 1});
            changed = true;
          } else if (now - std::min(now, p->delivery_ms_) < min_idle) {
            continue;
          }
          p->consumer_ = consumer_name;
          p->delivery_ms_ = delivery_time.value_or(now);
          if (retry_count.has_value())
            p->deliveries_ = *retry_count;
          else if (!justid)
            ++p->deliveries_;
          changed = true;
          entries.push_back(*entry);
        }
        if (!entries.empty()) {
          claim_consumer->active_ms_ = now;
          changed = true;
        }
        claim_justid = justid;
        return changed
                   ? Changed(std::move(stream))
                   : absl::StatusOr<storage::CompactValueUpdate>(NoChange());
      }
      case CommandKind::kXInfo: {
        if (EqualCi(a[1], "stream")) {
          if (!value) return absl::NotFoundError("no such key");
          info_stream = stream;
          return NoChange();
        }
        if (EqualCi(a[1], "groups")) {
          if (!value) return absl::NotFoundError("no such key");
          info_stream = stream;
          info_groups = stream.groups_;
          return NoChange();
        }
        if (EqualCi(a[1], "consumers")) {
          if (a.size() != 4) return absl::InvalidArgumentError("syntax error");
          if (!value) return absl::NotFoundError("no such key");
          Group* group = FindGroup(&stream, a[3]);
          if (!group)
            return absl::NotFoundError("NOGROUP No such consumer group");
          info_group = *group;
          info_consumers = group->consumers_;
          return NoChange();
        }
        return absl::InvalidArgumentError("unknown subcommand");
      }
      default:
        return absl::InvalidArgumentError("unsupported Stream command path");
    }
  };

  auto canonical_callback = [&](std::optional<storage::CompactValueView> value)
      -> absl::StatusOr<storage::CompactValueUpdate> {
    auto update = callback(value);
    if (!update.ok() || !update->changed_ || !group_state_write) return update;
    std::optional<storage::CompactValueView> next(
        std::in_place, storage::CompactValueView{
                           .encoded_ = update->encoded_,
                           .logical_size_ = update->logical_size_,
                           .expire_at_ms_ =
                               value.has_value() ? value->expire_at_ms_ : 0});
    auto stream = Decode(next);
    if (!stream.ok()) return stream.status();
    const std::string_view group_name =
        request.kind_ == CommandKind::kXGroup ? a[3] : a[2];
    const Group* group = nullptr;
    for (const Group& candidate : stream->groups_) {
      if (candidate.name_ == group_name) {
        group = &candidate;
        break;
      }
    }
    auto canonical = RestoreGroupArgs(
        a[request.kind_ == CommandKind::kXGroup ? 2 : 1], group_name, group);
    if (!canonical.ok()) return canonical.status();
    if (replication.has_value()) replication->args_ = *canonical;
    captured_group_args = std::move(*canonical);
    return update;
  };

  absl::Status status =
      co_await RunCompact(request, digest, tx, read_only, canonical_callback,
                          replication ? &*replication : nullptr);
  if (!status.ok()) co_return Built(StorageError(builder, status));
  if (!captured_group_args.empty()) {
    CaptureReplicationCommand(request, std::move(captured_group_args));
  }
  if (!captured_xadd.empty()) {
    CaptureReplicationCommand(request, std::move(captured_xadd));
  }
  if (!captured_xtrim.empty()) {
    CaptureReplicationCommand(request, std::move(captured_xtrim));
  }
  if (request.kind_ == CommandKind::kXAdd && !nil && appended_id.has_value()) {
    NotifyStreamBlockingKey(request, a[1], appended_id->ms_,
                            appended_id->seq_);
  } else if (!read_only) {
    // Metadata changes such as XGROUP SETID can make a consumer-group read
    // ready without appending a new stream ID. Wake candidates and let them
    // recheck their command-specific condition under the normal key lock.
    const std::size_t key_arg = request.kind_ == CommandKind::kXGroup ? 2 : 1;
    NotifyStreamBlockingKey(request, a[key_arg]);
  }
  switch (request.kind_) {
    case CommandKind::kXAdd:
      co_return Built(nil ? builder.AppendNull()
                          : builder.AppendBulkString(simple));
    case CommandKind::kXSetId:
      co_return Built(builder.AppendSimpleString(simple));
    case CommandKind::kXDel:
    case CommandKind::kXLen:
    case CommandKind::kXTrim:
    case CommandKind::kXAck:
      co_return Built(builder.AppendInteger(integer));
    case CommandKind::kXGroup:
      if (!simple.empty()) co_return Built(builder.AppendSimpleString(simple));
      co_return Built(builder.AppendInteger(integer));
    case CommandKind::kXRange:
    case CommandKind::kXRevRange:
    case CommandKind::kXClaim:
      if (null_range) co_return Built(builder.AppendNullArray());
      builder.AppendArrayHeader(entries.size());
      for (const Entry& entry : entries) {
        if (claim_justid)
          builder.AppendBulkString(FormatId(entry.id_));
        else
          AppendEntry(builder, entry);
      }
      co_return Built(builder.View());
    case CommandKind::kXAutoClaim:
      builder.AppendArrayHeader(3);
      builder.AppendBulkString(FormatId(next_id));
      builder.AppendArrayHeader(entries.size());
      for (const Entry& entry : entries) {
        if (claim_justid)
          builder.AppendBulkString(FormatId(entry.id_));
        else
          AppendEntry(builder, entry);
      }
      builder.AppendArrayHeader(deleted_claim_ids.size());
      for (Id id : deleted_claim_ids) builder.AppendBulkString(FormatId(id));
      co_return Built(builder.View());
    case CommandKind::kXPending:
      if (a.size() == 3) {
        builder.AppendArrayHeader(4);
        builder.AppendInteger(info_group.pending_.size());
        if (info_group.pending_.empty()) {
          builder.AppendNull();
          builder.AppendNull();
        } else {
          builder.AppendBulkString(FormatId(info_group.pending_.front().id_));
          builder.AppendBulkString(FormatId(info_group.pending_.back().id_));
        }
        std::vector<std::pair<std::string, std::uint64_t>> counts;
        for (const Pending& p : info_group.pending_) {
          auto it = std::find_if(
              counts.begin(), counts.end(),
              [&](const auto& x) { return x.first == p.consumer_; });
          if (it == counts.end())
            counts.emplace_back(p.consumer_, 1);
          else
            ++it->second;
        }
        if (counts.empty()) {
          builder.AppendNullArray();
        } else {
          builder.AppendArrayHeader(counts.size());
          for (const auto& [name, count] : counts) {
            builder.AppendArrayHeader(2);
            builder.AppendBulkString(name);
            builder.AppendInteger(count);
          }
        }
      } else {
        builder.AppendArrayHeader(pending_output.size());
        for (const Pending& p : pending_output) {
          builder.AppendArrayHeader(4);
          builder.AppendBulkString(FormatId(p.id_));
          builder.AppendBulkString(p.consumer_);
          builder.AppendInteger(NowMs() - std::min(NowMs(), p.delivery_ms_));
          builder.AppendInteger(p.deliveries_);
        }
      }
      co_return Built(builder.View());
    case CommandKind::kXInfo:
      if (EqualCi(a[1], "stream")) {
        if (xinfo_full) {
          builder.AppendMapHeader(9);
          builder.AppendBulkString("length");
          builder.AppendInteger(info_stream.entries_.size());
          builder.AppendBulkString("radix-tree-keys");
          builder.AppendInteger(info_stream.node_entries_.size());
          builder.AppendBulkString("radix-tree-nodes");
          builder.AppendInteger(info_stream.entries_.empty() ? 1 : 2);
          builder.AppendBulkString("last-generated-id");
          builder.AppendBulkString(FormatId(info_stream.last_id_));
          builder.AppendBulkString("max-deleted-entry-id");
          builder.AppendBulkString(FormatId(info_stream.max_deleted_id_));
          builder.AppendBulkString("entries-added");
          builder.AppendInteger(info_stream.entries_added_);
          builder.AppendBulkString("recorded-first-entry-id");
          builder.AppendBulkString(
              info_stream.entries_.empty()
                  ? "0-0"
                  : FormatId(info_stream.entries_.front().id_));
          builder.AppendBulkString("entries");
          const std::size_t entry_count =
              XInfoLimitedCount(info_stream.entries_.size(), xinfo_count);
          builder.AppendArrayHeader(entry_count);
          for (std::size_t i = 0; i < entry_count; ++i)
            AppendEntry(builder, info_stream.entries_[i]);
          builder.AppendBulkString("groups");
          builder.AppendArrayHeader(info_stream.groups_.size());
          for (const Group& group : info_stream.groups_) {
            builder.AppendMapHeader(7);
            builder.AppendBulkString("name");
            builder.AppendBulkString(group.name_);
            builder.AppendBulkString("last-delivered-id");
            builder.AppendBulkString(FormatId(group.last_id_));
            builder.AppendBulkString("entries-read");
            if (group.entries_read_ < 0)
              builder.AppendNull();
            else
              builder.AppendInteger(group.entries_read_);
            builder.AppendBulkString("lag");
            const std::optional<std::uint64_t> lag =
                GroupLag(info_stream, group);
            if (!lag.has_value())
              builder.AppendNull();
            else
              builder.AppendInteger(*lag);
            builder.AppendBulkString("pel-count");
            builder.AppendInteger(group.pending_.size());
            builder.AppendBulkString("pending");
            const std::size_t pending_count =
                XInfoLimitedCount(group.pending_.size(), xinfo_count);
            builder.AppendArrayHeader(pending_count);
            for (std::size_t i = 0; i < pending_count; ++i) {
              const Pending& pending = group.pending_[i];
              builder.AppendArrayHeader(4);
              builder.AppendBulkString(FormatId(pending.id_));
              builder.AppendBulkString(pending.consumer_);
              builder.AppendInteger(pending.delivery_ms_);
              builder.AppendInteger(pending.deliveries_);
            }
            builder.AppendBulkString("consumers");
            builder.AppendArrayHeader(group.consumers_.size());
            for (const Consumer& consumer : group.consumers_) {
              builder.AppendMapHeader(5);
              builder.AppendBulkString("name");
              builder.AppendBulkString(consumer.name_);
              builder.AppendBulkString("seen-time");
              builder.AppendInteger(consumer.seen_ms_);
              builder.AppendBulkString("active-time");
              builder.AppendInteger(consumer.active_ms_);
              std::vector<const Pending*> consumer_pending;
              for (const Pending& pending : group.pending_) {
                if (pending.consumer_ == consumer.name_)
                  consumer_pending.push_back(&pending);
              }
              builder.AppendBulkString("pel-count");
              builder.AppendInteger(consumer_pending.size());
              builder.AppendBulkString("pending");
              const std::size_t consumer_pending_count =
                  XInfoLimitedCount(consumer_pending.size(), xinfo_count);
              builder.AppendArrayHeader(consumer_pending_count);
              for (std::size_t i = 0; i < consumer_pending_count; ++i) {
                const Pending& pending = *consumer_pending[i];
                builder.AppendArrayHeader(3);
                builder.AppendBulkString(FormatId(pending.id_));
                builder.AppendInteger(pending.delivery_ms_);
                builder.AppendInteger(pending.deliveries_);
              }
            }
          }
          co_return Built(builder.View());
        }
        builder.AppendMapHeader(10);
        builder.AppendBulkString("length");
        builder.AppendInteger(info_stream.entries_.size());
        builder.AppendBulkString("radix-tree-keys");
        builder.AppendInteger(info_stream.node_entries_.size());
        builder.AppendBulkString("radix-tree-nodes");
        builder.AppendInteger(info_stream.entries_.empty() ? 1 : 2);
        builder.AppendBulkString("last-generated-id");
        builder.AppendBulkString(FormatId(info_stream.last_id_));
        builder.AppendBulkString("max-deleted-entry-id");
        builder.AppendBulkString(FormatId(info_stream.max_deleted_id_));
        builder.AppendBulkString("entries-added");
        builder.AppendInteger(info_stream.entries_added_);
        builder.AppendBulkString("recorded-first-entry-id");
        builder.AppendBulkString(
            info_stream.entries_.empty()
                ? "0-0"
                : FormatId(info_stream.entries_.front().id_));
        builder.AppendBulkString("groups");
        builder.AppendInteger(info_stream.groups_.size());
        builder.AppendBulkString("first-entry");
        if (info_stream.entries_.empty())
          builder.AppendNull();
        else
          AppendEntry(builder, info_stream.entries_.front());
        builder.AppendBulkString("last-entry");
        if (info_stream.entries_.empty())
          builder.AppendNull();
        else
          AppendEntry(builder, info_stream.entries_.back());
      } else if (EqualCi(a[1], "groups")) {
        builder.AppendArrayHeader(info_groups.size());
        for (const Group& group : info_groups) {
          builder.AppendMapHeader(6);
          builder.AppendBulkString("name");
          builder.AppendBulkString(group.name_);
          builder.AppendBulkString("consumers");
          builder.AppendInteger(group.consumers_.size());
          builder.AppendBulkString("pending");
          builder.AppendInteger(group.pending_.size());
          builder.AppendBulkString("last-delivered-id");
          builder.AppendBulkString(FormatId(group.last_id_));
          builder.AppendBulkString("entries-read");
          if (group.entries_read_ < 0)
            builder.AppendNull();
          else
            builder.AppendInteger(group.entries_read_);
          builder.AppendBulkString("lag");
          const std::optional<std::uint64_t> lag = GroupLag(info_stream, group);
          if (!lag.has_value())
            builder.AppendNull();
          else
            builder.AppendInteger(*lag);
        }
      } else {
        builder.AppendArrayHeader(info_consumers.size());
        for (const Consumer& consumer : info_consumers) {
          const auto pending = std::count_if(
              info_group.pending_.begin(), info_group.pending_.end(),
              [&](const Pending& p) { return p.consumer_ == consumer.name_; });
          builder.AppendMapHeader(4);
          builder.AppendBulkString("name");
          builder.AppendBulkString(consumer.name_);
          builder.AppendBulkString("pending");
          builder.AppendInteger(pending);
          builder.AppendBulkString("idle");
          builder.AppendInteger(NowMs() - std::min(NowMs(), consumer.seen_ms_));
          builder.AppendBulkString("inactive");
          builder.AppendInteger(consumer.active_ms_
                                    ? NowMs() -
                                          std::min(NowMs(), consumer.active_ms_)
                                    : -1);
        }
      }
      co_return Built(builder.View());
    default:
      break;
  }
  co_return Built(builder.AppendError("ERR unreachable Stream reply"));
}

}  // namespace

std::uint32_t StreamNodeMaxEntries() noexcept {
  return g_stream_node_max_entries.load(std::memory_order_relaxed);
}

absl::Status SetStreamNodeMaxEntries(std::uint64_t value) {
  if (value == 0 || value > std::numeric_limits<std::uint32_t>::max()) {
    return absl::InvalidArgumentError(
        "stream-node-max-entries must be between 1 and 4294967295");
  }
  g_stream_node_max_entries.store(static_cast<std::uint32_t>(value),
                                  std::memory_order_relaxed);
  return absl::OkStatus();
}

void InitStreamCommandStorage(storage::StorageEngine* engine) {
  g_storage = engine;
}

Task<CommandReply> ExecuteStreamCommand(const CommandRequest& request,
                                        ReplyBuilder& reply_builder,
                                        std::uint64_t client_id) {
  return ExecuteImpl(request, nullptr, nullptr, reply_builder, client_id);
}
Task<CommandReply> ExecuteStreamCommandLocked(const CommandRequest& request,
                                              const storage::Digest& digest,
                                              storage::TxShardWrites* tx,
                                              ReplyBuilder& reply_builder) {
  return ExecuteImpl(request, &digest, tx, reply_builder);
}

Task<std::string> ExecuteStreamReadLocked(
    const CommandRequest& request, std::span<const StreamExecKey> keys,
    std::vector<storage::TxShardWrites>& tx_writes) {
  ReplyBuilder builder(request.resp_version_);
  CommandReply reply = co_await ExecuteRead(request, builder, keys, &tx_writes);
  co_return std::string(reply.encoded_);
}

}  // namespace keylane
