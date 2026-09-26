/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "stream_command.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <span>

#include "absl/strings/str_cat.h"
#include "blocking_wait.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"
#include "cluster_gate.h"
#include "lavik/fault_pause.h"
#include "lavik/memory.h"
#include "lavik/resp.h"
#include "lavik/storage/detail/stream_records.h"
#include "lavik/tx/tx_shard.h"

namespace lavik {
namespace {

// The current unreleased v1 layout includes macro-node counts. Earlier
// development layouts are not decoded or reconstructed from live settings.
constexpr std::string_view kMagic = "LXS1";
constexpr std::string_view kGroupStateMagic = "LXG1";
constexpr std::string_view kRestoreGroupSubcommand = "__lavik_restore_group_v2";
constexpr std::string_view kLegacyRestoreGroupSubcommand =
    "__lavik_restore_group_v1";
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
  bool operator==(const Pending&) const = default;
};

struct Consumer {
  std::string name_;
  std::uint64_t seen_ms_ = 0;
  std::uint64_t active_ms_ = 0;
  bool operator==(const Consumer&) const = default;
};

struct Group {
  std::string name_;
  Id last_id_;
  std::int64_t entries_read_ = -1;
  std::vector<Consumer> consumers_;
  std::vector<Pending> pending_;
  std::optional<storage::StreamGroupSummary> summary_;
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
  std::optional<std::uint64_t> total_entries_;
  std::optional<Id> first_entry_id_;
  std::optional<std::uint64_t> total_nodes_, total_groups_;
  std::vector<Group> groups_;
};

std::uint64_t GroupPendingCount(const Group& group) {
  return group.summary_ ? group.summary_->pending_ : group.pending_.size();
}
std::uint64_t GroupConsumerCount(const Group& group) {
  return group.summary_ ? group.summary_->consumers_ : group.consumers_.size();
}
std::uint64_t ConsumerPendingCount(const Group& group, std::string_view name) {
  if (group.summary_) {
    const auto found =
        group.summary_->consumer_pending_.find(std::string(name));
    return found == group.summary_->consumer_pending_.end() ? 0 : found->second;
  }
  return std::count_if(group.pending_.begin(), group.pending_.end(),
                       [&](const Pending& p) { return p.consumer_ == name; });
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
  if (!in.starts_with(kMagic))
    return absl::InternalError("invalid persisted Stream");
  std::size_t at = kMagic.size();
  Stream stream;
  stream.total_entries_ = value->stream_length_;
  if (value->stream_first_id_)
    stream.first_entry_id_ =
        Id{(*value->stream_first_id_)[0], (*value->stream_first_id_)[1]};
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
  std::uint32_t node_count = 0;
  if (!Get32(in, &at, &node_count) || node_count > stream.entries_.size()) {
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
    if (value->stream_inspection_) {
      const auto& summaries = value->stream_inspection_->summaries_;
      auto found = std::find_if(
          summaries.begin(), summaries.end(),
          [&](const auto& summary) { return summary.name_ == group.name_; });
      if (found != summaries.end()) group.summary_ = *found;
    }
    stream.groups_.push_back(std::move(group));
  }
  if (value->stream_inspection_) {
    stream.total_nodes_ = value->stream_inspection_->nodes_;
    stream.total_groups_ = value->stream_inspection_->groups_;
  }
  if (at != in.size())
    return absl::InternalError("trailing persisted Stream bytes");
  return stream;
}

absl::StatusOr<std::string> Encode(const Stream& stream) {
  auto fits32 = [](std::size_t size) { return size <= UINT32_MAX; };
  if (!fits32(stream.entries_.size()) || !fits32(stream.node_entries_.size()) ||
      !fits32(stream.groups_.size()) || !ValidStreamNodes(stream))
    return absl::OutOfRangeError("Stream exceeds storage limits");
  std::uint64_t encoded_bytes = kMagic.size() + 2 * 16 + 8 + 4;
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
  out.append(kMagic);
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
  if (!GetString(in, &at, &group.name_) || !GetId(in, &at, &group.last_id_) ||
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
      return absl::InvalidArgumentError("truncated replicated Stream consumer");
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
    if (!GetId(in, &at, &item.id_) || !GetString(in, &at, &item.consumer_) ||
        !Get64(in, &at, &item.delivery_ms_) ||
        !Get64(in, &at, &item.deliveries_)) {
      return absl::InvalidArgumentError(
          "truncated replicated Stream pending entry");
    }
    if (!group.pending_.empty() && !(group.pending_.back().id_ < item.id_)) {
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

struct GroupDelta {
  Group upserts_;
  std::vector<std::string> removed_consumers_{};
  std::vector<Id> removed_pending_{};
};

absl::StatusOr<std::string> EncodeGroupDelta(const Group& before,
                                             const Group& after) {
  GroupDelta delta;
  delta.upserts_.name_ = after.name_;
  delta.upserts_.last_id_ = after.last_id_;
  delta.upserts_.entries_read_ = after.entries_read_;
  std::map<std::string_view, const Consumer*> consumers;
  for (const auto& consumer : before.consumers_)
    consumers.emplace(consumer.name_, &consumer);
  for (const auto& consumer : after.consumers_) {
    const auto found = consumers.find(consumer.name_);
    if (found == consumers.end() || *found->second != consumer)
      delta.upserts_.consumers_.push_back(consumer);
    if (found != consumers.end()) consumers.erase(found);
  }
  for (const auto& [name, consumer] : consumers)
    delta.removed_consumers_.emplace_back(name);
  std::size_t old = 0, next = 0;
  while (old < before.pending_.size() || next < after.pending_.size()) {
    if (next == after.pending_.size() ||
        (old < before.pending_.size() &&
         before.pending_[old].id_ < after.pending_[next].id_)) {
      delta.removed_pending_.push_back(before.pending_[old++].id_);
    } else if (old == before.pending_.size() ||
               after.pending_[next].id_ < before.pending_[old].id_) {
      delta.upserts_.pending_.push_back(after.pending_[next++]);
    } else {
      if (before.pending_[old] != after.pending_[next])
        delta.upserts_.pending_.push_back(after.pending_[next]);
      ++old;
      ++next;
    }
  }
  auto upserts = EncodeGroupState(delta.upserts_);
  if (!upserts.ok()) return upserts.status();
  std::string out("LXD1");
  PutString(&out, *upserts);
  Put32(&out, delta.removed_consumers_.size());
  for (const auto& name : delta.removed_consumers_) PutString(&out, name);
  Put32(&out, delta.removed_pending_.size());
  for (Id id : delta.removed_pending_) PutId(&out, id);
  if (out.size() > storage::kMaxStringBytes)
    return absl::OutOfRangeError("Stream group delta exceeds limits");
  return out;
}

absl::StatusOr<GroupDelta> DecodeGroupDelta(std::string_view bytes) {
  if (!bytes.starts_with("LXD1"))
    return absl::InvalidArgumentError("invalid Stream group delta");
  std::size_t at = 4;
  std::string payload;
  if (!GetString(bytes, &at, &payload))
    return absl::InvalidArgumentError("truncated Stream group delta");
  auto group = DecodeGroupState(payload);
  if (!group.ok()) return group.status();
  GroupDelta delta{.upserts_ = std::move(*group)};
  std::uint32_t count = 0;
  if (!Get32(bytes, &at, &count) || count > (bytes.size() - at) / 4)
    return absl::InvalidArgumentError("invalid Stream consumer delta");
  for (std::uint32_t i = 0; i < count; ++i) {
    std::string name;
    if (!GetString(bytes, &at, &name))
      return absl::InvalidArgumentError("truncated Stream consumer delta");
    delta.removed_consumers_.push_back(std::move(name));
  }
  if (!Get32(bytes, &at, &count) || count > (bytes.size() - at) / 16)
    return absl::InvalidArgumentError("invalid Stream pending delta");
  for (std::uint32_t i = 0; i < count; ++i) {
    Id id;
    if (!GetId(bytes, &at, &id))
      return absl::InvalidArgumentError("truncated Stream pending delta");
    if (!delta.removed_pending_.empty() && id <= delta.removed_pending_.back())
      return absl::InvalidArgumentError("unordered Stream pending delta");
    delta.removed_pending_.push_back(id);
  }
  if (at != bytes.size())
    return absl::InvalidArgumentError("trailing Stream group delta");
  return delta;
}

void ApplyGroupDelta(Group* group, const GroupDelta& delta) {
  group->last_id_ = delta.upserts_.last_id_;
  group->entries_read_ = delta.upserts_.entries_read_;
  for (const auto& name : delta.removed_consumers_)
    std::erase_if(group->consumers_,
                  [&](const Consumer& c) { return c.name_ == name; });
  for (const auto& consumer : delta.upserts_.consumers_) {
    auto found = std::find_if(
        group->consumers_.begin(), group->consumers_.end(),
        [&](const Consumer& c) { return c.name_ == consumer.name_; });
    if (found == group->consumers_.end())
      group->consumers_.push_back(consumer);
    else
      *found = consumer;
  }
  for (Id id : delta.removed_pending_) {
    auto found = std::lower_bound(
        group->pending_.begin(), group->pending_.end(), id,
        [](const Pending& p, Id wanted) { return p.id_ < wanted; });
    if (found != group->pending_.end() && found->id_ == id)
      group->pending_.erase(found);
  }
  for (const auto& pending : delta.upserts_.pending_) {
    auto found = std::lower_bound(
        group->pending_.begin(), group->pending_.end(), pending.id_,
        [](const Pending& p, Id wanted) { return p.id_ < wanted; });
    if (found != group->pending_.end() && found->id_ == pending.id_)
      *found = pending;
    else
      group->pending_.insert(found, pending);
  }
}

absl::StatusOr<std::vector<std::string>> RestoreGroupArgs(
    std::string_view key, std::string_view group_name, const Group* group,
    const Group* before = nullptr) {
  std::vector<std::string> args{"XGROUP", std::string(kRestoreGroupSubcommand),
                                std::string(key), std::string(group_name),
                                group == nullptr    ? "0"
                                : before == nullptr ? "1"
                                                    : "2"};
  if (group != nullptr) {
    // Replay carries exact timestamps/counters for touched consumers and PEL
    // entries. Sending the whole group would make each delivery/ACK grow with
    // the existing PEL even though the durable update is page-local.
    auto encoded =
        before ? EncodeGroupDelta(*before, *group) : EncodeGroupState(*group);
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
  const auto length = stream.total_entries_.value_or(stream.entries_.size());
  if (stream.entries_added_ == 0) return 0;
  if (stream.entries_added_ >
      static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
    return -1;
  if (length == 0 && id <= stream.last_id_)
    return static_cast<std::int64_t>(stream.entries_added_);
  if (id == stream.last_id_)
    return static_cast<std::int64_t>(stream.entries_added_);
  if (id > stream.last_id_ || length == 0) return -1;
  const Id first = stream.first_entry_id_.value_or(
      stream.entries_.empty() ? Id{} : stream.entries_.front().id_);
  if (stream.max_deleted_id_ == Id{} || stream.max_deleted_id_ < first) {
    if (stream.entries_added_ < length) return -1;
    const std::uint64_t before_first = stream.entries_added_ - length;
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
  try {
    const std::string_view key = request.args_[KeyIndex(request.kind_)];
    std::optional<MemoryReservation> append_admission;
    if (request.kind_ == CommandKind::kXAdd) {
      // Existing-page admission cannot cover a new large entry. Reserve its
      // decoded fields, canonical command and encoded copies before invoking
      // either the compact or grouped callback.
      std::size_t bytes = 0;
      for (const auto& arg : request.args_) {
        if (arg.size() > SIZE_MAX / 8 - 64 ||
            bytes > SIZE_MAX / 8 - 64 - arg.size())
          co_return absl::ResourceExhaustedError("Stream append size overflow");
        bytes += arg.size() + 64;
      }
      append_admission = TryReserveMemory(bytes * 8);
      if (!append_admission)
        co_return absl::ResourceExhaustedError("OOM Stream append input");
    }
    storage::CompactAccessOptions access;
    access.metadata_only_ = request.kind_ == CommandKind::kXLen;
    if (request.kind_ == CommandKind::kXAdd)
      access.stream_append_node_max_entries_ = StreamNodeMaxEntries();
    access.stream_trim_ = request.kind_ == CommandKind::kXTrim;
    access.stream_header_ = request.kind_ == CommandKind::kXSetId;
    if (request.kind_ == CommandKind::kXPending && request.args_.size() == 3)
      access.stream_inspect_ = storage::StreamInspectAccess{
          .kind_ = storage::StreamInspectAccess::Kind::kPending,
          .group_ = request.args_[2]};
    if (request.kind_ == CommandKind::kXInfo) {
      using Kind = storage::StreamInspectAccess::Kind;
      storage::StreamInspectAccess inspect;
      const auto& a = request.args_;
      if (EqualCi(a[1], "groups"))
        inspect.kind_ = Kind::kGroups;
      else if (EqualCi(a[1], "consumers")) {
        inspect.kind_ = Kind::kConsumers;
        inspect.group_ = a[3];
      } else if (a.size() >= 4 && EqualCi(a[3], "full")) {
        inspect.kind_ = Kind::kFull;
        if (a.size() == 6) {
          std::int64_t count = 0;
          if (!ParseInt(a[5], &count))
            co_return absl::InvalidArgumentError(
                "value is not an integer or out of range");
          inspect.count_ = count < 0 ? 10 : count;
        }
      }
      access.stream_inspect_ = std::move(inspect);
    }
    if (request.kind_ == CommandKind::kXRange ||
        request.kind_ == CommandKind::kXRevRange) {
      storage::StreamRangeAccess range;
      range.reverse_ = request.kind_ == CommandKind::kXRevRange;
      std::string_view first = request.args_[range.reverse_ ? 3 : 2];
      std::string_view last = request.args_[range.reverse_ ? 2 : 3];
      range.first_exclusive_ = first.starts_with('(');
      range.last_exclusive_ = last.starts_with('(');
      if (range.first_exclusive_) first.remove_prefix(1);
      if (range.last_exclusive_) last.remove_prefix(1);
      auto low = ParseId(first, false, true);
      auto high = ParseId(last, true, true);
      bool valid_count = request.args_.size() == 4;
      if (request.args_.size() == 6 && EqualCi(request.args_[4], "count")) {
        std::int64_t count = 0;
        valid_count = ParseInt(request.args_[5], &count);
        range.count_ = count <= 0 ? 0 : count;
      }
      // Syntax and exclusive-endpoint errors remain the callback's
      // responsibility.
      if (low.ok() && high.ok() && valid_count) {
        range.first_ = {low->ms_, low->seq_};
        range.last_ = {high->ms_, high->seq_};
        access.stream_range_ = range;
      }
    }
    if (request.kind_ == CommandKind::kXDel) {
      storage::StreamDeleteAccess selected;
      for (std::size_t i = 2; i < request.args_.size(); ++i) {
        auto id = ParseId(request.args_[i]);
        if (!id.ok()) co_return id.status();
        selected.ids_.push_back({id->ms_, id->seq_});
      }
      access.stream_delete_ = std::move(selected);
    }
    if (request.kind_ == CommandKind::kXAck) {
      storage::StreamAckAccess ack{.group_ = request.args_[2]};
      bool valid = true;
      for (std::size_t i = 3; i < request.args_.size(); ++i) {
        auto id = ParseId(request.args_[i]);
        if (!id.ok()) {
          valid = false;
          break;
        }
        ack.ids_.push_back({id->ms_, id->seq_});
      }
      if (valid) access.stream_ack_ = std::move(ack);
    }
    if (request.kind_ == CommandKind::kXGroup && request.replication_origin_ &&
        request.args_.size() == 6 &&
        EqualCi(request.args_[1], kRestoreGroupSubcommand) &&
        request.args_[4] == "2") {
      auto delta = DecodeGroupDelta(request.args_[5]);
      if (!delta.ok()) co_return delta.status();
      storage::StreamGroupAccess group{.group_ = request.args_[3]};
      group.consumers_ = delta->removed_consumers_;
      for (const auto& consumer : delta->upserts_.consumers_)
        group.consumers_.push_back(consumer.name_);
      for (const auto& pending : delta->upserts_.pending_)
        group.pending_ids_.push_back({pending.id_.ms_, pending.id_.seq_});
      for (const auto& id : delta->removed_pending_)
        group.pending_ids_.push_back({id.ms_, id.seq_});
      access.stream_group_ = std::move(group);
    }
    if (request.kind_ == CommandKind::kXGroup &&
        (EqualCi(request.args_[1], "setid") ||
         EqualCi(request.args_[1], "createconsumer") ||
         EqualCi(request.args_[1], "create") ||
         EqualCi(request.args_[1], "destroy") ||
         EqualCi(request.args_[1], "delconsumer"))) {
      storage::StreamGroupAccess group{.group_ = request.args_[3]};
      if (EqualCi(request.args_[1], "createconsumer") ||
          EqualCi(request.args_[1], "delconsumer"))
        group.consumers_.push_back(request.args_[4]);
      if (EqualCi(request.args_[1], "delconsumer"))
        group.remove_consumer_ = request.args_[4];
      group.create_ = EqualCi(request.args_[1], "create");
      group.destroy_ = EqualCi(request.args_[1], "destroy");
      access.stream_group_ = std::move(group);
    }
    if (request.kind_ == CommandKind::kXGroup && request.replication_origin_ &&
        EqualCi(request.args_[1], kRestoreGroupSubcommand) &&
        request.args_[4] == "0") {
      access.stream_group_ = storage::StreamGroupAccess{
          .group_ = request.args_[3], .destroy_ = true};
    }
    if (request.kind_ == CommandKind::kXPending && request.args_.size() > 3) {
      const auto& a = request.args_;
      std::size_t at = 3;
      storage::StreamPendingAccess scan;
      scan.now_ms_ = NowMs();
      if (EqualCi(a[at], "idle")) {
        if (at + 1 >= a.size() || !ParseInt(a[at + 1], &scan.min_idle_ms_))
          co_return absl::InvalidArgumentError(
              "value is not an integer or out of range");
        at += 2;
      }
      if (a.size() - at != 3 && a.size() - at != 4)
        co_return absl::InvalidArgumentError("syntax error");
      std::string_view low = a[at], high = a[at + 1];
      scan.range_.first_exclusive_ = low.starts_with('(');
      scan.range_.last_exclusive_ = high.starts_with('(');
      if (scan.range_.first_exclusive_) low.remove_prefix(1);
      if (scan.range_.last_exclusive_) high.remove_prefix(1);
      auto first = ParseId(low, false, true), last = ParseId(high, true, true);
      if (!first.ok()) co_return first.status();
      if (!last.ok()) co_return last.status();
      std::int64_t count = 0;
      if (!ParseInt(a[at + 2], &count))
        co_return absl::InvalidArgumentError(
            "value is not an integer or out of range");
      scan.range_.first_ = {first->ms_, first->seq_};
      scan.range_.last_ = {last->ms_, last->seq_};
      scan.range_.count_ = count <= 0 ? 0 : count;
      if (a.size() - at == 4 && !a[at + 3].empty()) scan.consumer_ = a[at + 3];
      access.stream_group_ = storage::StreamGroupAccess{
          .group_ = a[2], .pending_scan_ = std::move(scan)};
    }
    if (request.kind_ == CommandKind::kXAutoClaim) {
      const auto& a = request.args_;
      std::string_view start = a[5];
      const bool exclusive = start.starts_with('(');
      if (exclusive) start.remove_prefix(1);
      auto first = ParseId(start, false, !exclusive);
      if (!first.ok()) co_return first.status();
      std::uint64_t count = 100;
      for (std::size_t at = 6; at < a.size();) {
        if (EqualCi(a[at], "count") && at + 1 < a.size()) {
          if (!ParseInt(a[at + 1], &count) || count == 0)
            co_return absl::InvalidArgumentError("COUNT must be > 0");
          at += 2;
        } else if (EqualCi(a[at], "justid"))
          ++at;
        else
          co_return absl::InvalidArgumentError("syntax error");
      }
      // One lookahead row gives the exact continuation cursor when the
      // command exhausts its ten-attempts-per-result scan budget.
      const auto scans =
          count > (UINT64_MAX - 1) / 10 ? UINT64_MAX : count * 10 + 1;
      access.stream_group_ = storage::StreamGroupAccess{
          .group_ = a[2],
          .consumers_ = {a[3]},
          .pending_scan_ = storage::StreamPendingAccess{
              .range_ = {.first_ = {first->ms_, first->seq_},
                         .count_ = scans,
                         .first_exclusive_ = exclusive},
              .load_entries_ = true}};
    }
    if (request.kind_ == CommandKind::kXClaim) {
      storage::StreamGroupAccess group{.group_ = request.args_[2],
                                       .consumers_ = {request.args_[3]}};
      bool valid = true;
      for (std::size_t i = 5; i < request.args_.size(); ++i) {
        const auto& arg = request.args_[i];
        if (EqualCi(arg, "idle") || EqualCi(arg, "time") ||
            EqualCi(arg, "retrycount") || EqualCi(arg, "force") ||
            EqualCi(arg, "justid") || EqualCi(arg, "lastid"))
          break;
        auto id = ParseId(arg);
        if (!id.ok()) {
          valid = false;
          break;
        }
        group.pending_ids_.push_back({id->ms_, id->seq_});
        group.entry_ids_.push_back({id->ms_, id->seq_});
      }
      if (valid) access.stream_group_ = std::move(group);
    }
    if (!digest) {
      const storage::MutationPrecondition mutation_precondition =
          ClusterMutationPrecondition(request);
      co_return co_await g_storage->ExecuteCompact(
          request.db_id_, key, storage::ValueType::kStream, read_only, callback,
          0, replication, &mutation_precondition, access);
    }
    co_return co_await g_storage->ExecuteCompactLocked(
        request.db_id_, key, *digest, storage::ValueType::kStream, read_only,
        callback, tx, 0, replication, nullptr, access);
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM Stream command workspace");
  }
}

void AppendEntry(ReplyBuilder& builder, const Entry& entry) {
  builder.AppendArrayHeader(2);
  builder.AppendBulkString(FormatId(entry.id_));
  builder.AppendArrayHeader(entry.fields_.size());
  for (const std::string& field : entry.fields_)
    builder.AppendBulkString(field);
}

struct StreamRangeReplyState {
  RetainedMemoryCharge state_charge_;
  // Replies cross from a key owner to the connection/EXEC worker. Pending
  // reservations are worker-affine; retained charges may follow that ownership.
  RetainedMemoryCharge selection_charge_;
  RetainedMemoryCharge entry_charge_;
  storage::CollectionPageReader reader_;
  std::optional<storage::CollectionPage> page_;
  std::vector<Entry> compact_;
  storage::StreamRangeAccess range_;
  RespVersion version_;
  std::string pending_;
  std::size_t index_ = 0, offset_ = 0;
  std::uint64_t remaining_ = 0;
  bool done_ = false;
  struct Selection {
    Id id_;
    bool missing_;
  };
  std::vector<Selection> selection_;
  std::size_t selected_at_ = 0;

  bool Matches(Id id) const {
    const auto value = std::array{id.ms_, id.seq_};
    return (range_.first_exclusive_ ? value > range_.first_
                                    : value >= range_.first_) &&
           (range_.last_exclusive_ ? value < range_.last_
                                   : value <= range_.last_);
  }
  Task<absl::StatusOr<std::string>> Next() {
    try {
      for (;;) {
        if (offset_ < pending_.size()) {
          auto chunk = pending_.substr(offset_, 64 * 1024);
          offset_ += chunk.size();
          co_return chunk;
        }
        pending_ = std::string{};
        offset_ = 0;
        entry_charge_.Reset();
        if (remaining_ == 0 || done_) {
          page_.reset();
          reader_ = {};
          co_return std::string{};
        }
        if (!selection_.empty() && selection_[selected_at_].missing_) {
          ReplyBuilder builder(version_);
          builder.AppendArrayHeader(2);
          builder.AppendBulkString(FormatId(selection_[selected_at_++].id_));
          builder.AppendNullArray();
          pending_ = std::string(builder.View());
          --remaining_;
          continue;
        }
        Entry entry;
        if (!reader_) {
          bool found = false;
          while (index_ < compact_.size()) {
            auto& candidate = compact_[index_++];
            if (!Matches(candidate.id_) ||
                (!selection_.empty() &&
                 candidate.id_ != selection_[selected_at_].id_))
              continue;
            entry = std::move(candidate);
            found = true;
            break;
          }
          if (!found)
            co_return absl::DataLossError("Stream reply count mismatch");
        } else {
          if (!page_ || index_ == page_->elements_.size()) {
            const bool end = page_ && page_->done_;
            page_.reset();
            if (end) co_return absl::DataLossError("Stream reply ended early");
            auto page = co_await reader_();
            if (!page.ok()) co_return page.status();
            page_ = std::move(*page);
            index_ = 0;
            continue;
          }
          const auto& row = page_->elements_[index_++];
          auto key = storage::StreamRecordKey(row);
          auto payload = storage::StreamRecordPayload(row);
          if (!key.ok()) co_return key.status();
          if (!payload.ok()) co_return payload.status();
          if ((*key)[0] != '\1') continue;
          std::size_t at = 0;
          std::uint32_t fields = 0;
          if (!GetId(*payload, &at, &entry.id_) ||
              !Get32(*payload, &at, &fields) || fields % 2 ||
              fields > (payload->size() - at) / 4)
            co_return absl::DataLossError("invalid Stream reply entry");
          if (!Matches(entry.id_) ||
              (!selection_.empty() &&
               entry.id_ != selection_[selected_at_].id_))
            continue;
          if (payload->size() > (SIZE_MAX - 4096) / 12)
            co_return absl::ResourceExhaustedError(
                "Stream reply entry size overflow");
          auto admission = TryReserveMemory(payload->size() * 12 + 4096);
          if (!admission)
            co_return absl::ResourceExhaustedError("OOM Stream reply entry");
          entry_charge_.Adopt(&*admission, admission->bytes());
          entry.fields_.reserve(fields);
          for (std::uint32_t i = 0; i < fields; ++i) {
            std::string field;
            if (!GetString(*payload, &at, &field))
              co_return absl::DataLossError("truncated Stream reply field");
            entry.fields_.push_back(std::move(field));
          }
          if (at != payload->size())
            co_return absl::DataLossError("trailing Stream reply entry");
        }
        ReplyBuilder builder(version_);
        AppendEntry(builder, entry);
        if (!selection_.empty()) ++selected_at_;
        pending_ = std::string(builder.View());
        --remaining_;
      }
    } catch (const std::bad_alloc&) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM Stream reply");
    }
  }
};

// The reader pins the command-position graph across EXEC, replacement,
// deletion and defrag. Storage counts matching records from directory ranks
// and reads only boundary pages before the RESP array header is emitted.
Task<absl::StatusOr<std::shared_ptr<StreamRangeReplyState>>>
PrepareStreamRangeReply(std::uint8_t db, std::string_view key,
                        const storage::Digest* locked_digest,
                        const storage::StreamRangeAccess& range,
                        RespVersion version) {
  const auto digest =
      locked_digest ? *locked_digest : storage::ComputeDigest(key);
  tx::TxShard::Guard guard;
  if (!locked_digest)
    guard = co_await tx::CurrentTxShard().AcquireKey(
        db, tx::FingerprintOf(digest), tx::LockMode::kShared);
  auto source =
      co_await g_storage->ReadValueForTransferLocked(db, key, digest, range);
  if (!source.ok()) co_return source.status();
  if (source->metadata_.value_type_ != storage::ValueType::kStream)
    co_return absl::InvalidArgumentError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  const auto compact_bytes = source->metadata_.encoded_.size();
  if (compact_bytes > (SIZE_MAX - sizeof(StreamRangeReplyState) - 4096) / 12)
    co_return absl::ResourceExhaustedError(
        "Stream reply snapshot size overflow");
  const auto bytes = sizeof(StreamRangeReplyState) + compact_bytes * 12 + 4096;
  auto reservation = TryReserveMemory(bytes);
  if (!reservation)
    co_return absl::ResourceExhaustedError("OOM Stream reply snapshot");
  auto state = std::make_shared<StreamRangeReplyState>();
  state->state_charge_.Adopt(&*reservation, bytes);
  state->range_ = range;
  state->version_ = version;
  if (!source->reader_) {
    auto decoded = Decode(storage::CompactValueView{
        .encoded_ = source->metadata_.encoded_,
        .logical_size_ = source->metadata_.logical_size_});
    if (!decoded.ok()) co_return decoded.status();
    state->compact_ = std::move(decoded->entries_);
    if (range.reverse_)
      std::reverse(state->compact_.begin(), state->compact_.end());
    for (const auto& entry : state->compact_)
      if (state->Matches(entry.id_) && state->remaining_ < range.count_)
        ++state->remaining_;
  } else {
    state->reader_ = std::move(source->reader_);
    state->remaining_ = source->metadata_.logical_size_;
  }
  co_return state;
}

Task<CommandReply> ExecuteStreamRangeReply(const CommandRequest& request,
                                           const storage::Digest* digest,
                                           ReplyBuilder& builder) {
  try {
    const auto& a = request.args_;
    storage::StreamRangeAccess range;
    range.reverse_ = request.kind_ == CommandKind::kXRevRange;
    std::string_view low = a[range.reverse_ ? 3 : 2],
                     high = a[range.reverse_ ? 2 : 3];
    range.first_exclusive_ = low.starts_with('(');
    range.last_exclusive_ = high.starts_with('(');
    if (range.first_exclusive_) low.remove_prefix(1);
    if (range.last_exclusive_) high.remove_prefix(1);
    if ((range.first_exclusive_ && (low == "-" || low == "+")) ||
        (range.last_exclusive_ && (high == "-" || high == "+")))
      co_return Built(builder.AppendError(
          "ERR Invalid stream ID specified as stream command argument"));
    auto first = ParseId(low, false, true), last = ParseId(high, true, true);
    if (!first.ok()) co_return Built(StorageError(builder, first.status()));
    if (!last.ok()) co_return Built(StorageError(builder, last.status()));
    range.first_ = {first->ms_, first->seq_};
    range.last_ = {last->ms_, last->seq_};
    if ((range.first_exclusive_ &&
         range.first_ ==
             std::array<std::uint64_t, 2>{UINT64_MAX, UINT64_MAX}) ||
        (range.last_exclusive_ &&
         range.last_ == std::array<std::uint64_t, 2>{}))
      co_return Built(builder.AppendError(
          "ERR Invalid stream ID specified as stream command argument"));
    if (a.size() != 4) {
      std::int64_t count = 0;
      if (a.size() != 6 || !EqualCi(a[4], "count") || !ParseInt(a[5], &count))
        co_return Built(builder.AppendError("ERR syntax error"));
      if (count <= 0) co_return Built(builder.AppendNullArray());
      range.count_ = count;
    }
    auto state = co_await PrepareStreamRangeReply(request.db_id_, a[1], digest,
                                                  range, request.resp_version_);
    if (!state.ok()) {
      if (state.status().code() == absl::StatusCode::kNotFound)
        co_return Built(builder.AppendArrayHeader(0));
      co_return Built(StorageError(builder, state.status()));
    }
    auto reply = Built(builder.AppendArrayHeader((*state)->remaining_));
    if ((*state)->remaining_ != 0)
      reply.chunks_ = std::make_unique<ReplyChunkSource>(
          [state = std::move(*state)] { return state->Next(); });
    co_return reply;
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return Built(builder.AppendError("OOM Stream reply"));
  }
}

struct ReadOneResult {
  struct Item {
    Id id_;
    std::optional<Entry> entry_;
  };
  std::vector<Item> entries_;
  Id cursor_;
  std::shared_ptr<StreamRangeReplyState> stream_;
};

Task<absl::StatusOr<ReadOneResult>> ReadOneLocal(
    std::uint8_t db_id, std::string key, Id cursor, bool dollar,
    bool group_read, std::string group_name, std::string consumer_name,
    bool new_messages, bool noack, std::uint64_t count,
    const storage::Digest* locked_digest = nullptr,
    storage::TxShardWrites* tx = nullptr,
    const CommandRequest* request = nullptr) {
  if (!group_read && !dollar) {
    auto state = co_await PrepareStreamRangeReply(
        db_id, key, locked_digest,
        storage::StreamRangeAccess{.first_ = {cursor.ms_, cursor.seq_},
                                   .count_ = count,
                                   .first_exclusive_ = true},
        request ? request->resp_version_ : RespVersion::k2);
    if (!state.ok()) {
      if (absl::IsNotFound(state.status()))
        co_return ReadOneResult{.cursor_ = cursor};
      co_return state.status();
    }
    co_return ReadOneResult{.cursor_ = cursor, .stream_ = std::move(*state)};
  }
  tx::TxShard::Guard delivery_guard;
  storage::Digest delivery_digest;
  if (group_read && !locked_digest) {
    delivery_digest = storage::ComputeDigest(key);
    delivery_guard = co_await tx::CurrentTxShard().AcquireKey(
        db_id, tx::FingerprintOf(delivery_digest), tx::LockMode::kExclusive);
    locked_digest = &delivery_digest;
  }
  const storage::MutationPrecondition mutation_precondition =
      request != nullptr ? ClusterMutationPrecondition(*request)
                         : storage::MutationPrecondition{};
  const storage::MutationPrecondition* mutation_precondition_ptr =
      request != nullptr && tx == nullptr ? &mutation_precondition : nullptr;
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
    const Group before_group = *group;
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
    auto canonical = RestoreGroupArgs(key, group_name, group, &before_group);
    if (!canonical.ok()) return canonical.status();
    if (replication.has_value()) replication->args_ = *canonical;
    captured_group_args = std::move(*canonical);
    return Changed(std::move(stream));
  };
  storage::CompactAccessOptions access;
  if (!group_read)
    access.stream_range_ =
        storage::StreamRangeAccess{.first_ = {cursor.ms_, cursor.seq_},
                                   .count_ = dollar ? 0 : count,
                                   .first_exclusive_ = true};
  if (group_read && new_messages)
    access.stream_group_ =
        storage::StreamGroupAccess{.group_ = group_name,
                                   .consumers_ = {consumer_name},
                                   .read_new_count_ = count,
                                   .entry_ids_only_ = true};
  if (group_read && !new_messages)
    access.stream_group_ = storage::StreamGroupAccess{
        .group_ = group_name,
        .consumers_ = {consumer_name},
        .pending_scan_ =
            storage::StreamPendingAccess{
                .range_ = {.first_ = {cursor.ms_, cursor.seq_},
                           .count_ = count,
                           .first_exclusive_ = true},
                .consumer_ = consumer_name,
                .load_entries_ = true},
        .entry_ids_only_ = true};
  absl::Status status;
  if (locked_digest == nullptr) {
    status = co_await g_storage->ExecuteCompact(
        db_id, key, storage::ValueType::kStream, !group_read, callback, 0,
        replication ? &*replication : nullptr, mutation_precondition_ptr,
        access);
  } else {
    status = co_await g_storage->ExecuteCompactLocked(
        db_id, key, *locked_digest, storage::ValueType::kStream, !group_read,
        callback, group_read ? tx : nullptr, 0,
        replication ? &*replication : nullptr, mutation_precondition_ptr,
        access);
  }
  if (!status.ok()) co_return status;
  if (request != nullptr && !captured_group_args.empty()) {
    CaptureReplicationCommand(*request, db_id, std::move(captured_group_args));
  }
  if (group_read && !result.entries_.empty()) {
    if (result.entries_.size() > (SIZE_MAX - 256) / 128)
      co_return absl::ResourceExhaustedError("Stream reply selection overflow");
    auto selection_charge =
        TryReserveMemory(result.entries_.size() * 128 + 256);
    if (!selection_charge)
      co_return absl::ResourceExhaustedError("OOM Stream reply selection");
    const auto first = result.entries_.front().id_,
               last = result.entries_.back().id_;
    storage::StreamRangeAccess range{.first_ = {first.ms_, first.seq_},
                                     .last_ = {last.ms_, last.seq_},
                                     .count_ = result.entries_.size()};
    if (!new_messages) {
      for (const auto& item : result.entries_)
        if (item.entry_)
          range.selected_ids_.push_back({item.id_.ms_, item.id_.seq_});
      range.count_ = range.selected_ids_.size();
    }
    auto state = co_await PrepareStreamRangeReply(
        db_id, key, locked_digest, range,
        request ? request->resp_version_ : RespVersion::k2);
    if (!state.ok()) co_return state.status();
    result.stream_ = std::move(*state);
    result.stream_->selection_charge_.Adopt(&*selection_charge,
                                            selection_charge->bytes());
    if (!new_messages) {
      result.stream_->remaining_ = result.entries_.size();
      for (const auto& item : result.entries_)
        result.stream_->selection_.push_back(
            {item.id_, !item.entry_.has_value()});
    }
    result.entries_ = {};
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
    std::vector<std::pair<std::string, ReadOneResult>> found;
    std::optional<CommandReply> attempt_reply;
    bool db_gate_closed = false;
    auto attempt = [&]() -> Task<absl::Status> {
      const bool owns_attempt_gate = locked_keys.empty();
      if (owns_attempt_gate && !TryBeginCommandDbOperation(request.db_id_)) {
        db_gate_closed = true;
        co_return absl::OkStatus();
      }
      AttemptDbGuard db_guard(request.db_id_, owns_attempt_gate);
      if (group_read && owns_attempt_gate &&
          !CommandWriteAdmissionIsCurrent(request)) {
        attempt_reply = Built(builder.AppendError(
            "TRYAGAIN replication role changed; retry command"));
        co_return absl::OkStatus();
      }
      if (const char* error = CommandServingGenerationError(request);
          error != nullptr) [[unlikely]] {
        attempt_reply = Built(builder.AppendError(error));
        co_return absl::OkStatus();
      }
      // A top-level XREADGROUP can remain dormant indefinitely but mutates
      // consumer/PENDING state on each concrete read attempt. Its assignment
      // guard covers only those owner hops, not waiter registration or sleep
      // below. EXEC/Lua locked execution already retains its enclosing
      // transaction/script authority window and must not re-admit one child
      // against a newer projection midway through that atomic operation.
      cluster::AuthorityInFlightGuards attempt_authority;
      if (group_read && owns_attempt_gate) {
        attempt_reply = RegisterClusterBlockingWriteAttempt(
            attempt_request, builder, &attempt_authority);
        // Re-admission may replace the proof. The outer finalizer must observe
        // this attempt's mutation/failed-check markers, including partial hops.
        request.cluster_authority_admission_ =
            attempt_request.cluster_authority_admission_;
        if (attempt_reply.has_value()) co_return absl::OkStatus();
        LAVIK_FAULT_INJECT({
          auto paused = co_await fault_injection::PauseWhileFileExists(
              "LAVIK_STREAM_AFTER_AUTHORITY_HOLD_FILE");
          if (!paused.ok()) co_return paused;
        });
      }
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
            co_return absl::InternalError("Stream key is missing");
          }
        }
        const unsigned owner = locked_key == nullptr
                                   ? g_storage->OwnerForKey(key)
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
            locked_key == nullptr || tx_writes == nullptr
                ? nullptr
                : &(*tx_writes)[owner];
        if (owner == bycorf::ThisWorker().id_) {
          one = co_await ReadOneLocal(
              request.db_id_, key, cursors[k], initialize, group_read,
              group_name, consumer_name, new_messages[k], noack, count,
              locked_digest ? &*locked_digest : nullptr, local_tx,
              &attempt_request);
        } else {
          one = co_await bycorf::SubmitTaskTo(
              owner,
              [db = request.db_id_, key = std::move(key), cursor = cursors[k],
               initialize, group_read, group_name, consumer_name,
               is_new = new_messages[k], noack, count, locked_digest, local_tx,
               request_ptr = &attempt_request]() mutable
                  -> Task<absl::StatusOr<ReadOneResult>> {
                co_return co_await ReadOneLocal(
                    db, std::move(key), cursor, initialize, group_read,
                    std::move(group_name), std::move(consumer_name), is_new,
                    noack, count, locked_digest ? &*locked_digest : nullptr,
                    local_tx, request_ptr);
              });
        }
        if (!one.ok()) co_return one.status();
        LAVIK_FAULT_INJECT(if (group_read && owns_attempt_gate && k == 0) {
          auto paused = co_await fault_injection::PauseWhileFileExists(
              "LAVIK_STREAM_AFTER_FIRST_KEY_HOLD_FILE");
          if (!paused.ok()) co_return paused;
        });
        cursors[k] = one->cursor_;
        if (!one->entries_.empty() ||
            (one->stream_ && one->stream_->remaining_) ||
            (group_read && !new_messages[k])) {
          found.emplace_back(a[first_key + k], std::move(*one));
        }
      }
      // DB and authority guards end here, before publisher release and sleep.
      co_return absl::OkStatus();
    };
    absl::Status attempted;
    if (group_read && locked_keys.empty()) {
      attempted =
          co_await RunReplicationAdmittedAttempt(attempt_request, attempt);
    } else {
      attempted = co_await attempt();
    }
    if (!attempted.ok()) co_return Built(StorageError(builder, attempted));
    if (attempt_reply.has_value()) co_return std::move(*attempt_reply);
    if (db_gate_closed) {
      // The attempt wrapper has released its publisher reservation. Keeping
      // it across a DB-gate wait can block FULL on an UNSTARTED partition.
      // No keys were examined, so retain cursors and the original deadline.
      if (deadline && std::chrono::steady_clock::now() >= *deadline) {
        co_return Built(builder.AppendNullArray());
      }
      cascade_completion.Finish();
      absl::Status slept = co_await bycorf::SleepFor(
          *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return Built(StorageError(builder, slept));
      continue;
    }
    initialized_dollars = true;
    if (!found.empty()) {
      if (builder.version() == RespVersion::k3)
        builder.AppendMapHeader(found.size());
      else
        builder.AppendArrayHeader(found.size());
      {
        struct Replies {
          std::vector<std::pair<std::string, ReadOneResult>> found_;
          RespVersion version_;
          std::size_t index_ = 0, entry_ = 0;
          bool header_ = true;
          Task<absl::StatusOr<std::string>> Next() {
            while (index_ < found_.size()) {
              auto& [key, result] = found_[index_];
              if (header_) {
                ReplyBuilder builder(version_);
                if (version_ == RespVersion::k2) builder.AppendArrayHeader(2);
                builder.AppendBulkString(key);
                builder.AppendArrayHeader(result.stream_
                                              ? result.stream_->remaining_
                                              : result.entries_.size());
                header_ = false;
                co_return std::string(builder.View());
              }
              if (result.stream_) {
                auto chunk = co_await result.stream_->Next();
                if (!chunk.ok()) co_return chunk.status();
                if (!chunk->empty()) co_return std::move(*chunk);
                result.stream_.reset();
              } else if (entry_ < result.entries_.size()) {
                ReplyBuilder builder(version_);
                const auto& item = result.entries_[entry_++];
                if (item.entry_)
                  AppendEntry(builder, *item.entry_);
                else {
                  builder.AppendArrayHeader(2);
                  builder.AppendBulkString(FormatId(item.id_));
                  builder.AppendNullArray();
                }
                co_return std::string(builder.View());
              }
              result.entries_ = {};
              entry_ = 0;
              ++index_;
              header_ = true;
            }
            co_return std::string{};
          }
        };
        auto state = std::make_shared<Replies>();
        state->found_ = std::move(found);
        state->version_ = builder.version();
        auto reply = Built(builder.View());
        reply.chunks_ = std::make_unique<ReplyChunkSource>(
            [state] { return state->Next(); });
        co_return reply;
      }
    }
    if (has_history) co_return Built(builder.AppendNullArray());
    if (!block) co_return Built(builder.AppendNullArray());
    if (block_ms != 0 && std::chrono::steady_clock::now() - started >=
                             std::chrono::milliseconds(block_ms))
      co_return Built(builder.AppendNullArray());
    cascade_completion.Finish();
    if (!wait_handle) {
      LAVIK_FAULT_INJECT({
        auto paused = co_await fault_injection::PauseWhileFileExists(
            "LAVIK_STREAM_BEFORE_WAIT_REGISTRATION_HOLD_FILE");
        if (!paused.ok()) co_return Built(StorageError(builder, paused));
      });
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
      co_return Built(
          builder.AppendError("UNBLOCKED client unblocked via CLIENT UNBLOCK"));
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
  if (request.kind_ == CommandKind::kXRange ||
      request.kind_ == CommandKind::kXRevRange)
    co_return co_await ExecuteStreamRangeReply(request, digest, builder);
  if (request.kind_ == CommandKind::kXGroup && a.size() >= 2 &&
      (EqualCi(a[1], kRestoreGroupSubcommand) ||
       EqualCi(a[1], kLegacyRestoreGroupSubcommand))) {
    if (!request.replication_origin_ || (a.size() != 5 && a.size() != 6) ||
        (a[4] != "0" && a[4] != "1" && a[4] != "2") ||
        (a[4] == "2" && EqualCi(a[1], kLegacyRestoreGroupSubcommand)) ||
        (a[4] == "0" && a.size() != 5) || (a[4] != "0" && a.size() != 6)) {
      co_return Built(
          builder.AppendError("ERR invalid replicated Stream group state"));
    }
    std::optional<GroupDelta> delta;
    if (a[4] == "2") {
      auto decoded = DecodeGroupDelta(a[5]);
      if (!decoded.ok() || decoded->upserts_.name_ != a[3])
        co_return Built(
            builder.AppendError("ERR invalid replicated Stream group delta"));
      delta = std::move(*decoded);
    }
    std::optional<Group> restored;
    if (a[4] == "1") {
      auto decoded = DecodeGroupState(a[5]);
      if (!decoded.ok() || decoded->name_ != a[3]) {
        co_return Built(
            builder.AppendError("ERR invalid replicated Stream group payload"));
      }
      restored = std::move(*decoded);
    }
    auto restore = [&](std::optional<storage::CompactValueView> value)
        -> absl::StatusOr<storage::CompactValueUpdate> {
      auto decoded = Decode(value);
      if (!decoded.ok()) return decoded.status();
      if (!value && !restored.has_value()) {
        return absl::NotFoundError("NOGROUP No such key or consumer group");
      }
      Stream stream = std::move(*decoded);
      auto found =
          std::find_if(stream.groups_.begin(), stream.groups_.end(),
                       [&](const Group& group) { return group.name_ == a[3]; });
      if (delta) {
        if (found == stream.groups_.end())
          return absl::NotFoundError("NOGROUP No such consumer group");
        ApplyGroupDelta(&*found, *delta);
      } else if (restored.has_value()) {
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
  const bool group_state_write = request.kind_ == CommandKind::kXGroup ||
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
      request.kind_ == CommandKind::kXSetId || group_state_write;
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

  tx::TxShard::Guard inspection_guard;
  storage::Digest inspection_digest;
  std::shared_ptr<StreamRangeReplyState> info_entries;
  if (xinfo_full) {
    if (!digest) {
      inspection_digest = storage::ComputeDigest(a[2]);
      inspection_guard = co_await tx::CurrentTxShard().AcquireKey(
          request.db_id_, tx::FingerprintOf(inspection_digest),
          tx::LockMode::kShared);
      digest = &inspection_digest;
    }
    auto prepared = co_await PrepareStreamRangeReply(
        request.db_id_, a[2], digest,
        storage::StreamRangeAccess{.count_ = xinfo_count == 0 ? UINT64_MAX
                                                              : xinfo_count},
        request.resp_version_);
    if (!prepared.ok())
      co_return Built(StorageError(builder, prepared.status()));
    info_entries = std::move(*prepared);
  }

  auto callback = [&](std::optional<storage::CompactValueView> value)
      -> absl::StatusOr<storage::CompactValueUpdate> {
    if (request.kind_ == CommandKind::kXLen) {
      integer = value ? value->logical_size_ : 0;
      return NoChange();
    }
    auto decoded = Decode(value);
    if (!decoded.ok()) return decoded.status();
    Stream stream = std::move(*decoded);
    const std::string_view group_name =
        group_state_write
            ? (request.kind_ == CommandKind::kXGroup ? a[3] : a[2])
            : std::string_view{};
    std::optional<Group> before_group;
    if (group_state_write) {
      if (const auto* group = FindGroup(&stream, group_name))
        before_group = *group;
    }
    auto publish_stream =
        [&](Stream after) -> absl::StatusOr<storage::CompactValueUpdate> {
      if (request.kind_ == CommandKind::kXAck ||
          (request.kind_ == CommandKind::kXGroup &&
           EqualCi(a[1], "delconsumer"))) {
        // ACK and consumer removal are deterministic and has no clock-dependent
        // after-state. Replay the ID removals directly so both peers can use
        // the sparse PEL path.
        captured_group_args = request.args_;
        if (replication) replication->args_ = captured_group_args;
      } else if (request.kind_ == CommandKind::kXGroup &&
                 EqualCi(a[1], "create")) {
        const auto* group = FindGroup(&after, group_name);
        if (!group) return absl::InternalError("missing created Stream group");
        captured_group_args = {"XGROUP",
                               "CREATE",
                               a[2],
                               a[3],
                               FormatId(group->last_id_),
                               "MKSTREAM",
                               "ENTRIESREAD",
                               std::to_string(group->entries_read_)};
        if (replication) replication->args_ = captured_group_args;
      } else if (group_state_write) {
        auto canonical =
            RestoreGroupArgs(a[KeyIndex(request.kind_)], group_name,
                             FindGroup(&after, group_name),
                             before_group ? &*before_group : nullptr);
        if (!canonical.ok()) return canonical.status();
        if (replication) replication->args_ = *canonical;
        captured_group_args = std::move(*canonical);
      }
      return Changed(std::move(after));
    };
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
        if (value && value->stream_incremental_trim_) {
          auto update = Changed(std::move(stream));
          if (!update.ok()) return update.status();
          if (trim != Trim::kNone) {
            update->stream_trim_ = storage::StreamTrimRequest{
                .max_length_ = trim == Trim::kMaxLen ? std::optional(maxlen)
                                                     : std::nullopt,
                .min_id_ = {minid.ms_, minid.seq_},
                .approximate_ = trim_approximate,
                .limit_ = effective_limit};
            update->stream_trim_complete_ =
                [&, trim, trim_approximate, trim_specifier_arg,
                 trim_threshold_arg](const storage::StreamTrimResult& result) {
                  if (trim_approximate) {
                    captured_xadd[*trim_specifier_arg] = "=";
                    captured_xadd[*trim_threshold_arg] =
                        trim == Trim::kMaxLen
                            ? std::to_string(result.length_)
                            : FormatId(result.first_id_
                                           ? Id{(*result.first_id_)[0],
                                                (*result.first_id_)[1]}
                                           : Id{UINT64_MAX, UINT64_MAX});
                    const auto limit_at = *trim_threshold_arg + 1;
                    if (limit_at < captured_xadd.size() &&
                        EqualCi(captured_xadd[limit_at], "limit"))
                      captured_xadd.erase(captured_xadd.begin() + limit_at,
                                          captured_xadd.begin() + limit_at + 2);
                  }
                  if (replication) replication->args_ = captured_xadd;
                };
          }
          if (replication) replication->args_ = captured_xadd;
          return update;
        }
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
          const auto limit_at = *trim_threshold_arg + 1;
          if (limit_at < captured_xadd.size() &&
              EqualCi(captured_xadd[limit_at], "limit"))
            captured_xadd.erase(captured_xadd.begin() + limit_at,
                                captured_xadd.begin() + limit_at + 2);
        }
        if (replication.has_value()) replication->args_ = captured_xadd;
        return publish_stream(std::move(stream));
      }
      case CommandKind::kXDel: {
        std::vector<Id> ids;
        for (std::size_t i = 2; i < a.size(); ++i) {
          auto id = ParseId(a[i]);
          if (!id.ok()) return id.status();
          ids.push_back(*id);
        }
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        std::size_t read = 0, write = 0, node_write = 0;
        // One pass preserves logical node membership while compacting
        // survivors. Grouped callbacks carry only the requested IDs; storage
        // maintains the real node boundaries outside this partial view.
        for (const auto node_count : stream.node_entries_) {
          std::uint32_t live = 0;
          for (std::uint32_t i = 0; i < node_count; ++i, ++read) {
            auto& entry = stream.entries_[read];
            if (std::binary_search(ids.begin(), ids.end(), entry.id_)) {
              ++integer;
              stream.max_deleted_id_ =
                  std::max(stream.max_deleted_id_, entry.id_);
            } else {
              if (write != read) stream.entries_[write] = std::move(entry);
              ++write;
              ++live;
            }
          }
          if (live) stream.node_entries_[node_write++] = live;
        }
        stream.entries_.resize(write);
        stream.node_entries_.resize(node_write);
        return integer
                   ? publish_stream(std::move(stream))
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
        if (value && value->stream_incremental_trim_) {
          auto update = Changed(std::move(stream));
          if (!update.ok()) return update.status();
          update->stream_trim_ = storage::StreamTrimRequest{
              .max_length_ = maxlen_mode ? std::optional(maxlen) : std::nullopt,
              .min_id_ = {minid.ms_, minid.seq_},
              .approximate_ = approximate,
              .limit_ = limit};
          update->stream_trim_complete_ =
              [&, approximate, maxlen_mode, trim_specifier_arg,
               trim_threshold_arg](const storage::StreamTrimResult& result) {
                integer = result.removed_;
                if (approximate && result.removed_ != 0) {
                  captured_xtrim = request.args_;
                  captured_xtrim[*trim_specifier_arg] = "=";
                  captured_xtrim[trim_threshold_arg] =
                      maxlen_mode ? std::to_string(result.length_)
                                  : FormatId(result.first_id_
                                                 ? Id{(*result.first_id_)[0],
                                                      (*result.first_id_)[1]}
                                                 : Id{UINT64_MAX, UINT64_MAX});
                  captured_xtrim.resize(trim_threshold_arg + 1);
                  if (replication) replication->args_ = captured_xtrim;
                  MarkReplicationCommandHandled(request);
                }
              };
          return update;
        }
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
          captured_xtrim.resize(trim_threshold_arg + 1);
          if (replication.has_value()) replication->args_ = captured_xtrim;
          MarkReplicationCommandHandled(request);
        }
        return integer
                   ? publish_stream(std::move(stream))
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
            *entries_added <
                stream.total_entries_.value_or(stream.entries_.size()))
          return absl::InvalidArgumentError(
              "The entries_added specified in XSETID is smaller than the "
              "target stream length");
        stream.last_id_ = *id;
        if (entries_added.has_value()) stream.entries_added_ = *entries_added;
        if (max_deleted_id.has_value() && *max_deleted_id != Id{})
          stream.max_deleted_id_ = *max_deleted_id;
        simple = "OK";
        return publish_stream(std::move(stream));
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
          return publish_stream(std::move(stream));
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
          return publish_stream(std::move(stream));
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
          return publish_stream(std::move(stream));
        }
        if (EqualCi(sub, "createconsumer")) {
          if (a.size() != 5) return absl::InvalidArgumentError("syntax error");
          if (FindConsumer(group, a[4])) {
            integer = 0;
            return NoChange();
          }
          group->consumers_.push_back(Consumer{a[4], now, 0});
          integer = 1;
          return publish_stream(std::move(stream));
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
          auto update = publish_stream(std::move(stream));
          if (update.ok())
            update->stream_pending_removed_ = [&](std::uint64_t removed) {
              integer = removed;
            };
          return update;
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
                   ? publish_stream(std::move(stream))
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
                   ? publish_stream(std::move(stream))
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

  absl::Status status =
      co_await RunCompact(request, digest, tx, read_only, callback,
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
    NotifyStreamBlockingKey(request, a[1], appended_id->ms_, appended_id->seq_);
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
        builder.AppendInteger(GroupPendingCount(info_group));
        if (GroupPendingCount(info_group) == 0) {
          builder.AppendNull();
          builder.AppendNull();
        } else if (info_group.summary_) {
          const auto& summary = *info_group.summary_;
          builder.AppendBulkString(FormatId(
              Id{(*summary.first_pending_)[0], (*summary.first_pending_)[1]}));
          builder.AppendBulkString(FormatId(
              Id{(*summary.last_pending_)[0], (*summary.last_pending_)[1]}));
        } else {
          builder.AppendBulkString(FormatId(info_group.pending_.front().id_));
          builder.AppendBulkString(FormatId(info_group.pending_.back().id_));
        }
        std::vector<std::pair<std::string, std::uint64_t>> counts;
        if (info_group.summary_)
          for (const auto& item : info_group.summary_->consumer_pending_)
            if (item.second != 0) counts.push_back(item);
        if (!info_group.summary_)
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
          builder.AppendInteger(
              info_stream.total_entries_.value_or(info_stream.entries_.size()));
          builder.AppendBulkString("radix-tree-keys");
          builder.AppendInteger(info_stream.total_nodes_.value_or(
              info_stream.node_entries_.size()));
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
          builder.AppendArrayHeader(info_entries->remaining_);
          const auto prefix_size = builder.View().size();
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
            builder.AppendInteger(GroupPendingCount(group));
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
              builder.AppendInteger(
                  ConsumerPendingCount(group, consumer.name_));
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
          struct FullReply {
            std::shared_ptr<StreamRangeReplyState> entries_;
            std::string suffix_;
            std::size_t offset_ = 0;
            Task<absl::StatusOr<std::string>> Next() {
              if (entries_) {
                auto chunk = co_await entries_->Next();
                if (!chunk.ok()) co_return chunk.status();
                if (!chunk->empty()) co_return std::move(*chunk);
                entries_.reset();
              }
              auto chunk = suffix_.substr(offset_, 64 * 1024);
              offset_ += chunk.size();
              co_return chunk;
            }
          };
          auto state = std::make_shared<FullReply>();
          state->entries_ = std::move(info_entries);
          state->suffix_ = builder.View().substr(prefix_size);
          auto reply = Built(builder.View().substr(0, prefix_size));
          reply.chunks_ = std::make_unique<ReplyChunkSource>(
              [state] { return state->Next(); });
          co_return reply;
        }
        builder.AppendMapHeader(10);
        builder.AppendBulkString("length");
        builder.AppendInteger(
            info_stream.total_entries_.value_or(info_stream.entries_.size()));
        builder.AppendBulkString("radix-tree-keys");
        builder.AppendInteger(info_stream.total_nodes_.value_or(
            info_stream.node_entries_.size()));
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
        builder.AppendInteger(
            info_stream.total_groups_.value_or(info_stream.groups_.size()));
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
          builder.AppendInteger(GroupConsumerCount(group));
          builder.AppendBulkString("pending");
          builder.AppendInteger(GroupPendingCount(group));
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
          const auto pending = ConsumerPendingCount(info_group, consumer.name_);
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
    std::vector<storage::TxShardWrites>& tx_writes, ReplyChunkSource* chunks) {
  ReplyBuilder builder(request.resp_version_);
  CommandReply reply = co_await ExecuteRead(request, builder, keys, &tx_writes);
  std::string encoded(reply.encoded_);
  if (reply.chunks_) {
    if (chunks)
      *chunks = std::move(*reply.chunks_);
    else {
      // Lua consumes an owned RESP value inside the script. Network/EXEC
      // consumers preserve the lazy snapshot instead.
      for (;;) {
        auto chunk = co_await (*reply.chunks_)();
        if (!chunk.ok())
          co_return std::string(StorageError(builder, chunk.status()));
        if (chunk->empty()) break;
        encoded += *chunk;
      }
    }
  }
  co_return encoded;
}

}  // namespace lavik
