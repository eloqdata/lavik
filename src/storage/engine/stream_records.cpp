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

#include "lavik/storage/detail/stream_records.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>

namespace lavik::storage {
namespace {

void Put(std::string& out, std::uint64_t value, unsigned width) {
  for (unsigned i = 0; i < width; ++i) out.push_back(value >> (8 * i));
}
std::uint64_t Get(std::string_view in, std::size_t at, unsigned width) {
  std::uint64_t value = 0;
  for (unsigned i = 0; i < width; ++i)
    value |= std::uint64_t(static_cast<unsigned char>(in[at + i])) << (8 * i);
  return value;
}
void KeyId(std::string& key, std::string_view id) {
  // LXS1 IDs are two little-endian uint64s. Routing uses the complete ID in
  // big-endian order, without floating-point rounding of either component.
  for (unsigned half = 0; half < 2; ++half)
    for (unsigned i = 8; i != 0; --i) key.push_back(id[half * 8 + i - 1]);
}
void KeyName(std::string& key, std::string_view name) {
  // Escape NUL and terminate names so arbitrary binary names retain byte order
  // and cannot alias their prefix, another record kind, or another group.
  for (char ch : name) {
    key.push_back(ch);
    if (ch == '\0') key.push_back('\xff');
  }
  key.append("\0\0", 2);
}

class Parser {
 public:
  explicit Parser(std::string_view in) : in_(in) {}
  bool Skip(std::size_t size) {
    if (size > in_.size() - at_) return false;
    at_ += size;
    return true;
  }
  bool Count(std::uint32_t& count) {
    if (!Skip(4)) return false;
    count = Get(in_, at_ - 4, 4);
    return true;
  }
  bool String(std::string_view& value) {
    std::uint32_t size = 0;
    if (!Count(size) || !Skip(size)) return false;
    value = in_.substr(at_ - size, size);
    return true;
  }
  void Record(std::string key, std::size_t begin) {
    const auto key_bytes = key.size();
    key.append(in_.substr(begin, at_ - begin));
    Put(key, key_bytes, 4);
    records_.push_back({.value_ = std::move(key)});
  }
  std::string_view in_;
  std::size_t at_ = 0;
  std::vector<OrderedCollectionEntry> records_;
};

absl::Status Invalid() { return absl::DataLossError("invalid Stream records"); }

}  // namespace

absl::StatusOr<std::vector<OrderedCollectionEntry>> DecodeStreamRecords(
    std::string_view encoded, std::uint64_t expected_count) {
  Parser p(encoded);
  if (!encoded.starts_with("LXS1") || !p.Skip(48) ||
      Get(encoded, 44, 4) != expected_count)
    return Invalid();
  p.Record(std::string(1, '\0'), 0);
  std::vector<std::string_view> ids;
  if (expected_count > (encoded.size() - p.at_) / 20) return Invalid();
  ids.reserve(expected_count);
  std::string previous;
  for (std::uint64_t i = 0; i < expected_count; ++i) {
    const auto begin = p.at_;
    std::uint32_t fields = 0;
    if (!p.Skip(16) || !p.Count(fields) || fields == 0 || fields % 2 ||
        fields > (encoded.size() - p.at_) / 4)
      return Invalid();
    ids.push_back(encoded.substr(begin, 16));
    std::string key(1, '\1');
    KeyId(key, ids.back());
    if (!previous.empty() && key <= previous) return Invalid();
    previous = key;
    for (std::uint32_t f = 0; f < fields; ++f) {
      std::string_view field;
      if (!p.String(field)) return Invalid();
    }
    p.Record(std::move(key), begin);
  }
  std::uint32_t nodes = 0;
  auto begin = p.at_;
  if (!p.Count(nodes) || nodes > expected_count) return Invalid();
  p.Record(std::string(1, '\2'), begin);
  std::uint64_t entries = 0;
  for (std::uint32_t i = 0; i < nodes; ++i) {
    begin = p.at_;
    std::uint32_t count = 0;
    if (!p.Count(count) || count == 0 || count > expected_count - entries)
      return Invalid();
    std::string key(1, '\3');
    KeyId(key, ids[entries]);
    p.Record(std::move(key), begin);
    entries += count;
  }
  if (entries != expected_count) return Invalid();
  begin = p.at_;
  std::uint32_t groups = 0;
  if (!p.Count(groups) || groups > (encoded.size() - p.at_) / 36)
    return Invalid();
  p.Record(std::string(1, '\4'), begin);
  for (std::uint32_t i = 0; i < groups; ++i) {
    begin = p.at_;
    std::string_view name;
    std::uint32_t consumers = 0;
    if (!p.String(name) || !p.Skip(24) || !p.Count(consumers) ||
        consumers > (encoded.size() - p.at_) / 20)
      return Invalid();
    std::string prefix(1, '\5');
    KeyName(prefix, name);
    p.Record(prefix + '\0', begin);
    for (std::uint32_t c = 0; c < consumers; ++c) {
      begin = p.at_;
      std::string_view consumer;
      if (!p.String(consumer) || !p.Skip(16)) return Invalid();
      std::string key = prefix + '\1';
      KeyName(key, consumer);
      p.Record(std::move(key), begin);
    }
    begin = p.at_;
    std::uint32_t pending = 0;
    if (!p.Count(pending) || pending > (encoded.size() - p.at_) / 36)
      return Invalid();
    p.Record(prefix + '\2', begin);
    previous.clear();
    for (std::uint32_t n = 0; n < pending; ++n) {
      begin = p.at_;
      std::string_view consumer;
      if (!p.Skip(16) || !p.String(consumer) || !p.Skip(16)) return Invalid();
      std::string key = prefix + '\3';
      KeyId(key, encoded.substr(begin, 16));
      if (!previous.empty() && key <= previous) return Invalid();
      previous = key;
      p.Record(std::move(key), begin);
    }
  }
  if (p.at_ != encoded.size()) return Invalid();
  std::sort(p.records_.begin(), p.records_.end(), OrderedEntryLess);
  // A full record includes the payload, so ordinary value uniqueness alone
  // cannot catch repeated group/consumer names with differing metadata.
  std::string_view prior_key;
  for (const auto& record : p.records_) {
    const auto size = Get(record.value_, record.value_.size() - 4, 4);
    const auto key = std::string_view(record.value_).substr(0, size);
    if (!prior_key.empty() && key <= prior_key) return Invalid();
    prior_key = key;
  }
  return std::move(p.records_);
}

absl::StatusOr<std::string_view> StreamRecordKey(std::string_view record) {
  if (record.size() < 5) return Invalid();
  const auto size = Get(record, record.size() - 4, 4);
  if (size == 0 || size > record.size() - 4 ||
      static_cast<unsigned char>(record.front()) > 5)
    return Invalid();
  const auto key = record.substr(0, size);
  const auto kind = static_cast<unsigned char>(key.front());
  if (kind < 5) {
    if (key.size() != (kind == 1 || kind == 3 ? 17U : 1U)) return Invalid();
  } else {
    std::size_t at = 1;
    auto name = [&]() {
      while (at < key.size()) {
        if (key[at++] != '\0') continue;
        if (at == key.size()) return false;
        const auto escaped = static_cast<unsigned char>(key[at++]);
        if (escaped == 0) return true;
        if (escaped != 255) return false;
      }
      return false;
    };
    if (!name() || at == key.size()) return Invalid();
    const auto sub = static_cast<unsigned char>(key[at++]);
    if (sub == 1) {
      if (!name()) return Invalid();
    } else if (sub == 3) {
      if (key.size() - at != 16) return Invalid();
      at += 16;
    } else if (sub != 0 && sub != 2)
      return Invalid();
    if (at != key.size()) return Invalid();
  }
  return record.substr(0, size);
}

absl::StatusOr<std::string_view> StreamRecordPayload(std::string_view record) {
  auto key = StreamRecordKey(record);
  if (!key.ok()) return key.status();
  return record.substr(key->size(), record.size() - key->size() - 4);
}

absl::StatusOr<std::string_view> StreamGroupHeaderPayload(
    std::string_view record) {
  auto payload = StreamRecordPayload(record);
  if (!payload.ok()) return payload.status();
  if (payload->size() < 32 || Get(*payload, 0, 4) != payload->size() - 32)
    return absl::DataLossError("invalid Stream group header size");
  return *payload;
}

absl::Status StreamRecordValidator::Read(std::string_view record) {
  if (failed_) return Invalid();
  auto read = [&]() -> absl::Status {
    auto key = StreamRecordKey(record);
    auto payload = StreamRecordPayload(record);
    if (!key.ok() || !payload.ok() || (!previous_.empty() && *key <= previous_))
      return Invalid();
    Parser p(*payload);
    const auto kind = static_cast<unsigned char>(key->front());
    if (kind == 0) {
      if (phase_ != 0 || length_ > UINT32_MAX || payload->size() != 48 ||
          !payload->starts_with("LXS1") || Get(*payload, 44, 4) != length_)
        return Invalid();
      phase_ = 1;
    } else if (kind == 1) {
      std::uint32_t fields = 0;
      if (phase_ != 1 || entries_ == length_ || !p.Skip(16) ||
          !p.Count(fields) || fields == 0 || fields % 2 ||
          fields > (payload->size() - p.at_) / 4)
        return Invalid();
      std::string expected(1, '\1');
      KeyId(expected, payload->substr(0, 16));
      if (*key != expected) return Invalid();
      for (std::uint32_t i = 0; i < fields; ++i) {
        std::string_view field;
        if (!p.String(field) || field.size() > kMaxStringBytes)
          return Invalid();
      }
      if (p.at_ != payload->size()) return Invalid();
      ++entries_;
    } else if (kind == 2) {
      if (phase_ != 1 || entries_ != length_ || payload->size() != 4 ||
          !p.Count(expected_nodes_) || expected_nodes_ > length_ ||
          ((expected_nodes_ == 0) != (length_ == 0)))
        return Invalid();
      phase_ = 2;
    } else if (kind == 3) {
      std::uint32_t count = 0;
      if (phase_ != 2 || nodes_ == expected_nodes_ || payload->size() != 4 ||
          !p.Count(count) || count == 0 || count > length_ - node_entries_)
        return Invalid();
      ++nodes_;
      node_entries_ += count;
    } else if (kind == 4) {
      if (phase_ != 2 || nodes_ != expected_nodes_ ||
          node_entries_ != length_ || payload->size() != 4 ||
          !p.Count(expected_groups_))
        return Invalid();
      phase_ = 3;
    } else {
      // The key parser already checked escapes and key framing.
      std::size_t sub_at = 1;
      while (sub_at < key->size()) {
        if ((*key)[sub_at++] != '\0') continue;
        if ((*key)[sub_at++] == '\0') break;
      }
      const auto sub = static_cast<unsigned char>((*key)[sub_at]);
      if (sub == 0) {
        if ((phase_ != 3 && (phase_ != 5 || pending_ != 0)) ||
            groups_ == expected_groups_)
          return Invalid();
        std::string_view name;
        if (!p.String(name) || !p.Skip(24) || !p.Count(consumers_) ||
            p.at_ != payload->size())
          return Invalid();
        prefix_.assign(1, '\5');
        KeyName(prefix_, name);
        if (*key != prefix_ + '\0') return Invalid();
        ++groups_;
        phase_ = 4;
      } else if (sub == 1) {
        std::string_view name;
        if (phase_ != 4 || consumers_ == 0 || !p.String(name) || !p.Skip(16) ||
            p.at_ != payload->size())
          return Invalid();
        std::string expected = prefix_ + '\1';
        KeyName(expected, name);
        if (*key != expected) return Invalid();
        --consumers_;
      } else if (sub == 2) {
        if (phase_ != 4 || consumers_ != 0 || *key != prefix_ + '\2' ||
            payload->size() != 4 || !p.Count(pending_))
          return Invalid();
        phase_ = 5;
      } else {
        std::string_view consumer;
        if (phase_ != 5 || pending_ == 0 || !p.Skip(16) ||
            !p.String(consumer) || !p.Skip(16) || p.at_ != payload->size())
          return Invalid();
        std::string expected = prefix_ + '\3';
        KeyId(expected, payload->substr(0, 16));
        if (*key != expected) return Invalid();
        --pending_;
      }
    }
    previous_ = *key;
    return absl::OkStatus();
  };
  auto status = read();
  failed_ = !status.ok();
  return status;
}

absl::Status StreamRecordValidator::Finish() const {
  if (failed_ || groups_ != expected_groups_ ||
      !((phase_ == 3 && expected_groups_ == 0) ||
        (phase_ == 5 && pending_ == 0)))
    return Invalid();
  return absl::OkStatus();
}

absl::StatusOr<std::string> EncodeStreamRecords(
    std::span<const OrderedCollectionEntry> records) {
  std::string output;
  for (const auto& record : records) {
    auto payload = StreamRecordPayload(record.value_);
    if (!payload.ok()) return payload.status();
    if (payload->size() > output.max_size() - output.size())
      return absl::ResourceExhaustedError("Stream image size overflow");
    output.append(*payload);
  }
  return output;
}

absl::StatusOr<OrderedCollectionMutationPlan> PlanStreamRecordChanges(
    const OrderedGroupDirectory& directory,
    std::vector<LoadedOrderedGroup> loaded,
    std::vector<StreamRecordChange> changes, std::uint64_t stream_length,
    std::span<const std::uint64_t> retired_pages) {
  const auto& root = directory.root();
  if (root.kind_ != OrderedCollectionKind::kStream ||
      stream_length > UINT32_MAX)
    return absl::InvalidArgumentError("invalid Stream mutation source");
  std::map<std::uint64_t, OrderedGroupSnapshot> pages;
  for (auto& value : loaded) {
    auto& page = value.snapshot_;
    const auto* metadata = directory.Find(page.id_);
    if (!metadata || page.kind_ != root.kind_ || page.retired_ ||
        page.incarnation_ != root.incarnation_ ||
        value.sequence_ != metadata->sequence_ ||
        page.previous_ != metadata->previous_ ||
        page.next_ != metadata->next_ ||
        page.entries_.size() != metadata->item_count_)
      return absl::AbortedError("stale Stream mutation page");
    auto valid = OrderedGroupEncoder::Create(page);
    if (!valid.ok()) return valid.status();
    if (!pages.emplace(page.id_, std::move(page)).second)
      return absl::InvalidArgumentError("duplicate Stream mutation page");
  }
  std::set<std::uint64_t> touched;
  std::set<std::string> keys;
  std::int64_t count = root.item_count_;
  // Whole-page range deletion needs no message payload. The caller resolves
  // both range boundaries against this same immutable directory beforehand.
  std::set<std::uint64_t> retired;
  for (const auto id : retired_pages) {
    const auto* metadata = directory.Find(id);
    if (!metadata || !retired.insert(id).second)
      return absl::InvalidArgumentError("invalid Stream retired page");
    count -= metadata->item_count_;
    touched.insert(id);
  }
  // Check routing against the original page boundaries before any mutation
  // changes a boundary key. Payload changes never change record ownership.
  for (const auto& change : changes) {
    auto page = pages.find(change.page_id_);
    if (page == pages.end() || !keys.insert(change.key_).second)
      return absl::InvalidArgumentError(
          "missing/repeated Stream mutation route");
    const auto& source = page->second;
    if ((source.previous_ && !pages.contains(source.previous_) &&
         !retired.contains(source.previous_)) ||
        (source.next_ && !pages.contains(source.next_) &&
         !retired.contains(source.next_)))
      return absl::InvalidArgumentError("Stream mutation needs adjacent pages");
    auto last = StreamRecordKey(source.entries_.back().value_);
    if (!last.ok()) return last.status();
    if (source.next_ && change.key_ > *last)
      return absl::InvalidArgumentError("Stream mutation exceeds page bound");
    if (source.previous_ && !retired.contains(source.previous_)) {
      auto lower =
          StreamRecordKey(pages.at(source.previous_).entries_.back().value_);
      if (!lower.ok()) return lower.status();
      if (change.key_ <= *lower)
        return absl::InvalidArgumentError(
            "Stream mutation precedes page bound");
    }
    if (change.record_) {
      auto key = StreamRecordKey(*change.record_);
      if (!key.ok() || *key != change.key_)
        return absl::InvalidArgumentError("Stream mutation key mismatch");
    }
  }
  // Many new PEL records can route to the same original tail page. Merge
  // its sorted changes once: searching and inserting each row separately
  // would make one large delivery quadratic in the delivered message count.
  std::map<std::uint64_t, std::vector<StreamRecordChange*>> by_page;
  for (auto& change : changes) by_page[change.page_id_].push_back(&change);
  for (auto& [id, updates] : by_page) {
    std::sort(updates.begin(), updates.end(),
              [](const auto* a, const auto* b) { return a->key_ < b->key_; });
    auto& original = pages.at(id).entries_;
    std::vector<OrderedCollectionEntry> merged;
    merged.reserve(original.size() + updates.size());
    auto entry = original.begin();
    for (auto* change : updates) {
      while (entry != original.end()) {
        auto key = StreamRecordKey(entry->value_);
        if (!key.ok()) return key.status();
        if (*key >= change->key_) break;
        merged.push_back(std::move(*entry++));
      }
      bool exists = false;
      if (entry != original.end()) {
        auto key = StreamRecordKey(entry->value_);
        if (!key.ok()) return key.status();
        exists = *key == change->key_;
      }
      if (exists && change->record_ && entry->value_ == *change->record_) {
        merged.push_back(std::move(*entry++));
        continue;
      }
      if (!exists && !change->record_) continue;
      touched.insert(id);
      if (exists) {
        ++entry;
        --count;
      }
      if (change->record_) {
        merged.push_back({.value_ = std::move(*change->record_)});
        ++count;
      }
    }
    for (; entry != original.end(); ++entry)
      merged.push_back(std::move(*entry));
    original = std::move(merged);
  }
  if (count <= 0 || count > UINT32_MAX)
    return absl::OutOfRangeError("Stream record count overflow");
  OrderedCollectionMutationPlan plan{
      .root_ = root, .expected_sequence_ = directory.sequence(), .writes_ = {}};
  if (touched.empty()) {
    if (stream_length != root.logical_size())
      return absl::InvalidArgumentError(
          "Stream length changed without records");
    return plan;
  }
  plan.changed_ = true;
  plan.root_.revision_ = 0;
  plan.root_.item_count_ = count;
  plan.root_.stream_length_ = stream_length;
  std::vector<std::uint64_t> chain;
  std::map<std::uint64_t, OrderedGroupSnapshot> replacements;
  for (const auto& old : directory.groups()) {
    if (!touched.contains(old.id_)) {
      chain.push_back(old.id_);
      continue;
    }
    auto page = retired.contains(old.id_)
                    ? OrderedGroupSnapshot{.kind_ = root.kind_,
                                           .incarnation_ = root.incarnation_,
                                           .id_ = old.id_,
                                           .entries_ = {}}
                    : std::move(pages.at(old.id_));
    if (page.entries_.empty()) {
      plan.writes_.push_back({.kind_ = root.kind_,
                              .incarnation_ = root.incarnation_,
                              .id_ = old.id_,
                              .retired_ = true,
                              .entries_ = {}});
      continue;
    }
    auto split = SplitOrderedGroup(std::move(page), plan.root_.next_group_id_);
    if (!split.ok()) return split.status();
    plan.root_.next_group_id_ = split->next_group_id_;
    for (auto& part : split->groups_) {
      chain.push_back(part.id_);
      replacements.emplace(part.id_, std::move(part));
    }
  }
  for (std::size_t i = 0; i < chain.size(); ++i) {
    const auto id = chain[i];
    const auto previous = i == 0 ? 0 : chain[i - 1];
    const auto next = i + 1 == chain.size() ? 0 : chain[i + 1];
    auto found = replacements.find(id);
    if (found == replacements.end()) {
      const auto* old = directory.Find(id);
      if (old->previous_ == previous && old->next_ == next) continue;
      auto source = pages.find(id);
      if (source == pages.end())
        return absl::InvalidArgumentError("missing changed Stream neighbour");
      found = replacements.emplace(id, std::move(source->second)).first;
    }
    found->second.previous_ = previous;
    found->second.next_ = next;
  }
  for (auto& [id, page] : replacements) plan.writes_.push_back(std::move(page));
  plan.root_.first_group_ = chain.front();
  plan.root_.last_group_ = chain.back();
  plan.root_.group_count_ = chain.size();
  return plan;
}

}  // namespace lavik::storage
