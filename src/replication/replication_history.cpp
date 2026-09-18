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

#include "lavik/replication_history.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <deque>
#include <limits>
#include <optional>
#include <utility>

#include "lavik/memory.h"
#include "lavik/storage/format.h"
#include "log_block.h"

namespace lavik {
namespace {
// Private, boot-local framing: a complete effect never straddles blocks. Its
// internal sequence is used only for block lookup. Original flow/LSN and bytes
// stay in each participant; none of this framing goes on the replication wire.
struct EffectHeader {
  std::uint64_t sequence_;
  std::uint32_t bytes_;
  std::uint32_t records_;
};
struct RecordHeader {
  std::uint64_t lsn_;
  std::uint32_t flow_id_;
  std::uint32_t bytes_;
};
template <typename T>
T Load(const std::byte* data) {
  T value;
  std::memcpy(&value, data, sizeof(value));
  return value;
}
template <typename T>
void Store(std::byte* data, const T& value) {
  std::memcpy(data, &value, sizeof(value));
}
template <typename F>
void ForEachRecord(const std::byte* effect, F&& visit) {
  auto header = Load<EffectHeader>(effect);
  auto* at = effect + sizeof(EffectHeader);
  for (std::uint32_t i = 0; i < header.records_; ++i) {
    auto record = Load<RecordHeader>(at);
    at += sizeof(RecordHeader);
    visit(record, at);
    at += record.bytes_;
  }
}
}  // namespace

struct ReplicationHistory::Impl {
  struct IndexEntry {
    std::uint64_t lsn_;
    std::uint64_t effect_;
    unsigned flow_id_;
  };
  // Export builds this index on demand, once per immutable prefix. Normal
  // receive/apply never allocates or updates a per-record lookup index.
  struct Block : detail::ReplicationLogBlock {
    using Index =
        std::vector<IndexEntry, storage::RetainedAllocator<IndexEntry>>;
    explicit Block(detail::ReplicationLogBlock block)
        : detail::ReplicationLogBlock(std::move(block)),
          index_(storage::RetainedAllocator<IndexEntry>(
              storage::RetainedAllocationDomain{.externally_admitted_ =
                                                    true})) {}
    Index index_;
    std::uint32_t record_count_ = 0;
    std::uint32_t indexed_bytes_ = 0;
    std::uint32_t indexed_frames_ = 0;

    void Recycle() {
      Reset();
      Index(index_.get_allocator()).swap(index_);
      record_count_ = indexed_bytes_ = indexed_frames_ = 0;
    }

    bool BuildIndex() {
      const auto bytes =
          AllocatorUsableSizeForRequest(record_count_ * sizeof(IndexEntry));
      auto reservation = TryReserveMemory(bytes);
      if (!reservation) return false;
      Index next(index_.get_allocator());
      next.reserve(record_count_);
      for (std::uint32_t offset = 0; offset < committed_bytes_;) {
        auto* effect = bytes_.get() + offset;
        auto header = Load<EffectHeader>(effect);
        ForEachRecord(effect, [&](auto record, const auto*) {
          next.push_back({record.lsn_, header.sequence_, record.flow_id_});
        });
        offset += header.bytes_;
      }
      std::ranges::sort(next, [](const auto& left, const auto& right) {
        return std::pair{left.flow_id_, left.lsn_} <
               std::pair{right.flow_id_, right.lsn_};
      });
      index_.swap(next);
      indexed_bytes_ = committed_bytes_;
      indexed_frames_ = frame_count_;
      reservation->Release();
      return true;
    }

    const std::byte* Find(unsigned flow, std::uint64_t lsn) {
      // A mutable tail may receive more effects during an export. Existing
      // offsets remain valid; scan at most 63 newly appended effects between
      // index rebuilds. Under memory pressure a linear read needs no
      // admission.
      if (indexed_bytes_ == 0 || frame_count_ - indexed_frames_ >= 64)
        (void)BuildIndex();
      const auto found =
          std::lower_bound(index_.begin(), index_.end(), std::pair{flow, lsn},
                           [](const auto& entry, const auto& key) {
                             return std::pair{entry.flow_id_, entry.lsn_} < key;
                           });
      if (found != index_.end() && found->flow_id_ == flow &&
          found->lsn_ == lsn) {
        auto offset = FindOffset(found->effect_);
        while (offset < committed_bytes_) {
          auto* effect = bytes_.get() + offset;
          auto header = Load<EffectHeader>(effect);
          if (header.sequence_ == found->effect_) return effect;
          offset += header.bytes_;
        }
        std::terminate();
      }
      for (auto offset = indexed_bytes_; offset < committed_bytes_;) {
        auto* effect = bytes_.get() + offset;
        bool match = false;
        ForEachRecord(effect, [&](auto record, const auto*) {
          match |= record.flow_id_ == flow && record.lsn_ == lsn;
        });
        if (match) return effect;
        offset += Load<EffectHeader>(effect).bytes_;
      }
      return nullptr;
    }
  };

  std::size_t capacity_ = 0;
  std::size_t primary_bytes_ = 0;
  std::size_t secondary_bytes_ = 0;
  std::string history_id_;
  // Apply publishes in increasing origin-flow order, even when consecutive
  // events for that flow have different local apply owners. Admission may
  // leave holes; a late/duplicate insertion must never invent coverage in them.
  std::vector<std::uint64_t> last_lsns_;
  std::uint64_t next_effect_ = 1;
  std::deque<Block> blocks_;

  void Evict() {
    assert(!blocks_.empty());
    secondary_bytes_ -= blocks_.front().capacity();
    blocks_.pop_front();
  }
  std::size_t SecondaryCapacity() const {
    return primary_bytes_ >= capacity_ ? 0 : capacity_ - primary_bytes_;
  }
  void Trim() {
    while (secondary_bytes_ > SecondaryCapacity()) Evict();
  }
  const std::byte* Find(unsigned flow, std::uint64_t lsn) {
    for (auto& block : blocks_)
      if (auto* effect = block.Find(flow, lsn)) return effect;
    return nullptr;
  }
};

ReplicationHistory::PrimaryCharge::PrimaryCharge(std::shared_ptr<Impl> impl,
                                                 std::size_t bytes)
    : impl_(std::move(impl)), bytes_(bytes) {}
ReplicationHistory::PrimaryCharge::PrimaryCharge(PrimaryCharge&& other) noexcept
    : impl_(std::move(other.impl_)), bytes_(std::exchange(other.bytes_, 0)) {}
ReplicationHistory::PrimaryCharge& ReplicationHistory::PrimaryCharge::operator=(
    PrimaryCharge&& other) noexcept {
  if (this != &other) {
    Reset();
    impl_ = std::move(other.impl_);
    bytes_ = std::exchange(other.bytes_, 0);
  }
  return *this;
}
ReplicationHistory::PrimaryCharge::~PrimaryCharge() { Reset(); }
void ReplicationHistory::PrimaryCharge::Reset() noexcept {
  if (!impl_) return;
  assert(impl_->primary_bytes_ >= bytes_);
  impl_->primary_bytes_ -= bytes_;
  bytes_ = 0;
  impl_.reset();
}

ReplicationHistory::ReplicationHistory(std::size_t capacity_bytes)
    : impl_(std::make_shared<Impl>()) {
  impl_->capacity_ = capacity_bytes;
}
absl::Status ReplicationHistory::Reset(std::string history_id,
                                       unsigned flow_count) {
  if (history_id.empty() || flow_count == 0 || flow_count > 1024)
    return absl::InvalidArgumentError(
        "invalid retained native history identity or layout");
  while (!impl_->blocks_.empty()) impl_->Evict();
  impl_->last_lsns_.assign(flow_count, 0);
  impl_->next_effect_ = 1;
  impl_->history_id_ = std::move(history_id);
  return absl::OkStatus();
}
void ReplicationHistory::SetCapacity(std::size_t capacity_bytes) {
  impl_->capacity_ = capacity_bytes;
  impl_->Trim();
}
ReplicationHistory::PrimaryCharge ReplicationHistory::ChargePrimary(
    std::size_t bytes) {
  if (bytes > std::numeric_limits<std::size_t>::max() - impl_->primary_bytes_)
    std::terminate();
  impl_->primary_bytes_ += bytes;
  impl_->Trim();
  return PrimaryCharge(impl_, bytes);
}
void ReplicationHistory::ReclaimSecondary(std::size_t bytes) {
  const auto target =
      impl_->secondary_bytes_ - std::min(bytes, impl_->secondary_bytes_);
  while (impl_->secondary_bytes_ > target) impl_->Evict();
}

bool ReplicationHistory::TryRetain(std::string_view history_id,
                                   std::vector<NativeHistoryRecord> records) {
  std::ranges::sort(records, {}, &NativeHistoryRecord::flow_id_);
  return RetainSorted(history_id, records);
}

bool ReplicationHistory::TryRetainOne(std::string_view history_id,
                                      const NativeHistoryRecord& record) {
  return RetainSorted(history_id, {&record, 1});
}

bool ReplicationHistory::RetainSorted(
    std::string_view history_id, std::span<const NativeHistoryRecord> records) {
  if (records.empty() || history_id != impl_->history_id_ ||
      records.size() > impl_->last_lsns_.size() ||
      impl_->next_effect_ == std::numeric_limits<std::uint64_t>::max())
    return false;
  std::size_t bytes = sizeof(EffectHeader);
  for (std::size_t i = 0; i < records.size(); ++i) {
    const auto& record = records[i];
    if (record.flow_id_ >= impl_->last_lsns_.size() ||
        record.lsn_ <= impl_->last_lsns_[record.flow_id_] ||
        record.lsn_ == std::numeric_limits<std::uint64_t>::max() ||
        record.canonical_.empty() ||
        (i != 0 && records[i - 1].flow_id_ == record.flow_id_) ||
        bytes >
            std::numeric_limits<std::uint32_t>::max() - sizeof(RecordHeader) ||
        record.canonical_.size() > std::numeric_limits<std::uint32_t>::max() -
                                       bytes - sizeof(RecordHeader))
      return false;
    bytes += sizeof(RecordHeader) + record.canonical_.size();
  }
  if (bytes > std::numeric_limits<std::uint32_t>::max() - 7) return false;
  bytes = storage::AlignRecord(bytes);
  const auto capacity = impl_->SecondaryCapacity();
  if (bytes > capacity) return false;

  if (impl_->blocks_.empty() ||
      bytes > impl_->blocks_.back().capacity() -
                  impl_->blocks_.back().committed_bytes_) {
    // Small budgets use a smaller block; effects larger than the normal block
    // get a dedicated allocation. Keeping each complete effect together makes
    // rollover and eviction atomic without a per-record ownership graph.
    const auto block_bytes = std::max(
        bytes, std::min<std::size_t>(storage::kStorageBlockBytes, capacity));
    std::optional<Impl::Block> recycled;
    while (impl_->secondary_bytes_ > capacity - block_bytes) {
      auto& front = impl_->blocks_.front();
      const auto retired_bytes = front.capacity();
      if (!recycled && front.capacity() == block_bytes) {
        recycled.emplace(std::move(front));
        recycled->Recycle();
      }
      impl_->secondary_bytes_ -= retired_bytes;
      impl_->blocks_.pop_front();
    }
    if (recycled) {
      impl_->blocks_.push_back(std::move(*recycled));
    } else {
      auto allocated = detail::ReplicationLogBlock::Allocate(block_bytes);
      if (!allocated.ok()) return false;
      impl_->blocks_.emplace_back(std::move(*allocated));
    }
    impl_->secondary_bytes_ += block_bytes;
  }
  auto& block = impl_->blocks_.back();
  auto output = block.AppendBuffer(bytes);
  const EffectHeader header{impl_->next_effect_++,
                            static_cast<std::uint32_t>(bytes),
                            static_cast<std::uint32_t>(records.size())};
  Store(output.data(), header);
  auto* at = output.data() + sizeof(header);
  for (const auto& record : records) {
    Store(at,
          RecordHeader{record.lsn_, record.flow_id_,
                       static_cast<std::uint32_t>(record.canonical_.size())});
    at += sizeof(RecordHeader);
    std::memcpy(at, record.canonical_.data(), record.canonical_.size());
    at += record.canonical_.size();
    impl_->last_lsns_[record.flow_id_] = record.lsn_;
  }
  block.record_count_ += records.size();
  block.CommitAppend(header.sequence_, 0, bytes);
  return true;
}

std::vector<std::vector<NativeHistoryRange>> ReplicationHistory::Coverage(
    std::string_view history_id, std::size_t max_ranges_per_flow) const {
  if (history_id != impl_->history_id_ || max_ranges_per_flow == 0) return {};
  std::vector<std::vector<NativeHistoryRange>> result(impl_->last_lsns_.size());
  for (const auto& block : impl_->blocks_) {
    for (std::uint32_t offset = 0; offset < block.committed_bytes_;) {
      const auto* effect = block.bytes_.get() + offset;
      ForEachRecord(effect, [&](auto record, const auto*) {
        auto& ranges = result[record.flow_id_];
        if (!ranges.empty() && ranges.back().end_lsn_ == record.lsn_)
          ranges.back().end_lsn_ = record.lsn_ + 1;
        else
          ranges.push_back({record.lsn_, record.lsn_ + 1});
      });
      offset += Load<EffectHeader>(effect).bytes_;
    }
  }
  for (auto& ranges : result) {
    if (ranges.size() > max_ranges_per_flow) {
      std::vector<NativeHistoryRange> newest(ranges.end() - max_ranges_per_flow,
                                             ranges.end());
      ranges.swap(newest);
    }
  }
  return result;
}

absl::StatusOr<std::vector<NativeHistoryRecordInfo>>
ReplicationHistory::DescribeEffect(std::string_view history_id,
                                   unsigned flow_id, std::uint64_t lsn) const {
  if (history_id != impl_->history_id_ || flow_id >= impl_->last_lsns_.size())
    return absl::NotFoundError("retained history lineage is unavailable");
  const auto* effect = impl_->Find(flow_id, lsn);
  if (!effect) return absl::NotFoundError("retained history has a gap");
  std::vector<NativeHistoryRecordInfo> records;
  ForEachRecord(effect, [&](auto record, const auto*) {
    records.push_back({record.flow_id_, record.lsn_, record.bytes_});
  });
  return records;
}

absl::StatusOr<NativeHistoryChunk> ReplicationHistory::Read(
    std::string_view history_id, unsigned flow_id, std::uint64_t lsn,
    std::size_t offset, std::size_t max_bytes) const {
  if (max_bytes == 0 || max_bytes > 64 * 1024)
    return absl::InvalidArgumentError("invalid retained-history chunk bound");
  if (history_id != impl_->history_id_ || flow_id >= impl_->last_lsns_.size())
    return absl::NotFoundError("retained history lineage is unavailable");
  const auto* effect = impl_->Find(flow_id, lsn);
  if (!effect) return absl::NotFoundError("retained history has a gap");
  std::string_view payload;
  ForEachRecord(effect, [&](auto record, const auto* bytes) {
    if (record.flow_id_ == flow_id && record.lsn_ == lsn)
      payload = {reinterpret_cast<const char*>(bytes), record.bytes_};
  });
  if (offset >= payload.size())
    return absl::OutOfRangeError("retained event chunk offset is past its end");
  return NativeHistoryChunk{payload.size(),
                            std::string(payload.substr(offset, max_bytes))};
}
std::size_t ReplicationHistory::primary_bytes() const {
  return impl_->primary_bytes_;
}
std::size_t ReplicationHistory::secondary_bytes() const {
  return impl_->secondary_bytes_;
}
}  // namespace lavik
