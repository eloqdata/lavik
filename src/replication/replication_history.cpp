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
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <utility>

#include "lavik/memory.h"

namespace lavik {

struct ReplicationHistory::Impl {
  struct Effect {
    std::vector<NativeHistoryRecord> records_;
    std::size_t bytes_ = 0;
    RetainedMemoryCharge charge_;
  };
  mutable std::mutex mutex_;
  std::size_t capacity_ = 0;
  std::size_t primary_bytes_ = 0;
  std::size_t secondary_bytes_ = 0;
  std::string history_id_;
  struct Entry {
    const NativeHistoryRecord* record_;
    const Effect* effect_;
  };
  std::vector<std::map<std::uint64_t, Entry>> flows_;
  std::deque<std::unique_ptr<Effect>> effects_;

  void Evict() {
    assert(!effects_.empty());
    const auto& effect = effects_.front();
    for (const auto& record : effect->records_)
      flows_[record.flow_id_].erase(record.lsn_);
    secondary_bytes_ -= effect->bytes_;
    effects_.pop_front();
  }
  std::size_t SecondaryCapacity() const {
    return primary_bytes_ >= capacity_ ? 0 : capacity_ - primary_bytes_;
  }
  void Trim() {
    while (secondary_bytes_ > SecondaryCapacity()) Evict();
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
  {
    std::lock_guard lock(impl_->mutex_);
    assert(impl_->primary_bytes_ >= bytes_);
    impl_->primary_bytes_ -= bytes_;
  }
  bytes_ = 0;
  impl_.reset();
}

ReplicationHistory::ReplicationHistory(std::size_t capacity_bytes)
    : impl_(std::make_shared<Impl>()) {
  impl_->capacity_ = capacity_bytes;
}
absl::Status ReplicationHistory::Reset(std::string history_id,
                                       unsigned flow_count) {
  // This is the native protocol's maximum origin layout, independent of the
  // donor's local worker count. Empty identities never establish a lineage.
  if (history_id.empty() || flow_count == 0 || flow_count > 1024) {
    return absl::InvalidArgumentError(
        "invalid retained native history identity or layout");
  }
  std::lock_guard lock(impl_->mutex_);
  while (!impl_->effects_.empty()) impl_->Evict();
  impl_->flows_.clear();
  impl_->flows_.resize(flow_count);
  impl_->history_id_ = std::move(history_id);
  return absl::OkStatus();
}
void ReplicationHistory::SetCapacity(std::size_t capacity_bytes) {
  std::lock_guard lock(impl_->mutex_);
  impl_->capacity_ = capacity_bytes;
  impl_->Trim();
}
ReplicationHistory::PrimaryCharge ReplicationHistory::ChargePrimary(
    std::size_t bytes) {
  std::lock_guard lock(impl_->mutex_);
  // Published storage blocks cannot reach size_t overflow within the bounded
  // per-worker log capacities. Treat a broken accounting invariant as fatal.
  if (bytes > std::numeric_limits<std::size_t>::max() - impl_->primary_bytes_)
    std::terminate();
  impl_->primary_bytes_ += bytes;
  impl_->Trim();
  return PrimaryCharge(impl_, bytes);
}

void ReplicationHistory::ReclaimSecondary(std::size_t bytes) {
  std::lock_guard lock(impl_->mutex_);
  const auto target =
      impl_->secondary_bytes_ - std::min(bytes, impl_->secondary_bytes_);
  while (impl_->secondary_bytes_ > target) impl_->Evict();
}

bool ReplicationHistory::TryRetain(std::string_view history_id,
                                   std::vector<NativeHistoryRecord> records) {
  if (records.empty()) return false;
  std::ranges::sort(records, {}, &NativeHistoryRecord::flow_id_);
  std::size_t bytes = sizeof(Impl::Effect);
  // Account vector capacity, tree nodes and ownership overhead conservatively
  // alongside payload capacity. Short strings intentionally overestimate.
  constexpr std::size_t kIndexBytes = 96;
  if (records.capacity() > (std::numeric_limits<std::size_t>::max() - bytes) /
                               (sizeof(NativeHistoryRecord) + kIndexBytes))
    return false;
  bytes += records.capacity() * (sizeof(NativeHistoryRecord) + kIndexBytes);
  for (const auto& record : records) {
    if (record.canonical_.capacity() >
        std::numeric_limits<std::size_t>::max() - bytes)
      return false;
    bytes += record.canonical_.capacity();
  }
  std::lock_guard lock(impl_->mutex_);
  if (history_id != impl_->history_id_ || bytes > impl_->SecondaryCapacity())
    return false;
  for (std::size_t index = 0; index < records.size(); ++index) {
    const auto& record = records[index];
    if (record.flow_id_ >= impl_->flows_.size() || record.lsn_ == 0 ||
        record.lsn_ == std::numeric_limits<std::uint64_t>::max() ||
        record.canonical_.empty() ||
        (index != 0 && records[index - 1].flow_id_ == record.flow_id_) ||
        impl_->flows_[record.flow_id_].contains(record.lsn_))
      return false;
  }
  while (impl_->secondary_bytes_ > impl_->SecondaryCapacity() - bytes)
    impl_->Evict();
  auto reservation = TryReserveMemory(bytes);
  if (!reservation.has_value()) return false;
  auto effect = std::make_unique<Impl::Effect>();
  effect->records_ = std::move(records);
  effect->bytes_ = bytes;
  effect->charge_.Adopt(&*reservation, bytes);
  for (const auto& record : effect->records_)
    impl_->flows_[record.flow_id_].emplace(record.lsn_,
                                           Impl::Entry{&record, effect.get()});
  impl_->effects_.push_back(std::move(effect));
  impl_->secondary_bytes_ += bytes;
  return true;
}

std::vector<std::vector<NativeHistoryRange>> ReplicationHistory::Coverage(
    std::string_view history_id, std::size_t max_ranges_per_flow) const {
  std::lock_guard lock(impl_->mutex_);
  if (history_id != impl_->history_id_ || max_ranges_per_flow == 0 ||
      max_ranges_per_flow > 8)
    return {};
  std::vector<std::vector<NativeHistoryRange>> result(impl_->flows_.size());
  for (std::size_t flow = 0; flow < impl_->flows_.size(); ++flow) {
    auto& ranges = result[flow];
    // Bound discovery metadata independently of history size. Prefer the newest
    // actual intervals; omitted older ranges are optional missed recovery
    // paths.
    for (auto record = impl_->flows_[flow].rbegin();
         record != impl_->flows_[flow].rend(); ++record) {
      const auto lsn = record->first;
      if (!ranges.empty() && ranges.back().first_lsn_ == lsn + 1)
        ranges.back().first_lsn_ = lsn;
      else {
        if (ranges.size() == max_ranges_per_flow) break;
        ranges.push_back({lsn, lsn + 1});
      }
    }
    std::ranges::reverse(ranges);
  }
  return result;
}

absl::StatusOr<std::vector<NativeHistoryRecordInfo>>
ReplicationHistory::DescribeEffect(std::string_view history_id,
                                   unsigned flow_id, std::uint64_t lsn) const {
  std::lock_guard lock(impl_->mutex_);
  if (history_id != impl_->history_id_ || flow_id >= impl_->flows_.size())
    return absl::NotFoundError("retained history lineage is unavailable");
  const auto found = impl_->flows_[flow_id].find(lsn);
  if (found == impl_->flows_[flow_id].end())
    return absl::NotFoundError("retained history has a gap");
  std::vector<NativeHistoryRecordInfo> records;
  for (const auto& record : found->second.effect_->records_)
    records.push_back({record.flow_id_, record.lsn_, record.canonical_.size()});
  return records;
}

absl::StatusOr<NativeHistoryChunk> ReplicationHistory::Read(
    std::string_view history_id, unsigned flow_id, std::uint64_t lsn,
    std::size_t offset, std::size_t max_bytes) const {
  if (max_bytes == 0 || max_bytes > 64 * 1024)
    return absl::InvalidArgumentError("invalid retained-history chunk bound");
  std::lock_guard lock(impl_->mutex_);
  if (history_id != impl_->history_id_ || flow_id >= impl_->flows_.size()) {
    return absl::NotFoundError("retained history lineage is unavailable");
  }
  const auto record = impl_->flows_[flow_id].find(lsn);
  if (record == impl_->flows_[flow_id].end())
    return absl::NotFoundError("retained history has a gap");
  const auto& payload = record->second.record_->canonical_;
  if (offset >= payload.size())
    return absl::OutOfRangeError("retained event chunk offset is past its end");
  return NativeHistoryChunk{
      payload.size(),
      payload.substr(offset, std::min(max_bytes, payload.size() - offset))};
}
std::size_t ReplicationHistory::primary_bytes() const {
  std::lock_guard lock(impl_->mutex_);
  return impl_->primary_bytes_;
}
std::size_t ReplicationHistory::secondary_bytes() const {
  std::lock_guard lock(impl_->mutex_);
  return impl_->secondary_bytes_;
}

}  // namespace lavik
