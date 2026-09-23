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

#include "impl.h"

namespace lavik::storage {
namespace {

OrderedCollectionRoot StringRoot(std::size_t bytes, std::uint64_t incarnation) {
  const auto groups = (bytes + kStringGroupBytes - 1) / kStringGroupBytes;
  return {.kind_ = OrderedCollectionKind::kString,
          .incarnation_ = incarnation,
          .item_count_ = bytes,
          .first_group_ = 1,
          .last_group_ = groups,
          .next_group_id_ = groups + 1,
          .group_count_ = static_cast<std::uint32_t>(groups)};
}

OrderedGroupSnapshot StringPage(const OrderedCollectionRoot& root,
                                std::uint64_t id, std::string bytes) {
  return {.kind_ = OrderedCollectionKind::kString,
          .incarnation_ = root.incarnation_,
          .id_ = id,
          .previous_ = id - 1,
          .next_ = id == root.last_group_ ? 0 : id + 1,
          .entries_ = {{.value_ = std::move(bytes)}}};
}

}  // namespace

Task<absl::Status> StorageEngine::Impl::WriteGroupedStringLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::string_view value, std::uint64_t expire_at_ms, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition,
    GroupedHashObject::Handle previous, std::string_view before) {
  try {
    if (value.empty() || value.size() > kMaxStringBytes)
      co_return absl::OutOfRangeError("invalid grouped String length");
    // Whole-value shrink can bypass segment preparation altogether. A String
    // compact image is its raw bytes, so this threshold has no codec step.
    if (previous && value.size() < kCollectionGroupTargetBytes) {
      co_return co_await AppendLocked(
          store, partition, db_id, key, digest, value, RecordKind::kValue,
          ValueType::kString, expire_at_ms, tx, value.size(), nullptr, nullptr,
          replication, nullptr, true, nullptr, mutation_precondition);
    }
    if (previous && (before.size() != previous->version().root_.logical_size_ ||
                     value.size() < before.size()))
      previous.reset();
    // Whole replacements allocate all segments privately; incremental callbacks
    // retain only changed segment bodies. Admission covers both possibilities.
    auto admission = TryReserveMemory(
        value.size() +
        ((value.size() + kStringGroupBytes - 1) / kStringGroupBytes) * 512);
    if (!admission) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM preparing String segments");
    }
    OrderedCollectionMutationPlan plan{
        .root_ =
            StringRoot(value.size(), previous ? previous->incarnation() : 1),
        .expected_sequence_ = previous ? previous->revision() : 0,
        .changed_ = true,
        .writes_ = {}};
    for (std::size_t offset = 0; offset < value.size();
         offset += kStringGroupBytes) {
      const auto bytes = value.substr(offset, kStringGroupBytes);
      const auto id = offset / kStringGroupBytes + 1;
      // Growing past a full tail changes its next link even if its bytes match.
      if (previous && offset < before.size() &&
          before.substr(offset, kStringGroupBytes) == bytes &&
          !(id == previous->group_count() && id != plan.root_.last_group_))
        continue;
      plan.writes_.push_back(StringPage(plan.root_, id, std::string(bytes)));
    }
    if (plan.writes_.empty())
      co_return co_await UpdateGroupedExpirationLocked(
          store, partition, db_id, key, digest, previous, expire_at_ms, tx,
          replication, mutation_precondition);
    co_return co_await CommitGroupedOrderedMutationLocked(
        store, partition, db_id, key, digest, previous, std::move(plan),
        expire_at_ms, tx, replication, mutation_precondition);
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "OOM preparing grouped String write");
  }
}

Task<absl::StatusOr<StringSegmentResult>>
StorageEngine::ExecuteStringSegmentLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const StringSegmentOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteStringSegmentLocked(db_id, key, digest, operation, tx,
                                           replication, mutation_precondition);
}

Task<absl::StatusOr<StringSegmentResult>>
StorageEngine::Impl::ExecuteStringSegmentLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const StringSegmentOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  try {
    using Kind = StringSegmentOperation::Kind;
    const bool read_only =
        operation.kind_ == Kind::kReadRange || operation.kind_ == Kind::kGetBit;
    const bool bit =
        operation.kind_ == Kind::kGetBit || operation.kind_ == Kind::kSetBit;
    if (db_id >= options_.database_count_ || digest != ComputeDigest(key) ||
        (operation.kind_ != Kind::kReadRange && operation.start_ < 0))
      co_return absl::InvalidArgumentError("invalid String segment operation");
    auto& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    auto resolved = co_await FindVerifiedEntry(store, partition.indexes_[db_id],
                                               digest, key);
    if (!resolved.ok()) co_return resolved.status();
    auto* found = *resolved;
    GroupedHashObject::Handle grouped;
    if (found && found->value_.grouped()) {
      auto object = partition.grouped_objects_[db_id].Lookup(
          key, GroupedObjectVersion{
                   .root_ = MaterializeIndexLocation(*found),
                   .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
                   .replication_epoch_ = partition.replication_epoch_,
                   .index_generation_ = partition.grouped_generations_[db_id]});
      if (!object.ok()) co_return object.status();
      grouped = std::move(*object);
    }
    const bool exists = found && found->value_.kind() == RecordKind::kValue &&
                        !IsExpiredNow(*found);
    if (exists && found->value_.value_type() != ValueType::kString)
      co_return absl::InvalidArgumentError(
          "WRONGTYPE Operation against a key holding the wrong kind of value");
    if (!exists) grouped.reset();
    const auto location =
        exists ? MaterializeIndexLocation(*found) : RecordLocation{};
    const std::uint64_t old_size = exists ? location.logical_size_ : 0;
    StringSegmentResult result{.value_ = {}, .length_ = old_size};
    std::uint64_t start = bit ? static_cast<std::uint64_t>(operation.start_) / 8
                              : static_cast<std::uint64_t>(operation.start_);
    std::uint64_t count = bit ? 1 : operation.value_.size();
    if (operation.kind_ == Kind::kAppend) start = old_size;
    if (operation.kind_ == Kind::kReadRange) {
      auto first = operation.start_;
      auto last = operation.stop_;
      if (old_size == 0 || (first < 0 && last < 0 && first > last))
        co_return result;
      if (first < 0)
        first = std::max<std::int64_t>(
            0, first + static_cast<std::int64_t>(old_size));
      if (last < 0)
        last = std::max<std::int64_t>(
            0, last + static_cast<std::int64_t>(old_size));
      if (first > last || static_cast<std::uint64_t>(first) >= old_size)
        co_return result;
      start = first;
      count = std::min<std::uint64_t>(last, old_size - 1) - start + 1;
    }
    if (read_only && start >= old_size) co_return result;
    // Empty SETRANGE never creates a key, even for an offset past the limit.
    // APPEND of an empty string does create one.
    if (operation.kind_ == Kind::kWriteRange && count == 0) co_return result;
    if (!read_only &&
        (start > kMaxStringBytes || count > kMaxStringBytes - start))
      co_return absl::OutOfRangeError(
          "string exceeds maximum allowed size (proto-max-bulk-len)");
    if (grouped && operation.kind_ == Kind::kAppend && count == 0) {
      const auto status = co_await UpdateGroupedExpirationLocked(
          store, partition, db_id, key, digest, grouped, location.expire_at_ms_,
          tx, replication, mutation_precondition);
      if (!status.ok()) co_return status;
      result.changed_ = true;
      co_return result;
    }
    const auto new_size =
        read_only ? old_size : std::max(old_size, start + count);
    // Extending past the end materializes zero-filled intervening segments.
    const auto first_id = std::min(start, old_size) / kStringGroupBytes + 1;
    const auto last_id =
        count == 0 ? first_id : (start + count - 1) / kStringGroupBytes + 1;
    const auto scratch_bytes =
        grouped ? (last_id - first_id + 2) * (kStringGroupBytes + 512) +
                      (read_only ? count : 0)
                : std::max(old_size, new_size) * 2 + 1024;
    auto admission = TryReserveMemory(scratch_bytes);
    if (!admission) {
      RecordMemoryRejection();
      co_return absl::ResourceExhaustedError("OOM String segment workspace");
    }
    auto apply = [&](std::string& bytes, std::uint64_t base) {
      if (bit) {
        const auto offset = start - base;
        const auto mask = 1U << (7 - operation.start_ % 8);
        result.bit_ = (static_cast<unsigned char>(bytes[offset]) & mask) != 0;
        if (!read_only)
          bytes[offset] = static_cast<char>(
              operation.bit_
                  ? static_cast<unsigned char>(bytes[offset]) | mask
                  : static_cast<unsigned char>(bytes[offset]) & ~mask);
      } else {
        const auto begin = std::max(start, base);
        const auto end = std::min(start + count, base + bytes.size());
        if (end <= begin) return;
        if (read_only)
          result.value_.append(bytes, begin - base, end - begin);
        else
          bytes.replace(begin - base, end - begin,
                        operation.value_.substr(begin - start, end - begin));
      }
    };
    if (!grouped) {
      std::string bytes;
      if (exists) {
        auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                         location, ExtentsFor(store, found));
        if (!loaded.ok()) co_return loaded.status();
        auto data = loaded->value();
        bytes.assign(reinterpret_cast<const char*>(data.data()), data.size());
      }
      if (!read_only) bytes.resize(new_size, '\0');
      apply(bytes, 0);
      if (operation.kind_ == Kind::kSetBit && start < old_size &&
          result.bit_ == operation.bit_)
        co_return result;
      if (!read_only) {
        const auto status = co_await AppendLocked(
            store, partition, db_id, key, digest, bytes, RecordKind::kValue,
            ValueType::kString, exists ? location.expire_at_ms_ : 0, tx,
            bytes.size(), nullptr, nullptr, replication, nullptr, true, nullptr,
            mutation_precondition);
        if (!status.ok()) co_return status;
      }
    } else {
      OrderedCollectionMutationPlan plan{
          .root_ = StringRoot(new_size, grouped->incarnation()),
          .expected_sequence_ = grouped->revision(),
          .changed_ = !read_only,
          .writes_ = {}};
      if (read_only) result.value_.reserve(bit ? 0 : count);
      auto begin_id = first_id;
      // A full old tail must acquire its next link when appending a new
      // segment.
      if (!read_only && new_size > old_size &&
          old_size % kStringGroupBytes == 0)
        begin_id = std::min<std::uint64_t>(begin_id, grouped->group_count());
      for (auto id = begin_id; id <= last_id; ++id) {
        const auto base = (id - 1) * kStringGroupBytes;
        std::string bytes;
        if (id <= grouped->group_count()) {
          auto loaded = co_await LoadOrderedGroupSnapshot(
              store, partition, db_id, key, digest, grouped, id);
          if (!loaded.ok()) co_return loaded.status();
          bytes = std::move(loaded->snapshot_.entries_.front().value_);
        }
        if (!read_only)
          bytes.resize(
              std::min<std::uint64_t>(kStringGroupBytes, new_size - base),
              '\0');
        if (!bit || (start >= base && start < base + bytes.size()))
          apply(bytes, base);
        if (operation.kind_ == Kind::kSetBit && start < old_size &&
            result.bit_ == operation.bit_)
          co_return result;
        if (!read_only)
          plan.writes_.push_back(StringPage(plan.root_, id, std::move(bytes)));
      }
      if (!read_only) {
        const auto status = co_await CommitGroupedOrderedMutationLocked(
            store, partition, db_id, key, digest, grouped, std::move(plan),
            location.expire_at_ms_, tx, replication, mutation_precondition);
        if (!status.ok()) co_return status;
      }
    }
    result.length_ = new_size;
    result.changed_ = !read_only;
    co_return result;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError("OOM String segment operation");
  }
}

}  // namespace lavik::storage
