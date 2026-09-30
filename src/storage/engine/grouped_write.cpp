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

#include "grouped_dependency_test_hook.h"
#include "impl.h"

namespace lavik::storage {

absl::StatusOr<std::shared_ptr<GroupedCommitDecision>>
StorageEngine::Impl::PrepareGroupedDecision(TxShardWrites& tx,
                                            bool local_completion) {
  if (tx.txid_ == 0 || tx.transaction_lease_ == nullptr) {
    return absl::InvalidArgumentError(
        "grouped mutation has no transaction lease");
  }
  if (tx.grouped_decision_ != nullptr) return tx.grouped_decision_;
  auto reservation = TryReserveMemory(
      AllocatorUsableSizeForRequest(sizeof(GroupedCommitDecision) + 1024));
  if (!reservation.has_value()) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError(
        "OOM grouped commit dependency exceeds maxmemory");
  }
  tx.grouped_decision_ = std::allocate_shared<GroupedCommitDecision>(
      RetainedAllocator<GroupedCommitDecision>(RetainedAllocationDomain{
          .owner_shard_ = CurrentMemoryAccountingShard(),
          .externally_admitted_ = true,
      }),
      tx.txid_,
      local_completion ? CurrentStore().worker_->id()
                       : GroupedCommitDecision::kRemoteCompletion);
  return tx.grouped_decision_;
}

Task<absl::Status> StorageEngine::Impl::AwaitGroupedDependencyLocked(
    WorkerStore& store, const GroupedHashObject::Handle& object,
    std::uint64_t successor_txid) {
  auto decision = object == nullptr ? nullptr : object->version().decision_;
  if (decision == nullptr || decision->txid_ == successor_txid) {
    co_return absl::OkStatus();
  }
  for (;;) {
    const auto state = decision->state_.load(std::memory_order_acquire);
    if (state == GroupedCommitDecision::State::kDurable)
      co_return absl::OkStatus();
    if (state == GroupedCommitDecision::State::kFailed || store.write_failed_) {
      co_return absl::FailedPreconditionError(
          "prior grouped transaction did not commit");
    }
    if (store.worker_->stop_requested())
      co_return absl::CancelledError("worker stopped before grouped commit");
    store.store_state_mutex_.Unlock(*store.worker_);
    if (decision->completion_owner_ == store.worker_->id()) {
      // No suspension separates the state check from waiter registration:
      // Unlock only enqueues another coroutine. The local commit queue wakes
      // after publishing either outcome, including a failed commit. Flushes
      // can also wake us before the decision, so always recheck its state.
#if LAVIK_FAULTS_ENABLED
      co_await GroupedDependencyTestWaiter(store.durability_progress_,
                                           decision->txid_);
#else
      co_await store.durability_progress_.Wait();
#endif
      co_await store.store_state_mutex_.Lock();
      continue;
    }
    const auto waited = co_await bycorf::SleepFor(
        *store.worker_, std::chrono::microseconds(50));
    co_await store.store_state_mutex_.Lock();
    if (!waited.ok()) co_return waited;
  }
}

Task<absl::StatusOr<HashGroupLocation>>
StorageEngine::Impl::WriteHashGroupRecordLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashGroupSnapshot& snapshot, HashGroupEncoder encoder,
    std::uint64_t sequence, TxShardWrites& tx, ValueType value_type,
    std::uint64_t batch_txid) {
  if (value_type != ValueType::kHash && value_type != ValueType::kSet &&
      value_type != ValueType::kSortedSet) {
    co_return absl::InvalidArgumentError(
        "invalid prefix-group collection type");
  }
  // The fixed inline threshold leaves room for the auxiliary identity in
  // the bounded record header; long parent keys contribute only their UUID.
  const bool key_indirect = key.size() > kInlineKeyMaxBytes;
  if (!ValidRecordKeySize(key.size()) ||
      encoder.encoded_bytes() > kMaxRecordPayloadBytes) {
    co_return absl::OutOfRangeError(
        "group snapshot exceeds the record payload limit");
  }
  const std::size_t inline_bytes = AlignRecord(
      RecordHeaderBytes(key.size(), key_indirect, true, false, true) +
      encoder.encoded_bytes());
  const bool external = inline_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
                        inline_bytes > options_.buffers_.write_buffer_bytes_;
  ExtentManifest extents;
  std::string payload;
  if (external) {
    RecordPayloadCursor cursor(encoder, std::string_view{});
    auto written = co_await WriteExtentValueLocked(store, {}, {}, &cursor, key);
    if (!written.ok()) co_return written.status();
    extents = std::move(*written);
    payload = EncodeManifest(*extents);
    LAVIK_MAYBE_CRASH_AT("group-extents-durable-before-record");
  } else {
    payload.resize(encoder.encoded_bytes());
    RecordPayloadCursor cursor(encoder);
    auto encoded = cursor.Read(std::as_writable_bytes(std::span(payload)));
    if (encoded.ok()) encoded = cursor.Finish();
    if (!encoded.ok()) co_return encoded;
  }
  const GroupRecordWrite identity{
      .auxiliary_ = true,
      .incarnation_ = snapshot.incarnation_,
      .id_ = snapshot.id_,
      .retired_ = snapshot.retired_,
      .batch_txid_ = batch_txid,
      .prepared_root_ = nullptr,
      .publication_ = nullptr,
      .prepare_root_ = {},
      .changed_groups_ = {},
      .root_incarnation_ = 0,
  };
  RecordLocation location;
  const RecordWriteRequest record_write{
      .key_ = key,
      .value_ = payload,
      .digest_ = digest,
      .mutation_sequence_ = sequence,
      .logical_size_ = snapshot.field_count(),
      .written_location_ = &location,
      .tx_ = &tx,
      .known_partition_ = &partition,
      .group_ = &identity,
      .db_id_ = db_id,
      .value_type_ = value_type,
      .external_ = external,
      .key_indirect_ = key_indirect,
  };
  auto written = co_await WriteRecordLocked(store, record_write, extents);
  if (!written.ok()) {
    if (extents != nullptr) SpawnExtentReclaim(store, extents);
    co_return written;
  }
  LAVIK_MAYBE_CRASH_AT("group-record-staged-before-root");
  co_return HashGroupLocation{
      .id_ = snapshot.id_,
      .location_ = location,
      .extents_ = std::move(extents),
      .retired_ = snapshot.retired_,
  };
}

}  // namespace lavik::storage
