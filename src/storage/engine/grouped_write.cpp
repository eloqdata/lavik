#include "impl.h"

namespace keylane::storage {

absl::StatusOr<std::shared_ptr<GroupedCommitDecision>>
StorageEngine::Impl::PrepareGroupedDecision(TxShardWrites& tx) {
  if (tx.txid_ == 0 || tx.generation_ == 0 || tx.generation_lease_ == nullptr) {
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
      tx.txid_);
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
    store.store_state_mutex_.Unlock(*store.worker_);
    const auto waited =
        co_await celer::SleepFor(*store.worker_, std::chrono::microseconds(50));
    co_await store.store_state_mutex_.Lock();
    if (!waited.ok()) co_return waited;
  }
}

Task<absl::StatusOr<HashGroupLocation>>
StorageEngine::Impl::WriteHashGroupRecordLocked(
    WorkerStore& store, WorkerStore::PartitionStore& partition,
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashGroupSnapshot& snapshot, std::uint64_t sequence,
    TxShardWrites& tx, ValueType value_type, std::uint64_t batch_txid) {
  if (value_type != ValueType::kHash && value_type != ValueType::kSet &&
      value_type != ValueType::kSortedSet) {
    co_return absl::InvalidArgumentError(
        "invalid prefix-group collection type");
  }
  auto encoder = HashGroupEncoder::Create(snapshot);
  if (!encoder.ok()) co_return encoder.status();
  // An auxiliary identity adds 32 bytes to the optional ordinary header.
  // Decide key externalization with that framing included, not the ordinary
  // key threshold alone, or a boundary-length key would overrun one page.
  const bool key_external = key.size() > options_.inline_key_max_bytes_ ||
                            RecordHeaderBytes(key.size(), false, true, false,
                                              true) > kBlockHeaderSlotBytes;
  const std::size_t prefix_bytes = key_external ? key.size() : 0;
  if (!ValidRecordKeySize(key.size()) ||
      prefix_bytes > kMaxRecordPayloadBytes - encoder->encoded_bytes()) {
    co_return absl::OutOfRangeError(
        "group snapshot and external key exceed the record payload limit");
  }
  const std::size_t inline_bytes = AlignRecord(
      RecordHeaderBytes(key.size(), key_external, true, false, true) +
      prefix_bytes + encoder->encoded_bytes());
  const bool external = inline_bytes > kStorageBlockBytes - kBlockHeaderBytes ||
                        inline_bytes > options_.buffers_.write_buffer_bytes_;
  ExtentManifest extents;
  std::string payload;
  if (external) {
    RecordPayloadCursor cursor(*encoder,
                               key_external ? key : std::string_view{});
    auto written = co_await WriteExtentValueLocked(store, {}, {}, &cursor);
    if (!written.ok()) co_return written.status();
    extents = std::move(*written);
    payload = EncodeManifest(*extents);
    KEYLANE_MAYBE_CRASH_AT("group-extents-durable-before-record");
  } else {
    payload.resize(encoder->encoded_bytes());
    RecordPayloadCursor cursor(*encoder);
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
  };
  RecordLocation location;
  auto written = co_await WriteRecordLocked(
      store, db_id, key, payload, RecordKind::kValue, value_type, 0, digest,
      tx.txid_, sequence, false, true, external, key_external,
      snapshot.value_.entries_.size(), extents, &location, nullptr, &tx,
      nullptr, nullptr, nullptr, nullptr, &partition, &identity);
  if (!written.ok()) {
    if (extents != nullptr) SpawnExtentReclaim(store, extents);
    co_return written;
  }
  KEYLANE_MAYBE_CRASH_AT("group-record-staged-before-root");
  co_return HashGroupLocation{
      .id_ = snapshot.id_,
      .location_ = location,
      .extents_ = std::move(extents),
      .retired_ = snapshot.retired_,
  };
}

}  // namespace keylane::storage
