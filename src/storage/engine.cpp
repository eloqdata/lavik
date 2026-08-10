#include "keylane/storage/engine.h"

#include "engine/impl.h"

namespace keylane::storage {

StorageEngine::StorageEngine(StorageEngineOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

StorageEngine::~StorageEngine() = default;

absl::Status StorageEngine::Prepare(unsigned worker_count) {
  return impl_->Prepare(worker_count);
}

Task<absl::Status> StorageEngine::InitializeWorker(Worker& worker) {
  return impl_->InitializeWorker(worker);
}

absl::Status StorageEngine::FlushForShutdown() {
  return impl_->FlushForShutdown();
}

unsigned StorageEngine::OwnerForKey(std::string_view key) const noexcept {
  return impl_->OwnerForKey(key);
}

unsigned StorageEngine::worker_count() const noexcept {
  return impl_->worker_count();
}

std::size_t StorageEngine::LocalSize(std::uint8_t db_id) const noexcept {
  return impl_->LocalSize(db_id);
}

Task<absl::StatusOr<ScanBatch>> StorageEngine::ScanPartition(
    std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
    std::size_t count, std::uint64_t now_ms, std::size_t max_bytes) {
  return impl_->ScanPartition(partition_id, db_id, cursor, count, now_ms,
                              max_bytes);
}

Task<absl::Status> StorageEngine::QuiesceExpiration() {
  return impl_->QuiesceExpiration();
}

void StorageEngine::ResumeExpiration() noexcept { impl_->ResumeExpiration(); }

TombRaiderTotals StorageEngine::TombRaiderStats() const noexcept {
  return impl_->TombRaiderStats();
}

Task<absl::Status> StorageEngine::ConfigureTombRaider(
    TombRaiderConfigUpdate update) {
  return impl_->ConfigureTombRaider(update);
}

DefragTotals StorageEngine::DefragStats() const noexcept {
  return impl_->DefragStats();
}

Task<absl::Status> StorageEngine::ConfigureDefrag(
    DefragConfigUpdate update) {
  return impl_->ConfigureDefrag(update);
}

Task<StorageMetricsSnapshot> StorageEngine::CollectMetrics() const {
  return impl_->CollectMetrics();
}

Task<absl::Status> StorageEngine::FlushDbDetach(std::uint8_t db_id) {
  return impl_->FlushDbDetach(db_id);
}

Task<absl::Status> StorageEngine::FlushDbReclaim(bool wait) {
  return impl_->FlushDbReclaim(wait);
}

std::uint64_t StorageEngine::DbEpoch(std::uint8_t db_id) const noexcept {
  return impl_->DbEpoch(db_id);
}

PartitionReplicationStart StorageEngine::BeginPartitionReplication(
    std::uint16_t partition_id) {
  return impl_->BeginPartitionReplication(partition_id);
}

Task<absl::StatusOr<PartitionSnapshotBatch>> StorageEngine::SnapshotPartition(
    std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
    std::size_t count) {
  return impl_->SnapshotPartition(partition_id, db_id, cursor, count);
}

PartitionDeltaBatch StorageEngine::ReadPartitionDeltas(
    std::uint16_t partition_id, std::uint64_t after_sequence,
    std::size_t count) {
  return impl_->ReadPartitionDeltas(partition_id, after_sequence, count);
}

bool StorageEngine::TryTakeReplicationReady(std::uint16_t* partition_id) {
  return impl_->TryTakeReplicationReady(partition_id);
}

void StorageEngine::AcknowledgePartitionDeltas(std::uint16_t partition_id,
                                               std::uint64_t through_sequence) {
  impl_->AcknowledgePartitionDeltas(partition_id, through_sequence);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::ResetReplicaPartition(
    std::uint16_t partition_id,
    std::span<const std::uint64_t, 16> source_db_epochs) {
  return impl_->ResetReplicaPartition(partition_id, source_db_epochs);
}

Task<absl::Status> StorageEngine::ApplyReplicaRecords(
    std::uint16_t partition_id, std::uint64_t replication_epoch,
    std::span<const SnapshotRecord> records) {
  return impl_->ApplyReplicaRecords(partition_id, replication_epoch, records);
}

Task<absl::StatusOr<DiskValue>> StorageEngine::Get(std::uint8_t db_id,
                                                   std::string_view key,
                                                   ReadLatencyTrace* trace) {
  return impl_->Get(db_id, key, trace);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::StringLength(
    std::uint8_t db_id, std::string_view key) {
  return impl_->StringLength(db_id, key);
}

Task<absl::StatusOr<SetResult>> StorageEngine::Set(std::uint8_t db_id,
                                                   std::string_view key,
                                                   std::string_view value,
                                                   SetOptions options) {
  return impl_->Set(db_id, key, value, options);
}

Task<ExpirationInfo> StorageEngine::GetExpiration(std::uint8_t db_id,
                                                  std::string_view key) {
  return impl_->GetExpiration(db_id, key);
}

Task<absl::StatusOr<bool>> StorageEngine::UpdateExpiration(
    std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
    ExpirationCondition condition) {
  return impl_->UpdateExpiration(db_id, key, expire_at_ms, condition);
}

Task<absl::StatusOr<bool>> StorageEngine::Delete(std::uint8_t db_id,
                                                 std::string_view key) {
  return impl_->Delete(db_id, key);
}

Task<bool> StorageEngine::Exists(std::uint8_t db_id, std::string_view key) {
  return impl_->Exists(db_id, key);
}

Task<absl::StatusOr<std::int64_t>> StorageEngine::Increment(
    std::uint8_t db_id, std::string_view key) {
  return impl_->Increment(db_id, key);
}

Task<absl::StatusOr<DiskValue>> StorageEngine::GetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    ReadLatencyTrace* trace) {
  return impl_->GetLocked(db_id, key, digest, trace);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::StringLengthLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  return impl_->StringLengthLocked(db_id, key, digest);
}

Task<absl::StatusOr<SetResult>> StorageEngine::SetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::string_view value, SetOptions options, TxShardWrites* tx) {
  return impl_->SetLocked(db_id, key, digest, value, options, tx);
}

Task<ExpirationInfo> StorageEngine::GetExpirationLocked(std::uint8_t db_id,
                                                        std::string_view key,
                                                        const Digest& digest) {
  return impl_->GetExpirationLocked(db_id, key, digest);
}

Task<absl::StatusOr<bool>> StorageEngine::UpdateExpirationLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::uint64_t expire_at_ms, ExpirationCondition condition,
    TxShardWrites* tx) {
  return impl_->UpdateExpirationLocked(db_id, key, digest, expire_at_ms,
                                       condition, tx);
}

Task<absl::StatusOr<bool>> StorageEngine::DeleteLocked(std::uint8_t db_id,
                                                       std::string_view key,
                                                       const Digest& digest,
                                                       TxShardWrites* tx) {
  return impl_->DeleteLocked(db_id, key, digest, tx);
}

Task<bool> StorageEngine::ExistsLocked(std::uint8_t db_id, std::string_view key,
                                       const Digest& digest) {
  return impl_->ExistsLocked(db_id, key, digest);
}

Task<absl::StatusOr<std::int64_t>> StorageEngine::IncrementLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    TxShardWrites* tx) {
  return impl_->IncrementLocked(db_id, key, digest, tx);
}

Task<absl::Status> StorageEngine::CommitTxWrites(
    std::uint64_t txid, std::vector<TxShardWrites*> shards) {
  return impl_->CommitTxWrites(txid, std::move(shards));
}

std::uint64_t StorageEngine::AllocateWriteTxid() noexcept {
  return tx::TxRuntime::Get()->next_txid_.fetch_add(1,
                                                    std::memory_order_relaxed);
}

void StorageEngine::NoteTxCommitStarted() noexcept {
  impl_->NoteTxCommitStarted();
}

void StorageEngine::NoteTxCommitFinished() noexcept {
  impl_->NoteTxCommitFinished();
}

Task<absl::Status> StorageEngine::RollbackTxLocal(std::uint64_t txid) {
  return impl_->RollbackTxLocal(txid);
}

Task<absl::Status> StorageEngine::DiscardTxUndoLocal(std::uint64_t txid) {
  return impl_->DiscardTxUndoLocal(txid);
}

Task<bool> StorageEngine::KeyLive(std::uint8_t db_id, std::string_view key,
                                  const Digest& digest) {
  return impl_->KeyLive(db_id, key, digest);
}

}  // namespace keylane::storage
