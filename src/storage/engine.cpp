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

#include "lavik/storage/engine.h"

#include "engine/impl.h"

namespace lavik::storage {

StorageEngine::StorageEngine(StorageEngineOptions options)
    : impl_(std::make_unique<Impl>(std::move(options))) {}

StorageEngine::~StorageEngine() = default;

absl::Status StorageEngine::Prepare(unsigned worker_count) {
  return impl_->Prepare(worker_count);
}

Task<absl::Status> StorageEngine::InitializeWorker(Worker& worker) {
  return impl_->InitializeWorker(worker);
}

void StorageEngine::FinalizeWorker(Worker& worker) noexcept {
  impl_->FinalizeWorker(worker.id());
}

absl::Status StorageEngine::FlushForShutdown() {
  return impl_->FlushForShutdown();
}

bool StorageEngine::ShutdownCheckpointEnabled() const noexcept {
  return impl_->ShutdownCheckpointEnabled();
}

void StorageEngine::ConfigureShutdownCheckpoint(bool enabled) noexcept {
  impl_->ConfigureShutdownCheckpoint(enabled);
}

bool StorageEngine::AbandonWorkerStateForProcessExit() noexcept {
  return impl_->AbandonWorkerStateForProcessExit();
}

Task<absl::StatusOr<CatalogDurabilityToken>>
StorageEngine::CommitFunctionCatalog(std::string_view dump) {
  return impl_->CommitFunctionCatalog(dump);
}

absl::StatusOr<std::optional<RecoveredFunctionCatalog>>
StorageEngine::RecoverFunctionCatalog() const {
  return impl_->RecoverFunctionCatalog();
}

Task<absl::Status> StorageEngine::MakeDurable(
    const DurabilityFrontier& frontier, std::string_view opaque_accumulator) {
  return impl_->MakeDurable(frontier, opaque_accumulator);
}

Task<absl::Status> StorageEngine::CommitPromotionBase(PromotionBase base) {
  return impl_->CommitPromotionBase(std::move(base));
}

absl::StatusOr<std::optional<PromotionBase>>
StorageEngine::RecoverPromotionBase() const {
  return impl_->RecoverPromotionBase();
}

Task<absl::Status> StorageEngine::CommitPopulationIdentity(
    std::string identity) {
  return impl_->CommitPopulationIdentity(std::move(identity));
}

Task<absl::StatusOr<std::optional<PopulationRecoveryRecord>>>
StorageEngine::ConsumePopulationRecovery() {
  return impl_->ConsumePopulationRecovery();
}

void StorageEngine::StageCleanShutdownProof(std::string proof) {
  impl_->StageCleanShutdownProof(std::move(proof));
}

absl::StatusOr<PopulationToken> StorageEngine::RecoverPopulationToken() const {
  return impl_->RecoverPopulationToken();
}

Task<absl::Status> StorageEngine::BeginReplicaFullSync(
    std::uint64_t session_id) {
  return impl_->BeginReplicaFullSync(session_id);
}

Task<absl::Status> StorageEngine::CompleteReplicaFullSync(
    std::uint64_t session_id, PopulationToken population) {
  return impl_->CompleteReplicaFullSync(session_id, population);
}

bool StorageEngine::ReplicaRecoveryFenced() const noexcept {
  return impl_->ReplicaRecoveryFenced();
}

void StorageEngine::FenceRequestServingUntilRestart() noexcept {
  impl_->FenceRequestServingUntilRestart();
}

bool StorageEngine::RuntimeFailureLatched() const noexcept {
  return impl_->RuntimeFailureLatched();
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

absl::Status StorageEngine::BeginRdbSnapshot(std::uint64_t session_id,
                                             std::uint64_t snapshot_time_ms) {
  return impl_->BeginRdbSnapshot(session_id, snapshot_time_ms);
}

Task<absl::StatusOr<RdbSnapshotBatch>> StorageEngine::ReadRdbSnapshotBatch(
    std::uint64_t session_id, RdbSnapshotCursor cursor, std::size_t count,
    std::size_t max_bytes) {
  return impl_->ReadRdbSnapshotBatch(session_id, cursor, count, max_bytes);
}

Task<absl::Status> StorageEngine::EndRdbSnapshot(std::uint64_t session_id) {
  return impl_->EndRdbSnapshot(session_id);
}

Task<absl::StatusOr<CollectionPage>> StorageEngine::ReadRdbCollectionPage(
    std::uint64_t session_id, std::uint64_t token, std::uint64_t cursor) {
  return impl_->ReadRdbCollectionPage(session_id, token, cursor);
}

Task<absl::Status> StorageEngine::FinishRdbCollection(std::uint64_t session_id,
                                                      std::uint64_t token) {
  return impl_->FinishRdbCollection(session_id, token);
}

Task<absl::StatusOr<std::optional<std::string>>> StorageEngine::RandomKeyLocal(
    std::uint8_t db_id) {
  return impl_->RandomKeyLocal(db_id);
}

ScanPartitionAwaitable StorageEngine::ScanPartition(
    std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor,
    std::size_t count, std::uint64_t now_ms, std::size_t max_bytes) {
  return impl_->ScanPartition(partition_id, db_id, cursor, count, now_ms,
                              max_bytes);
}

Task<absl::Status> StorageEngine::QuiesceExpiration() {
  return impl_->QuiesceExpiration();
}

void StorageEngine::ResumeExpiration() noexcept { impl_->ResumeExpiration(); }

void StorageEngine::SetExpirationAuthority(bool authority) noexcept {
  impl_->SetExpirationAuthority(authority);
}

absl::Status StorageEngine::SetExpirationAuthorityUntil(
    std::chrono::nanoseconds deadline_since_boot) noexcept {
  return impl_->SetExpirationAuthorityUntil(deadline_since_boot);
}

std::uint32_t StorageEngine::ExpirationPauseCount() const noexcept {
  return impl_->ExpirationPauseCount();
}

std::uint32_t StorageEngine::ActiveExpirationConfigValue(
    ActiveExpirationConfigKey key) const noexcept {
  return impl_->ActiveExpirationConfigValue(key);
}

absl::Status StorageEngine::ConfigureActiveExpiration(
    ActiveExpirationConfigKey key, std::uint64_t value) {
  return impl_->ConfigureActiveExpiration(key, value);
}

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

Task<absl::Status> StorageEngine::ConfigureDefrag(DefragConfigUpdate update) {
  return impl_->ConfigureDefrag(update);
}

TxCleanerTotals StorageEngine::TxCleanerStats() const noexcept {
  return impl_->TxCleanerStats();
}

std::uint32_t StorageEngine::TxCleanerCooldownMs() const noexcept {
  return impl_->TxCleanerCooldownMs();
}

absl::Status StorageEngine::ConfigureTxCleanerCooldown(
    std::uint64_t cooldown_ms) {
  return impl_->ConfigureTxCleanerCooldown(cooldown_ms);
}

Task<StorageDurabilityStats> StorageEngine::DurabilityStats() const {
  return impl_->DurabilityStats();
}

Task<StorageMetricsSnapshot> StorageEngine::CollectMetrics() const {
  return impl_->CollectMetrics();
}

Task<absl::Status> StorageEngine::FlushDbDetach(std::uint8_t db_id) {
  return impl_->FlushDbDetach(db_id);
}

Task<absl::Status> StorageEngine::FlushAllDetach() {
  return impl_->FlushAllDetach();
}

Task<absl::Status> StorageEngine::FlushDbReclaim(bool wait) {
  return impl_->FlushDbReclaim(wait);
}

std::uint64_t StorageEngine::DbEpoch(std::uint8_t db_id) const noexcept {
  return impl_->DbEpoch(db_id);
}

Task<absl::Status> StorageEngine::PublishFlushDbReplication(
    std::uint8_t db_id, std::uint64_t db_epoch) {
  return impl_->PublishFlushDbReplication(db_id, db_epoch);
}

Task<absl::Status> StorageEngine::PublishFlushAllReplication(
    const std::array<std::uint64_t, kLogicalDatabaseCount>& db_epochs) {
  return impl_->PublishFlushAllReplication(db_epochs);
}

Task<absl::Status> StorageEngine::ApplyReplicatedFlushDb(
    std::uint8_t db_id, std::uint64_t db_epoch) {
  return impl_->ApplyReplicatedFlushDb(db_id, db_epoch);
}

Task<absl::Status> StorageEngine::ApplyReplicatedFlushAll(
    const std::array<std::uint64_t, kLogicalDatabaseCount>& db_epochs) {
  return impl_->ApplyReplicatedFlushAll(db_epochs);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::FenceReplicationLog() {
  return impl_->FenceReplicationLog();
}

absl::StatusOr<FullSyncSessionStart> StorageEngine::BeginFullSyncSession(
    std::uint64_t session_id) {
  return impl_->BeginFullSyncSession(session_id);
}

bool StorageEngine::FullSyncSessionValid(
    std::uint64_t session_id) const noexcept {
  return impl_->FullSyncSessionValid(session_id);
}

void StorageEngine::EndFullSyncSession(std::uint64_t session_id) {
  impl_->EndFullSyncSession(session_id);
}

absl::StatusOr<PartitionReplicationStart>
StorageEngine::BeginPartitionReplication(std::uint64_t session_id,
                                         std::uint16_t partition_id) {
  return impl_->BeginPartitionReplication(session_id, partition_id);
}

absl::Status StorageEngine::BeginPartitionDbReplication(
    std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id) {
  return impl_->BeginPartitionDbReplication(session_id, partition_id, db_id);
}

void StorageEngine::EndPartitionReplication(std::uint64_t session_id,
                                            std::uint16_t partition_id) {
  impl_->EndPartitionReplication(session_id, partition_id);
}

Task<absl::StatusOr<PartitionSnapshotBatch>> StorageEngine::SnapshotPartition(
    std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id,
    std::uint64_t cursor, std::size_t count, std::size_t read_concurrency,
    std::size_t max_bytes) {
  return impl_->SnapshotPartition(session_id, partition_id, db_id, cursor,
                                  count, read_concurrency, max_bytes);
}

Task<absl::StatusOr<PartitionFullSyncBatch>>
StorageEngine::ReadPartitionFullSyncOverrides(std::uint64_t session_id,
                                              std::uint16_t partition_id,
                                              std::size_t count,
                                              std::size_t max_bytes) {
  return impl_->ReadPartitionFullSyncOverrides(session_id, partition_id, count,
                                               max_bytes);
}

Task<absl::StatusOr<SnapshotRecord>>
StorageEngine::MaterializeFullSyncPublishRecord(
    std::uint64_t session_id, std::uint16_t partition_id,
    const SnapshotRecord& requested) {
  return impl_->MaterializeFullSyncPublishRecord(session_id, partition_id,
                                                 requested);
}

Task<absl::StatusOr<std::string>> StorageEngine::ReadFullSyncValueChunk(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t source_id, std::uint64_t offset, std::size_t max_bytes) {
  return impl_->ReadFullSyncValueChunk(session_id, partition_id, source_id,
                                       offset, max_bytes);
}

void StorageEngine::ReleaseFullSyncValue(std::uint64_t session_id,
                                         std::uint16_t partition_id,
                                         std::uint64_t source_id) {
  impl_->ReleaseFullSyncValue(session_id, partition_id, source_id);
}

Task<absl::Status> StorageEngine::EnableReplicationLog(
    std::uint64_t log_epoch, std::size_t capacity_bytes) {
  return impl_->EnableReplicationLog(log_epoch, capacity_bytes);
}

Task<absl::Status> StorageEngine::SetReplicationLogCapacity(
    std::size_t capacity_bytes) {
  return impl_->SetReplicationLogCapacity(capacity_bytes);
}

Task<absl::Status> StorageEngine::SetReplicationBacklogBackpressure(
    bool enabled) {
  return impl_->SetReplicationBacklogBackpressure(enabled);
}

Task<absl::Status> StorageEngine::SetReplicationPublishQueueCapacity(
    std::size_t capacity_bytes) {
  return impl_->SetReplicationPublishQueueCapacity(capacity_bytes);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::AppendReplicationLog(
    ReplicationLogAppend event) {
  return impl_->AppendReplicationLog(event);
}

Task<absl::StatusOr<ReplicationLogBatch>> StorageEngine::ReadReplicationLog(
    ReplicationLogCursor next, std::size_t max_bytes, std::size_t max_frames) {
  return impl_->ReadReplicationLog(next, max_bytes, max_frames);
}

absl::Status StorageEngine::RetainReplicationLog(std::uint64_t session_id,
                                                 std::uint64_t keep_from_lsn) {
  return impl_->RetainReplicationLog(session_id, keep_from_lsn);
}

void StorageEngine::ReleaseReplicationLogRetention(std::uint64_t session_id) {
  impl_->ReleaseReplicationLogRetention(session_id);
}

Task<absl::Status> StorageEngine::TrimReplicationLog(
    std::uint64_t keep_from_lsn) {
  return impl_->TrimReplicationLog(keep_from_lsn);
}

Task<absl::Status> StorageEngine::DisableReplicationLog() {
  return impl_->DisableReplicationLog();
}

ReplicationLogInfo StorageEngine::LocalReplicationLogInfo() const {
  return impl_->LocalReplicationLogInfo();
}

bool StorageEngine::ReplicationLogActive() const noexcept {
  return impl_->ReplicationLogActive();
}

Task<absl::StatusOr<ReplicationPublisherAdmission>>
StorageEngine::AcquireReplicationPublisherAdmission(
    std::size_t logical_bytes,
    std::optional<ReplicationPublisherTarget> target) {
  return impl_->AcquireReplicationPublisherAdmission(logical_bytes, target);
}

void StorageEngine::ReleaseReplicationPublisherAdmission(
    const ReplicationPublisherAdmission& admission, std::size_t logical_bytes) {
  impl_->ReleaseReplicationPublisherAdmission(admission, logical_bytes);
}

bool StorageEngine::TryEnqueueReplicationCommand(
    ReplicationCommandAppend command) {
  return impl_->TryEnqueueReplicationCommand(std::move(command));
}

Task<absl::Status> StorageEngine::PublishEphemeralReplicationCommand(
    std::uint16_t partition_id, std::vector<std::string> args,
    MutationPrecondition mutation_precondition) {
  return impl_->PublishEphemeralReplicationCommand(
      partition_id, std::move(args), std::move(mutation_precondition));
}

absl::StatusOr<PreparedReplicationCommandPublication>
StorageEngine::PrepareAdmittedReplicationCommand(
    const ReplicationPublisherAdmission& admission, ReplicationEventKind kind,
    std::uint16_t partition_id, std::vector<std::string> args,
    std::optional<std::vector<std::string>> fullsync_projection) {
  return impl_->PrepareAdmittedReplicationCommand(
      admission, kind, partition_id, std::move(args),
      std::move(fullsync_projection));
}

absl::Status StorageEngine::PublishPreparedReplicationCommand(
    const ReplicationPublisherAdmission& admission,
    PreparedReplicationCommandPublication publication) {
  return impl_->PublishPreparedReplicationCommand(admission,
                                                  std::move(publication));
}

absl::Status StorageEngine::PublishLateAdmittedReplicationCommand(
    const ReplicationPublisherAdmission& admission, ReplicationEventKind kind,
    std::uint16_t partition_id, std::vector<std::string> args,
    std::optional<std::vector<std::string>> fullsync_projection) {
  return impl_->PublishLateAdmittedReplicationCommand(
      admission, kind, partition_id, std::move(args),
      std::move(fullsync_projection));
}

bool StorageEngine::TryEnqueueReplicationTransaction(
    std::shared_ptr<ReplicationTransaction> transaction) {
  return impl_->TryEnqueueReplicationTransaction(std::move(transaction));
}

void StorageEngine::AcknowledgePartitionFullSyncOverrides(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::span<const SnapshotRecord> records) {
  impl_->AcknowledgePartitionFullSyncOverrides(session_id, partition_id,
                                               records);
}

void StorageEngine::AcknowledgePartitionSnapshotRecords(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::span<const SnapshotRecord> records) {
  impl_->AcknowledgePartitionSnapshotRecords(session_id, partition_id, records);
}

absl::Status StorageEngine::CompletePartitionReplication(
    std::uint64_t session_id, std::uint16_t partition_id) {
  return impl_->CompletePartitionReplication(session_id, partition_id);
}

absl::Status StorageEngine::CompletePartitionDbReplication(
    std::uint64_t session_id, std::uint16_t partition_id, std::uint8_t db_id) {
  return impl_->CompletePartitionDbReplication(session_id, partition_id, db_id);
}

absl::StatusOr<std::vector<FullSyncPublishItem>>
StorageEngine::PeekFullSyncPublishItems(std::uint64_t session_id,
                                        std::size_t max_items) {
  return impl_->PeekFullSyncPublishItems(session_id, max_items);
}

absl::StatusOr<FullSyncPublishQueueInfo>
StorageEngine::GetFullSyncPublishQueueInfo(std::uint64_t session_id) const {
  return impl_->GetFullSyncPublishQueueInfo(session_id);
}

void StorageEngine::AcknowledgeFullSyncPublishItem(std::uint64_t session_id,
                                                   std::uint64_t item_id) {
  impl_->AcknowledgeFullSyncPublishItem(session_id, item_id);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::ResetReplicaPartition(
    std::uint16_t partition_id,
    std::span<const std::uint64_t, 16> source_db_epochs) {
  return impl_->ResetReplicaPartition(partition_id, source_db_epochs);
}

Task<absl::StatusOr<std::vector<ReplicaPartitionEpoch>>>
StorageEngine::ResetReplicaPartitions(
    std::uint64_t session_id, std::span<const ReplicaPartitionReset> resets) {
  return impl_->ResetReplicaPartitions(session_id, resets);
}

Task<absl::Status> StorageEngine::ResetPartitionsDetach(
    std::span<const std::uint16_t> partition_ids) {
  return impl_->ResetPartitionsDetach(partition_ids);
}

Task<absl::Status> StorageEngine::HandoffReplicaPartition(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t replication_epoch) {
  return impl_->HandoffReplicaPartition(session_id, partition_id,
                                        replication_epoch);
}

Task<absl::Status> StorageEngine::BeginReplicaTailCommand(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t partition_sequence) {
  return impl_->BeginReplicaTailCommand(session_id, partition_id,
                                        partition_sequence);
}

Task<absl::Status> StorageEngine::EndReplicaTailCommand(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t partition_sequence) {
  return impl_->EndReplicaTailCommand(session_id, partition_id,
                                      partition_sequence);
}

Task<absl::Status> StorageEngine::ApplyReplicaRecords(
    std::uint64_t session_id, std::uint16_t partition_id,
    std::uint64_t replication_epoch, std::span<const SnapshotRecord> records) {
  return impl_->ApplyReplicaRecords(session_id, partition_id, replication_epoch,
                                    records);
}

Task<absl::Status> StorageEngine::PromoteReplicaRoot(std::uint64_t session_id) {
  return impl_->PromoteReplicaRoot(session_id);
}

Task<absl::Status> StorageEngine::AbortReplicaRoot(std::uint64_t session_id) {
  return impl_->AbortReplicaRoot(session_id);
}

void StorageEngine::SetReplicaLoading(bool loading) noexcept {
  impl_->SetReplicaLoading(loading);
}

Task<absl::StatusOr<DiskValue>> StorageEngine::Get(
    std::uint8_t db_id, std::string_view key, ReadLatencyTrace* trace,
    std::optional<std::uint16_t> routed_partition_id) {
  return impl_->Get(db_id, key, trace, routed_partition_id);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::StringLength(
    std::uint8_t db_id, std::string_view key) {
  return impl_->StringLength(db_id, key);
}

Task<absl::StatusOr<SetResult>> StorageEngine::Set(
    std::uint8_t db_id, std::string_view key, std::string_view value,
    SetOptions options, ReplicationCommandAppend* replication,
    SetLatencyTrace* trace, std::optional<std::uint16_t> routed_partition_id,
    const MutationPrecondition* mutation_precondition) {
  return impl_->Set(db_id, key, value, options, replication, trace,
                    routed_partition_id, mutation_precondition);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::ListPush(
    std::uint8_t db_id, std::string_view key,
    std::span<const std::string_view> values,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ListPush(db_id, key, values, replication,
                         mutation_precondition);
}

Task<absl::StatusOr<ListResult>> StorageEngine::ExecuteList(
    std::uint8_t db_id, std::string_view key, const ListOperation& operation,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteList(db_id, key, operation, replication,
                            mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::ExecuteHash(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteHash(db_id, key, operation, replication,
                            mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::ExecuteSet(
    std::uint8_t db_id, std::string_view key, const HashOperation& operation,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteSet(db_id, key, operation, replication,
                           mutation_precondition);
}

Task<absl::Status> StorageEngine::ExecuteCompact(
    std::uint8_t db_id, std::string_view key, ValueType value_type,
    bool read_only, const CompactValueCallback& callback, std::uint64_t now_ms,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteCompact(db_id, key, value_type, read_only, callback,
                               now_ms, replication, mutation_precondition);
}

Task<ExpirationInfo> StorageEngine::GetExpiration(std::uint8_t db_id,
                                                  std::string_view key) {
  return impl_->GetExpiration(db_id, key);
}

Task<absl::StatusOr<bool>> StorageEngine::UpdateExpiration(
    std::uint8_t db_id, std::string_view key, std::uint64_t expire_at_ms,
    ExpirationCondition condition, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->UpdateExpiration(db_id, key, expire_at_ms, condition,
                                 replication, mutation_precondition);
}

Task<absl::StatusOr<bool>> StorageEngine::Delete(
    std::uint8_t db_id, std::string_view key,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->Delete(db_id, key, replication, mutation_precondition);
}

Task<bool> StorageEngine::Exists(std::uint8_t db_id, std::string_view key) {
  return impl_->Exists(db_id, key);
}

Task<absl::StatusOr<DiskValue>> StorageEngine::GetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    ReadLatencyTrace* trace) {
  return impl_->GetLocked(db_id, key, digest, trace);
}

Task<std::vector<BatchGetValue>> StorageEngine::BatchGetLocked(
    std::uint8_t db_id, std::span<const BatchGetRequest> requests) {
  return impl_->BatchGetLocked(db_id, requests);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::StringLengthLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  return impl_->StringLengthLocked(db_id, key, digest);
}

Task<absl::StatusOr<SetResult>> StorageEngine::SetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::string_view value, SetOptions options, TxShardWrites* tx,
    ReplicationCommandAppend* replication, SetLatencyTrace* trace,
    const MutationPrecondition* mutation_precondition) {
  return impl_->SetLocked(db_id, key, digest, value, options, tx, replication,
                          trace, std::nullopt, mutation_precondition);
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::ListPushLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::span<const std::string_view> values, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ListPushLocked(db_id, key, digest, values, tx, replication,
                               mutation_precondition);
}

Task<absl::StatusOr<ListResult>> StorageEngine::ExecuteListLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const ListOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteListLocked(db_id, key, digest, operation, tx,
                                  replication, mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::ExecuteHashLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteHashLocked(db_id, key, digest, operation, tx,
                                  replication, mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::ExecuteSetLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteSetLocked(db_id, key, digest, operation, tx, replication,
                                 mutation_precondition);
}

Task<absl::Status> StorageEngine::ExecuteCompactLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    ValueType value_type, bool read_only, const CompactValueCallback& callback,
    TxShardWrites* tx, std::uint64_t now_ms,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->ExecuteCompactLocked(db_id, key, digest, value_type, read_only,
                                     callback, tx, now_ms, replication, false,
                                     mutation_precondition);
}

Task<ExpirationInfo> StorageEngine::GetExpirationLocked(std::uint8_t db_id,
                                                        std::string_view key,
                                                        const Digest& digest) {
  return impl_->GetExpirationLocked(db_id, key, digest);
}

Task<absl::StatusOr<RawValue>> StorageEngine::ReadRawValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest) {
  return impl_->ReadRawValueLocked(db_id, key, digest);
}

Task<absl::StatusOr<RawValue>> StorageEngine::ReadRawValue(
    std::uint8_t db_id, std::string_view key) {
  return impl_->ReadRawValue(db_id, key);
}

Task<absl::StatusOr<RestoreRawResult>> StorageEngine::RestoreRawValue(
    std::uint8_t db_id, std::string_view key, const RawValue& value,
    bool replace, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->RestoreRawValue(db_id, key, value, replace, replication,
                                mutation_precondition);
}

Task<absl::StatusOr<RestoreRawResult>> StorageEngine::RestoreRawValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const RawValue& value, bool replace, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->RestoreRawValueLocked(db_id, key, digest, value, replace, tx,
                                      replication, mutation_precondition);
}

Task<absl::Status> StorageEngine::WriteRawValueLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const RawValue& value, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->WriteRawValueLocked(db_id, key, digest, value, tx, replication,
                                    mutation_precondition);
}

Task<absl::StatusOr<bool>> StorageEngine::UpdateExpirationLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    std::uint64_t expire_at_ms, ExpirationCondition condition,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->UpdateExpirationLocked(db_id, key, digest, expire_at_ms,
                                       condition, tx, replication,
                                       mutation_precondition);
}

Task<absl::StatusOr<bool>> StorageEngine::DeleteLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    TxShardWrites* tx, ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return impl_->DeleteLocked(db_id, key, digest, tx, replication,
                             mutation_precondition);
}

Task<bool> StorageEngine::ExistsLocked(std::uint8_t db_id, std::string_view key,
                                       const Digest& digest) {
  return impl_->ExistsLocked(db_id, key, digest);
}

Task<absl::Status> StorageEngine::CommitTxWrites(
    std::uint64_t txid, std::vector<TxShardWrites*> shards) {
  return impl_->CommitTxWrites(txid, std::move(shards));
}

absl::Status StorageEngine::ValidateTxCommit(
    std::span<const TxShardWrites> writes) {
  for (const auto& shard : writes) {
    if (shard.grouped_decision_ != nullptr &&
        shard.grouped_decision_->state_.load(std::memory_order_acquire) ==
            GroupedCommitDecision::State::kFailed) {
      return absl::FailedPreconditionError("grouped transaction was abandoned");
    }
  }
  return absl::OkStatus();
}

bool StorageEngine::EnqueueTxCommit(std::uint64_t txid,
                                    std::vector<TxShardWrites> writes) {
  return impl_->EnqueueTxCommit(txid, std::move(writes));
}

Task<absl::Status> StorageEngine::WaitForTxCommitCapacity() {
  return impl_->WaitForTxCommitCapacity();
}

void StorageEngine::PublishCommittedFullSyncEffects(TxShardWrites* shard) {
  impl_->PublishCommittedFullSyncEffects(shard);
}

std::uint64_t StorageEngine::AllocateWriteTxid() noexcept {
  return tx::TxRuntime::Get()->next_txid_.fetch_add(1,
                                                    std::memory_order_relaxed);
}

void StorageEngine::InitializeTxWrites(std::uint64_t txid,
                                       std::span<TxShardWrites> writes,
                                       MutationPrecondition precondition) {
  impl_->InitializeTxWrites(txid, writes, std::move(precondition));
}

void StorageEngine::NoteTxCommitStarted() noexcept {
  impl_->NoteTxCommitStarted();
}

void StorageEngine::NoteTxCommitFinished() noexcept {
  impl_->NoteTxCommitFinished();
}

TxCommitBatchTotals StorageEngine::TxCommitBatchStats() const noexcept {
  return impl_->TxCommitBatchStats();
}

Task<absl::Status> StorageEngine::RollbackTxLocal(std::uint64_t txid,
                                                  TxShardWrites* compensation) {
  return impl_->RollbackTxLocal(txid, compensation);
}

Task<absl::Status> StorageEngine::DiscardTxUndoLocal(std::uint64_t txid) {
  return impl_->DiscardTxUndoLocal(txid);
}

Task<bool> StorageEngine::KeyLive(std::uint8_t db_id, std::string_view key,
                                  const Digest& digest) {
  return impl_->KeyLive(db_id, key, digest);
}

}  // namespace lavik::storage
