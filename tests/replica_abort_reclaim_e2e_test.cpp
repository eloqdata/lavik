#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "celer/net/server.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"
#include "keylane/tx/tx_shard.h"

namespace {

using keylane::storage::PartitionSnapshotBatch;
using keylane::storage::ReplicaPartitionEpoch;
using keylane::storage::ReplicaPartitionReset;
using keylane::storage::SnapshotRecord;
using keylane::storage::StorageEngine;
using keylane::storage::StorageEngineOptions;

constexpr std::size_t kMiB = 1024 * 1024;
constexpr std::size_t kExternalValueBytes = 10 * kMiB;
constexpr std::uint64_t kRetainedTolerance = 512 * 1024;

void Check(bool condition, std::string_view message) {
  if (!condition) throw std::runtime_error(std::string(message));
}

class ScopedDataFile {
 public:
  explicit ScopedDataFile(std::string path) : path_(std::move(path)) {}
  ~ScopedDataFile() {
    if (owned_) (void)::unlink(path_.c_str());
  }

  const std::string& path() const noexcept { return path_; }

  void Create() {
    const int fd =
        ::open(path_.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    Check(fd >= 0, "failed to create replica-reclaim test data file");
    const int allocated = ::posix_fallocate(fd, 0, 256 * kMiB);
    const int closed = ::close(fd);
    Check(allocated == 0 && closed == 0,
          "failed to allocate replica-reclaim test data file");
    owned_ = true;
  }

 private:
  std::string path_;
  bool owned_ = false;
};

class ReplicaAbortReclaimService final : public celer::Service {
 public:
  explicit ReplicaAbortReclaimService(StorageEngine* storage)
      : storage_(storage) {}

  void Prepare(unsigned thread_count) override {
    Check(thread_count == 1, "replica-reclaim test requires one worker");
  }

  celer::Task<absl::Status> Run(celer::Worker& worker,
                                celer::ServiceContext) override {
    worker_ = &worker;
    keylane::BindMemoryAccountingShard(worker.id());
    keylane::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) result_ = co_await ExerciseRepeatedAbort();
    if (result_.ok()) result_ = co_await ExerciseRepeatedPromotion();
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  celer::Task<absl::StatusOr<PartitionSnapshotBatch>> PinActiveExternalValue(
      std::uint64_t session_id, std::uint8_t db_id, std::string_view key,
      char fill) {
    auto written = co_await storage_->Set(
        db_id, key, std::string(kExternalValueBytes, fill), {});
    if (!written.ok()) co_return written.status();

    const std::uint16_t partition = keylane::storage::RedisSlot(key);
    auto session = storage_->BeginFullSyncSession(session_id);
    if (!session.ok()) co_return session.status();
    auto start = storage_->BeginPartitionReplication(session_id, partition);
    if (!start.ok()) co_return start.status();
    absl::Status db =
        storage_->BeginPartitionDbReplication(session_id, partition, db_id);
    if (!db.ok()) co_return db;
    auto batch = co_await storage_->SnapshotPartition(
        session_id, partition, db_id, 0, 1, 1,
        keylane::storage::kReplicationTransferBytes);
    if (!batch.ok()) co_return batch.status();
    if (batch->records_.size() != 1 || batch->records_.front().key_ != key ||
        batch->records_.front().source_id_ == 0) {
      co_return absl::FailedPreconditionError(
          "reclaim test did not pin the active external value");
    }
    co_return std::move(*batch);
  }

  void ReleasePinnedValue(std::uint64_t session_id, std::string_view key,
                          const PartitionSnapshotBatch& batch) {
    const std::uint16_t partition = keylane::storage::RedisSlot(key);
    storage_->AcknowledgePartitionSnapshotRecords(session_id, partition,
                                                  batch.records_);
    storage_->EndPartitionReplication(session_id, partition);
    storage_->EndFullSyncSession(session_id);
  }

  celer::Task<absl::StatusOr<std::vector<ReplicaPartitionEpoch>>> ResetFullRoot(
      std::uint64_t session_id) {
    std::array<std::uint64_t, keylane::storage::kLogicalDatabaseCount>
        source_db_epochs{};
    for (std::uint8_t db_id = 0;
         db_id < keylane::storage::kLogicalDatabaseCount; ++db_id) {
      source_db_epochs[db_id] = storage_->DbEpoch(db_id);
    }
    std::vector<ReplicaPartitionReset> resets;
    resets.reserve(keylane::storage::kLogicalStorageShards);
    for (std::uint16_t partition = 0;
         partition < keylane::storage::kLogicalStorageShards; ++partition) {
      resets.push_back(ReplicaPartitionReset{
          .partition_id_ = partition,
          .db_epochs_ = source_db_epochs,
      });
    }
    co_return co_await storage_->ResetReplicaPartitions(session_id, resets);
  }

  celer::Task<absl::Status> ApplyCandidate(
      std::uint64_t session_id,
      std::span<const ReplicaPartitionEpoch> partition_epochs,
      std::uint8_t db_id, std::string_view key, char fill) {
    const std::uint16_t partition = keylane::storage::RedisSlot(key);
    Check(partition_epochs.size() == keylane::storage::kLogicalStorageShards,
          "replica reset did not cover the full root");
    Check(partition_epochs[partition].partition_id_ == partition,
          "replica reset epochs are not partition ordered");
    SnapshotRecord candidate{
        .kind_ = SnapshotRecord::Kind::kValue,
        .db_id_ = db_id,
        .db_epoch_ = storage_->DbEpoch(db_id),
        .mutation_sequence_ = 1,
        .value_type_ = keylane::storage::ValueType::kString,
        .logical_size_ = kExternalValueBytes,
        .key_ = std::string(key),
        .value_ = std::string(kExternalValueBytes, fill),
    };
    co_return co_await storage_->ApplyReplicaRecords(
        session_id, partition, partition_epochs[partition].replication_epoch_,
        std::span(&candidate, 1));
  }

  celer::Task<absl::Status> HandoffAll(
      std::uint64_t session_id,
      std::span<const ReplicaPartitionEpoch> partition_epochs) {
    for (const auto& epoch : partition_epochs) {
      absl::Status handed_off = co_await storage_->HandoffReplicaPartition(
          session_id, epoch.partition_id_, epoch.replication_epoch_);
      if (!handed_off.ok()) co_return handed_off;
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> WaitForCapacityIncrease(std::uint64_t previous,
                                                    std::string_view failure) {
    for (unsigned attempt = 0; attempt < 500; ++attempt) {
      absl::Status slept =
          co_await celer::SleepFor(*worker_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return slept;
      const auto metrics = co_await storage_->CollectMetrics();
      if (metrics.devices_.front().available_bytes_ > previous) {
        co_return absl::OkStatus();
      }
    }
    co_return absl::FailedPreconditionError(std::string(failure));
  }

  void CheckRetainedMemory(std::optional<std::uint64_t>& first_retained,
                           std::string_view failure) {
    keylane::RefreshMemoryStats();
    const std::uint64_t retained = keylane::GetMemoryStats().used_bytes_;
    if (first_retained.has_value()) {
      Check(retained <= *first_retained + kRetainedTolerance, failure);
    } else {
      first_retained = retained;
    }
  }

  celer::Task<absl::Status> ExerciseRepeatedAbort() {
    constexpr std::uint8_t kDb = 7;
    const std::string key = "replica-abort-candidate";
    std::optional<std::uint64_t> first_retained;

    for (unsigned round = 0; round < 2; ++round) {
      const std::uint64_t pin_session = 900 + round;
      const std::uint64_t replica_session = 910 + round;
      const auto capacity_baseline = co_await storage_->CollectMetrics();
      Check(capacity_baseline.devices_.size() == 1,
            "replica-abort test expected one storage device");
      auto pinned = co_await PinActiveExternalValue(
          pin_session, kDb, key, static_cast<char>('a' + round));
      if (!pinned.ok()) co_return pinned.status();

      auto reset = co_await ResetFullRoot(replica_session);
      if (!reset.ok()) co_return reset.status();
      absl::Status applied = co_await ApplyCandidate(
          replica_session, *reset, kDb, key, static_cast<char>('k' + round));
      if (!applied.ok()) co_return applied;
      absl::Status aborted =
          co_await storage_->AbortReplicaRoot(replica_session);
      if (!aborted.ok()) co_return aborted;
      Check(!co_await storage_->Exists(kDb, key),
            "replica abort left the candidate key visible");

      const auto while_pinned = co_await storage_->CollectMetrics();
      Check(while_pinned.devices_.front().available_bytes_ <=
                capacity_baseline.devices_.front().available_bytes_,
            "replica abort reported pinned retired capacity as available");
      ReleasePinnedValue(pin_session, key, *pinned);
      absl::Status reclaimed = co_await WaitForCapacityIncrease(
          while_pinned.devices_.front().available_bytes_,
          "replica-abort retired capacity did not become reusable");
      if (!reclaimed.ok()) co_return reclaimed;
      storage_->SetReplicaLoading(false);
      CheckRetainedMemory(first_retained,
                          "successive aborts accumulated detached index RAM");
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> ExerciseRepeatedPromotion() {
    constexpr std::uint8_t kDb = 9;
    const std::string key = "replica-promote-candidate";
    std::optional<std::uint64_t> first_retained;

    for (unsigned round = 0; round < 2; ++round) {
      const std::uint64_t pin_session = 950 + round;
      const std::uint64_t replica_session = 960 + round;
      const auto capacity_baseline = co_await storage_->CollectMetrics();
      auto pinned = co_await PinActiveExternalValue(
          pin_session, kDb, key, static_cast<char>('c' + round));
      if (!pinned.ok()) co_return pinned.status();

      auto reset = co_await ResetFullRoot(replica_session);
      if (!reset.ok()) co_return reset.status();
      absl::Status applied = co_await ApplyCandidate(
          replica_session, *reset, kDb, key, static_cast<char>('x' + round));
      if (!applied.ok()) co_return applied;
      absl::Status handed_off = co_await HandoffAll(replica_session, *reset);
      if (!handed_off.ok()) co_return handed_off;
      absl::Status promoted =
          co_await storage_->PromoteReplicaRoot(replica_session);
      if (!promoted.ok()) co_return promoted;
      Check(co_await storage_->Exists(kDb, key),
            "replica promotion did not publish the candidate key");

      const auto while_pinned = co_await storage_->CollectMetrics();
      Check(while_pinned.devices_.front().available_bytes_ <=
                capacity_baseline.devices_.front().available_bytes_,
            "replica promotion reported pinned retired capacity as available");
      ReleasePinnedValue(pin_session, key, *pinned);
      absl::Status reclaimed = co_await WaitForCapacityIncrease(
          while_pinned.devices_.front().available_bytes_,
          "replica-promotion retired capacity did not become reusable");
      if (!reclaimed.ok()) co_return reclaimed;
      // Production keeps replica loading enabled after root promotion so tail
      // commands continue to require their per-partition apply context. This
      // storage-only test seeds the next candidate with a direct client-style
      // write, so explicitly leave replica mode between independent rounds.
      storage_->SetReplicaLoading(false);
      CheckRetainedMemory(
          first_retained,
          "successive promotions accumulated detached index RAM");
    }
    co_return absl::OkStatus();
  }

  StorageEngine* storage_ = nullptr;
  celer::Worker* worker_ = nullptr;
  absl::Status result_ = absl::UnknownError("test service did not run");
};

int Run(const std::string& path) {
  StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  options.replication_publish_queue_bytes_ = 16 * kMiB;
  StorageEngine storage(std::move(options));
  keylane::InitWorkerMetrics(1);
  absl::Status memory = keylane::InitMemoryLimit(512 * kMiB, 1);
  if (!memory.ok()) {
    std::cerr << memory << '\n';
    return 1;
  }
  keylane::InitStorage(&storage, nullptr);
  absl::Status prepared = storage.Prepare(1);
  if (!prepared.ok()) {
    std::cerr << prepared << '\n';
    return 1;
  }
  keylane::tx::TxRuntime::Create(1);
  ReplicaAbortReclaimService service(&storage);
  celer::Server server;
  server.AddService(&service);
  celer::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  absl::Status started = server.Start(runtime);
  if (!started.ok()) {
    std::cerr << started << '\n';
    return 1;
  }
  server.WaitUntilStopped();
  if (!service.result().ok()) {
    std::cerr << service.result() << '\n';
    return 1;
  }
  return 0;
}

}  // namespace

int main() {
  try {
    ScopedDataFile data_file("/tmp/keylane-replica-abort-reclaim-" +
                             std::to_string(::getpid()) + ".data");
    data_file.Create();
    return Run(data_file.path());
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
