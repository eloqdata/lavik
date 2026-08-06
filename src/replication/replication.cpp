#include "keylane/replication.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "celer/base/status.h"
#include "celer/io/storage.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"
#include "spdlog/spdlog.h"

namespace keylane {
namespace {

using celer::Status;
using celer::StatusCode;
using celer::StatusOr;
using celer::Task;
using celer::rpc::Bytes;
using celer::rpc::BytesView;
using storage::PartitionDeltaBatch;
using storage::PartitionReplicationStart;
using storage::PartitionSnapshotBatch;
using storage::SnapshotRecord;

enum : std::uint16_t {
  kResetPartition = 1,
  kApplyRecords = 2,
};

constexpr std::size_t kSnapshotKeysPerBatch = 16;
constexpr std::size_t kDeltaRecordsPerBatch = 256;
constexpr std::size_t kMaxApplyPayload = 12U * 1024U * 1024U;

void PutU8(Bytes& out, std::uint8_t value) {
  out.push_back(static_cast<std::byte>(value));
}

void PutU16(Bytes& out, std::uint16_t value) {
  PutU8(out, static_cast<std::uint8_t>(value));
  PutU8(out, static_cast<std::uint8_t>(value >> 8));
}

void PutU32(Bytes& out, std::uint32_t value) {
  for (unsigned i = 0; i < 4; ++i) {
    PutU8(out, static_cast<std::uint8_t>(value >> (i * 8)));
  }
}

void PutU64(Bytes& out, std::uint64_t value) {
  for (unsigned i = 0; i < 8; ++i) {
    PutU8(out, static_cast<std::uint8_t>(value >> (i * 8)));
  }
}

void PutString(Bytes& out, std::string_view value) {
  out.insert(out.end(), reinterpret_cast<const std::byte*>(value.data()),
             reinterpret_cast<const std::byte*>(value.data() + value.size()));
}

class Reader {
 public:
  explicit Reader(BytesView bytes) : bytes_(bytes) {}

  bool U8(std::uint8_t* value) {
    if (remaining() < 1) return false;
    *value = std::to_integer<std::uint8_t>(bytes_[position_++]);
    return true;
  }

  bool U16(std::uint16_t* value) {
    std::uint8_t lo = 0;
    std::uint8_t hi = 0;
    if (!U8(&lo) || !U8(&hi)) return false;
    *value = static_cast<std::uint16_t>(lo) |
             (static_cast<std::uint16_t>(hi) << 8);
    return true;
  }

  bool U32(std::uint32_t* value) {
    std::uint32_t result = 0;
    for (unsigned i = 0; i < 4; ++i) {
      std::uint8_t part = 0;
      if (!U8(&part)) return false;
      result |= static_cast<std::uint32_t>(part) << (i * 8);
    }
    *value = result;
    return true;
  }

  bool U64(std::uint64_t* value) {
    std::uint64_t result = 0;
    for (unsigned i = 0; i < 8; ++i) {
      std::uint8_t part = 0;
      if (!U8(&part)) return false;
      result |= static_cast<std::uint64_t>(part) << (i * 8);
    }
    *value = result;
    return true;
  }

  bool String(std::uint32_t size, std::string* value) {
    if (remaining() < size) return false;
    value->assign(reinterpret_cast<const char*>(bytes_.data() + position_),
                  size);
    position_ += size;
    return true;
  }

  std::size_t remaining() const { return bytes_.size() - position_; }

 private:
  BytesView bytes_;
  std::size_t position_ = 0;
};

Bytes ErrorResponse(std::string_view message) {
  Bytes result;
  PutU8(result, 1);
  PutU32(result, static_cast<std::uint32_t>(message.size()));
  PutString(result, message);
  return result;
}

Bytes OkResponse() {
  Bytes result;
  PutU8(result, 0);
  return result;
}

Status DecodeStatus(BytesView response, Reader* reader) {
  Reader local(response);
  std::uint8_t error = 0;
  if (!local.U8(&error)) {
    return Status(StatusCode::kInternal, "truncated replication response");
  }
  if (error != 0) {
    std::uint32_t size = 0;
    std::string message;
    if (!local.U32(&size) || !local.String(size, &message) ||
        local.remaining() != 0) {
      return Status(StatusCode::kInternal,
                    "malformed replication error response");
    }
    return Status(StatusCode::kUnavailable, std::move(message));
  }
  if (reader != nullptr) {
    *reader = std::move(local);
  } else if (local.remaining() != 0) {
    return Status(StatusCode::kInternal,
                  "unexpected replication response payload");
  }
  return Status::Ok();
}

std::size_t EncodedRecordBytes(const SnapshotRecord& record) {
  return 1 + 1 + 8 + 8 + 4 + 4 + record.key.size() + record.value.size();
}

bool EncodeRecords(std::uint16_t partition_id, std::uint64_t epoch,
                   std::span<const SnapshotRecord> records, Bytes* output) {
  std::size_t bytes = 2 + 8 + 4;
  for (const SnapshotRecord& record : records) {
    if (record.key.size() > std::numeric_limits<std::uint32_t>::max() ||
        record.value.size() > std::numeric_limits<std::uint32_t>::max() ||
        bytes > kMaxApplyPayload - EncodedRecordBytes(record)) {
      return false;
    }
    bytes += EncodedRecordBytes(record);
  }
  output->clear();
  output->reserve(bytes);
  PutU16(*output, partition_id);
  PutU64(*output, epoch);
  PutU32(*output, static_cast<std::uint32_t>(records.size()));
  for (const SnapshotRecord& record : records) {
    PutU8(*output, static_cast<std::uint8_t>(record.kind));
    PutU8(*output, record.db_id);
    PutU64(*output, record.db_epoch);
    PutU64(*output, record.mutation_sequence);
    PutU32(*output, static_cast<std::uint32_t>(record.key.size()));
    PutU32(*output, static_cast<std::uint32_t>(record.value.size()));
    PutString(*output, record.key);
    PutString(*output, record.value);
  }
  return true;
}

StatusOr<std::tuple<std::uint16_t, std::uint64_t,
                    std::vector<SnapshotRecord>>>
DecodeRecords(BytesView payload) {
  Reader reader(payload);
  std::uint16_t partition_id = 0;
  std::uint64_t epoch = 0;
  std::uint32_t count = 0;
  if (!reader.U16(&partition_id) || !reader.U64(&epoch) ||
      !reader.U32(&count) || partition_id >= storage::kLogicalStorageShards ||
      epoch == 0 || count > 65536) {
    return Status(StatusCode::kInvalidArgument,
                  "malformed apply-records request");
  }
  std::vector<SnapshotRecord> records;
  records.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint8_t kind = 0;
    SnapshotRecord record;
    std::uint32_t key_size = 0;
    std::uint32_t value_size = 0;
    if (!reader.U8(&kind) || !reader.U8(&record.db_id) ||
        !reader.U64(&record.db_epoch) ||
        !reader.U64(&record.mutation_sequence) ||
        !reader.U32(&key_size) || !reader.U32(&value_size) ||
        kind < static_cast<std::uint8_t>(SnapshotRecord::Kind::kValue) ||
        kind > static_cast<std::uint8_t>(SnapshotRecord::Kind::kFlushDb) ||
        record.db_id >= storage::kLogicalDatabaseCount ||
        !reader.String(key_size, &record.key) ||
        !reader.String(value_size, &record.value)) {
      return Status(StatusCode::kInvalidArgument,
                    "malformed replicated record");
    }
    record.kind = static_cast<SnapshotRecord::Kind>(kind);
    records.push_back(std::move(record));
  }
  if (reader.remaining() != 0) {
    return Status(StatusCode::kInvalidArgument,
                  "trailing apply-records bytes");
  }
  return std::tuple{partition_id, epoch, std::move(records)};
}

}  // namespace

class ReplicationManager::Impl {
 public:
  Impl(storage::StorageEngine* storage, const ReplicationOptions& options)
      : storage_(storage), options_(options), rpc_server_(options.listen_port) {
    rpc_server_.OnVerbAsync(
        kResetPartition,
        [this](BytesView payload) { return HandleReset(payload); });
    rpc_server_.OnVerbAsync(
        kApplyRecords,
        [this](BytesView payload) { return HandleApply(payload); });
  }

  celer::Service* service() noexcept {
    return options_.listen_port == 0 ? nullptr : &rpc_server_;
  }

  void StorageReady(celer::Worker& worker) {
    ready_.store(true, std::memory_order_release);
    if (worker.id() == 0 && !options_.target_ip.empty() &&
        options_.target_port != 0 && !coordinator_started_) {
      coordinator_started_ = true;
      worker.Spawn(Coordinator());
    }
  }

 private:
  struct PartitionState {
    bool complete = false;
    std::uint64_t target_epoch = 0;
    std::uint64_t acknowledged = 0;
  };

  Task<Bytes> HandleReset(BytesView payload) {
    if (!ready_.load(std::memory_order_acquire)) {
      co_return ErrorResponse("replica storage is not ready");
    }
    Reader reader(payload);
    std::uint16_t partition_id = 0;
    std::array<std::uint64_t, storage::kLogicalDatabaseCount> epochs{};
    if (!reader.U16(&partition_id) ||
        partition_id >= storage::kLogicalStorageShards) {
      co_return ErrorResponse("malformed reset-partition request");
    }
    for (std::uint64_t& epoch : epochs) {
      if (!reader.U64(&epoch) || epoch == 0) {
        co_return ErrorResponse("malformed reset-partition epochs");
      }
    }
    if (reader.remaining() != 0) {
      co_return ErrorResponse("trailing reset-partition bytes");
    }
    const unsigned owner = partition_id % storage_->worker_count();
    StatusOr<std::uint64_t> reset = owner == celer::ThisWorker().id
        ? co_await storage_->ResetReplicaPartition(partition_id, epochs)
        : co_await celer::SubmitTaskTo(
              owner, [this, partition_id, epochs]() ->
                         Task<StatusOr<std::uint64_t>> {
                co_return co_await storage_->ResetReplicaPartition(
                    partition_id, epochs);
              });
    if (!reset.ok()) {
      co_return ErrorResponse(reset.status().message());
    }
    Bytes response = OkResponse();
    PutU64(response, *reset);
    co_return response;
  }

  Task<Bytes> HandleApply(BytesView payload) {
    if (!ready_.load(std::memory_order_acquire)) {
      co_return ErrorResponse("replica storage is not ready");
    }
    auto decoded = DecodeRecords(payload);
    if (!decoded.ok()) {
      co_return ErrorResponse(decoded.status().message());
    }
    auto [partition_id, epoch, records] = std::move(*decoded);
    const unsigned owner = partition_id % storage_->worker_count();
    Status status = owner == celer::ThisWorker().id
        ? co_await storage_->ApplyReplicaRecords(partition_id, epoch, records)
        : co_await celer::SubmitTaskTo(
              owner, [this, partition_id, epoch,
                      records = std::move(records)]() mutable {
                return ApplyReplicaRecordsOwned(partition_id, epoch,
                                                std::move(records));
              });
    co_return status.ok() ? OkResponse()
                          : ErrorResponse(status.message());
  }

  // Do not make the SubmitTaskTo closure itself a coroutine. Coroutine lambdas
  // retain a pointer to their closure, while this named coroutine moves the
  // record vector into its own target-worker frame before it can suspend.
  Task<Status> ApplyReplicaRecordsOwned(
      std::uint16_t partition_id, std::uint64_t epoch,
      std::vector<SnapshotRecord> records) {
    co_return co_await storage_->ApplyReplicaRecords(partition_id, epoch,
                                                     records);
  }

  Task<StatusOr<std::uint64_t>> ResetRemote(
      celer::rpc::RpcClient& client, std::uint16_t partition_id,
      const PartitionReplicationStart& start) {
    Bytes request;
    request.reserve(2 + 8 * storage::kLogicalDatabaseCount);
    PutU16(request, partition_id);
    for (std::uint64_t epoch : start.db_epochs) PutU64(request, epoch);
    auto response = co_await client.Call(kResetPartition, request);
    if (!response.ok()) co_return response.status();
    Reader reader(BytesView(response->data(), response->size()));
    std::uint8_t error = 0;
    if (!reader.U8(&error)) {
      co_return Status(StatusCode::kInternal, "truncated reset response");
    }
    if (error != 0) {
      std::uint32_t size = 0;
      std::string message;
      if (!reader.U32(&size) || !reader.String(size, &message)) {
        co_return Status(StatusCode::kInternal,
                         "malformed reset error response");
      }
      co_return Status(StatusCode::kUnavailable, std::move(message));
    }
    std::uint64_t epoch = 0;
    if (!reader.U64(&epoch) || epoch == 0 || reader.remaining() != 0) {
      co_return Status(StatusCode::kInternal, "malformed reset response");
    }
    co_return epoch;
  }

  Task<Status> ApplyRemote(celer::rpc::RpcClient& client,
                           std::uint16_t partition_id,
                           std::uint64_t epoch,
                           std::span<const SnapshotRecord> records) {
    if (records.empty()) co_return Status::Ok();
    std::size_t begin = 0;
    while (begin < records.size()) {
      std::size_t end = begin;
      std::size_t bytes = 2 + 8 + 4;
      while (end < records.size()) {
        const std::size_t record_bytes = EncodedRecordBytes(records[end]);
        if (end != begin && bytes + record_bytes > kMaxApplyPayload) break;
        if (bytes + record_bytes > kMaxApplyPayload) {
          co_return Status(StatusCode::kOutOfRange,
                           "replicated record exceeds RPC payload limit");
        }
        bytes += record_bytes;
        ++end;
      }
      Bytes request;
      if (!EncodeRecords(partition_id, epoch, records.subspan(begin, end - begin),
                         &request)) {
        co_return Status(StatusCode::kOutOfRange,
                         "failed to encode replication batch");
      }
      auto response = co_await client.Call(kApplyRecords, request);
      if (!response.ok()) co_return response.status();
      Status status = DecodeStatus(
          BytesView(response->data(), response->size()), nullptr);
      if (!status.ok()) co_return status;
      begin = end;
    }
    co_return Status::Ok();
  }

  Task<PartitionReplicationStart> BeginLocal(std::uint16_t partition_id) {
    const unsigned owner = partition_id % storage_->worker_count();
    if (owner == celer::ThisWorker().id) {
      co_return storage_->BeginPartitionReplication(partition_id);
    }
    co_return co_await celer::SubmitTo(
        owner, [this, partition_id] {
          return storage_->BeginPartitionReplication(partition_id);
        });
  }

  Task<StatusOr<PartitionSnapshotBatch>> SnapshotLocal(
      std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor) {
    const unsigned owner = partition_id % storage_->worker_count();
    if (owner == celer::ThisWorker().id) {
      co_return co_await storage_->SnapshotPartition(
          partition_id, db_id, cursor, kSnapshotKeysPerBatch);
    }
    co_return co_await celer::SubmitTaskTo(
        owner, [this, partition_id, db_id, cursor]() ->
                   Task<StatusOr<PartitionSnapshotBatch>> {
          co_return co_await storage_->SnapshotPartition(
              partition_id, db_id, cursor, kSnapshotKeysPerBatch);
        });
  }

  Task<PartitionDeltaBatch> ReadDeltasLocal(std::uint16_t partition_id,
                                            std::uint64_t after) {
    const unsigned owner = partition_id % storage_->worker_count();
    if (owner == celer::ThisWorker().id) {
      co_return storage_->ReadPartitionDeltas(
          partition_id, after, kDeltaRecordsPerBatch);
    }
    co_return co_await celer::SubmitTo(
        owner, [this, partition_id, after] {
          return storage_->ReadPartitionDeltas(
              partition_id, after, kDeltaRecordsPerBatch);
        });
  }

  Task<Status> AckLocal(std::uint16_t partition_id, std::uint64_t through) {
    const unsigned owner = partition_id % storage_->worker_count();
    if (owner == celer::ThisWorker().id) {
      storage_->AcknowledgePartitionDeltas(partition_id, through);
      co_return Status::Ok();
    }
    co_return co_await celer::SubmitTo(owner, [this, partition_id, through] {
      storage_->AcknowledgePartitionDeltas(partition_id, through);
      return Status::Ok();
    });
  }

  Task<Status> CatchUp(celer::rpc::RpcClient& client,
                       std::uint16_t partition_id, PartitionState* state) {
    while (true) {
      PartitionDeltaBatch batch =
          co_await ReadDeltasLocal(partition_id, state->acknowledged);
      if (batch.overflow) {
        co_return Status(StatusCode::kAborted,
                         "partition delta retention overflow");
      }
      Status applied = co_await ApplyRemote(
          client, partition_id, state->target_epoch, batch.records);
      if (!applied.ok()) co_return applied;
      if (!batch.records.empty()) {
        state->acknowledged = batch.records.back().mutation_sequence;
      } else {
        state->acknowledged = batch.watermark;
      }
      Status acknowledged =
          co_await AckLocal(partition_id, state->acknowledged);
      if (!acknowledged.ok()) co_return acknowledged;
      if (state->acknowledged >= batch.watermark &&
          batch.records.size() < kDeltaRecordsPerBatch) {
        co_return Status::Ok();
      }
    }
  }

  Task<Status> SynchronizeReadyPartition(
      celer::rpc::RpcClient& client, std::uint16_t partition_id,
      std::array<PartitionState, storage::kLogicalStorageShards>* states) {
    PartitionState& state = (*states)[partition_id];
    Status caught_up = co_await CatchUp(client, partition_id, &state);
    while (caught_up.code() == StatusCode::kAborted) {
      spdlog::warn("replication partition {} delta overflow; restarting",
                   partition_id);
      caught_up = co_await CopyPartition(client, partition_id, &state,
                                         states, nullptr, false);
    }
    co_return caught_up;
  }

  // Forward mutations for partitions whose snapshot has already completed
  // while later partitions are still being copied. A token for the partition
  // currently under snapshot is kept deferred; ReadPartitionDeltas will clear
  // its queued bit after that partition reaches CATCHUP/SYNCED.
  Task<Status> DrainReadyPartitions(
      celer::rpc::RpcClient& client,
      std::array<PartitionState, storage::kLogicalStorageShards>* states,
      std::deque<std::uint16_t>* deferred, std::size_t maximum,
      std::uint32_t skip_partition = storage::kLogicalStorageShards) {
    std::size_t processed = 0;
    if (deferred != nullptr) {
      const std::size_t pending = deferred->size();
      for (std::size_t i = 0; i < pending && processed < maximum; ++i) {
        const std::uint16_t partition_id = deferred->front();
        deferred->pop_front();
        if (partition_id == skip_partition ||
            !(*states)[partition_id].complete) {
          deferred->push_back(partition_id);
          continue;
        }
        Status synced = co_await SynchronizeReadyPartition(
            client, partition_id, states);
        if (!synced.ok()) co_return synced;
        ++processed;
      }
    }

    while (processed < maximum) {
      std::uint16_t partition_id = 0;
      if (!storage_->TryTakeReplicationReady(&partition_id)) break;
      if (partition_id == skip_partition ||
          !(*states)[partition_id].complete) {
        if (deferred != nullptr) deferred->push_back(partition_id);
        continue;
      }
      Status synced = co_await SynchronizeReadyPartition(
          client, partition_id, states);
      if (!synced.ok()) co_return synced;
      ++processed;
    }
    co_return Status::Ok();
  }

  Task<Status> CopyPartition(celer::rpc::RpcClient& client,
                             std::uint16_t partition_id,
                             PartitionState* state,
                             std::array<PartitionState,
                                        storage::kLogicalStorageShards>* states,
                             std::deque<std::uint16_t>* deferred,
                             bool drain_others = true) {
    PartitionReplicationStart start = co_await BeginLocal(partition_id);
    auto target_epoch = co_await ResetRemote(client, partition_id, start);
    if (!target_epoch.ok()) co_return target_epoch.status();
    state->complete = false;
    state->target_epoch = *target_epoch;
    state->acknowledged = start.snapshot_sequence;

    for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
         ++db_id) {
      if ((start.nonempty_db_mask & (std::uint16_t{1} << db_id)) == 0) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        auto batch = co_await SnapshotLocal(partition_id, db_id, cursor);
        if (!batch.ok()) co_return batch.status();
        Status applied = co_await ApplyRemote(
            client, partition_id, state->target_epoch, batch->records);
        if (!applied.ok()) co_return applied;
        cursor = batch->cursor;
        if (drain_others && states != nullptr) {
          Status forwarded = co_await DrainReadyPartitions(
              client, states, deferred, 8, partition_id);
          if (!forwarded.ok()) co_return forwarded;
        }
      } while (cursor != 0);
    }
    Status caught_up = co_await CatchUp(client, partition_id, state);
    if (!caught_up.ok()) co_return caught_up;
    state->complete = true;
    co_return Status::Ok();
  }

  Task<Status> ReplicateOnce(celer::rpc::RpcClient& client) {
    std::array<PartitionState, storage::kLogicalStorageShards> states{};
    std::deque<std::uint16_t> deferred;
    for (std::uint32_t partition = 0;
         partition < storage::kLogicalStorageShards; ++partition) {
      const auto partition_id = static_cast<std::uint16_t>(partition);
      while (true) {
        Status copied = co_await CopyPartition(
            client, partition_id, &states[partition_id], &states, &deferred);
        if (copied.ok()) break;
        if (copied.code() != StatusCode::kAborted) co_return copied;
        spdlog::warn("replication partition {} delta overflow; restarting",
                     partition_id);
      }
      Status forwarded = co_await DrainReadyPartitions(
          client, &states, &deferred, 64);
      if (!forwarded.ok()) co_return forwarded;
      if ((partition + 1) % 256 == 0) {
        spdlog::info("replication baseline partitions={}/{}", partition + 1,
                     storage::kLogicalStorageShards);
      }
    }
    Status forwarded = co_await DrainReadyPartitions(
        client, &states, &deferred, std::numeric_limits<std::size_t>::max());
    if (!forwarded.ok()) co_return forwarded;
    spdlog::info("replication baseline complete; entering steady state");

    while (client.connected()) {
      std::uint16_t partition_id = 0;
      if (!storage_->TryTakeReplicationReady(&partition_id)) {
        Status slept = co_await celer::SleepFor(
            *celer::ThisWorker().self, std::chrono::milliseconds(1));
        if (!slept.ok()) co_return slept;
        continue;
      }
      if (!states[partition_id].complete) continue;
      Status caught_up = co_await SynchronizeReadyPartition(
          client, partition_id, &states);
      if (!caught_up.ok()) co_return caught_up;
    }
    co_return Status(StatusCode::kUnavailable,
                     "replication connection closed");
  }

  Task<Status> Coordinator() {
    while (!celer::ThisWorker().self->stop_requested()) {
      auto client = std::make_unique<celer::rpc::RpcClient>();
      Status connected = co_await client->Connect(options_.target_ip,
                                                   options_.target_port);
      if (!connected.ok()) {
        spdlog::warn("replication connect to {}:{} failed: {}",
                     options_.target_ip, options_.target_port,
                     connected.message());
      } else {
        spdlog::info("replication connected to {}:{}", options_.target_ip,
                     options_.target_port);
        Status replicated = co_await ReplicateOnce(*client);
        spdlog::warn("replication session ended: {}", replicated.message());
        client->Close();
      }
      Status slept = co_await celer::SleepFor(
          *celer::ThisWorker().self, std::chrono::seconds(1));
      if (!slept.ok()) co_return slept;
    }
    co_return Status::Ok();
  }

  storage::StorageEngine* storage_;
  ReplicationOptions options_;
  celer::rpc::RpcServer rpc_server_;
  std::atomic<bool> ready_{false};
  bool coordinator_started_ = false;
};

ReplicationManager::ReplicationManager(storage::StorageEngine* storage,
                                       ReplicationOptions options)
    : impl_(std::make_unique<Impl>(storage, options)),
      options_(std::move(options)) {}

ReplicationManager::~ReplicationManager() = default;

celer::Service* ReplicationManager::service() noexcept {
  return impl_->service();
}

void ReplicationManager::StorageReady(celer::Worker& worker) {
  impl_->StorageReady(worker);
}

}  // namespace keylane
