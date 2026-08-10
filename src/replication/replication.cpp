#include "keylane/replication.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/io/storage.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/worker.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"
#include "spdlog/spdlog.h"

namespace keylane {
namespace {

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
    *value =
        static_cast<std::uint16_t>(lo) | (static_cast<std::uint16_t>(hi) << 8);
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

absl::Status DecodeStatus(BytesView response, Reader* reader) {
  Reader local(response);
  std::uint8_t error = 0;
  if (!local.U8(&error)) {
    return absl::Status(absl::StatusCode::kInternal,
                        "truncated replication response");
  }
  if (error != 0) {
    std::uint32_t size = 0;
    std::string message;
    if (!local.U32(&size) || !local.String(size, &message) ||
        local.remaining() != 0) {
      return absl::Status(absl::StatusCode::kInternal,
                          "malformed replication error response");
    }
    return absl::Status(absl::StatusCode::kUnavailable, std::move(message));
  }
  if (reader != nullptr) {
    *reader = std::move(local);
  } else if (local.remaining() != 0) {
    return absl::Status(absl::StatusCode::kInternal,
                        "unexpected replication response payload");
  }
  return absl::OkStatus();
}

std::size_t EncodedRecordBytes(const SnapshotRecord& record) {
  return 1 + 1 + 1 + 8 + 8 + 8 + 8 + 4 + 4 + 4 + 4 + record.key_.size() +
         record.value_.size();
}

bool EncodeRecords(std::uint16_t partition_id, std::uint64_t epoch,
                   std::span<const SnapshotRecord> records, Bytes* output) {
  std::size_t bytes = 2 + 8 + 4;
  for (const SnapshotRecord& record : records) {
    if (record.key_.size() > std::numeric_limits<std::uint32_t>::max() ||
        record.value_.size() > std::numeric_limits<std::uint32_t>::max() ||
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
    PutU8(*output, static_cast<std::uint8_t>(record.kind_));
    PutU8(*output, record.db_id_);
    PutU8(*output, static_cast<std::uint8_t>(record.value_type_));
    PutU64(*output, record.db_epoch_);
    PutU64(*output, record.mutation_sequence_);
    PutU64(*output, record.expire_at_ms_);
    PutU64(*output, record.logical_size_);
    PutU32(*output, record.chunk_index_);
    PutU32(*output, record.chunk_count_);
    PutU32(*output, static_cast<std::uint32_t>(record.key_.size()));
    PutU32(*output, static_cast<std::uint32_t>(record.value_.size()));
    PutString(*output, record.key_);
    PutString(*output, record.value_);
  }
  return true;
}

absl::StatusOr<
    std::tuple<std::uint16_t, std::uint64_t, std::vector<SnapshotRecord>>>
DecodeRecords(BytesView payload) {
  Reader reader(payload);
  std::uint16_t partition_id = 0;
  std::uint64_t epoch = 0;
  std::uint32_t count = 0;
  if (!reader.U16(&partition_id) || !reader.U64(&epoch) ||
      !reader.U32(&count) || partition_id >= storage::kLogicalStorageShards ||
      epoch == 0 || count > 65536) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "malformed apply-records request");
  }
  std::vector<SnapshotRecord> records;
  records.reserve(count);
  for (std::uint32_t i = 0; i < count; ++i) {
    std::uint8_t kind = 0;
    std::uint8_t value_type = 0;
    SnapshotRecord record;
    std::uint32_t key_size = 0;
    std::uint32_t value_size = 0;
    if (!reader.U8(&kind) || !reader.U8(&record.db_id_) ||
        !reader.U8(&value_type) || !reader.U64(&record.db_epoch_) ||
        !reader.U64(&record.mutation_sequence_) ||
        !reader.U64(&record.expire_at_ms_) ||
        !reader.U64(&record.logical_size_) ||
        !reader.U32(&record.chunk_index_) ||
        !reader.U32(&record.chunk_count_) || !reader.U32(&key_size) ||
        !reader.U32(&value_size) ||
        kind < static_cast<std::uint8_t>(SnapshotRecord::Kind::kValue) ||
        kind > static_cast<std::uint8_t>(SnapshotRecord::Kind::kValueCommit) ||
        record.db_id_ >= storage::kLogicalDatabaseCount ||
        value_type > static_cast<std::uint8_t>(storage::ValueType::kStream) ||
        !reader.String(key_size, &record.key_) ||
        !reader.String(value_size, &record.value_)) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "malformed replicated record");
    }
    record.kind_ = static_cast<SnapshotRecord::Kind>(kind);
    record.value_type_ = static_cast<storage::ValueType>(value_type);
    const bool value_frame =
        record.kind_ == SnapshotRecord::Kind::kValue ||
        record.kind_ == SnapshotRecord::Kind::kValueBegin ||
        record.kind_ == SnapshotRecord::Kind::kValueChunk ||
        record.kind_ == SnapshotRecord::Kind::kValueCommit;
    if ((value_frame && record.value_type_ == storage::ValueType::kNone) ||
        (!value_frame && (record.value_type_ != storage::ValueType::kNone ||
                          record.expire_at_ms_ != 0))) {
      return absl::Status(absl::StatusCode::kInvalidArgument,
                          "invalid replicated value metadata");
    }
    records.push_back(std::move(record));
  }
  if (reader.remaining() != 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "trailing apply-records bytes");
  }
  return std::tuple{partition_id, epoch, std::move(records)};
}

}  // namespace

class ReplicationManager::Impl {
 public:
  Impl(storage::StorageEngine* storage, const ReplicationOptions& options)
      : storage_(storage),
        options_(options),
        rpc_server_(options.listen_port_) {
    rpc_server_.OnVerbAsync(kResetPartition, [this](BytesView payload) {
      return HandleReset(payload);
    });
    rpc_server_.OnVerbAsync(kApplyRecords, [this](BytesView payload) {
      return HandleApply(payload);
    });
  }

  celer::Service* service() noexcept {
    return options_.listen_port_ == 0 ? nullptr : &rpc_server_;
  }

  void StorageReady(celer::Worker& worker) {
    ready_.store(true, std::memory_order_release);
    if (worker.id() == 0 && !options_.target_ip_.empty() &&
        options_.target_port_ != 0 && !coordinator_started_) {
      coordinator_started_ = true;
      worker.Spawn(Coordinator());
    }
  }

 private:
  struct PartitionState {
    bool complete_ = false;
    std::uint64_t target_epoch_ = 0;
    std::uint64_t acknowledged_ = 0;
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
    // if/else, not ?:, to keep the two co_awaits in separate full
    // expressions (GCC coroutine frame-slot aliasing).
    std::optional<absl::StatusOr<std::uint64_t>> reset;
    if (owner == celer::ThisWorker().id_) {
      reset.emplace(
          co_await storage_->ResetReplicaPartition(partition_id, epochs));
    } else {
      reset.emplace(co_await celer::SubmitTaskTo(
          owner,
          [this, partition_id,
           epochs]() -> Task<absl::StatusOr<std::uint64_t>> {
            co_return co_await storage_->ResetReplicaPartition(partition_id,
                                                               epochs);
          }));
    }
    if (!reset->ok()) {
      co_return ErrorResponse(reset->status().message());
    }
    Bytes response = OkResponse();
    PutU64(response, **reset);
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
    absl::Status status;
    if (owner == celer::ThisWorker().id_) {
      status =
          co_await storage_->ApplyReplicaRecords(partition_id, epoch, records);
    } else {
      status = co_await celer::SubmitTaskTo(
          owner,
          [this, partition_id, epoch, records = std::move(records)]() mutable {
            return ApplyReplicaRecordsOwned(partition_id, epoch,
                                            std::move(records));
          });
    }
    co_return status.ok() ? OkResponse() : ErrorResponse(status.message());
  }

  // Do not make the SubmitTaskTo closure itself a coroutine. Coroutine lambdas
  // retain a pointer to their closure, while this named coroutine moves the
  // record vector into its own target-worker frame before it can suspend.
  Task<absl::Status> ApplyReplicaRecordsOwned(
      std::uint16_t partition_id, std::uint64_t epoch,
      std::vector<SnapshotRecord> records) {
    co_return co_await storage_->ApplyReplicaRecords(partition_id, epoch,
                                                     records);
  }

  Task<absl::StatusOr<std::uint64_t>> ResetRemote(
      celer::rpc::RpcClient& client, std::uint16_t partition_id,
      const PartitionReplicationStart& start) {
    Bytes request;
    request.reserve(2 + 8 * storage::kLogicalDatabaseCount);
    PutU16(request, partition_id);
    for (std::uint64_t epoch : start.db_epochs_) PutU64(request, epoch);
    auto response = co_await client.Call(kResetPartition, request);
    if (!response.ok()) co_return response.status();
    Reader reader(BytesView(response->data(), response->size()));
    std::uint8_t error = 0;
    if (!reader.U8(&error)) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "truncated reset response");
    }
    if (error != 0) {
      std::uint32_t size = 0;
      std::string message;
      if (!reader.U32(&size) || !reader.String(size, &message)) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "malformed reset error response");
      }
      co_return absl::Status(absl::StatusCode::kUnavailable,
                             std::move(message));
    }
    std::uint64_t epoch = 0;
    if (!reader.U64(&epoch) || epoch == 0 || reader.remaining() != 0) {
      co_return absl::Status(absl::StatusCode::kInternal,
                             "malformed reset response");
    }
    co_return epoch;
  }

  Task<absl::Status> ApplyRemote(celer::rpc::RpcClient& client,
                                 std::uint16_t partition_id,
                                 std::uint64_t epoch,
                                 std::span<const SnapshotRecord> records) {
    if (records.empty()) co_return absl::OkStatus();
    std::size_t begin = 0;
    while (begin < records.size()) {
      if (EncodedRecordBytes(records[begin]) + 2 + 8 + 4 > kMaxApplyPayload) {
        const SnapshotRecord& large = records[begin];
        if (large.kind_ != SnapshotRecord::Kind::kValue ||
            large.value_type_ != storage::ValueType::kString ||
            large.value_.size() > storage::kMaxStringBytes) {
          co_return absl::Status(absl::StatusCode::kOutOfRange,
                                 "replicated record exceeds RPC payload limit");
        }
        const std::uint32_t chunk_count = static_cast<std::uint32_t>(
            (large.value_.size() + storage::kExtentPayloadBytes - 1) /
            storage::kExtentPayloadBytes);
        SnapshotRecord frame = large;
        frame.kind_ = SnapshotRecord::Kind::kValueBegin;
        frame.logical_size_ = large.value_.size();
        frame.chunk_index_ = 0;
        frame.chunk_count_ = chunk_count;
        frame.value_.clear();
        absl::Status sent =
            co_await ApplyRemote(client, partition_id, epoch,
                                 std::span<const SnapshotRecord>(&frame, 1));
        if (!sent.ok()) co_return sent;
        for (std::uint32_t index = 0; index < chunk_count; ++index) {
          const std::size_t offset =
              static_cast<std::size_t>(index) * storage::kExtentPayloadBytes;
          const std::size_t bytes = std::min(storage::kExtentPayloadBytes,
                                             large.value_.size() - offset);
          frame.kind_ = SnapshotRecord::Kind::kValueChunk;
          frame.chunk_index_ = index;
          frame.value_.assign(large.value_.data() + offset, bytes);
          sent =
              co_await ApplyRemote(client, partition_id, epoch,
                                   std::span<const SnapshotRecord>(&frame, 1));
          if (!sent.ok()) co_return sent;
        }
        frame.kind_ = SnapshotRecord::Kind::kValueCommit;
        frame.chunk_index_ = chunk_count;
        frame.value_.clear();
        sent = co_await ApplyRemote(client, partition_id, epoch,
                                    std::span<const SnapshotRecord>(&frame, 1));
        if (!sent.ok()) co_return sent;
        ++begin;
        continue;
      }
      std::size_t end = begin;
      std::size_t bytes = 2 + 8 + 4;
      while (end < records.size()) {
        const std::size_t record_bytes = EncodedRecordBytes(records[end]);
        if (end != begin && bytes + record_bytes > kMaxApplyPayload) break;
        if (bytes + record_bytes > kMaxApplyPayload) {
          co_return absl::Status(absl::StatusCode::kOutOfRange,
                                 "replicated record exceeds RPC payload limit");
        }
        bytes += record_bytes;
        ++end;
      }
      Bytes request;
      if (!EncodeRecords(partition_id, epoch,
                         records.subspan(begin, end - begin), &request)) {
        co_return absl::Status(absl::StatusCode::kOutOfRange,
                               "failed to encode replication batch");
      }
      auto response = co_await client.Call(kApplyRecords, request);
      if (!response.ok()) co_return response.status();
      absl::Status status =
          DecodeStatus(BytesView(response->data(), response->size()), nullptr);
      if (!status.ok()) co_return status;
      begin = end;
    }
    co_return absl::OkStatus();
  }

  Task<PartitionReplicationStart> BeginLocal(std::uint16_t partition_id) {
    const unsigned owner = partition_id % storage_->worker_count();
    if (owner == celer::ThisWorker().id_) {
      co_return storage_->BeginPartitionReplication(partition_id);
    }
    co_return co_await celer::SubmitTo(owner, [this, partition_id] {
      return storage_->BeginPartitionReplication(partition_id);
    });
  }

  Task<absl::StatusOr<PartitionSnapshotBatch>> SnapshotLocal(
      std::uint16_t partition_id, std::uint8_t db_id, std::uint64_t cursor) {
    const unsigned owner = partition_id % storage_->worker_count();
    if (owner == celer::ThisWorker().id_) {
      co_return co_await storage_->SnapshotPartition(
          partition_id, db_id, cursor, kSnapshotKeysPerBatch);
    }
    co_return co_await celer::SubmitTaskTo(
        owner,
        [this, partition_id, db_id,
         cursor]() -> Task<absl::StatusOr<PartitionSnapshotBatch>> {
          co_return co_await storage_->SnapshotPartition(
              partition_id, db_id, cursor, kSnapshotKeysPerBatch);
        });
  }

  Task<PartitionDeltaBatch> ReadDeltasLocal(std::uint16_t partition_id,
                                            std::uint64_t after) {
    const unsigned owner = partition_id % storage_->worker_count();
    if (owner == celer::ThisWorker().id_) {
      co_return storage_->ReadPartitionDeltas(partition_id, after,
                                              kDeltaRecordsPerBatch);
    }
    co_return co_await celer::SubmitTo(owner, [this, partition_id, after] {
      return storage_->ReadPartitionDeltas(partition_id, after,
                                           kDeltaRecordsPerBatch);
    });
  }

  Task<absl::Status> AckLocal(std::uint16_t partition_id,
                              std::uint64_t through) {
    const unsigned owner = partition_id % storage_->worker_count();
    if (owner == celer::ThisWorker().id_) {
      storage_->AcknowledgePartitionDeltas(partition_id, through);
      co_return absl::OkStatus();
    }
    co_return co_await celer::SubmitTo(owner, [this, partition_id, through] {
      storage_->AcknowledgePartitionDeltas(partition_id, through);
      return absl::OkStatus();
    });
  }

  Task<absl::Status> CatchUp(celer::rpc::RpcClient& client,
                             std::uint16_t partition_id,
                             PartitionState* state) {
    while (true) {
      PartitionDeltaBatch batch =
          co_await ReadDeltasLocal(partition_id, state->acknowledged_);
      if (batch.overflow_) {
        co_return absl::Status(absl::StatusCode::kAborted,
                               "partition delta retention overflow");
      }
      absl::Status applied = co_await ApplyRemote(
          client, partition_id, state->target_epoch_, batch.records_);
      if (!applied.ok()) co_return applied;
      if (!batch.records_.empty()) {
        state->acknowledged_ = batch.records_.back().mutation_sequence_;
      } else {
        state->acknowledged_ = batch.watermark_;
      }
      absl::Status acknowledged =
          co_await AckLocal(partition_id, state->acknowledged_);
      if (!acknowledged.ok()) co_return acknowledged;
      if (state->acknowledged_ >= batch.watermark_ &&
          batch.records_.size() < kDeltaRecordsPerBatch) {
        co_return absl::OkStatus();
      }
    }
  }

  Task<absl::Status> SynchronizeReadyPartition(
      celer::rpc::RpcClient& client, std::uint16_t partition_id,
      std::array<PartitionState, storage::kLogicalStorageShards>* states) {
    PartitionState& state = (*states)[partition_id];
    absl::Status caught_up = co_await CatchUp(client, partition_id, &state);
    while (caught_up.code() == absl::StatusCode::kAborted) {
      spdlog::warn("replication partition {} delta overflow; restarting",
                   partition_id);
      caught_up = co_await CopyPartition(client, partition_id, &state, states,
                                         nullptr, false);
    }
    co_return caught_up;
  }

  // Forward mutations for partitions whose snapshot has already completed
  // while later partitions are still being copied. A token for the partition
  // currently under snapshot is kept deferred; ReadPartitionDeltas will clear
  // its queued bit after that partition reaches CATCHUP/SYNCED.
  Task<absl::Status> DrainReadyPartitions(
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
            !(*states)[partition_id].complete_) {
          deferred->push_back(partition_id);
          continue;
        }
        absl::Status synced =
            co_await SynchronizeReadyPartition(client, partition_id, states);
        if (!synced.ok()) co_return synced;
        ++processed;
      }
    }

    while (processed < maximum) {
      std::uint16_t partition_id = 0;
      if (!storage_->TryTakeReplicationReady(&partition_id)) break;
      if (partition_id == skip_partition ||
          !(*states)[partition_id].complete_) {
        if (deferred != nullptr) deferred->push_back(partition_id);
        continue;
      }
      absl::Status synced =
          co_await SynchronizeReadyPartition(client, partition_id, states);
      if (!synced.ok()) co_return synced;
      ++processed;
    }
    co_return absl::OkStatus();
  }

  Task<absl::Status> CopyPartition(
      celer::rpc::RpcClient& client, std::uint16_t partition_id,
      PartitionState* state,
      std::array<PartitionState, storage::kLogicalStorageShards>* states,
      std::deque<std::uint16_t>* deferred, bool drain_others = true) {
    PartitionReplicationStart start = co_await BeginLocal(partition_id);
    auto target_epoch = co_await ResetRemote(client, partition_id, start);
    if (!target_epoch.ok()) co_return target_epoch.status();
    state->complete_ = false;
    state->target_epoch_ = *target_epoch;
    state->acknowledged_ = start.snapshot_sequence_;

    for (std::uint8_t db_id = 0; db_id < storage::kLogicalDatabaseCount;
         ++db_id) {
      if ((start.nonempty_db_mask_ & (std::uint16_t{1} << db_id)) == 0) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        auto batch = co_await SnapshotLocal(partition_id, db_id, cursor);
        if (!batch.ok()) co_return batch.status();
        absl::Status applied = co_await ApplyRemote(
            client, partition_id, state->target_epoch_, batch->records_);
        if (!applied.ok()) co_return applied;
        cursor = batch->cursor_;
        if (drain_others && states != nullptr) {
          absl::Status forwarded = co_await DrainReadyPartitions(
              client, states, deferred, 8, partition_id);
          if (!forwarded.ok()) co_return forwarded;
        }
      } while (cursor != 0);
    }
    absl::Status caught_up = co_await CatchUp(client, partition_id, state);
    if (!caught_up.ok()) co_return caught_up;
    state->complete_ = true;
    co_return absl::OkStatus();
  }

  Task<absl::Status> ReplicateOnce(celer::rpc::RpcClient& client) {
    std::array<PartitionState, storage::kLogicalStorageShards> states{};
    std::deque<std::uint16_t> deferred;
    for (std::uint32_t partition = 0;
         partition < storage::kLogicalStorageShards; ++partition) {
      const auto partition_id = static_cast<std::uint16_t>(partition);
      while (true) {
        absl::Status copied = co_await CopyPartition(
            client, partition_id, &states[partition_id], &states, &deferred);
        if (copied.ok()) break;
        if (copied.code() != absl::StatusCode::kAborted) co_return copied;
        spdlog::warn("replication partition {} delta overflow; restarting",
                     partition_id);
      }
      absl::Status forwarded =
          co_await DrainReadyPartitions(client, &states, &deferred, 64);
      if (!forwarded.ok()) co_return forwarded;
      if ((partition + 1) % 256 == 0) {
        spdlog::info("replication baseline partitions={}/{}", partition + 1,
                     storage::kLogicalStorageShards);
      }
    }
    absl::Status forwarded = co_await DrainReadyPartitions(
        client, &states, &deferred, std::numeric_limits<std::size_t>::max());
    if (!forwarded.ok()) co_return forwarded;
    spdlog::info("replication baseline complete; entering steady state");

    while (client.connected()) {
      std::uint16_t partition_id = 0;
      if (!storage_->TryTakeReplicationReady(&partition_id)) {
        absl::Status slept = co_await celer::SleepFor(
            *celer::ThisWorker().self_, std::chrono::milliseconds(1));
        if (!slept.ok()) co_return slept;
        continue;
      }
      if (!states[partition_id].complete_) continue;
      absl::Status caught_up =
          co_await SynchronizeReadyPartition(client, partition_id, &states);
      if (!caught_up.ok()) co_return caught_up;
    }
    co_return absl::Status(absl::StatusCode::kUnavailable,
                           "replication connection closed");
  }

  Task<absl::Status> Coordinator() {
    while (!celer::ThisWorker().self_->stop_requested()) {
      auto client = std::make_unique<celer::rpc::RpcClient>();
      absl::Status connected =
          co_await client->Connect(options_.target_ip_, options_.target_port_);
      if (!connected.ok()) {
        spdlog::warn("replication connect to {}:{} failed: {}",
                     options_.target_ip_, options_.target_port_,
                     connected.message());
      } else {
        spdlog::info("replication connected to {}:{}", options_.target_ip_,
                     options_.target_port_);
        absl::Status replicated = co_await ReplicateOnce(*client);
        spdlog::warn("replication session ended: {}", replicated.message());
        client->Close();
      }
      absl::Status slept = co_await celer::SleepFor(*celer::ThisWorker().self_,
                                                    std::chrono::seconds(1));
      if (!slept.ok()) co_return slept;
    }
    co_return absl::OkStatus();
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
