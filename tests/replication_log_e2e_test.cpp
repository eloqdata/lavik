#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "celer/net/server.h"
#include "keylane/command.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/replication_command.h"
#include "keylane/resp.h"
#include "keylane/storage/engine.h"
#include "keylane/tx/tx_shard.h"

namespace {

using keylane::ReplicatedCommand;
using keylane::storage::ReplicationEventKind;
using keylane::storage::ReplicationLogAppend;
using keylane::storage::ReplicationLogCursor;
using keylane::storage::ReplicationLogPayloadSource;
using keylane::storage::ReplicationLogState;
using keylane::storage::StorageEngine;
using keylane::storage::StorageEngineOptions;

constexpr std::size_t kMiB = 1024 * 1024;

void Check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error(std::string(message));
  }
}

void CreateDataFile(const std::string& path) {
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  Check(fd >= 0, "failed to create replication-log test file");
  const int allocated = ::posix_fallocate(fd, 0, 256 * kMiB);
  const int closed = ::close(fd);
  Check(allocated == 0 && closed == 0,
        "failed to allocate replication-log test file");
}

class RepeatedByteSource final : public ReplicationLogPayloadSource {
 public:
  RepeatedByteSource(std::size_t size, char byte) : size_(size), byte_(byte) {}

  std::uint64_t size() const noexcept override { return size_; }

  celer::Task<absl::Status> Read(std::uint64_t offset,
                                 std::span<std::byte> output) override {
    if (offset > size_ || output.size() > size_ - offset) {
      co_return absl::Status(absl::StatusCode::kOutOfRange,
                             "test payload source read is out of range");
    }
    std::fill(output.begin(), output.end(), static_cast<std::byte>(byte_));
    co_return absl::OkStatus();
  }

 private:
  std::size_t size_ = 0;
  char byte_ = 0;
};

class ReplicationLogService final : public celer::Service {
 public:
  ReplicationLogService(StorageEngine* storage, bool exercise)
      : storage_(storage), exercise_(exercise) {}

  void Prepare(unsigned thread_count) override {
    Check(thread_count == 1, "replication log test requires one worker");
  }

  celer::Task<absl::Status> Run(celer::Worker& worker,
                                celer::ServiceContext) override {
    worker_ = &worker;
    keylane::BindMemoryAccountingShard(worker.id());
    keylane::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok() && exercise_) {
      result_ = co_await Exercise();
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  const absl::Status& result() const noexcept { return result_; }

 private:
  celer::Task<absl::Status> ExecuteClientCommand(
      std::uint8_t db_id, std::vector<std::string> args,
      std::string_view expected_reply) {
    auto request = keylane::BuildCommandRequest(
        keylane::RespCommand{.args_ = std::move(args)}, db_id);
    if (!request.ok()) co_return request.status();
    keylane::ReplyBuilder reply_builder;
    keylane::CommandReply reply =
        co_await keylane::ExecuteCommand(*request, reply_builder);
    std::string actual(reply.encoded_);
    if (reply.disk_value_.has_value()) {
      const auto bytes = reply.disk_value_->network_bytes();
      actual.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    if (reply.chunks_ || actual != expected_reply) {
      co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                             "client replication command returned '" +
                                 actual +
                                 "' instead of '" +
                                 std::string(expected_reply) + "'");
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> WaitForReplicationTail(
      std::uint64_t expected_tail) {
    for (unsigned attempt = 0; attempt < 5'000; ++attempt) {
      const auto info = storage_->LocalReplicationLogInfo();
      if (info.state_ != ReplicationLogState::kActive) {
        co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                               "replication publisher invalidated the log");
      }
      if (info.tail_lsn_ >= expected_tail) co_return absl::OkStatus();
      absl::Status slept =
          co_await celer::SleepFor(*worker_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return slept;
    }
    co_return absl::Status(absl::StatusCode::kDeadlineExceeded,
                           "replication publisher did not reach the tail");
  }

  celer::Task<absl::Status> Exercise() {
    // Snapshot handoff closes every worker's transaction admission word while
    // retaining the count that was already admitted on that worker.
    Check(keylane::TryBeginSnapshotTransaction(),
          "open snapshot transaction gate rejected an operation");
    Check(keylane::SnapshotTransactionsActive(),
          "snapshot transaction gate lost its local active count");
    Check(keylane::CloseSnapshotTransactionGate(),
          "snapshot transaction gate did not close");
    Check(!keylane::TryBeginSnapshotTransaction(),
          "closed snapshot transaction gate admitted an operation");
    Check(!keylane::CloseSnapshotTransactionGate(),
          "snapshot transaction gate allowed two cut owners");
    keylane::EndSnapshotTransaction();
    Check(!keylane::SnapshotTransactionsActive(),
          "snapshot transaction gate did not drain");
    keylane::OpenSnapshotTransactionGate();
    Check(keylane::TryBeginSnapshotTransaction(),
          "reopened snapshot transaction gate rejected an operation");
    keylane::EndSnapshotTransaction();
    Check(keylane::TryBeginReplicationTransactionOrder(),
          "replication transaction order did not admit first owner");
    Check(!keylane::TryBeginReplicationTransactionOrder(),
          "replication transaction order admitted two owners");
    keylane::EndReplicationTransactionOrder();
    Check(keylane::TryBeginReplicationTransactionOrder(),
          "replication transaction order did not reopen");
    keylane::EndReplicationTransactionOrder();

    absl::Status status = co_await storage_->EnableReplicationLog(3, 8 * kMiB);
    if (!status.ok()) co_return status;
    RepeatedByteSource too_large(9 * kMiB, 'x');
    auto rejected =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .partition_id_ = 3,
            .partition_sequence_ = 1,
            .payload_ = {},
            .payload_source_ = &too_large,
        });
    Check(!rejected.ok() && storage_->LocalReplicationLogInfo().state_ ==
                                ReplicationLogState::kInvalid,
          "oversized event did not invalidate the bounded backlog");
    auto primary_write =
        co_await storage_->Set(0, "primary-survives", "ok", {});
    if (!primary_write.ok()) co_return primary_write.status();
    auto primary_length =
        co_await storage_->StringLength(0, "primary-survives");
    if (!primary_length.ok()) co_return primary_length.status();
    Check(*primary_length == 2,
          "replication backlog failure affected primary storage");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    status = co_await storage_->EnableReplicationLog(4, 8 * kMiB);
    if (!status.ok()) co_return status;
    std::vector<std::string> overflow_args;
    overflow_args.emplace_back("SET");
    overflow_args.emplace_back("publisher-overflow");
    overflow_args.emplace_back(9 * kMiB, 'Q');
    status =
        co_await ExecuteClientCommand(0, std::move(overflow_args), "+OK\r\n");
    if (!status.ok()) co_return status;
    Check(storage_->LocalReplicationLogInfo().state_ ==
              ReplicationLogState::kInvalid,
          "publisher overflow did not invalidate the replication flow");
    auto overflow_length =
        co_await storage_->StringLength(0, "publisher-overflow");
    if (!overflow_length.ok()) co_return overflow_length.status();
    Check(*overflow_length == 9 * kMiB,
          "publisher overflow backpressured the primary write");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    status = co_await storage_->EnableReplicationLog(17, 3 * 8 * kMiB);
    if (!status.ok()) co_return status;
    std::string payload(kMiB, 'a');
    for (std::uint64_t sequence = 1; sequence <= 12; ++sequence) {
      std::fill(payload.begin(), payload.end(),
                static_cast<char>('a' + sequence - 1));
      auto appended =
          co_await storage_->AppendReplicationLog(ReplicationLogAppend{
              .kind_ = ReplicationEventKind::kMutation,
              .partition_id_ = 7,
              .partition_sequence_ = sequence,
              .payload_ = payload,
              .payload_source_ = nullptr,
          });
      if (!appended.ok()) co_return appended.status();
      Check(*appended == sequence, "unexpected replication LSN");
    }

    ReplicationLogCursor cursor{};
    std::uint64_t expected_lsn = 1;
    while (true) {
      auto batch = co_await storage_->ReadReplicationLog(cursor, 600 * 1024, 2);
      if (!batch.ok()) co_return batch.status();
      for (const auto& frame : batch->frames_) {
        Check(frame.header_.lsn_ == expected_lsn,
              "replication read returned an unexpected LSN");
        Check(frame.header_.fragment_index_ == 0,
              "small event was unexpectedly fragmented");
        Check(frame.payload_.size() == kMiB,
              "replication payload length changed");
        Check(
            frame.payload_.front() == static_cast<char>('a' + expected_lsn - 1),
            "replication payload content changed");
        ++expected_lsn;
      }
      cursor = batch->next_;
      if (batch->at_tail_) break;
    }
    Check(expected_lsn == 13, "replication read did not reach the tail");

    status = co_await storage_->TrimReplicationLog(8);
    if (!status.ok()) co_return status;
    const auto after_trim = storage_->LocalReplicationLogInfo();
    Check(after_trim.floor_lsn_ == 8,
          "trim did not advance the replication floor");
    auto below_floor = co_await storage_->ReadReplicationLog(
        ReplicationLogCursor{.lsn_ = 1}, kMiB, 1);
    Check(!below_floor.ok() &&
              below_floor.status().code() == absl::StatusCode::kOutOfRange,
          "cursor below floor did not require a full synchronization");

    RepeatedByteSource large(9 * kMiB, 'L');
    auto large_lsn =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .kind_ = ReplicationEventKind::kMutation,
            .partition_id_ = 9,
            .partition_sequence_ = 13,
            .payload_ = {},
            .payload_source_ = &large,
        });
    if (!large_lsn.ok()) co_return large_lsn.status();
    Check(*large_lsn == 13, "large event received an unexpected LSN");

    cursor = {.lsn_ = 13};
    std::size_t large_bytes = 0;
    std::uint32_t expected_fragment = 0;
    while (true) {
      auto batch = co_await storage_->ReadReplicationLog(cursor, kMiB, 1);
      if (!batch.ok()) co_return batch.status();
      Check(batch->frames_.size() == 1,
            "bounded large-event read returned the wrong frame count");
      const auto& frame = batch->frames_.front();
      Check(frame.header_.lsn_ == 13 &&
                frame.header_.fragment_index_ == expected_fragment,
            "large event fragment cursor is discontinuous");
      Check(!frame.payload_.empty() && frame.payload_.front() == 'L' &&
                frame.payload_.back() == 'L',
            "large event fragment payload changed");
      large_bytes += frame.payload_.size();
      ++expected_fragment;
      cursor = batch->next_;
      if (batch->at_tail_) break;
    }
    Check(large_bytes == large.size() && expected_fragment == 2,
          "large event was not reconstructed from two fragments");
    auto bad_cursor = co_await storage_->ReadReplicationLog(
        ReplicationLogCursor{.lsn_ = 13, .fragment_index_ = 99}, kMiB, 1);
    Check(
        !bad_cursor.ok() &&
            bad_cursor.status().code() == absl::StatusCode::kInvalidArgument &&
            storage_->LocalReplicationLogInfo().state_ ==
                ReplicationLogState::kActive,
        "one invalid replica cursor poisoned the shared backlog");

    auto next = co_await storage_->AppendReplicationLog(ReplicationLogAppend{
        .kind_ = ReplicationEventKind::kControl,
        .partition_id_ = 9,
        .partition_sequence_ = 14,
        .payload_ = "control",
        .payload_source_ = nullptr,
    });
    if (!next.ok()) co_return next.status();
    Check(*next == 14, "capacity eviction changed the next LSN");
    status = co_await storage_->TrimReplicationLog(14);
    if (!status.ok()) co_return status;
    Check(storage_->LocalReplicationLogInfo().floor_lsn_ == 14,
          "fragmented-event trim left a partial logical event");

    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    Check(storage_->LocalReplicationLogInfo().state_ ==
                  ReplicationLogState::kDisabled &&
              storage_->LocalReplicationLogInfo().block_count_ == 0,
          "disable did not reclaim the replication log");

    // Client command dispatch transfers committed writes to the asynchronous
    // publisher. Large arguments may span replication frames, but decode and
    // apply still see one command and one LSN.
    status = co_await storage_->EnableReplicationLog(19, 3 * 8 * kMiB);
    if (!status.ok()) co_return status;
    constexpr std::uint64_t kExpireAt = 4'102'444'800'000ULL;
    std::vector<std::string> set_args{"SET", "replication-set", "value", "PXAT",
                                      std::to_string(kExpireAt)};
    status = co_await ExecuteClientCommand(2, std::move(set_args), "+OK\r\n");
    if (!status.ok()) co_return status;
    status = co_await WaitForReplicationTail(1);
    if (!status.ok()) co_return status;
    std::vector<std::string> skipped_args{"SET", "replication-set", "ignored",
                                          "NX"};
    status =
        co_await ExecuteClientCommand(2, std::move(skipped_args), "$-1\r\n");
    if (!status.ok()) co_return status;
    co_await celer::Yield(*worker_);
    Check(storage_->LocalReplicationLogInfo().tail_lsn_ == 1,
          "conditional SET no-op published a replication command");

    std::string large_value(9 * kMiB, 'V');
    std::vector<std::string> large_args;
    large_args.emplace_back("SET");
    large_args.emplace_back("replication-large");
    large_args.emplace_back(std::move(large_value));
    status = co_await ExecuteClientCommand(2, std::move(large_args), "+OK\r\n");
    if (!status.ok()) co_return status;
    status = co_await WaitForReplicationTail(2);
    if (!status.ok()) co_return status;

    std::vector<ReplicatedCommand> commands;
    cursor = {};
    while (cursor.lsn_ <= 2) {
      const std::uint64_t command_lsn = cursor.lsn_;
      std::string encoded;
      std::uint32_t fragments = 0;
      do {
        auto batch = co_await storage_->ReadReplicationLog(cursor, kMiB, 1);
        if (!batch.ok()) co_return batch.status();
        Check(batch->frames_.size() == 1 &&
                  batch->frames_.front().header_.lsn_ == command_lsn,
              "command frame crossed an LSN boundary");
        encoded.append(batch->frames_.front().payload_);
        ++fragments;
        cursor = batch->next_;
      } while (cursor.lsn_ == command_lsn);
      auto decoded = keylane::DecodeReplicationCommand(encoded);
      if (!decoded.ok()) co_return decoded.status();
      Check(encoded.size() > 1 &&
                !keylane::DecodeReplicationCommand(
                     std::string_view(encoded).substr(0, encoded.size() - 1))
                     .ok(),
            "truncated replication command was accepted");
      if (command_lsn == 1) {
        Check(fragments == 1 && decoded->db_id_ == 2 &&
                  decoded->args_ == std::vector<std::string>(
                                        {"SET", "replication-set", "value",
                                         "PXAT", std::to_string(kExpireAt)}),
              "SET was not normalized with its absolute expiry");
      } else {
        Check(fragments > 1 && decoded->db_id_ == 2 &&
                  decoded->args_.size() == 3 && decoded->args_[0] == "SET" &&
                  decoded->args_[1] == "replication-large" &&
                  decoded->args_[2].size() == 9 * kMiB &&
                  decoded->args_[2].front() == 'V' &&
                  decoded->args_[2].back() == 'V',
              "large SET was not reconstructed as one logical command");
      }
      commands.push_back(std::move(*decoded));
    }

    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    auto db_zero_sentinel =
        co_await storage_->Set(0, "replication-set", "db-zero-sentinel", {});
    if (!db_zero_sentinel.ok()) co_return db_zero_sentinel.status();
    auto removed_set = co_await storage_->Delete(2, "replication-set");
    auto removed_large = co_await storage_->Delete(2, "replication-large");
    if (!removed_set.ok()) co_return removed_set.status();
    if (!removed_large.ok()) co_return removed_large.status();
    for (const ReplicatedCommand& command : commands) {
      status = co_await keylane::ApplyReplicatedCommand(command);
      if (!status.ok()) co_return status;
    }
    auto restored_length =
        co_await storage_->StringLength(2, "replication-set");
    auto restored_large_length =
        co_await storage_->StringLength(2, "replication-large");
    auto db_zero_sentinel_length =
        co_await storage_->StringLength(0, "replication-set");
    if (!restored_length.ok()) co_return restored_length.status();
    if (!restored_large_length.ok()) co_return restored_large_length.status();
    if (!db_zero_sentinel_length.ok()) {
      co_return db_zero_sentinel_length.status();
    }
    const auto restored_expiry =
        co_await storage_->GetExpiration(2, "replication-set");
    Check(*restored_length == 5 && *restored_large_length == 9 * kMiB &&
              *db_zero_sentinel_length == 16 && restored_expiry.exists_ &&
              restored_expiry.expire_at_ms_ == kExpireAt,
          "replica SET did not preserve its logical database");

    status = co_await storage_->EnableReplicationLog(20, 8 * kMiB);
    if (!status.ok()) co_return status;
    std::vector<std::string> delete_args{"DEL", "replication-set"};
    status = co_await ExecuteClientCommand(2, std::move(delete_args), ":1\r\n");
    if (!status.ok()) co_return status;
    status = co_await WaitForReplicationTail(1);
    if (!status.ok()) co_return status;
    std::vector<std::string> missing_args{"DEL", "replication-missing"};
    status =
        co_await ExecuteClientCommand(2, std::move(missing_args), ":0\r\n");
    if (!status.ok()) co_return status;
    co_await celer::Yield(*worker_);
    Check(storage_->LocalReplicationLogInfo().tail_lsn_ == 1,
          "DEL publication did not follow its logical result");
    auto del_batch = co_await storage_->ReadReplicationLog({}, kMiB, 1);
    if (!del_batch.ok()) co_return del_batch.status();
    Check(del_batch->frames_.size() == 1 && del_batch->at_tail_,
          "single-key DEL did not produce one frame");
    auto del_command =
        keylane::DecodeReplicationCommand(del_batch->frames_.front().payload_);
    if (!del_command.ok()) co_return del_command.status();
    Check(del_command->db_id_ == 2 &&
              del_command->args_ ==
                  std::vector<std::string>({"DEL", "replication-set"}),
          "DEL command payload changed");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    auto recreated = co_await storage_->Set(2, "replication-set", "again", {});
    if (!recreated.ok()) co_return recreated.status();
    status = co_await keylane::ApplyReplicatedCommand(*del_command);
    if (!status.ok()) co_return status;
    Check(!co_await storage_->Exists(2, "replication-set"),
          "replica DEL did not remove the key");

    // Single-key writes from every value family are journaled at the storage
    // mutation ordering point and replay through the normal command path.
    status = co_await storage_->EnableReplicationLog(22, 8 * kMiB);
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        4, {"LPUSH", "journal-list", "a", "b"}, ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        4, {"HSET", "journal-hash", "field", "value"}, ":1\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        4, {"SADD", "journal-set", "one", "two"}, ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        4, {"ZADD", "journal-zset", "1", "member"}, ":1\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(4, {"INCR", "journal-counter"},
                                           ":1\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        4, {"PEXPIRE", "journal-counter", "600000"}, ":1\r\n");
    if (!status.ok()) co_return status;
    auto family_fence = co_await storage_->FenceReplicationLog();
    if (!family_fence.ok()) co_return family_fence.status();
    Check(*family_fence == 7 &&
              storage_->LocalReplicationLogInfo().tail_lsn_ == 6,
          "publisher fence did not separate prior and future commands");

    std::vector<ReplicatedCommand> family_commands;
    cursor = {};
    while (cursor.lsn_ <= 6) {
      const std::uint64_t lsn = cursor.lsn_;
      std::string encoded;
      do {
        auto batch = co_await storage_->ReadReplicationLog(cursor, kMiB, 1);
        if (!batch.ok()) co_return batch.status();
        Check(batch->frames_.size() == 1 &&
                  batch->frames_.front().header_.lsn_ == lsn,
              "single-key command crossed an LSN boundary");
        encoded.append(batch->frames_.front().payload_);
        cursor = batch->next_;
      } while (cursor.lsn_ == lsn);
      auto decoded = keylane::DecodeReplicationCommand(encoded);
      if (!decoded.ok()) co_return decoded.status();
      family_commands.push_back(std::move(*decoded));
    }
    const std::vector<std::string> expected_names{
        "LPUSH", "HSET", "SADD", "ZADD", "INCR", "PEXPIREAT"};
    Check(family_commands.size() == expected_names.size(),
          "single-key command journal count changed");
    for (std::size_t i = 0; i < expected_names.size(); ++i) {
      Check(family_commands[i].db_id_ == 4 &&
                !family_commands[i].args_.empty() &&
                family_commands[i].args_.front() == expected_names[i],
            "single-key command journal order changed");
    }
    Check(family_commands.back().args_.size() == 3 &&
              family_commands.back().args_[1] == "journal-counter",
          "relative expiry was not normalized to PEXPIREAT");

    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    for (std::string_view key : {"journal-list", "journal-hash", "journal-set",
                                 "journal-zset", "journal-counter"}) {
      auto removed = co_await storage_->Delete(4, key);
      if (!removed.ok()) co_return removed.status();
    }
    for (const ReplicatedCommand& command : family_commands) {
      status = co_await keylane::ApplyReplicatedCommand(command);
      if (!status.ok()) co_return status;
    }
    status = co_await ExecuteClientCommand(4, {"LLEN", "journal-list"},
                                           ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        4, {"HGET", "journal-hash", "field"}, "$5\r\nvalue\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(4, {"SCARD", "journal-set"},
                                           ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        4, {"ZSCORE", "journal-zset", "member"}, "$1\r\n1\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(4, {"GET", "journal-counter"},
                                           "$1\r\n1\r\n");
    if (!status.ok()) co_return status;
    const auto journal_expiry =
        co_await storage_->GetExpiration(4, "journal-counter");
    Check(journal_expiry.exists_ && journal_expiry.expire_at_ms_ != 0,
          "replayed PEXPIREAT did not preserve the expiration");

    auto flush_victim = co_await storage_->Set(2, "flush-victim", "gone", {});
    auto flush_survivor =
        co_await storage_->Set(3, "flush-survivor", "kept", {});
    if (!flush_victim.ok()) co_return flush_victim.status();
    if (!flush_survivor.ok()) co_return flush_survivor.status();
    status = co_await storage_->EnableReplicationLog(21, 8 * kMiB);
    if (!status.ok()) co_return status;
    std::vector<std::string> flush_args{"FLUSHDB"};
    status = co_await ExecuteClientCommand(2, std::move(flush_args), "+OK\r\n");
    if (!status.ok()) co_return status;
    status = co_await WaitForReplicationTail(1);
    if (!status.ok()) co_return status;
    Check(!co_await storage_->Exists(2, "flush-victim") &&
              co_await storage_->Exists(3, "flush-survivor"),
          "source FLUSHDB affected the wrong database");
    auto flush_batch = co_await storage_->ReadReplicationLog({}, kMiB, 1);
    if (!flush_batch.ok()) co_return flush_batch.status();
    Check(flush_batch->frames_.size() == 1 && flush_batch->at_tail_ &&
              flush_batch->frames_.front().header_.kind_ ==
                  ReplicationEventKind::kControl,
          "FLUSHDB did not produce one control frame");
    auto flush_command = keylane::DecodeReplicationCommand(
        flush_batch->frames_.front().payload_);
    if (!flush_command.ok()) co_return flush_command.status();
    Check(flush_command->db_id_ == 2 && flush_command->args_.size() == 2 &&
              flush_command->args_[0] == "FLUSHDB" &&
              flush_command->args_[1] == std::to_string(storage_->DbEpoch(2)),
          "FLUSHDB barrier did not carry its installed database epoch");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    auto replica_victim =
        co_await storage_->Set(2, "replica-flush-victim", "gone", {});
    if (!replica_victim.ok()) co_return replica_victim.status();
    flush_command->args_[1] = std::to_string(storage_->DbEpoch(2) + 1);
    status = co_await keylane::ApplyReplicatedCommand(*flush_command);
    if (!status.ok()) co_return status;
    Check(!co_await storage_->Exists(2, "replica-flush-victim") &&
              co_await storage_->Exists(3, "flush-survivor"),
          "replica FLUSHDB barrier affected the wrong database");

    // Leave one sealed block and one active block behind. A fresh process must
    // recognize both as runtime-only backlog and recover without parsing them
    // as primary records.
    status = co_await storage_->EnableReplicationLog(23, 2 * 8 * kMiB);
    if (!status.ok()) co_return status;
    for (std::uint64_t sequence = 1; sequence <= 8; ++sequence) {
      auto appended =
          co_await storage_->AppendReplicationLog(ReplicationLogAppend{
              .partition_id_ = 11,
              .partition_sequence_ = sequence,
              .payload_ = payload,
              .payload_source_ = nullptr,
          });
      if (!appended.ok()) co_return appended.status();
    }
    co_return absl::OkStatus();
  }

  StorageEngine* storage_ = nullptr;
  celer::Worker* worker_ = nullptr;
  bool exercise_ = false;
  absl::Status result_ = absl::UnknownError("test service did not run");
};

int RunOnce(const std::string& path, bool exercise) {
  StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  StorageEngine storage(std::move(options));
  keylane::InitWorkerMetrics(1);
  const absl::Status memory = keylane::InitMemoryLimit(512 * kMiB, 1);
  if (!memory.ok()) {
    std::cerr << memory << '\n';
    return 1;
  }
  keylane::InitStorage(&storage, nullptr);
  const absl::Status prepared = storage.Prepare(1);
  if (!prepared.ok()) {
    std::cerr << prepared << '\n';
    return 1;
  }
  keylane::tx::TxRuntime::Create(1);
  ReplicationLogService service(&storage, exercise);
  celer::Server server;
  server.AddService(&service);
  celer::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  const absl::Status started = server.Start(runtime);
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

int main(int argc, char** argv) {
  try {
    if (argc == 3 && std::string_view(argv[1]) == "--recover") {
      return RunOnce(argv[2], false);
    }
    Check(argc == 1, "unexpected replication-log test arguments");
    const std::string path =
        "/tmp/keylane-replication-log-" + std::to_string(::getpid()) + ".data";
    CreateDataFile(path);
    const int writer = RunOnce(path, true);
    if (writer != 0) {
      (void)::unlink(path.c_str());
      return writer;
    }
    const pid_t child = ::fork();
    Check(child >= 0, "failed to fork recovery verifier");
    if (child == 0) {
      ::execl(argv[0], argv[0], "--recover", path.c_str(), nullptr);
      _exit(127);
    }
    int status = 0;
    Check(::waitpid(child, &status, 0) == child,
          "failed to wait for recovery verifier");
    (void)::unlink(path.c_str());
    Check(WIFEXITED(status) && WEXITSTATUS(status) == 0,
          "recovery verifier rejected the old replication backlog");
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
