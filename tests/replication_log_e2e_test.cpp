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

#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

#include "../src/redis/list_command.h"
#include "../src/redis/set_command.h"
#include "../src/redis/sort_command.h"
#include "../src/redis/string_command.h"
#include "../src/redis/zset_command.h"
#include "bycorf/net/server.h"
#include "lavik/command.h"
#include "lavik/command_table.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/replication_command.h"
#include "lavik/replication_history.h"
#include "lavik/resp.h"
#include "lavik/storage/engine.h"
#include "lavik/storage/format.h"
#include "lavik/storage/scan_hash_map.h"
#include "lavik/tx/tx_shard.h"
#include "support/test_data_path.h"

namespace {

using lavik::ReplicatedCommand;
using lavik::storage::PartitionFullSyncBatch;
using lavik::storage::PartitionReplicationStart;
using lavik::storage::ReplicationEventKind;
using lavik::storage::ReplicationLogAppend;
using lavik::storage::ReplicationLogCursor;
using lavik::storage::ReplicationLogPayloadSource;
using lavik::storage::ReplicationLogState;
using lavik::storage::ScanHashMapEntryArena;
using lavik::storage::StorageEngine;
using lavik::storage::StorageEngineOptions;

constexpr std::size_t kMiB = 1024 * 1024;

void Check(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error(std::string(message));
  }
}

struct MutationPreconditionProbe {
  bool allow_ = false;
  mutable unsigned calls_ = 0;
};

absl::Status ValidateMutationPreconditionProbe(const void* opaque) {
  const auto* probe = static_cast<const MutationPreconditionProbe*>(opaque);
  ++probe->calls_;
  return probe->allow_
             ? absl::OkStatus()
             : absl::FailedPreconditionError("test mutation rejected");
}

std::vector<std::string> ReplicatedEffectAt(const ReplicatedCommand& command,
                                            std::size_t wanted,
                                            std::uint8_t* db_id = nullptr) {
  if (command.args_.empty() ||
      command.args_.front() != lavik::kReplicatedExecCommand) {
    Check(wanted == 0, "replication effect index is out of range");
    if (db_id != nullptr) *db_id = command.db_id_;
    return command.args_;
  }
  Check(command.args_.size() >= 2, "replicated EXEC header is truncated");
  const std::size_t count = std::stoull(command.args_[1]);
  Check(wanted < count, "replication effect index is out of range");
  std::size_t offset = 2;
  for (std::size_t index = 0; index < count; ++index) {
    Check(offset + 2 <= command.args_.size(),
          "replicated EXEC command header is truncated");
    const auto effect_db =
        static_cast<std::uint8_t>(std::stoull(command.args_[offset++]));
    const std::size_t argc = std::stoull(command.args_[offset++]);
    Check(argc != 0 && argc <= command.args_.size() - offset,
          "replicated EXEC command is truncated");
    if (index == wanted) {
      if (db_id != nullptr) *db_id = effect_db;
      return std::vector<std::string>(command.args_.begin() + offset,
                                      command.args_.begin() + offset + argc);
    }
    offset += argc;
  }
  throw std::runtime_error("replication effect was not found");
}

ReplicatedCommand ReplicationTransactionBody(ReplicatedCommand command) {
  if (command.args_.empty() ||
      !lavik::IsReplicationTransactionEnvelope(command.args_.front())) {
    return command;
  }
  auto envelope =
      lavik::DecodeReplicationTransactionEnvelope(command.args_.front());
  Check(envelope.ok(), "replication transaction envelope is malformed");
  Check(command.args_.size() > 1,
        "replication transaction command body is missing");
  command.args_.erase(command.args_.begin());
  return command;
}

void CheckReplicationTransactionEnvelopeCodec() {
  auto encoded = lavik::EncodeReplicationTransactionEnvelope(
      lavik::ReplicationTransactionEnvelope{
          .id_ = 0x8877665544332211ULL,
          .payload_flow_ = 3,
          .participants_ = {7, 0, 3},
      });
  Check(encoded.ok() && encoded->size() == 17 &&
            lavik::IsReplicationTransactionEnvelope(*encoded),
        "transaction envelope was not encoded as compact V1 metadata");
  auto decoded = lavik::DecodeReplicationTransactionEnvelope(*encoded);
  Check(decoded.ok() && decoded->id_ == 0x8877665544332211ULL &&
            decoded->payload_flow_ == 3 &&
            decoded->participants_ == std::vector<unsigned>({0, 3, 7}),
        "transaction envelope V1 metadata did not round trip");

  std::string noncanonical = *encoded;
  noncanonical.push_back('\0');
  noncanonical[14] = 2;
  Check(!lavik::DecodeReplicationTransactionEnvelope(noncanonical).ok(),
        "transaction envelope accepted a trailing zero bitmap byte");
  Check(!lavik::EncodeReplicationTransactionEnvelope(
             lavik::ReplicationTransactionEnvelope{
                 .id_ = 1, .payload_flow_ = 0, .participants_ = {0, 0}})
             .ok(),
        "transaction envelope accepted a duplicate participant");
}

std::string EncodeTestTransactionEnvelope(
    std::uint64_t id, unsigned payload_flow,
    const std::vector<unsigned>& participants) {
  auto encoded = lavik::EncodeReplicationTransactionEnvelope(
      lavik::ReplicationTransactionEnvelope{
          .id_ = id,
          .payload_flow_ = payload_flow,
          .participants_ = participants,
      });
  Check(encoded.ok(), "test transaction envelope encoding failed");
  return std::move(*encoded);
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

  bycorf::Task<absl::Status> Read(std::uint64_t offset,
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

class ReplicationLogService final : public bycorf::Service {
 public:
  ReplicationLogService(StorageEngine* storage, bool exercise)
      : storage_(storage), exercise_(exercise) {}

  void Prepare(unsigned thread_count) override {
    Check(thread_count == 1, "replication log test requires one worker");
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    worker_ = &worker;
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
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
  bycorf::Task<absl::Status> ExerciseSharedHistoryQuota() {
    auto history = std::make_shared<lavik::ReplicationHistory>(8 * kMiB);
    auto reset = history->Reset("parent", 2);
    if (!reset.ok()) co_return reset;
    storage_->SetReplicationHistory(0, history);
    auto enabled = co_await storage_->EnableReplicationLog(900, 8 * kMiB);
    if (!enabled.ok()) co_return enabled;
    Check(history->TryRetain("parent", {{0, 10, "payload"}, {1, 20, "marker"}}),
          "secondary effect was not admitted before child publication");
    auto appended = co_await storage_->AppendReplicationLog(
        ReplicationLogAppend{.partition_id_ = 3,
                             .partition_sequence_ = 1,
                             .payload_ = "child",
                             .payload_source_ = nullptr});
    if (!appended.ok()) co_return appended.status();
    Check(
        history->primary_bytes() == 8 * kMiB && history->secondary_bytes() == 0,
        "published child block did not reclaim the shared secondary quota");
    Check(!history->TryRetain("parent", {{0, 11, "tail"}}),
          "secondary retention overspent quota held by a child block");
    auto disabled = co_await storage_->DisableReplicationLog();
    if (!disabled.ok()) co_return disabled;
    Check(history->primary_bytes() == 0,
          "retired child block leaked its history charge");
    storage_->SetReplicationHistory(0, nullptr);
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseMutationPrecondition() {
    auto rejected_probe = std::make_shared<MutationPreconditionProbe>();
    const lavik::storage::MutationPrecondition rejected_precondition(
        std::shared_ptr<const void>(rejected_probe),
        &ValidateMutationPreconditionProbe);
    auto rejected_write = co_await storage_->Set(
        15, "mutation-precondition", "blocked", {}, nullptr, nullptr,
        std::nullopt, &rejected_precondition);
    if (rejected_write.ok() ||
        !absl::IsFailedPrecondition(rejected_write.status())) {
      co_return absl::FailedPreconditionError(
          "storage mutation precondition did not reject SET");
    }
    if (rejected_probe->calls_ != 1 ||
        co_await storage_->Exists(15, "mutation-precondition")) {
      co_return absl::FailedPreconditionError(
          "rejected storage mutation changed the keyspace");
    }

    auto accepted_probe = std::make_shared<MutationPreconditionProbe>();
    accepted_probe->allow_ = true;
    const lavik::storage::MutationPrecondition accepted_precondition(
        std::shared_ptr<const void>(accepted_probe),
        &ValidateMutationPreconditionProbe);
    auto accepted_write = co_await storage_->Set(
        15, "mutation-precondition", "accepted", {}, nullptr, nullptr,
        std::nullopt, &accepted_precondition);
    if (!accepted_write.ok() || !accepted_write->applied_ ||
        accepted_probe->calls_ != 1 ||
        !co_await storage_->Exists(15, "mutation-precondition")) {
      co_return absl::FailedPreconditionError(
          "accepted storage mutation precondition did not publish SET");
    }

    auto rejected_ephemeral_probe =
        std::make_shared<MutationPreconditionProbe>();
    const lavik::storage::MutationPrecondition rejected_ephemeral_precondition(
        std::shared_ptr<const void>(rejected_ephemeral_probe),
        &ValidateMutationPreconditionProbe);
    absl::Status status = co_await storage_->EnableReplicationLog(27, 8 * kMiB);
    if (!status.ok()) co_return status;
    const auto before_ephemeral = storage_->LocalReplicationLogInfo();
    status = co_await storage_->PublishEphemeralReplicationCommand(
        0, {"PUBLISH", "mutation-precondition-channel", "blocked"},
        rejected_ephemeral_precondition);
    const auto after_ephemeral = storage_->LocalReplicationLogInfo();
    if (!absl::IsFailedPrecondition(status) ||
        rejected_ephemeral_probe->calls_ != 1 ||
        after_ephemeral.tail_lsn_ != before_ephemeral.tail_lsn_ ||
        after_ephemeral.publish_queue_bytes_ != 0) {
      co_return absl::FailedPreconditionError(
          "rejected ephemeral mutation entered the replication publisher");
    }
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExecuteClientCommand(
      std::uint8_t db_id, std::vector<std::string> args,
      std::string_view expected_reply) {
    auto request = lavik::BuildCommandRequest(
        lavik::RespCommand{.args_ = std::move(args)}, db_id);
    if (!request.ok()) co_return request.status();
    lavik::ReplyBuilder reply_builder;
    lavik::CommandReply reply =
        co_await lavik::ExecuteCommand(*request, reply_builder);
    std::string actual(reply.encoded_);
    if (reply.disk_value_.valid()) {
      const auto bytes = reply.disk_value_.network_bytes();
      actual.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    if (reply.chunks_ || actual != expected_reply) {
      co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                             "client replication command returned '" + actual +
                                 "' instead of '" +
                                 std::string(expected_reply) + "'");
    }
    co_return absl::OkStatus();
  }

  absl::Status LimitRetainedHeadroom() {
    lavik::RefreshMemoryStats();
    const std::uint64_t used = lavik::GetMemoryStats().used_bytes_;
    constexpr std::uint64_t kAdmissionHeadroom = 128 * 1024;
    if (used > std::numeric_limits<std::uint64_t>::max() - kAdmissionHeadroom) {
      return absl::ResourceExhaustedError(
          "cannot construct transaction-guard admission test limit");
    }
    const std::uint64_t steady_target = used + kAdmissionHeadroom;
    const std::uint64_t steady_allowance = steady_target / 9 + 1;
    if (steady_target >
        std::numeric_limits<std::uint64_t>::max() - steady_allowance) {
      return absl::ResourceExhaustedError(
          "transaction-guard admission test limit overflows");
    }
    // Retained allocations use ninety percent of the configured limit. Choose
    // a limit whose retained portion leaves a small positive margin, then
    // bypass top-level dispatch so this specifically exercises each handler's
    // guard check rather than an earlier scratch-allocation admission.
    return lavik::InitMemoryLimit(steady_target + steady_allowance, 1);
  }

  bycorf::Task<absl::Status> ExerciseTransactionGuardAdmission(
      std::vector<std::string> args) {
    auto request = lavik::BuildCommandRequest(
        lavik::RespCommand{.args_ = std::move(args)}, 0);
    if (!request.ok()) co_return request.status();

    absl::Status status = LimitRetainedHeadroom();
    if (!status.ok()) co_return status;

    lavik::ReplyBuilder builder;
    lavik::CommandReply reply;
    switch (request->kind_) {
      case lavik::CommandKind::kLMPop:
        reply = co_await lavik::ExecuteListMultiKey(*request, builder);
        break;
      case lavik::CommandKind::kSUnionStore:
        reply = co_await lavik::ExecuteSetMultiKey(*request, builder);
        break;
      case lavik::CommandKind::kZUnionStore:
        reply = co_await lavik::ExecuteZSetMultiKey(*request, builder);
        break;
      case lavik::CommandKind::kSort:
        reply = co_await lavik::ExecuteSortCommand(*request, builder);
        break;
      default:
        status = absl::InvalidArgumentError(
            "unsupported transaction-guard admission test command");
        break;
    }

    const absl::Status restored = lavik::InitMemoryLimit(512 * kMiB, 1);
    if (!restored.ok()) co_return restored;
    if (!status.ok()) co_return status;
    constexpr std::string_view kExpected =
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n";
    if (reply.encoded_ != kExpected) {
      co_return absl::FailedPreconditionError(
          request->args_.front() + " transaction guard returned '" +
          std::string(reply.encoded_) + "' instead of a Redis OOM error");
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseBitOpPayloadAdmission() {
    std::vector<std::string> source_args{"SET", "bitop-admission-source",
                                         std::string(kMiB, 'B')};
    absl::Status status =
        co_await ExecuteClientCommand(0, std::move(source_args), "+OK\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        0, {"SET", "bitop-admission-destination", "old"}, "+OK\r\n");
    if (!status.ok()) co_return status;
    // Do not tighten maxmemory while either setup event still owns a pending
    // publisher reservation; the test targets canonical payload growth, not
    // backlog materialization.
    status = co_await WaitForReplicationTail(2);
    if (!status.ok()) co_return status;

    auto request = lavik::BuildCommandRequest(
        lavik::RespCommand{.args_ = {"BITOP", "OR",
                                     "bitop-admission-destination",
                                     "bitop-admission-source"}},
        0);
    if (!request.ok()) co_return request.status();
    status = LimitRetainedHeadroom();
    if (!status.ok()) co_return status;
    {
      auto probe = lavik::TryReserveMemory(kMiB);
      if (probe.has_value()) {
        co_return absl::FailedPreconditionError(
            "transaction admission test left at least one MiB of headroom");
      }
    }

    lavik::ReplyBuilder builder;
    lavik::CommandReply reply =
        co_await lavik::ExecuteBitOpCommand(*request, builder);
    const absl::Status restored = lavik::InitMemoryLimit(512 * kMiB, 1);
    if (!restored.ok()) co_return restored;
    constexpr std::string_view kExpected =
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n";
    if (reply.encoded_ != kExpected) {
      co_return absl::FailedPreconditionError(
          "BITOP payload admission returned '" + std::string(reply.encoded_) +
          "' instead of a Redis OOM error");
    }
    co_return co_await ExecuteClientCommand(
        0, {"GET", "bitop-admission-destination"}, "$3\r\nold\r\n");
  }

  bycorf::Task<absl::Status> WaitForReplicationTail(
      std::uint64_t expected_tail) {
    for (unsigned attempt = 0; attempt < 5'000; ++attempt) {
      const auto info = storage_->LocalReplicationLogInfo();
      if (info.state_ != ReplicationLogState::kActive) {
        co_return absl::Status(absl::StatusCode::kFailedPrecondition,
                               "replication publisher invalidated the log");
      }
      if (info.tail_lsn_ >= expected_tail) co_return absl::OkStatus();
      absl::Status slept =
          co_await bycorf::SleepFor(*worker_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return slept;
    }
    co_return absl::Status(absl::StatusCode::kDeadlineExceeded,
                           "replication publisher did not reach the tail");
  }

  bycorf::Task<absl::Status> ExerciseCanonicalTransactionGrowth() {
    // A transaction can canonicalize to an after-image larger than both its
    // request and the queue waterline. Keep another command behind it: the
    // single publisher must process the admitted head even though the byte
    // metric is temporarily above the waterline, rather than waiting for its
    // own tail to disappear.
    absl::Status status =
        co_await storage_->EnableReplicationLog(18, 4 * 8 * kMiB);
    if (!status.ok()) co_return status;
    status = co_await storage_->SetReplicationPublishQueueCapacity(kMiB);
    if (!status.ok()) co_return status;
    auto head_admission =
        co_await storage_->AcquireReplicationPublisherAdmission(128);
    if (!head_admission.ok()) co_return head_admission.status();
    auto transaction =
        std::make_shared<lavik::storage::ReplicationTransaction>();
    transaction->id_ = 9001;
    transaction->db_id_ = 0;
    transaction->participants_ = {worker_->id()};
    transaction->payload_flow_ = worker_->id();
    transaction->envelope_metadata_ = EncodeTestTransactionEnvelope(
        transaction->id_, transaction->payload_flow_,
        transaction->participants_);
    transaction->command_args_ = {"SET", "canonical-head", "small"};
    auto transaction_bytes =
        lavik::storage::ReplicationTransactionAllocationBytes(
            transaction->participants_.capacity(),
            std::span<const std::string>(&transaction->envelope_metadata_, 1),
            transaction->command_args_);
    Check(transaction_bytes.has_value(),
          "transaction retained size was not representable");
    transaction->retained_charge_.Account(lavik::CurrentMemoryAccountingShard(),
                                          *transaction_bytes);
    Check(storage_->TryEnqueueReplicationTransaction(transaction),
          "canonical head transaction was not enqueued");

    auto tail_admission =
        co_await storage_->AcquireReplicationPublisherAdmission(128);
    if (!tail_admission.ok()) co_return tail_admission.status();
    Check(storage_->TryEnqueueReplicationCommand(
              lavik::storage::ReplicationCommandAppend{
                  .kind_ = ReplicationEventKind::kMutation,
                  .db_id_ = 0,
                  .partition_id_ = 0,
                  .partition_sequence_ = 9002,
                  .args_ = {"SET", "canonical-tail", "value"},
              }),
          "canonical tail command was not enqueued");

    std::vector<std::string> final_command{"SET", "canonical-head",
                                           std::string(2 * kMiB, 'c')};
    const auto final_bytes =
        lavik::storage::ReplicationTransactionAllocationBytes(
            transaction->participants_.capacity(),
            std::span<const std::string>(&transaction->envelope_metadata_, 1),
            final_command);
    Check(final_bytes.has_value() && *final_bytes >= *transaction_bytes,
          "canonical transaction growth was not representable");
    auto canonical_growth = lavik::TryReserveMemory(
        *final_bytes - transaction->retained_charge_.bytes());
    if (!canonical_growth.has_value()) {
      co_return absl::ResourceExhaustedError(
          "canonical transaction test growth was not admitted");
    }
    transaction->command_args_.swap(final_command);
    transaction->retained_charge_.Resize(*final_bytes);
    canonical_growth->Release();
    transaction->resolution_.store(
        lavik::storage::ReplicationTransactionResolution::kPublish,
        std::memory_order_release);
    storage_->ReleaseReplicationPublisherAdmission(*tail_admission, 128);
    storage_->ReleaseReplicationPublisherAdmission(*head_admission, 128);
    status = co_await WaitForReplicationTail(2);
    if (!status.ok()) co_return status;
    co_return co_await storage_->DisableReplicationLog();
  }

  bycorf::Task<absl::Status> PrepareSourceAfterImagesPartOne() {
    constexpr std::uint8_t kDb = 7;
    absl::Status status = co_await ExecuteClientCommand(
        kDb, {"RPUSH", "late-list-source", "left", "moved"}, ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"SADD", "late-smove-source", "kept", "moved"}, ":2\r\n");
    if (!status.ok()) co_return status;
    std::vector<std::string> bit_a{"SET", "late-bit-a", std::string(1, '\x0f')};
    status = co_await ExecuteClientCommand(kDb, std::move(bit_a), "+OK\r\n");
    if (!status.ok()) co_return status;
    std::vector<std::string> bit_b{"SET", "late-bit-b", std::string(1, '\xf0')};
    status = co_await ExecuteClientCommand(kDb, std::move(bit_b), "+OK\r\n");
    if (!status.ok()) co_return status;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> PrepareSourceAfterImagesPartTwo() {
    constexpr std::uint8_t kDb = 7;
    absl::Status status = co_await ExecuteClientCommand(
        kDb, {"SADD", "late-set-a", "a", "b"}, ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"SADD", "late-set-b", "b", "c"}, ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"ZADD", "late-zset-a", "1", "a"}, ":1\r\n");
    if (!status.ok()) co_return status;
    co_return co_await ExecuteClientCommand(
        kDb, {"ZADD", "late-zset-b", "2", "a", "4", "b"}, ":2\r\n");
  }

  bycorf::Task<absl::Status> PrepareMultiPopAfterImages() {
    constexpr std::uint8_t kDb = 7;
    absl::Status status = co_await ExecuteClientCommand(
        kDb, {"RPUSH", "late-pop-first", "a", "b"}, ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"RPUSH", "late-pop-second", "x"}, ":1\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"ZADD", "late-zpop-first", "1", "a", "2", "b"}, ":2\r\n");
    if (!status.ok()) co_return status;
    co_return co_await ExecuteClientCommand(
        kDb, {"ZADD", "late-zpop-second", "1", "x"}, ":1\r\n");
  }

  bycorf::Task<absl::Status> ExpireSourceAfterImages() {
    constexpr std::uint8_t kDb = 7;
    absl::Status status;
    constexpr std::array<std::string_view, 10> kSources{
        "late-list-source", "late-smove-source", "late-bit-a",  "late-bit-b",
        "late-set-a",       "late-set-b",        "late-zset-a", "late-zset-b",
        "late-pop-first",   "late-zpop-first"};
    for (std::string_view key : kSources) {
      std::vector<std::string> expiry{"PEXPIRE", std::string(key), "2000"};
      status = co_await ExecuteClientCommand(kDb, std::move(expiry), ":1\r\n");
      if (!status.ok()) co_return status;
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> PrepareSourceAfterImages() {
    absl::Status status = co_await PrepareSourceAfterImagesPartOne();
    if (!status.ok()) co_return status;
    status = co_await PrepareSourceAfterImagesPartTwo();
    if (!status.ok()) co_return status;
    status = co_await PrepareMultiPopAfterImages();
    if (!status.ok()) co_return status;
    co_return co_await ExpireSourceAfterImages();
  }

  bycorf::Task<absl::StatusOr<std::vector<ReplicatedCommand>>>
  JournalSourceAfterImages() {
    constexpr std::uint8_t kDb = 7;
    absl::Status status = co_await storage_->EnableReplicationLog(25, 8 * kMiB);
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb,
        {"LMOVE", "late-list-source", "late-list-destination", "RIGHT", "LEFT"},
        "$5\r\nmoved\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"SMOVE", "late-smove-source", "late-smove-destination", "moved"},
        ":1\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb,
        {"BITOP", "OR", "late-bit-destination", "late-bit-a", "late-bit-b"},
        ":1\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb,
        {"SUNIONSTORE", "late-set-destination", "late-set-a", "late-set-b"},
        ":3\r\n");
    if (!status.ok()) co_return status;
    status =
        co_await ExecuteClientCommand(kDb,
                                      {"ZUNIONSTORE", "late-zset-destination",
                                       "2", "late-zset-a", "late-zset-b"},
                                      ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb,
        {"LMPOP", "2", "late-pop-first", "late-pop-second", "LEFT", "COUNT",
         "1"},
        "*2\r\n$14\r\nlate-pop-first\r\n*1\r\n$1\r\na\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb,
        {"ZMPOP", "2", "late-zpop-first", "late-zpop-second", "MIN", "COUNT",
         "1"},
        "*2\r\n$15\r\nlate-zpop-first\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n");
    if (!status.ok()) co_return status;
    auto fence = co_await storage_->FenceReplicationLog();
    if (!fence.ok()) co_return fence.status();
    Check(*fence == 8,
          "source-dependent writes did not publish seven commands");

    std::vector<ReplicatedCommand> commands;
    ReplicationLogCursor cursor;
    while (cursor.lsn_ <= 7) {
      const std::uint64_t lsn = cursor.lsn_;
      std::string encoded;
      do {
        auto batch = co_await storage_->ReadReplicationLog(cursor, kMiB, 1);
        if (!batch.ok()) co_return batch.status();
        Check(batch->frames_.size() == 1 &&
                  batch->frames_.front().header_.lsn_ == lsn,
              "after-image command crossed an LSN boundary");
        encoded.append(batch->frames_.front().payload_);
        cursor = batch->next_;
      } while (cursor.lsn_ == lsn);
      auto decoded = lavik::DecodeReplicationCommand(encoded);
      if (!decoded.ok()) co_return decoded.status();
      commands.push_back(ReplicationTransactionBody(std::move(*decoded)));
    }

    const auto list_source_effect = ReplicatedEffectAt(commands[0], 0);
    Check(list_source_effect ==
              std::vector<std::string>({"RPOP", "late-list-source"}),
          "LMOVE source effect was not deterministic");
    Check(ReplicatedEffectAt(commands[0], 1) ==
              std::vector<std::string>(
                  {"LPUSH", "late-list-destination", "moved"}),
          "LMOVE destination effect omitted the moved value");
    Check(ReplicatedEffectAt(commands[1], 0) ==
              std::vector<std::string>({"SREM", "late-smove-source", "moved"}),
          "SMOVE source effect was not deterministic");
    Check(ReplicatedEffectAt(commands[1], 1) ==
              std::vector<std::string>(
                  {"SADD", "late-smove-destination", "moved"}),
          "SMOVE destination effect omitted the moved member");
    Check(ReplicatedEffectAt(commands[2], 0) ==
              std::vector<std::string>(
                  {"SET", "late-bit-destination", std::string(1, '\xff')}),
          "BITOP did not publish its destination after-image");
    Check(ReplicatedEffectAt(commands[3], 0) ==
                  std::vector<std::string>({"DEL", "late-set-destination"}) &&
              ReplicatedEffectAt(commands[3], 1).front() == "SADD",
          "Set STORE did not publish its destination after-image");
    Check(ReplicatedEffectAt(commands[4], 0) ==
                  std::vector<std::string>({"DEL", "late-zset-destination"}) &&
              ReplicatedEffectAt(commands[4], 1).front() == "ZADD",
          "Sorted Set STORE did not publish its destination after-image");
    Check(ReplicatedEffectAt(commands[5], 0) ==
              std::vector<std::string>({"LPOP", "late-pop-first", "1"}),
          "LMPOP retained the original key-selection command");
    Check(ReplicatedEffectAt(commands[6], 0) ==
              std::vector<std::string>({"ZPOPMIN", "late-zpop-first", "1"}),
          "ZMPOP retained the original key-selection command");
    co_return commands;
  }

  bycorf::Task<absl::Status> ReplaySourceAfterImages(
      std::vector<ReplicatedCommand> commands) {
    constexpr std::uint8_t kDb = 7;
    absl::Status status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    constexpr std::array<std::string_view, 5> kDestinations{
        "late-list-destination", "late-smove-destination",
        "late-bit-destination", "late-set-destination",
        "late-zset-destination"};
    for (std::string_view key : kDestinations) {
      auto removed = co_await storage_->Delete(kDb, key);
      if (!removed.ok()) co_return removed.status();
    }
    status =
        co_await bycorf::SleepFor(*worker_, std::chrono::milliseconds(2100));
    if (!status.ok()) co_return status;
    for (const ReplicatedCommand& command : commands) {
      status = co_await lavik::ApplyReplicatedCommand(command);
      if (!status.ok()) co_return status;
    }
    status = co_await ExecuteClientCommand(
        kDb, {"LINDEX", "late-list-destination", "0"}, "$5\r\nmoved\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"SISMEMBER", "late-smove-destination", "moved"}, ":1\r\n");
    if (!status.ok()) co_return status;
    std::string expected_bit = "$1\r\n";
    expected_bit.push_back(static_cast<char>(0xff));
    expected_bit.append("\r\n");
    status = co_await ExecuteClientCommand(kDb, {"GET", "late-bit-destination"},
                                           expected_bit);
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"SCARD", "late-set-destination"}, ":3\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"ZSCORE", "late-zset-destination", "a"}, "$1\r\n3\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        kDb, {"LINDEX", "late-pop-second", "0"}, "$1\r\nx\r\n");
    if (!status.ok()) co_return status;
    co_return co_await ExecuteClientCommand(
        kDb, {"ZSCORE", "late-zpop-second", "x"}, "$1\r\n1\r\n");
  }

  bycorf::Task<absl::Status> ExerciseSourceAfterImages() {
    // Source-dependent writes publish deterministic destination after-images.
    // Replay after every source deadline has passed must still reproduce the
    // committed destination instead of silently becoming a replica no-op.
    absl::Status status = co_await PrepareSourceAfterImages();
    if (!status.ok()) co_return status;
    auto commands = co_await JournalSourceAfterImages();
    if (!commands.ok()) co_return commands.status();
    co_return co_await ReplaySourceAfterImages(std::move(*commands));
  }

  bycorf::Task<absl::Status> ExerciseFullSyncOverrides() {
    constexpr std::uint64_t kFirstSession = 101;
    constexpr std::uint64_t kSecondSession = 202;
    constexpr std::uint8_t kDb = 3;
    const std::string key = "fullsync-override{coalesce}";
    const std::string sequence_bump = "fullsync-sequence{coalesce}";
    const std::uint16_t partition_id = lavik::storage::RedisSlot(key);
    const std::uint64_t reserved_before =
        lavik::GetMemoryStats().fullsync_reserved_bytes_;

    auto old_record = co_await storage_->Set(kDb, key, "before-fence", {});
    if (!old_record.ok()) co_return old_record.status();
    auto bumped = co_await storage_->Set(kDb, sequence_bump, "bump", {});
    if (!bumped.ok()) co_return bumped.status();

    // A session must obtain its fixed publisher staging budget before it can
    // expose any coverage state. Prove a deliberately tight worker limit
    // rejects the session without leaving partial ownership behind.
    absl::Status tight_limit = lavik::InitMemoryLimit(
        ScanHashMapEntryArena::kSmallSpanAdmissionBytes - 1, 1);
    if (!tight_limit.ok()) co_return tight_limit;
    auto under_reserved = storage_->BeginFullSyncSession(100);
    Check(!under_reserved.ok(),
          "full-sync session bypassed first-span admission");
    absl::Status restored_limit = lavik::InitMemoryLimit(512 * kMiB, 1);
    if (!restored_limit.ok()) co_return restored_limit;

    lavik::RefreshMemoryStats();
    const std::uint64_t retained_before_staging =
        lavik::GetMemoryStats().used_bytes_;
    auto first_session = storage_->BeginFullSyncSession(kFirstSession);
    if (!first_session.ok()) co_return first_session.status();
    auto second_session = storage_->BeginFullSyncSession(kSecondSession);
    if (!second_session.ok()) co_return second_session.status();
    lavik::RefreshMemoryStats();
    const std::uint64_t staging_capacity =
        storage_->LocalReplicationLogInfo().publish_queue_capacity_bytes_;
    Check(lavik::GetMemoryStats().used_bytes_ >=
              retained_before_staging + 2 * staging_capacity,
          "full-sync sessions did not hold fixed publisher staging budgets");
    const std::uint64_t reserved_after =
        lavik::GetMemoryStats().fullsync_reserved_bytes_;
    Check(reserved_after >=
              reserved_before +
                  2 * ScanHashMapEntryArena::kSmallSpanAdmissionBytes,
          "full-sync sessions did not reserve their first arena spans");
    auto first_start_result =
        storage_->BeginPartitionReplication(kFirstSession, partition_id);
    if (!first_start_result.ok()) co_return first_start_result.status();
    absl::Status first_db =
        storage_->BeginPartitionDbReplication(kFirstSession, partition_id, kDb);
    if (!first_db.ok()) co_return first_db;
    const PartitionReplicationStart first_start = *first_start_result;
    auto second_start_result =
        storage_->BeginPartitionReplication(kSecondSession, partition_id);
    if (!second_start_result.ok()) co_return second_start_result.status();
    absl::Status second_db = storage_->BeginPartitionDbReplication(
        kSecondSession, partition_id, kDb);
    if (!second_db.ok()) co_return second_db;
    const PartitionReplicationStart second_start = *second_start_result;
    Check(first_start.baseline_version_ == second_start.baseline_version_ &&
              first_start.baseline_version_ >= 2,
          "full-sync sessions did not fence the current runtime version");

    const std::uint64_t reserved_before_snapshot =
        lavik::GetMemoryStats().fullsync_reserved_bytes_;
    bool found_old_record = false;
    std::uint64_t cursor = 0;
    do {
      auto snapshot = co_await storage_->SnapshotPartition(
          kFirstSession, partition_id, kDb, cursor, 16, 2);
      if (!snapshot.ok()) co_return snapshot.status();
      for (const auto& record : snapshot->records_) {
        if (record.key_ != key) continue;
        found_old_record = true;
        Check(record.value_ == "before-fence" &&
                  record.mutation_sequence_ == first_start.baseline_version_,
              "baseline exposed a record's historical mutation sequence");
      }
      cursor = snapshot->cursor_;
    } while (cursor != 0);
    Check(found_old_record, "full-sync baseline did not enumerate the old key");
    Check(lavik::GetMemoryStats().fullsync_reserved_bytes_ <
              reserved_before_snapshot,
          "full-sync coverage allocation did not consume reserved credit");

    auto first = co_await storage_->Set(kDb, key, "first", {});
    if (!first.ok()) co_return first.status();
    auto stale_result = co_await storage_->ReadPartitionFullSyncOverrides(
        kFirstSession, partition_id, 16);
    if (!stale_result.ok()) co_return stale_result.status();
    PartitionFullSyncBatch stale = std::move(*stale_result);
    Check(
        stale.records_.size() == 1 && stale.records_.front().value_ == "first",
        "full-sync subscriber did not capture the first committed value");

    auto second = co_await storage_->Set(kDb, key, "second", {});
    if (!second.ok()) co_return second.status();
    storage_->AcknowledgePartitionFullSyncOverrides(kFirstSession, partition_id,
                                                    stale.records_);

    auto first_latest_result =
        co_await storage_->ReadPartitionFullSyncOverrides(kFirstSession,
                                                          partition_id, 16);
    if (!first_latest_result.ok()) co_return first_latest_result.status();
    auto second_latest_result =
        co_await storage_->ReadPartitionFullSyncOverrides(kSecondSession,
                                                          partition_id, 16);
    if (!second_latest_result.ok()) co_return second_latest_result.status();
    PartitionFullSyncBatch first_latest = std::move(*first_latest_result);
    PartitionFullSyncBatch second_latest = std::move(*second_latest_result);
    Check(first_latest.records_.size() == 1 &&
              first_latest.records_.front().value_ == "second",
          "an old full-sync ACK removed a newer override");
    Check(second_latest.records_.size() == 1 &&
              second_latest.records_.front().value_ == "second",
          "full-sync sessions did not coalesce independently");

    storage_->AcknowledgePartitionFullSyncOverrides(kFirstSession, partition_id,
                                                    first_latest.records_);
    storage_->AcknowledgePartitionFullSyncOverrides(
        kSecondSession, partition_id, second_latest.records_);
    auto first_empty = co_await storage_->ReadPartitionFullSyncOverrides(
        kFirstSession, partition_id, 16);
    if (!first_empty.ok()) co_return first_empty.status();
    Check(first_empty->records_.empty(),
          "full-sync ACK did not release the first session override");
    auto second_empty = co_await storage_->ReadPartitionFullSyncOverrides(
        kSecondSession, partition_id, 16);
    if (!second_empty.ok()) co_return second_empty.status();
    Check(second_empty->records_.empty(),
          "full-sync ACK did not release the second session override");

    storage_->EndPartitionReplication(kFirstSession, partition_id);
    auto third = co_await storage_->Set(kDb, key, "third", {});
    if (!third.ok()) co_return third.status();
    auto ended = co_await storage_->ReadPartitionFullSyncOverrides(
        kFirstSession, partition_id, 16);
    Check(!ended.ok(), "ended full-sync session remained subscribed");
    auto remaining_result = co_await storage_->ReadPartitionFullSyncOverrides(
        kSecondSession, partition_id, 16);
    if (!remaining_result.ok()) co_return remaining_result.status();
    PartitionFullSyncBatch remaining = std::move(*remaining_result);
    Check(remaining.records_.size() == 1 &&
              remaining.records_.front().value_ == "third",
          "ending one full-sync session affected another subscriber");
    storage_->EndPartitionReplication(kSecondSession, partition_id);
    storage_->EndFullSyncSession(kFirstSession);
    storage_->EndFullSyncSession(kSecondSession);
    Check(lavik::GetMemoryStats().fullsync_reserved_bytes_ == reserved_before,
          "full-sync coverage reservation was not released");

    // A post-fence identity can be larger than the pre-scanned map budget.
    // The primary write remains durable, while the lower-priority attempt is
    // invalidated before its override maps allocate beyond that credit.
    constexpr std::uint64_t kBudgetSession = 225;
    const std::uint64_t budget_before =
        lavik::GetMemoryStats().fullsync_reserved_bytes_;
    auto budget_session = storage_->BeginFullSyncSession(kBudgetSession);
    if (!budget_session.ok()) co_return budget_session.status();
    const std::uint64_t budget_after =
        lavik::GetMemoryStats().fullsync_reserved_bytes_;
    const std::uint64_t budget_bytes = budget_after - budget_before;
    if (budget_bytes / 2 + 1 > lavik::storage::MaxKeyBytes()) {
      co_return absl::ResourceExhaustedError(
          "full-sync test reservation cannot form a valid oversized key");
    }
    std::string budget_key(static_cast<std::size_t>(budget_bytes / 2 + 1), 'B');
    const std::uint16_t budget_partition =
        lavik::storage::RedisSlot(budget_key);
    auto budget_start =
        storage_->BeginPartitionReplication(kBudgetSession, budget_partition);
    if (!budget_start.ok()) co_return budget_start.status();
    absl::Status budget_db = storage_->BeginPartitionDbReplication(
        kBudgetSession, budget_partition, kDb);
    if (!budget_db.ok()) co_return budget_db;
    auto budget_write = co_await storage_->Set(kDb, budget_key, "kept", {});
    if (!budget_write.ok()) co_return budget_write.status();
    Check(!storage_->FullSyncSessionValid(kBudgetSession),
          "coverage exhaustion did not invalidate full sync");
    Check(co_await storage_->Exists(kDb, budget_key),
          "coverage exhaustion rolled back the foreground write");
    storage_->EndPartitionReplication(kBudgetSession, budget_partition);
    storage_->EndFullSyncSession(kBudgetSession);
    auto budget_removed = co_await storage_->Delete(kDb, budget_key);
    if (!budget_removed.ok()) co_return budget_removed.status();

    constexpr std::uint64_t kTxSession = 250;
    const std::string tx_key = "fullsync-tx{coalesce}";
    const auto tx_digest = lavik::storage::ComputeDigest(tx_key);
    const std::uint16_t tx_partition = lavik::storage::RedisSlot(tx_key);
    auto tx_session = storage_->BeginFullSyncSession(kTxSession);
    if (!tx_session.ok()) co_return tx_session.status();
    auto tx_start =
        storage_->BeginPartitionReplication(kTxSession, tx_partition);
    if (!tx_start.ok()) co_return tx_start.status();
    absl::Status tx_db =
        storage_->BeginPartitionDbReplication(kTxSession, tx_partition, kDb);
    if (!tx_db.ok()) co_return tx_db;
    lavik::storage::TxShardWrites committed_tx;
    const std::uint64_t committed_txid = StorageEngine::AllocateWriteTxid();
    storage_->InitializeTxWrites(committed_txid, std::span(&committed_tx, 1));
    committed_tx.collect_undo_ = true;
    {
      auto key_lock = co_await lavik::tx::CurrentTxShard().AcquireKey(
          kDb, lavik::tx::FingerprintOf(tx_digest),
          lavik::tx::LockMode::kExclusive);
      auto staged = co_await storage_->SetLocked(
          kDb, tx_key, tx_digest, "committed", {}, &committed_tx);
      if (!staged.ok()) co_return staged.status();
      auto before_commit = co_await storage_->ReadPartitionFullSyncOverrides(
          kTxSession, tx_partition, 16);
      if (!before_commit.ok()) co_return before_commit.status();
      Check(before_commit->records_.empty(),
            "transaction participant leaked before the commit decision");
      storage_->PublishCommittedFullSyncEffects(&committed_tx);
      key_lock.Reset();
    }
    auto committed_effect_result =
        co_await storage_->ReadPartitionFullSyncOverrides(kTxSession,
                                                          tx_partition, 16);
    if (!committed_effect_result.ok())
      co_return committed_effect_result.status();
    PartitionFullSyncBatch committed_effect =
        std::move(*committed_effect_result);
    Check(committed_effect.records_.size() == 1 &&
              committed_effect.records_.front().value_ == "committed",
          "committed transaction effect was not published");
    storage_->AcknowledgePartitionFullSyncOverrides(kTxSession, tx_partition,
                                                    committed_effect.records_);
    absl::Status discarded =
        co_await storage_->DiscardTxUndoLocal(committed_tx.txid_);
    if (!discarded.ok()) co_return discarded;
    std::vector<lavik::storage::TxShardWrites*> committed_shards{&committed_tx};
    absl::Status durable = co_await storage_->CommitTxWrites(
        committed_tx.txid_, std::move(committed_shards));
    if (!durable.ok()) co_return durable;

    const std::string rollback_key = "fullsync-rollback{coalesce}";
    const auto rollback_digest = lavik::storage::ComputeDigest(rollback_key);
    lavik::storage::TxShardWrites rolled_back_tx;
    const std::uint64_t rolled_back_txid = StorageEngine::AllocateWriteTxid();
    storage_->InitializeTxWrites(rolled_back_txid,
                                 std::span(&rolled_back_tx, 1));
    rolled_back_tx.collect_undo_ = true;
    {
      auto key_lock = co_await lavik::tx::CurrentTxShard().AcquireKey(
          kDb, lavik::tx::FingerprintOf(rollback_digest),
          lavik::tx::LockMode::kExclusive);
      auto staged = co_await storage_->SetLocked(
          kDb, rollback_key, rollback_digest, "aborted", {}, &rolled_back_tx);
      if (!staged.ok()) co_return staged.status();
      absl::Status rolled =
          co_await storage_->RollbackTxLocal(rolled_back_tx.txid_);
      if (!rolled.ok()) co_return rolled;
      key_lock.Reset();
    }
    auto after_rollback = co_await storage_->ReadPartitionFullSyncOverrides(
        kTxSession, tx_partition, 16);
    if (!after_rollback.ok()) co_return after_rollback.status();
    Check(after_rollback->records_.empty(),
          "rolled-back transaction emitted a full-sync effect");
    storage_->EndPartitionReplication(kTxSession, tx_partition);
    storage_->EndFullSyncSession(kTxSession);

    constexpr std::uint64_t kDiskBackedOverrideSession = 303;
    auto disk_backed_session =
        storage_->BeginFullSyncSession(kDiskBackedOverrideSession);
    if (!disk_backed_session.ok()) co_return disk_backed_session.status();
    auto disk_backed_start = storage_->BeginPartitionReplication(
        kDiskBackedOverrideSession, partition_id);
    if (!disk_backed_start.ok()) co_return disk_backed_start.status();
    absl::Status disk_backed_db = storage_->BeginPartitionDbReplication(
        kDiskBackedOverrideSession, partition_id, kDb);
    if (!disk_backed_db.ok()) co_return disk_backed_db;
    auto oversized =
        co_await storage_->Set(kDb, key, std::string(2048, 'x'), {});
    if (!oversized.ok()) co_return oversized.status();
    auto oversized_override = co_await storage_->ReadPartitionFullSyncOverrides(
        kDiskBackedOverrideSession, partition_id, 16);
    if (!oversized_override.ok()) co_return oversized_override.status();
    Check(oversized_override->records_.size() == 1 &&
              oversized_override->records_.front().value_.size() == 2048,
          "full-sync override did not materialize the current disk value");
    auto oversized_length = co_await storage_->StringLength(kDb, key);
    if (!oversized_length.ok()) co_return oversized_length.status();
    Check(*oversized_length == 2048,
          "disk-backed full-sync override changed the primary value");
    auto external =
        co_await storage_->Set(kDb, key, std::string(10 * kMiB, 'z'), {});
    if (!external.ok()) co_return external.status();
    auto streamed_override = co_await storage_->ReadPartitionFullSyncOverrides(
        kDiskBackedOverrideSession, partition_id, 16,
        lavik::storage::kReplicationTransferBytes);
    if (!streamed_override.ok()) co_return streamed_override.status();
    Check(streamed_override->records_.size() == 1 &&
              streamed_override->records_.front().value_.empty() &&
              streamed_override->records_.front().source_id_ != 0 &&
              streamed_override->records_.front().source_value_bytes_ ==
                  10 * kMiB,
          "large full-sync override was copied into the batch vector");
    const auto& streamed_record = streamed_override->records_.front();
    auto first_chunk = co_await storage_->ReadFullSyncValueChunk(
        kDiskBackedOverrideSession, partition_id, streamed_record.source_id_, 0,
        lavik::storage::kReplicationTransferBytes);
    if (!first_chunk.ok()) co_return first_chunk.status();
    auto last_chunk = co_await storage_->ReadFullSyncValueChunk(
        kDiskBackedOverrideSession, partition_id, streamed_record.source_id_,
        9 * kMiB, lavik::storage::kReplicationTransferBytes);
    if (!last_chunk.ok()) co_return last_chunk.status();
    Check(first_chunk->size() == 2 * kMiB && last_chunk->size() == kMiB &&
              first_chunk->front() == 'z' && last_chunk->back() == 'z',
          "large full-sync override did not stream bounded chunks");
    storage_->AcknowledgePartitionFullSyncOverrides(
        kDiskBackedOverrideSession, partition_id, streamed_override->records_);
    storage_->EndPartitionReplication(kDiskBackedOverrideSession, partition_id);
    storage_->EndFullSyncSession(kDiskBackedOverrideSession);

    constexpr std::uint64_t kReplacementRaceSession = 305;
    const std::string race_first = "fullsync-race-a{materialize-race}";
    const std::string race_second = "fullsync-race-b{materialize-race}";
    const std::uint16_t race_partition = lavik::storage::RedisSlot(race_first);
    Check(lavik::storage::RedisSlot(race_second) == race_partition,
          "replacement race keys do not share a partition");
    auto race_session = storage_->BeginFullSyncSession(kReplacementRaceSession);
    if (!race_session.ok()) co_return race_session.status();
    auto race_start = storage_->BeginPartitionReplication(
        kReplacementRaceSession, race_partition);
    if (!race_start.ok()) co_return race_start.status();
    absl::Status race_db = storage_->BeginPartitionDbReplication(
        kReplacementRaceSession, race_partition, kDb);
    if (!race_db.ok()) co_return race_db;
    auto race_first_small = co_await storage_->Set(kDb, race_first, "a", {});
    if (!race_first_small.ok()) co_return race_first_small.status();
    auto race_second_small = co_await storage_->Set(kDb, race_second, "b", {});
    if (!race_second_small.ok()) co_return race_second_small.status();

    auto race_lock = co_await lavik::tx::CurrentTxShard().AcquireKey(
        kDb,
        lavik::tx::FingerprintOf(lavik::storage::ComputeDigest(race_first)),
        lavik::tx::LockMode::kExclusive);
    bool race_read_finished = false;
    absl::Status race_read_status = absl::UnknownError("not started");
    std::optional<PartitionFullSyncBatch> race_batch;
    auto read_racing_batch = [&]() -> bycorf::Task<absl::Status> {
      auto read = co_await storage_->ReadPartitionFullSyncOverrides(
          kReplacementRaceSession, race_partition, 16,
          lavik::storage::kReplicationTransferBytes);
      if (read.ok()) race_batch.emplace(std::move(*read));
      race_read_status = read.status();
      race_read_finished = true;
      co_return absl::OkStatus();
    };
    worker_->Spawn(read_racing_batch());
    for (unsigned spin = 0; spin < 32; ++spin) {
      co_await bycorf::Yield(*worker_);
    }
    Check(!race_read_finished,
          "replacement race reader did not wait on the first key");
    auto race_second_large = co_await storage_->Set(
        kDb, race_second, std::string(10 * kMiB, 'r'), {});
    if (!race_second_large.ok()) co_return race_second_large.status();
    race_lock.Reset();
    while (!race_read_finished) co_await bycorf::Yield(*worker_);
    if (!race_read_status.ok()) co_return race_read_status;
    Check(race_batch.has_value() && race_batch->records_.size() == 1 &&
              race_batch->records_.front().key_ == race_first &&
              race_batch->records_.front().source_id_ == 0,
          "replacement materialization retained a raced large-value pin in a "
          "non-exclusive batch");
    storage_->AcknowledgePartitionFullSyncOverrides(
        kReplacementRaceSession, race_partition, race_batch->records_);
    auto raced_large = co_await storage_->ReadPartitionFullSyncOverrides(
        kReplacementRaceSession, race_partition, 16,
        lavik::storage::kReplicationTransferBytes);
    if (!raced_large.ok()) co_return raced_large.status();
    Check(raced_large->records_.size() == 1 &&
              raced_large->records_.front().key_ == race_second &&
              raced_large->records_.front().source_id_ != 0 &&
              raced_large->records_.front().source_value_bytes_ == 10 * kMiB,
          "raced large replacement was not deferred to an exclusive batch");
    storage_->AcknowledgePartitionFullSyncOverrides(
        kReplacementRaceSession, race_partition, raced_large->records_);
    storage_->EndPartitionReplication(kReplacementRaceSession, race_partition);
    storage_->EndFullSyncSession(kReplacementRaceSession);

    constexpr std::uint64_t kEpochSession = 304;
    auto epoch_session = storage_->BeginFullSyncSession(kEpochSession);
    if (!epoch_session.ok()) co_return epoch_session.status();
    auto epoch_start =
        storage_->BeginPartitionReplication(kEpochSession, partition_id);
    if (!epoch_start.ok()) co_return epoch_start.status();
    absl::Status epoch_db =
        storage_->BeginPartitionDbReplication(kEpochSession, partition_id, kDb);
    if (!epoch_db.ok()) co_return epoch_db;
    absl::Status flushed = co_await storage_->FlushDbDetach(kDb);
    if (!flushed.ok()) co_return flushed;
    auto invalidated = co_await storage_->ReadPartitionFullSyncOverrides(
        kEpochSession, partition_id, 16);
    Check(!invalidated.ok(),
          "database epoch advance did not invalidate full sync");
    auto invalid_partition =
        storage_->BeginPartitionReplication(kEpochSession, partition_id);
    Check(!invalid_partition.ok() && invalid_partition.status().code() ==
                                         absl::StatusCode::kFailedPrecondition,
          "invalidated full sync accepted another partition");
    storage_->EndFullSyncSession(kEpochSession);
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExercisePartitionHandoff() {
    constexpr std::uint64_t kSession = 404;
    constexpr std::uint8_t kDb = 4;
    const std::string key = "fullsync-handoff{ordered}";
    const std::uint16_t partition_id = lavik::storage::RedisSlot(key);

    auto fullsync_session = storage_->BeginFullSyncSession(kSession);
    if (!fullsync_session.ok()) co_return fullsync_session.status();
    auto start_result =
        storage_->BeginPartitionReplication(kSession, partition_id);
    if (!start_result.ok()) co_return start_result.status();
    absl::Status started_db =
        storage_->BeginPartitionDbReplication(kSession, partition_id, kDb);
    if (!started_db.ok()) co_return started_db;
    const PartitionReplicationStart start = *start_result;
    std::vector<std::string> before_args{"SET", key, "before-handoff"};
    absl::Status written =
        co_await ExecuteClientCommand(kDb, std::move(before_args), "+OK\r\n");
    if (!written.ok()) co_return written;
    auto replacement_result = co_await storage_->ReadPartitionFullSyncOverrides(
        kSession, partition_id, 16);
    if (!replacement_result.ok()) co_return replacement_result.status();
    PartitionFullSyncBatch replacement = std::move(*replacement_result);
    Check(replacement.records_.size() == 1 &&
              replacement.records_.front().mutation_sequence_ >
                  start.baseline_version_,
          "pre-handoff mutation was not captured as a replacement");
    const std::size_t replacement_credit =
        lavik::storage::kFullSyncReplacementMetadataBytes + key.size() * 2;
    Check(storage_->LocalReplicationLogInfo().fullsync_publish_queue_bytes_ >=
              replacement_credit,
          "pending replacement did not retain bounded queue credit");

    PartitionFullSyncBatch frozen = std::move(replacement);
    storage_->AcknowledgePartitionFullSyncOverrides(kSession, partition_id,
                                                    frozen.records_);
    Check(
        storage_->LocalReplicationLogInfo().fullsync_publish_queue_bytes_ == 0,
        "replacement ACK did not release queue credit");

    std::vector<std::string> after_args{"SET", key, "after-handoff"};
    written =
        co_await ExecuteClientCommand(kDb, std::move(after_args), "+OK\r\n");
    if (!written.ok()) co_return written;
    auto queued = storage_->PeekFullSyncPublishItems(kSession, 1);
    if (!queued.ok()) co_return queued.status();
    Check(queued->size() == 1 && queued->front().command_ != nullptr &&
              queued->front().command_->partition_id_ == partition_id,
          "covered key did not enter the full-sync publish queue");
    std::vector<std::string> batched_args{"SET", key, "batched-handoff"};
    written =
        co_await ExecuteClientCommand(kDb, std::move(batched_args), "+OK\r\n");
    if (!written.ok()) co_return written;
    auto batched = storage_->PeekFullSyncPublishItems(kSession, 2);
    if (!batched.ok()) co_return batched.status();
    Check(batched->size() == 2 && (*batched)[0].id_ == queued->front().id_ &&
              (*batched)[1].id_ == (*batched)[0].id_ + 1,
          "full-sync publish batch did not preserve FIFO item order");
    storage_->AcknowledgeFullSyncPublishItem(kSession, (*batched)[0].id_);
    auto batch_tail = storage_->PeekFullSyncPublishItems(kSession, 2);
    if (!batch_tail.ok()) co_return batch_tail.status();
    Check(
        batch_tail->size() == 1 && batch_tail->front().id_ == (*batched)[1].id_,
        "full-sync publish ACK did not release exactly one FIFO item");
    storage_->AcknowledgeFullSyncPublishItem(kSession, batch_tail->front().id_);

    absl::Status queue_capacity =
        co_await storage_->SetReplicationPublishQueueCapacity(kMiB);
    if (!queue_capacity.ok()) co_return queue_capacity;
    std::vector<std::string> large_args{"SET", key,
                                        std::string(700 * 1024, 'q')};
    written =
        co_await ExecuteClientCommand(kDb, std::move(large_args), "+OK\r\n");
    if (!written.ok()) co_return written;
    auto large_queued = storage_->PeekFullSyncPublishItems(kSession, 1);
    if (!large_queued.ok()) co_return large_queued.status();
    Check(large_queued->size() == 1,
          "large covered-key command did not enter full-sync queue");
    const auto queued_info = storage_->LocalReplicationLogInfo();
    Check(queued_info.fullsync_session_count_ == 1 &&
              queued_info.fullsync_publish_queue_bytes_ >= 700 * 1024 &&
              queued_info.fullsync_publish_queue_capacity_bytes_ == kMiB,
          "full-sync queue occupancy metrics do not describe the active "
          "session");
    std::uint16_t unstarted_partition =
        static_cast<std::uint16_t>((partition_id + storage_->worker_count()) %
                                   lavik::storage::kLogicalStorageShards);
    if (unstarted_partition == partition_id) {
      unstarted_partition = static_cast<std::uint16_t>(
          (partition_id + 1) % lavik::storage::kLogicalStorageShards);
    }
    auto unrelated = co_await storage_->AcquireReplicationPublisherAdmission(
        700 * 1024, lavik::storage::ReplicationPublisherTarget{
                        .partition_id_ = unstarted_partition, .db_id_ = kDb});
    if (!unrelated.ok()) co_return unrelated.status();
    Check(unrelated->fullsync_session_ids_.empty() &&
              unrelated->fullsync_unstarted_guards_.size() == 1,
          "UNSTARTED partition reserved queue credit or lost its phase guard");
    storage_->ReleaseReplicationPublisherAdmission(*unrelated, 700 * 1024);

    bool admission_finished = false;
    absl::Status admission_status =
        absl::UnknownError("full-sync admission waiter did not run");
    auto wait_for_fullsync_admission = [&]() -> bycorf::Task<absl::Status> {
      auto admission = co_await storage_->AcquireReplicationPublisherAdmission(
          700 * 1024, lavik::storage::ReplicationPublisherTarget{
                          .partition_id_ = partition_id, .db_id_ = kDb});
      if (!admission.ok()) {
        admission_status = admission.status();
      } else {
        storage_->ReleaseReplicationPublisherAdmission(*admission, 700 * 1024);
        admission_status = absl::OkStatus();
      }
      admission_finished = true;
      co_return absl::OkStatus();
    };
    worker_->Spawn(wait_for_fullsync_admission());
    for (unsigned spin = 0; spin < 32; ++spin) {
      co_await bycorf::Yield(*worker_);
    }
    Check(!admission_finished,
          "full-sync queue capacity did not backpressure the next writer");
    storage_->AcknowledgeFullSyncPublishItem(kSession,
                                             large_queued->front().id_);
    while (!admission_finished) co_await bycorf::Yield(*worker_);
    if (!admission_status.ok()) co_return admission_status;
    Check(storage_->LocalReplicationLogInfo().fullsync_backpressure_waits_ > 0,
          "full-sync queue backpressure wait was not observed");

    // Once an oversized request reaches the head of admission it must exclude
    // later small requests. Otherwise sustained small writes can keep the
    // queue nonempty and starve a large key forever.
    std::vector<std::string> refill_args{"SET", key,
                                         std::string(700 * 1024, 'r')};
    written =
        co_await ExecuteClientCommand(kDb, std::move(refill_args), "+OK\r\n");
    if (!written.ok()) co_return written;
    auto refill = storage_->PeekFullSyncPublishItems(kSession, 1);
    if (!refill.ok()) co_return refill.status();
    Check(refill->size() == 1, "failed to refill full-sync publish queue");

    bool oversized_finished = false;
    bool small_finished = false;
    std::optional<lavik::storage::ReplicationPublisherAdmission>
        oversized_admission;
    std::optional<lavik::storage::ReplicationPublisherAdmission>
        small_admission;
    absl::Status oversized_status = absl::UnknownError("not started");
    absl::Status small_status = absl::UnknownError("not started");
    auto wait_oversized = [&]() -> bycorf::Task<absl::Status> {
      auto result = co_await storage_->AcquireReplicationPublisherAdmission(
          2 * kMiB, lavik::storage::ReplicationPublisherTarget{
                        .partition_id_ = partition_id, .db_id_ = kDb});
      if (result.ok()) oversized_admission = std::move(*result);
      oversized_status = result.status();
      oversized_finished = true;
      co_return absl::OkStatus();
    };
    auto wait_small = [&]() -> bycorf::Task<absl::Status> {
      auto result = co_await storage_->AcquireReplicationPublisherAdmission(
          1, lavik::storage::ReplicationPublisherTarget{
                 .partition_id_ = partition_id, .db_id_ = kDb});
      if (result.ok()) small_admission = std::move(*result);
      small_status = result.status();
      small_finished = true;
      co_return absl::OkStatus();
    };
    worker_->Spawn(wait_oversized());
    worker_->Spawn(wait_small());
    for (unsigned spin = 0; spin < 32; ++spin) {
      co_await bycorf::Yield(*worker_);
    }
    Check(!oversized_finished && !small_finished,
          "publisher waiters bypassed occupied queue capacity");
    storage_->AcknowledgeFullSyncPublishItem(kSession, refill->front().id_);
    while (!oversized_finished) co_await bycorf::Yield(*worker_);
    if (!oversized_status.ok()) co_return oversized_status;
    Check(!small_finished,
          "small publisher admission bypassed an earlier oversized waiter");
    storage_->ReleaseReplicationPublisherAdmission(*oversized_admission,
                                                   2 * kMiB);
    while (!small_finished) co_await bycorf::Yield(*worker_);
    if (!small_status.ok()) co_return small_status;
    storage_->ReleaseReplicationPublisherAdmission(*small_admission, 1);

    absl::Status completed_db =
        storage_->CompletePartitionDbReplication(kSession, partition_id, kDb);
    if (!completed_db.ok()) co_return completed_db;

    // Active expiration has no client command admission. Fill the full-sync
    // queue after installing an expiring TAILING key: expiration must leave
    // its candidate pending instead of creating an uncredited replacement.
    const std::string expiring_key = "fullsync-expire{ordered}";
    std::vector<std::string> expiring_args{"SET", expiring_key, "alive", "PX",
                                           "100"};
    written =
        co_await ExecuteClientCommand(kDb, std::move(expiring_args), "+OK\r\n");
    if (!written.ok()) co_return written;
    auto expiring_command = storage_->PeekFullSyncPublishItems(kSession, 1);
    if (!expiring_command.ok()) co_return expiring_command.status();
    Check(expiring_command->size() == 1 &&
              expiring_command->front().command_ != nullptr,
          "expiring TAILING key did not enter the command FIFO");
    storage_->AcknowledgeFullSyncPublishItem(kSession,
                                             expiring_command->front().id_);

    std::vector<std::string> expiry_filler_args{"SET", key,
                                                std::string(700 * 1024, 'e')};
    written = co_await ExecuteClientCommand(kDb, std::move(expiry_filler_args),
                                            "+OK\r\n");
    if (!written.ok()) co_return written;
    auto expiry_filler = storage_->PeekFullSyncPublishItems(kSession, 1);
    if (!expiry_filler.ok()) co_return expiry_filler.status();
    Check(expiry_filler->size() == 1,
          "failed to fill the full-sync queue for active expiration");
    const std::size_t full_queue_bytes =
        storage_->LocalReplicationLogInfo().fullsync_publish_queue_bytes_;
    Check(full_queue_bytes != 0,
          "active-expiration test did not occupy full-sync queue credit");
    queue_capacity =
        co_await storage_->SetReplicationPublishQueueCapacity(full_queue_bytes);
    if (!queue_capacity.ok()) co_return queue_capacity;

    absl::Status slept =
        co_await bycorf::SleepFor(*worker_, std::chrono::milliseconds(150));
    if (!slept.ok()) co_return slept;
    Check(!co_await storage_->Exists(kDb, expiring_key),
          "active-expiration test key did not become logically expired");
    slept = co_await bycorf::SleepFor(*worker_, std::chrono::milliseconds(50));
    if (!slept.ok()) co_return slept;
    absl::Status quiesced = co_await storage_->QuiesceExpiration();
    if (!quiesced.ok()) co_return quiesced;
    auto blocked_expiration = co_await storage_->ReadPartitionFullSyncOverrides(
        kSession, partition_id, 16);
    if (!blocked_expiration.ok()) co_return blocked_expiration.status();
    Check(
        blocked_expiration->records_.empty() &&
            storage_->LocalReplicationLogInfo().fullsync_publish_queue_bytes_ ==
                full_queue_bytes,
        "active expiration bypassed full-sync replacement admission");

    storage_->AcknowledgeFullSyncPublishItem(kSession,
                                             expiry_filler->front().id_);
    storage_->ResumeExpiration();
    std::optional<PartitionFullSyncBatch> expired_replacement;
    for (unsigned attempt = 0; attempt < 200; ++attempt) {
      auto batch = co_await storage_->ReadPartitionFullSyncOverrides(
          kSession, partition_id, 16);
      if (!batch.ok()) co_return batch.status();
      if (!batch->records_.empty()) {
        expired_replacement.emplace(std::move(*batch));
        break;
      }
      slept =
          co_await bycorf::SleepFor(*worker_, std::chrono::milliseconds(10));
      if (!slept.ok()) co_return slept;
    }
    Check(expired_replacement.has_value() &&
              expired_replacement->records_.size() == 1 &&
              expired_replacement->records_.front().key_ == expiring_key &&
              expired_replacement->records_.front().kind_ ==
                  lavik::storage::SnapshotRecord::Kind::kDelete,
          "active expiration did not resume after full-sync credit was freed");
    storage_->AcknowledgePartitionFullSyncOverrides(
        kSession, partition_id, expired_replacement->records_);
    queue_capacity =
        co_await storage_->SetReplicationPublishQueueCapacity(kMiB);
    if (!queue_capacity.ok()) co_return queue_capacity;

    storage_->EndPartitionReplication(kSession, partition_id);
    storage_->EndFullSyncSession(kSession);

    constexpr std::uint64_t kPendingTxSession = 405;
    const std::string pending_key = "fullsync-handoff-tx{ordered}";
    const auto pending_digest = lavik::storage::ComputeDigest(pending_key);
    auto pending_session = storage_->BeginFullSyncSession(kPendingTxSession);
    if (!pending_session.ok()) co_return pending_session.status();
    auto pending_start =
        storage_->BeginPartitionReplication(kPendingTxSession, partition_id);
    if (!pending_start.ok()) co_return pending_start.status();
    absl::Status pending_db = storage_->BeginPartitionDbReplication(
        kPendingTxSession, partition_id, kDb);
    if (!pending_db.ok()) co_return pending_db;
    lavik::storage::TxShardWrites pending_tx;
    const std::uint64_t pending_txid = StorageEngine::AllocateWriteTxid();
    storage_->InitializeTxWrites(pending_txid, std::span(&pending_tx, 1));
    pending_tx.collect_undo_ = true;
    {
      auto key_lock = co_await lavik::tx::CurrentTxShard().AcquireKey(
          kDb, lavik::tx::FingerprintOf(pending_digest),
          lavik::tx::LockMode::kExclusive);
      auto staged = co_await storage_->SetLocked(
          kDb, pending_key, pending_digest, "10", {}, &pending_tx);
      if (!staged.ok()) co_return staged.status();
      key_lock.Reset();
    }
    absl::Status pending_completed = storage_->CompletePartitionDbReplication(
        kPendingTxSession, partition_id, kDb);
    if (!pending_completed.ok()) co_return pending_completed;
    storage_->PublishCommittedFullSyncEffects(&pending_tx);
    auto late_commit_result = co_await storage_->ReadPartitionFullSyncOverrides(
        kPendingTxSession, partition_id, 16);
    if (!late_commit_result.ok()) co_return late_commit_result.status();
    Check(late_commit_result->records_.empty(),
          "TAILING transaction escaped the ordered publish FIFO");
    std::vector<std::string> increment_args{"INCR", pending_key};
    written = co_await ExecuteClientCommand(kDb, std::move(increment_args),
                                            ":11\r\n");
    if (!written.ok()) co_return written;
    auto ordered = storage_->PeekFullSyncPublishItems(kPendingTxSession, 2);
    if (!ordered.ok()) co_return ordered.status();
    Check(ordered->size() == 2 && (*ordered)[0].record_.has_value() &&
              (*ordered)[0].command_ == nullptr &&
              (*ordered)[1].command_ != nullptr &&
              (*ordered)[0].id_ + 1 == (*ordered)[1].id_,
          "transaction after-image was overtaken by a later command");
    auto after_image = co_await storage_->MaterializeFullSyncPublishRecord(
        kPendingTxSession, partition_id, *(*ordered)[0].record_);
    if (!after_image.ok()) co_return after_image.status();
    Check(after_image->value_ == "11" &&
              after_image->mutation_sequence_ ==
                  (*ordered)[1].command_->partition_sequence_,
          "ordered transaction after-image did not materialize latest state");
    storage_->AcknowledgeFullSyncPublishItem(kPendingTxSession,
                                             (*ordered)[0].id_);
    storage_->AcknowledgeFullSyncPublishItem(kPendingTxSession,
                                             (*ordered)[1].id_);
    storage_->EndPartitionReplication(kPendingTxSession, partition_id);
    storage_->EndFullSyncSession(kPendingTxSession);
    absl::Status pending_discarded =
        co_await storage_->DiscardTxUndoLocal(pending_tx.txid_);
    if (!pending_discarded.ok()) co_return pending_discarded;
    std::vector<lavik::storage::TxShardWrites*> pending_shards{&pending_tx};
    absl::Status pending_durable = co_await storage_->CommitTxWrites(
        pending_tx.txid_, std::move(pending_shards));
    if (!pending_durable.ok()) co_return pending_durable;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseHardBacklogCap() {
    // With backpressure explicitly disabled, the backlog is a hard reconnect
    // window. Filling it revokes lagging coverage and evicts only complete
    // events; the sender then observes a floor gap and forces whole-group full
    // sync.
    absl::Status status =
        co_await storage_->SetReplicationBacklogBackpressure(false);
    if (!status.ok()) co_return status;
    status = co_await storage_->EnableReplicationLog(17, 8 * kMiB);
    if (!status.ok()) co_return status;
    status = storage_->RetainReplicationLog(77, 1);
    if (!status.ok()) co_return status;
    RepeatedByteSource pinned_payload(7 * kMiB, 'P');
    auto pinned_first =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .kind_ = ReplicationEventKind::kMutation,
            .partition_id_ = 9,
            .partition_sequence_ = 1,
            .payload_ = {},
            .payload_source_ = &pinned_payload,
        });
    if (!pinned_first.ok()) co_return pinned_first.status();
    auto pinned_second =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .kind_ = ReplicationEventKind::kMutation,
            .partition_id_ = 9,
            .partition_sequence_ = 2,
            .payload_ = {},
            .payload_source_ = &pinned_payload,
        });
    if (!pinned_second.ok()) co_return pinned_second.status();
    const auto pinned_info = storage_->LocalReplicationLogInfo();
    Check(pinned_info.retained_cursor_count_ == 0 &&
              pinned_info.coverage_revocations_ == 1,
          "hard-cap eviction did not revoke the lagging coverage claim");
    Check(pinned_info.floor_lsn_ == 2,
          "hard-cap eviction did not retain a complete newest event");
    auto disconnected_append =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .kind_ = ReplicationEventKind::kMutation,
            .partition_id_ = 9,
            .partition_sequence_ = 3,
            .payload_ = {},
            .payload_source_ = &pinned_payload,
        });
    if (!disconnected_append.ok()) co_return disconnected_append.status();
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    // A larger reconnect window follows the same rule: the first publication
    // that needs the ninth block revokes the stale claim and retains the
    // newest eight complete events.
    status = co_await storage_->EnableReplicationLog(20, 8 * 8 * kMiB);
    if (!status.ok()) co_return status;
    status = storage_->RetainReplicationLog(79, 1);
    if (!status.ok()) co_return status;
    for (std::uint64_t lsn = 1; lsn <= 8; ++lsn) {
      auto appended =
          co_await storage_->AppendReplicationLog(ReplicationLogAppend{
              .kind_ = ReplicationEventKind::kMutation,
              .partition_id_ = 9,
              .partition_sequence_ = lsn,
              .payload_ = {},
              .payload_source_ = &pinned_payload,
          });
      if (!appended.ok()) co_return appended.status();
    }
    auto reconnect_append =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .kind_ = ReplicationEventKind::kMutation,
            .partition_id_ = 9,
            .partition_sequence_ = 9,
            .payload_ = {},
            .payload_source_ = &pinned_payload,
        });
    if (!reconnect_append.ok()) co_return reconnect_append.status();
    const auto reconnect_info = storage_->LocalReplicationLogInfo();
    Check(reconnect_info.floor_lsn_ == 2 &&
              reconnect_info.retained_cursor_count_ == 0,
          "hard-cap eviction did not revoke the large-window claim");
    auto reconnect_cursor = co_await storage_->ReadReplicationLog(
        ReplicationLogCursor{.lsn_ = 2}, kMiB, 1);
    if (!reconnect_cursor.ok()) co_return reconnect_cursor.status();
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    // Runtime growth preserves the retained reconnect suffix after hard-cap
    // eviction; it never resurrects the revoked coverage claim.
    status = co_await storage_->EnableReplicationLog(18, 8 * kMiB);
    if (!status.ok()) co_return status;
    status = storage_->RetainReplicationLog(78, 1);
    if (!status.ok()) co_return status;
    for (std::uint64_t sequence = 1; sequence <= 2; ++sequence) {
      auto appended =
          co_await storage_->AppendReplicationLog(ReplicationLogAppend{
              .kind_ = ReplicationEventKind::kMutation,
              .partition_id_ = 9,
              .partition_sequence_ = sequence,
              .payload_ = {},
              .payload_source_ = &pinned_payload,
          });
      if (!appended.ok()) co_return appended.status();
    }
    status = co_await storage_->SetReplicationLogCapacity(2 * 8 * kMiB);
    if (!status.ok()) co_return status;
    Check(storage_->LocalReplicationLogInfo().floor_lsn_ == 2 &&
              storage_->LocalReplicationLogInfo().retained_cursor_count_ == 0,
          "backlog growth resurrected evicted coverage");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    co_return co_await storage_->SetReplicationBacklogBackpressure(true);
  }

  bycorf::Task<absl::Status> ExerciseBacklogBackpressurePolicy() {
    absl::Status status = co_await storage_->EnableReplicationLog(21, 8 * kMiB);
    if (!status.ok()) co_return status;
    status = storage_->RetainReplicationLog(80, 1);
    if (!status.ok()) co_return status;

    RepeatedByteSource payload(7 * kMiB, 'B');
    auto first = co_await storage_->AppendReplicationLog(ReplicationLogAppend{
        .kind_ = ReplicationEventKind::kMutation,
        .partition_id_ = 9,
        .partition_sequence_ = 1,
        .payload_ = {},
        .payload_source_ = &payload,
    });
    if (!first.ok()) co_return first.status();

    bool append_finished = false;
    absl::Status append_status =
        absl::UnknownError("backpressured append did not run");
    auto append = [&](std::uint64_t sequence) -> bycorf::Task<absl::Status> {
      auto result =
          co_await storage_->AppendReplicationLog(ReplicationLogAppend{
              .kind_ = ReplicationEventKind::kMutation,
              .partition_id_ = 9,
              .partition_sequence_ = sequence,
              .payload_ = {},
              .payload_source_ = &payload,
          });
      append_status = result.ok() ? absl::OkStatus() : result.status();
      append_finished = true;
      co_return absl::OkStatus();
    };

    worker_->Spawn(append(2));
    co_await bycorf::Yield(*worker_);
    Check(!append_finished &&
              storage_->LocalReplicationLogInfo().capacity_backpressured_,
          "default backlog policy did not wait for replica ACK");
    status = storage_->RetainReplicationLog(80, 2);
    if (!status.ok()) co_return status;
    while (!append_finished) co_await bycorf::Yield(*worker_);
    if (!append_status.ok()) co_return append_status;

    append_finished = false;
    append_status = absl::UnknownError("policy-change append did not run");
    worker_->Spawn(append(3));
    co_await bycorf::Yield(*worker_);
    Check(!append_finished &&
              storage_->LocalReplicationLogInfo().capacity_backpressured_,
          "second append did not enter backlog backpressure");
    status = co_await storage_->SetReplicationBacklogBackpressure(false);
    if (!status.ok()) co_return status;
    while (!append_finished) co_await bycorf::Yield(*worker_);
    if (!append_status.ok()) co_return append_status;

    const auto info = storage_->LocalReplicationLogInfo();
    Check(info.backpressure_waits_ >= 2 && info.coverage_revocations_ == 1 &&
              info.retained_cursor_count_ == 0,
          "runtime policy change did not wake and revoke lagging coverage");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    co_return co_await storage_->SetReplicationBacklogBackpressure(true);
  }

  // Keep the FLUSH control-barrier phase independent so failures leave the
  // surrounding replication-log exercise at a clear lifecycle boundary.
  bycorf::Task<absl::Status> ExerciseFlushControlBarriers() {
    auto flush_victim = co_await storage_->Set(2, "flush-victim", "gone", {});
    auto flush_survivor =
        co_await storage_->Set(3, "flush-survivor", "kept", {});
    if (!flush_victim.ok()) co_return flush_victim.status();
    if (!flush_survivor.ok()) co_return flush_survivor.status();
    absl::Status status = co_await storage_->EnableReplicationLog(21, 8 * kMiB);
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
    auto flush_command =
        lavik::DecodeReplicationCommand(flush_batch->frames_.front().payload_);
    if (!flush_command.ok()) co_return flush_command.status();
    Check(flush_command->db_id_ == 2 && flush_command->args_.size() == 3 &&
              flush_command->args_[0] == "FLUSHDB" &&
              flush_command->args_[1] != "0" &&
              flush_command->args_[2] == std::to_string(storage_->DbEpoch(2)),
          "FLUSHDB barrier did not carry its installed database epoch");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    auto replica_victim =
        co_await storage_->Set(2, "replica-flush-victim", "gone", {});
    if (!replica_victim.ok()) co_return replica_victim.status();
    flush_command->args_[2] = std::to_string(storage_->DbEpoch(2) + 1);
    status = co_await lavik::ApplyReplicatedCommand(*flush_command);
    if (!status.ok()) co_return status;
    Check(!co_await storage_->Exists(2, "replica-flush-victim") &&
              co_await storage_->Exists(3, "flush-survivor"),
          "replica FLUSHDB barrier affected the wrong database");

    for (const std::uint8_t db_id :
         {std::uint8_t{0}, std::uint8_t{3}, std::uint8_t{15}}) {
      auto seeded = co_await storage_->Set(
          db_id, "flushall-victim-" + std::to_string(db_id), "gone", {});
      if (!seeded.ok()) co_return seeded.status();
    }
    status = co_await storage_->EnableReplicationLog(26, 8 * kMiB);
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(0, {"FLUSHALL"}, "+OK\r\n");
    if (!status.ok()) co_return status;
    status = co_await WaitForReplicationTail(1);
    if (!status.ok()) co_return status;
    auto flushall_batch = co_await storage_->ReadReplicationLog({}, kMiB, 1);
    if (!flushall_batch.ok()) co_return flushall_batch.status();
    Check(flushall_batch->frames_.size() == 1 && flushall_batch->at_tail_ &&
              flushall_batch->frames_.front().header_.kind_ ==
                  ReplicationEventKind::kControl,
          "FLUSHALL did not produce one control frame");
    auto flushall_command = lavik::DecodeReplicationCommand(
        flushall_batch->frames_.front().payload_);
    if (!flushall_command.ok()) co_return flushall_command.status();
    Check(flushall_command->db_id_ == 0 &&
              flushall_command->args_.size() ==
                  2 + lavik::storage::kLogicalDatabaseCount &&
              flushall_command->args_[0] == "FLUSHALL" &&
              flushall_command->args_[1] != "0",
          "FLUSHALL barrier did not carry one complete epoch vector");
    for (std::uint8_t db_id = 0; db_id < lavik::storage::kLogicalDatabaseCount;
         ++db_id) {
      Check(flushall_command->args_[2 + db_id] ==
                std::to_string(storage_->DbEpoch(db_id)),
            "FLUSHALL barrier epoch vector changed");
    }
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    for (const std::uint8_t db_id :
         {std::uint8_t{0}, std::uint8_t{3}, std::uint8_t{15}}) {
      auto seeded = co_await storage_->Set(
          db_id, "replica-flushall-victim-" + std::to_string(db_id), "gone",
          {});
      if (!seeded.ok()) co_return seeded.status();
    }
    for (std::uint8_t db_id = 0; db_id < lavik::storage::kLogicalDatabaseCount;
         ++db_id) {
      flushall_command->args_[2 + db_id] =
          std::to_string(storage_->DbEpoch(db_id) + 1);
    }
    status = co_await lavik::ApplyReplicatedCommand(*flushall_command);
    if (!status.ok()) co_return status;
    for (const std::uint8_t db_id :
         {std::uint8_t{0}, std::uint8_t{3}, std::uint8_t{15}}) {
      Check(!co_await storage_->Exists(
                db_id, "replica-flushall-victim-" + std::to_string(db_id)),
            "replica FLUSHALL left one database visible");
    }
    co_return absl::OkStatus();
  }

  void CheckOrderingAdmission() {
    // Snapshot handoff closes every worker's transaction admission word while
    // retaining the count that was already admitted on that worker.
    Check(lavik::TryBeginSnapshotTransaction(),
          "open snapshot transaction gate rejected an operation");
    Check(lavik::SnapshotTransactionsActive(),
          "snapshot transaction gate lost its local active count");
    Check(lavik::CloseSnapshotTransactionGate(),
          "snapshot transaction gate did not close");
    Check(!lavik::TryBeginSnapshotTransaction(),
          "closed snapshot transaction gate admitted an operation");
    Check(!lavik::CloseSnapshotTransactionGate(),
          "snapshot transaction gate allowed two cut owners");
    lavik::EndSnapshotTransaction();
    Check(!lavik::SnapshotTransactionsActive(),
          "snapshot transaction gate did not drain");
    lavik::OpenSnapshotTransactionGate();
    Check(lavik::TryBeginSnapshotTransaction(),
          "reopened snapshot transaction gate rejected an operation");
    lavik::EndSnapshotTransaction();
    Check(lavik::TryBeginReplicationTransactionOrder(),
          "replication transaction order did not admit first owner");
    Check(!lavik::TryBeginReplicationTransactionOrder(),
          "replication transaction order admitted two owners");
    lavik::EndReplicationTransactionOrder();
    Check(lavik::TryBeginReplicationTransactionOrder(),
          "replication transaction order did not reopen");
    lavik::EndReplicationTransactionOrder();

    // Dynamic gate admission: with one worker every key view resolves to a
    // single shard, so flagged kinds skip the gate while unknown, malformed,
    // and view-incomplete kinds stay conservative. Multi-shard outcomes are
    // covered by CommandTableTest.RequestSpansMultipleShardsDecision and the
    // multikey e2e gate regression.
    auto admission_request = [](std::vector<std::string> args) {
      lavik::CommandRequest built;
      built.spec_ = lavik::FindCommand(args.front());
      built.kind_ = built.spec_ != nullptr ? built.spec_->kind_
                                           : lavik::CommandKind::kUnknown;
      built.args_ = std::move(args);
      return built;
    };
    Check(lavik::RequestSpansMultipleShards(admission_request({"nope"})),
          "unknown command skipped the replication order gate");
    Check(lavik::RequestSpansMultipleShards(admission_request({"del"})),
          "malformed DEL skipped the replication order gate");
    Check(lavik::RequestSpansMultipleShards(
              admission_request({"sort", "a", "store", "b"})),
          "SORT STORE skipped the replication order gate");
    Check(lavik::RequestSpansMultipleShards(
              admission_request({"zunionstore", "out", "2", "a", "b"})),
          "ZUNIONSTORE skipped the replication order gate");
    Check(lavik::RequestSpansMultipleShards(
              admission_request({"function", "flush"})),
          "FUNCTION skipped the replication order gate");
    Check(!lavik::RequestSpansMultipleShards(
              admission_request({"del", "a", "b"})),
          "single-shard DEL still took the replication order gate");
    Check(!lavik::RequestSpansMultipleShards(
              admission_request({"mset", "a", "1", "b", "2"})),
          "single-shard MSET still took the replication order gate");
  }

  bycorf::Task<absl::Status> ExercisePublisherTransactionAdmission() {
    absl::Status status;
    lavik::RefreshMemoryStats();
    const std::uint64_t retained_before_publisher =
        lavik::GetMemoryStats().used_bytes_;
    status = co_await storage_->EnableReplicationLog(3, 8 * kMiB);
    if (!status.ok()) co_return status;
    lavik::RefreshMemoryStats();
    const std::uint64_t publisher_capacity =
        storage_->LocalReplicationLogInfo().publish_queue_capacity_bytes_;
    Check(lavik::GetMemoryStats().used_bytes_ >=
              retained_before_publisher + publisher_capacity + 8 * kMiB,
          "replication log did not hold publisher staging plus one standby "
          "backlog block");

    const std::string oversized_transaction_key(kMiB, 'T');
    std::vector<std::vector<std::string>> transaction_commands{
        {"LMPOP", "2", oversized_transaction_key, "list-other", "LEFT"},
        {"SUNIONSTORE", "set-destination", oversized_transaction_key},
        {"ZUNIONSTORE", "zset-destination", "1", oversized_transaction_key},
        {"SORT", oversized_transaction_key, "STORE", "sort-destination"},
    };
    for (auto& args : transaction_commands) {
      status = co_await ExerciseTransactionGuardAdmission(std::move(args));
      if (!status.ok()) co_return status;
    }
    status = co_await ExerciseBitOpPayloadAdmission();
    if (!status.ok()) co_return status;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseReplicationCopyOom() {
    absl::Status status;
    // The parsed request already owns this value. Leave enough headroom for
    // command dispatch itself but not the replication journal's second copy;
    // SET must fail before publishing either durable state or a log event.
    std::vector<std::string> admission_args{"SET", "replication-copy-oom",
                                            std::string(kMiB, 'M')};
    lavik::RefreshMemoryStats();
    const std::uint64_t used = lavik::GetMemoryStats().used_bytes_;
    if (used > std::numeric_limits<std::uint64_t>::max() - 128 * 1024) {
      co_return absl::ResourceExhaustedError(
          "cannot construct replication-copy admission test limit");
    }
    status = lavik::InitMemoryLimit(used + 128 * 1024, 1);
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(
        0, std::move(admission_args),
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    if (!status.ok()) co_return status;
    Check(!co_await storage_->Exists(0, "replication-copy-oom"),
          "rejected replication copy still mutated storage");
    status = lavik::InitMemoryLimit(512 * kMiB, 1);
    if (!status.ok()) co_return status;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseAdmissionAndOrdering() {
    absl::Status status = co_await ExerciseFullSyncOverrides();
    if (!status.ok()) co_return status;

    CheckOrderingAdmission();
    status = co_await ExercisePublisherTransactionAdmission();
    if (!status.ok()) co_return status;
    status = co_await ExerciseReplicationCopyOom();
    if (!status.ok()) co_return status;

    status = co_await ExercisePartitionHandoff();
    if (!status.ok()) co_return status;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseBacklogStorage() {
    absl::Status status;
    const auto before_oversized = storage_->LocalReplicationLogInfo();
    RepeatedByteSource too_large(9 * kMiB, 'x');
    auto oversized =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .partition_id_ = 3,
            .partition_sequence_ = 1,
            .payload_ = {},
            .payload_source_ = &too_large,
        });
    Check(
        !oversized.ok() &&
            oversized.status().code() == absl::StatusCode::kResourceExhausted &&
            storage_->LocalReplicationLogInfo().state_ ==
                ReplicationLogState::kActive &&
            storage_->LocalReplicationLogInfo().tail_lsn_ ==
                before_oversized.tail_lsn_,
        "one oversized event exceeded the hard backlog cap");
    auto after_oversized =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .partition_id_ = 3,
            .partition_sequence_ = 2,
            .payload_ = "next",
            .payload_source_ = nullptr,
        });
    if (!after_oversized.ok()) co_return after_oversized.status();
    Check(*after_oversized == before_oversized.tail_lsn_ + 1,
          "an oversized admission failure consumed an event LSN");
    status = co_await storage_->SetReplicationLogCapacity(2 * 8 * kMiB);
    if (!status.ok()) co_return status;
    Check(
        storage_->LocalReplicationLogInfo().state_ ==
                ReplicationLogState::kActive &&
            storage_->LocalReplicationLogInfo().capacity_bytes_ == 2 * 8 * kMiB,
        "runtime backlog growth changed history state or lost capacity");
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

    status = co_await storage_->EnableReplicationLog(4, 3 * 8 * kMiB);
    if (!status.ok()) co_return status;
    std::vector<std::string> overflow_args;
    overflow_args.emplace_back("SET");
    overflow_args.emplace_back("publisher-overflow");
    overflow_args.emplace_back(17 * kMiB, 'Q');
    status =
        co_await ExecuteClientCommand(0, std::move(overflow_args), "+OK\r\n");
    if (!status.ok()) co_return status;
    status = co_await WaitForReplicationTail(1);
    if (!status.ok()) co_return status;
    Check(storage_->LocalReplicationLogInfo().state_ ==
              ReplicationLogState::kActive,
          "oversized publisher staging invalidated the replication flow");
    auto overflow_length =
        co_await storage_->StringLength(0, "publisher-overflow");
    if (!overflow_length.ok()) co_return overflow_length.status();
    Check(*overflow_length == 17 * kMiB,
          "oversized publisher staging lost the primary write");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    status = co_await storage_->EnableReplicationLog(15, 3 * 8 * kMiB);
    if (!status.ok()) co_return status;
    RepeatedByteSource resize_large(9 * kMiB, 'M');
    auto resize_large_lsn =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .kind_ = ReplicationEventKind::kMutation,
            .partition_id_ = 5,
            .partition_sequence_ = 1,
            .payload_ = {},
            .payload_source_ = &resize_large,
        });
    if (!resize_large_lsn.ok()) co_return resize_large_lsn.status();
    auto resize_survivor =
        co_await storage_->AppendReplicationLog(ReplicationLogAppend{
            .kind_ = ReplicationEventKind::kMutation,
            .partition_id_ = 5,
            .partition_sequence_ = 2,
            .payload_ = "survivor",
            .payload_source_ = nullptr,
        });
    if (!resize_survivor.ok()) co_return resize_survivor.status();
    Check(storage_->LocalReplicationLogInfo().block_count_ == 3,
          "fragmented resize fixture did not span three blocks");
    status = co_await storage_->SetReplicationLogCapacity(8 * kMiB);
    if (!status.ok()) co_return status;
    const auto fragmented_shrink = storage_->LocalReplicationLogInfo();
    Check(fragmented_shrink.block_count_ == 1 &&
              fragmented_shrink.floor_lsn_ == *resize_survivor,
          "runtime shrink retained a trailing fragmented-event block");
    auto fragmented_evicted = co_await storage_->ReadReplicationLog(
        ReplicationLogCursor{.lsn_ = *resize_large_lsn}, kMiB, 1);
    Check(!fragmented_evicted.ok() && fragmented_evicted.status().code() ==
                                          absl::StatusCode::kOutOfRange,
          "fragmented event remained partially resumable after shrink");
    auto surviving_batch = co_await storage_->ReadReplicationLog(
        ReplicationLogCursor{.lsn_ = *resize_survivor}, kMiB, 1);
    Check(surviving_batch.ok() && surviving_batch->frames_.size() == 1 &&
              surviving_batch->frames_.front().payload_ == "survivor",
          "runtime shrink removed the event after a fragmented eviction");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;

    status = co_await storage_->EnableReplicationLog(16, 3 * 8 * kMiB);
    if (!status.ok()) co_return status;
    std::string resize_payload(kMiB, 'r');
    for (std::uint64_t sequence = 1; sequence <= 18; ++sequence) {
      auto appended =
          co_await storage_->AppendReplicationLog(ReplicationLogAppend{
              .kind_ = ReplicationEventKind::kMutation,
              .partition_id_ = 6,
              .partition_sequence_ = sequence,
              .payload_ = resize_payload,
              .payload_source_ = nullptr,
          });
      if (!appended.ok()) co_return appended.status();
    }
    const auto before_resize = storage_->LocalReplicationLogInfo();
    Check(before_resize.capacity_bytes_ == 3 * 8 * kMiB &&
              before_resize.block_count_ == 3,
          "replication backlog did not fill the configured blocks");
    status = co_await storage_->SetReplicationLogCapacity(8 * kMiB);
    if (!status.ok()) co_return status;
    const auto after_shrink = storage_->LocalReplicationLogInfo();
    Check(after_shrink.capacity_bytes_ == 8 * kMiB &&
              after_shrink.block_count_ == 1 &&
              after_shrink.floor_lsn_ > before_resize.floor_lsn_,
          "runtime backlog shrink did not evict sealed history");
    auto evicted_cursor = co_await storage_->ReadReplicationLog(
        ReplicationLogCursor{.lsn_ = before_resize.floor_lsn_}, kMiB, 1);
    Check(!evicted_cursor.ok() &&
              evicted_cursor.status().code() == absl::StatusCode::kOutOfRange,
          "runtime backlog shrink left an evicted cursor resumable");
    status = co_await storage_->SetReplicationLogCapacity(3 * 8 * kMiB);
    if (!status.ok()) co_return status;
    const auto after_growth = storage_->LocalReplicationLogInfo();
    Check(after_growth.capacity_bytes_ == 3 * 8 * kMiB &&
              after_growth.block_count_ == after_shrink.block_count_ &&
              after_growth.floor_lsn_ == after_shrink.floor_lsn_,
          "runtime backlog growth allocated blocks or rewrote history");
    status = co_await storage_->SetReplicationLogCapacity(kMiB);
    Check(!status.ok() && storage_->LocalReplicationLogInfo().capacity_bytes_ ==
                              3 * 8 * kMiB,
          "invalid runtime backlog size changed the active capacity");
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

    status = co_await ExerciseBacklogBackpressurePolicy();
    if (!status.ok()) co_return status;

    status = co_await ExerciseHardBacklogCap();
    if (!status.ok()) co_return status;

    status = co_await ExerciseCanonicalTransactionGrowth();
    if (!status.ok()) co_return status;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExercisePublishedStringCommands() {
    absl::Status status;
    // Client command dispatch transfers committed writes to the asynchronous
    // publisher. Large arguments may span replication frames, but decode and
    // apply still see one command and one LSN.
    status = co_await storage_->EnableReplicationLog(19, 4 * 8 * kMiB);
    if (!status.ok()) co_return status;
    status = co_await storage_->SetReplicationPublishQueueCapacity(kMiB);
    if (!status.ok()) co_return status;
    Check(storage_->LocalReplicationLogInfo().publish_queue_capacity_bytes_ ==
              kMiB,
          "dynamic publisher shrink was not installed");

    // The configured publisher high-water mark admits one larger command only
    // as an exclusive staging item. A following writer must remain suspended
    // until that admission is released, then wake without invalidating the
    // replication history.
    auto exclusive_admission =
        co_await storage_->AcquireReplicationPublisherAdmission(17 * kMiB);
    if (!exclusive_admission.ok()) co_return exclusive_admission.status();
    Check(exclusive_admission->log_epoch_ != 0,
          "oversized publisher admission did not bind to the active history");
    bool waiter_finished = false;
    absl::Status waiter_status =
        absl::UnknownError("publisher admission waiter did not run");
    auto wait_for_publisher_admission = [&]() -> bycorf::Task<absl::Status> {
      auto admitted =
          co_await storage_->AcquireReplicationPublisherAdmission(1);
      if (!admitted.ok()) {
        waiter_status = admitted.status();
      } else {
        storage_->ReleaseReplicationPublisherAdmission(*admitted, 1);
        waiter_status = absl::OkStatus();
      }
      waiter_finished = true;
      co_return absl::OkStatus();
    };
    worker_->Spawn(wait_for_publisher_admission());
    co_await bycorf::Yield(*worker_);
    Check(!waiter_finished,
          "publisher admission did not apply backpressure at the high-water "
          "mark");
    storage_->ReleaseReplicationPublisherAdmission(*exclusive_admission,
                                                   17 * kMiB);
    while (!waiter_finished) co_await bycorf::Yield(*worker_);
    if (!waiter_status.ok()) co_return waiter_status;

    status = co_await storage_->SetReplicationPublishQueueCapacity(32 * kMiB);
    if (!status.ok()) co_return status;
    auto grown_first =
        co_await storage_->AcquireReplicationPublisherAdmission(17 * kMiB);
    if (!grown_first.ok()) co_return grown_first.status();
    auto grown_second =
        co_await storage_->AcquireReplicationPublisherAdmission(kMiB);
    if (!grown_second.ok()) co_return grown_second.status();
    storage_->ReleaseReplicationPublisherAdmission(*grown_second, kMiB);
    storage_->ReleaseReplicationPublisherAdmission(*grown_first, 17 * kMiB);

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
    co_await bycorf::Yield(*worker_);
    Check(storage_->LocalReplicationLogInfo().tail_lsn_ == 1,
          "conditional SET no-op published a replication command");

    // This value is larger than the publisher queue's normal 16 MiB bound.
    // It must still be forwarded as one logical command via exclusive heap
    // staging and fragmented only by the memory backlog frame format.
    std::string large_value(17 * kMiB, 'V');
    std::vector<std::string> large_args;
    large_args.emplace_back("SET");
    large_args.emplace_back("replication-large");
    large_args.emplace_back(std::move(large_value));
    status = co_await ExecuteClientCommand(2, std::move(large_args), "+OK\r\n");
    if (!status.ok()) co_return status;
    status = co_await WaitForReplicationTail(2);
    if (!status.ok()) co_return status;

    std::vector<ReplicatedCommand> commands;
    ReplicationLogCursor cursor{};
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
      auto decoded = lavik::DecodeReplicationCommand(encoded);
      if (!decoded.ok()) co_return decoded.status();
      Check(encoded.size() > 1 &&
                !lavik::DecodeReplicationCommand(
                     std::string_view(encoded).substr(0, encoded.size() - 1))
                     .ok(),
            "truncated replication command was accepted");
      if (command_lsn == 1) {
        std::uint8_t effect_db = 0;
        const auto set_effect = ReplicatedEffectAt(*decoded, 0, &effect_db);
        Check(fragments == 1 && decoded->db_id_ == 2 && effect_db == 2 &&
                  set_effect == std::vector<std::string>(
                                    {"SET", "replication-set", "value", "PXAT",
                                     std::to_string(kExpireAt)}),
              "SET did not carry its final absolute expiry directly");
      } else {
        const auto set_effect = ReplicatedEffectAt(*decoded, 0);
        Check(fragments > 1 && decoded->db_id_ == 2 && set_effect.size() == 3 &&
                  set_effect[0] == "SET" &&
                  set_effect[1] == "replication-large" &&
                  set_effect[2].size() == 17 * kMiB &&
                  set_effect[2].front() == 'V' && set_effect[2].back() == 'V',
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
      status = co_await lavik::ApplyReplicatedCommand(command);
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
    Check(*restored_length == 5 && *restored_large_length == 17 * kMiB &&
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
    co_await bycorf::Yield(*worker_);
    Check(storage_->LocalReplicationLogInfo().tail_lsn_ == 1,
          "DEL publication did not follow its logical result");
    auto del_batch = co_await storage_->ReadReplicationLog({}, kMiB, 1);
    if (!del_batch.ok()) co_return del_batch.status();
    Check(del_batch->frames_.size() == 1 && del_batch->at_tail_,
          "single-key DEL did not produce one frame");
    auto del_command =
        lavik::DecodeReplicationCommand(del_batch->frames_.front().payload_);
    if (!del_command.ok()) co_return del_command.status();
    Check(del_command->db_id_ == 2 &&
              del_command->args_ ==
                  std::vector<std::string>({"DEL", "replication-set"}),
          "DEL command payload changed");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    auto recreated = co_await storage_->Set(2, "replication-set", "again", {});
    if (!recreated.ok()) co_return recreated.status();
    status = co_await lavik::ApplyReplicatedCommand(*del_command);
    if (!status.ok()) co_return status;
    Check(!co_await storage_->Exists(2, "replication-set"),
          "replica DEL did not remove the key");
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExercisePublishedCollectionCommands() {
    absl::Status status;
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
    status =
        co_await ExecuteClientCommand(4, {"INCR", "journal-counter"}, ":1\r\n");
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
    ReplicationLogCursor cursor{};
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
      auto decoded = lavik::DecodeReplicationCommand(encoded);
      if (!decoded.ok()) co_return decoded.status();
      family_commands.push_back(std::move(*decoded));
    }
    const std::vector<std::string> expected_names{"LPUSH", "HSET", "SADD",
                                                  "ZADD",  "SET",  "PEXPIREAT"};
    Check(family_commands.size() == expected_names.size(),
          "single-key command journal count changed");
    for (std::size_t i = 0; i < expected_names.size(); ++i) {
      const auto effect = ReplicatedEffectAt(family_commands[i], 0);
      Check(family_commands[i].db_id_ == 4 && !effect.empty() &&
                effect.front() == expected_names[i],
            "single-key command journal order changed");
    }
    const auto expiry_effect = ReplicatedEffectAt(family_commands.back(), 0);
    Check(expiry_effect.size() == 3 && expiry_effect[1] == "journal-counter",
          "relative expiry was not normalized to PEXPIREAT");

    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    for (std::string_view key : {"journal-list", "journal-hash", "journal-set",
                                 "journal-zset", "journal-counter"}) {
      auto removed = co_await storage_->Delete(4, key);
      if (!removed.ok()) co_return removed.status();
    }
    for (const ReplicatedCommand& command : family_commands) {
      status = co_await lavik::ApplyReplicatedCommand(command);
      if (!status.ok()) co_return status;
    }
    status =
        co_await ExecuteClientCommand(4, {"LLEN", "journal-list"}, ":2\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(4, {"HGET", "journal-hash", "field"},
                                           "$5\r\nvalue\r\n");
    if (!status.ok()) co_return status;
    status =
        co_await ExecuteClientCommand(4, {"SCARD", "journal-set"}, ":2\r\n");
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

    // A collection mutation keeps the source key's absolute deadline in its
    // replication effect. If replay happens after that deadline, the command
    // may transiently recreate the key but the trailing PEXPIREAT must remove
    // it instead of leaving a permanent value behind.
    status = co_await storage_->EnableReplicationLog(24, 8 * kMiB);
    if (!status.ok()) co_return status;
    status =
        co_await ExecuteClientCommand(5, {"LPUSH", "late-ttl", "a"}, ":1\r\n");
    if (!status.ok()) co_return status;
    status = co_await ExecuteClientCommand(5, {"PEXPIRE", "late-ttl", "200"},
                                           ":1\r\n");
    if (!status.ok()) co_return status;
    status =
        co_await ExecuteClientCommand(5, {"LPUSH", "late-ttl", "b"}, ":2\r\n");
    if (!status.ok()) co_return status;
    auto late_fence = co_await storage_->FenceReplicationLog();
    if (!late_fence.ok()) co_return late_fence.status();
    Check(*late_fence == 4, "late TTL journal did not publish three commands");
    ReplicatedCommand late_command;
    cursor = {};
    for (unsigned index = 0; index < 3; ++index) {
      auto batch = co_await storage_->ReadReplicationLog(cursor, kMiB, 1);
      if (!batch.ok()) co_return batch.status();
      Check(batch->frames_.size() == 1,
            "late TTL journal command was not readable");
      auto decoded =
          lavik::DecodeReplicationCommand(batch->frames_.front().payload_);
      if (!decoded.ok()) co_return decoded.status();
      if (index == 2) late_command = std::move(*decoded);
      cursor = batch->next_;
    }
    const auto late_ttl_effect = ReplicatedEffectAt(late_command, 1);
    Check(late_ttl_effect.size() == 3 && late_ttl_effect[0] == "PEXPIREAT" &&
              late_ttl_effect[1] == "late-ttl",
          "collection mutation omitted its final absolute expiration");
    status = co_await storage_->DisableReplicationLog();
    if (!status.ok()) co_return status;
    status =
        co_await bycorf::SleepFor(*worker_, std::chrono::milliseconds(250));
    if (!status.ok()) co_return status;
    status = co_await lavik::ApplyReplicatedCommand(late_command);
    if (!status.ok()) co_return status;
    Check(!co_await storage_->Exists(5, "late-ttl"),
          "delayed collection replay resurrected an expired key");

    status = co_await ExerciseSourceAfterImages();
    if (!status.ok()) co_return status;

    // Replica replay is strict: an error in any EXEC child fails the apply
    // instead of returning a successful RESP array that the flow would ACK.
    auto wrong_type =
        co_await storage_->Set(6, "strict-wrongtype", "string", {});
    if (!wrong_type.ok()) co_return wrong_type.status();
    ReplicatedCommand strict_exec{
        .db_id_ = 6,
        .args_ = {std::string(lavik::kReplicatedExecCommand), "2", "6", "2",
                  "INCR", "strict-counter", "6", "4", "HSET",
                  "strict-wrongtype", "field", "value"},
    };
    status = co_await lavik::ApplyReplicatedCommand(strict_exec);
    Check(!status.ok(), "replicated EXEC acknowledged a child command error");
    status = co_await ExecuteClientCommand(6, {"GET", "strict-counter"},
                                           "$1\r\n1\r\n");
    if (!status.ok()) co_return status;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseFullSyncDuringTombstoneReaping() {
    constexpr std::uint8_t kDb = 9;
    constexpr std::uint64_t kSession = 0x534852494e4b;
    constexpr std::size_t kKeys = 256;
    constexpr std::size_t kPermanent = 16;
    std::vector<std::string> keys;
    const auto expire_at =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count() +
        1000;
    for (std::size_t i = 0; i < kKeys; ++i) {
      keys.push_back("fullsync-shrink{ttl-reap}-" + std::to_string(i));
      lavik::storage::SetOptions options;
      if (i >= kPermanent) options.expire_at_ms_ = expire_at;
      auto written =
          co_await storage_->Set(kDb, keys.back(), "baseline", options);
      if (!written.ok()) co_return written.status();
    }
    const auto partition_id = lavik::storage::StorageShardForKey(keys[0]);
    auto session = storage_->BeginFullSyncSession(kSession);
    if (!session.ok()) co_return session.status();
    auto start = storage_->BeginPartitionReplication(kSession, partition_id);
    if (!start.ok()) co_return start.status();
    absl::Status status =
        storage_->BeginPartitionDbReplication(kSession, partition_id, kDb);
    if (!status.ok()) co_return status;
    // Pause a real full-sync cursor after its first bucket. Reaping most of
    // this same-slot population will cross several shrink thresholds before
    // the next snapshot call, with a concurrent write using override capture.
    auto first =
        co_await storage_->SnapshotPartition(kSession, partition_id, kDb, 0, 1);
    if (!first.ok()) co_return first.status();
    Check(first->cursor_ != 0, "shrink fixture did not pause a full-sync scan");
    std::unordered_map<std::string, unsigned> seen;
    for (const auto& record : first->records_) ++seen[record.key_];
    storage_->AcknowledgePartitionSnapshotRecords(kSession, partition_id,
                                                  first->records_);
    std::size_t rewritten_index = 0;
    while (rewritten_index < kPermanent &&
           seen.contains(keys[rewritten_index])) {
      ++rewritten_index;
    }
    Check(rewritten_index < kPermanent,
          "fixture needs an uncovered stable key");
    auto rewritten =
        co_await storage_->Set(kDb, keys[rewritten_index], "rewritten", {});
    if (!rewritten.ok()) co_return rewritten.status();

    const auto reaped_before = storage_->TombRaiderStats().reaped_;
    status = co_await storage_->ConfigureTombRaider(
        {.action_ = lavik::storage::TombRaiderConfigAction::kBlockSleep,
         .value_ = 0});
    if (!status.ok()) co_return status;
    status = co_await storage_->ConfigureTombRaider(
        {.action_ = lavik::storage::TombRaiderConfigAction::kInterval,
         .value_ = 10});
    if (!status.ok()) co_return status;
    status = co_await storage_->ConfigureTombRaider(
        {.action_ = lavik::storage::TombRaiderConfigAction::kOn});
    if (!status.ok()) co_return status;
    status =
        co_await bycorf::SleepFor(*worker_, std::chrono::milliseconds(1100));
    if (!status.ok()) co_return status;
    for (std::size_t i = kPermanent; i < keys.size(); ++i) {
      auto expired = co_await storage_->Get(kDb, keys[i]);
      Check(absl::IsNotFound(expired.status()), "TTL fixture remained visible");
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (storage_->TombRaiderStats().reaped_ <
           reaped_before + kKeys - kPermanent) {
      Check(std::chrono::steady_clock::now() < deadline,
            "TTL tombstones were not erased during full sync");
      status =
          co_await bycorf::SleepFor(*worker_, std::chrono::milliseconds(10));
      if (!status.ok()) co_return status;
    }
    std::uint64_t cursor = first->cursor_;
    unsigned batches = 0;
    do {
      auto batch = co_await storage_->SnapshotPartition(kSession, partition_id,
                                                        kDb, cursor, 1);
      if (!batch.ok()) co_return batch.status();
      for (const auto& record : batch->records_) ++seen[record.key_];
      storage_->AcknowledgePartitionSnapshotRecords(kSession, partition_id,
                                                    batch->records_);
      cursor = batch->cursor_;
      Check(++batches <= kKeys,
            "full-sync cursor failed to terminate after shrink");
    } while (cursor != 0);
    for (std::size_t i = 0; i < kPermanent; ++i) {
      if (i == rewritten_index) continue;
      Check(seen[keys[i]] == 1,
            "full sync missed or duplicated a stable key during shrink");
    }
    auto overrides = co_await storage_->ReadPartitionFullSyncOverrides(
        kSession, partition_id, kKeys + 1);
    if (!overrides.ok()) co_return overrides.status();
    bool saw_rewrite = false;
    for (const auto& record : overrides->records_) {
      if (record.key_ == keys[rewritten_index] && record.value_ == "rewritten")
        saw_rewrite = true;
    }
    Check(saw_rewrite, "full sync lost the concurrent overwrite during shrink");
    storage_->EndPartitionReplication(kSession, partition_id);
    storage_->EndFullSyncSession(kSession);
    status = co_await storage_->QuiesceTombRaiderForReplica();
    if (!status.ok()) co_return status;
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> Exercise() {
    absl::Status status = co_await ExerciseSharedHistoryQuota();
    if (!status.ok()) co_return status;
    status = co_await ExerciseMutationPrecondition();
    if (!status.ok()) co_return status;

    status = co_await ExerciseAdmissionAndOrdering();
    if (!status.ok()) co_return status;

    status = co_await ExerciseBacklogStorage();
    if (!status.ok()) co_return status;

    status = co_await ExercisePublishedStringCommands();
    if (!status.ok()) co_return status;

    status = co_await ExercisePublishedCollectionCommands();
    if (!status.ok()) co_return status;

    status = co_await ExerciseFlushControlBarriers();
    if (!status.ok()) co_return status;

    status = co_await ExerciseFullSyncDuringTombstoneReaping();
    if (!status.ok()) co_return status;

    // The backlog is process memory only. Restart recovers primary records and
    // establishes a fresh replication history without any backlog cleanup.
    co_return absl::OkStatus();
  }

  StorageEngine* storage_ = nullptr;
  bycorf::Worker* worker_ = nullptr;
  bool exercise_ = false;
  absl::Status result_ = absl::UnknownError("test service did not run");
};

int RunOnce(const std::string& path, bool exercise) {
  StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  options.replication_publish_queue_bytes_ = 16 * kMiB;
  StorageEngine storage(std::move(options));
  lavik::InitWorkerMetrics(1);
  const absl::Status memory = lavik::InitMemoryLimit(512 * kMiB, 1);
  if (!memory.ok()) {
    std::cerr << memory << '\n';
    return 1;
  }
  lavik::InitStorage(&storage, nullptr);
  const absl::Status prepared = storage.Prepare(1);
  if (!prepared.ok()) {
    std::cerr << prepared << '\n';
    return 1;
  }
  lavik::tx::TxRuntime::Create(1);
  ReplicationLogService service(&storage, exercise);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
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
    CheckReplicationTransactionEnvelopeCodec();
    if (argc == 3 && std::string_view(argv[1]) == "--recover") {
      return RunOnce(argv[2], false);
    }
    Check(argc == 1, "unexpected replication-log test arguments");
    const std::string path = lavik::test::TestDataPath(
        "lavik-replication-log-" + std::to_string(::getpid()) + ".data");
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
          "recovery verifier rejected primary data after memory-backlog use");
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
