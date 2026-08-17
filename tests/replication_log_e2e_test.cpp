#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "celer/net/server.h"
#include "keylane/memory.h"
#include "keylane/storage/engine.h"
#include "keylane/tx/tx_shard.h"

namespace {

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
  celer::Task<absl::Status> Exercise() {
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
  bool exercise_ = false;
  absl::Status result_ = absl::UnknownError("test service did not run");
};

int RunOnce(const std::string& path, bool exercise) {
  StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  StorageEngine storage(std::move(options));
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
