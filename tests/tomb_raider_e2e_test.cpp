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

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "../src/storage/engine/impl.h"
#include "bycorf/net/server.h"
#include "lavik/memory.h"
#include "lavik/metrics.h"
#include "lavik/storage/engine.h"
#include "lavik/tx/tx_shard.h"
#include "support/test_data_path.h"

namespace lavik::storage {

// Exercise real rounds with scheduling disabled so every invalidation lands
// at an explicitly selected boundary rather than depending on timer races.
class TombRaiderTestPeer {
 public:
  using Token = StorageEngine::Impl::PopulationChangeToken;

  static auto Begin(StorageEngine& storage, std::uint64_t id) {
    return storage.impl_->BeginPopulationChange(id);
  }
  static auto Complete(StorageEngine& storage, Token token) {
    return storage.impl_->CompletePopulationChange(token);
  }
  static auto Cancel(StorageEngine& storage, Token token) {
    return storage.impl_->CancelPopulationChange(token);
  }

  enum class RetirementScenario {
    kBeforeSnapshot,
    kAfterRead,
    kPinned,
    kPrefixClaim,
    kRepeated,
    kCancelledWait
  };

  static bycorf::Task<absl::Status> Run(StorageEngine& storage) {
    return storage.impl_->RunTombRaider();
  }

#if LAVIK_FAULTS_ENABLED
  using Point = StorageEngine::Impl::TombRaiderTestPoint;
  using Hook = StorageEngine::Impl::TombRaiderTestHook;

  static void SetHook(StorageEngine& storage, Hook hook) {
    storage.impl_->tomb_raider_test_hook_ = std::move(hook);
  }

  static bycorf::Task<absl::Status> SealForSweep(StorageEngine& storage) {
    auto& store = storage.impl_->CurrentStore();
    storage.impl_->SealActiveBlocks(store);
    while (store.flush_running_) {
      auto waited = co_await bycorf::SleepFor(*store.worker_,
                                              std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }
    co_return absl::OkStatus();
  }

  static bycorf::Task<absl::StatusOr<bool>> IsTombstone(StorageEngine& storage,
                                                        std::uint8_t database,
                                                        std::string_view key) {
    auto& store = storage.impl_->CurrentStore();
    auto& partition = storage.impl_->PartitionForKey(store, key);
    auto found = co_await storage.impl_->FindVerifiedEntry(
        store, partition.indexes_[database], ComputeDigest(key), key);
    if (!found.ok()) co_return found.status();
    co_return *found != nullptr &&
        (*found)->value_.kind() == RecordKind::kTombstone;
  }
  static bycorf::Task<absl::Status> VerifyPendingBlockRetirement(
      StorageEngine& storage, RetirementScenario scenario) {
    auto* impl = storage.impl_.get();
    auto& store = impl->CurrentStore();
    auto& worker = *store.worker_;
    auto status = co_await storage.ConfigureTombRaider(
        {.action_ = TombRaiderConfigAction::kOff});
    if (!status.ok()) co_return status;
    status = co_await storage.ConfigureTombRaider(
        {.action_ = TombRaiderConfigAction::kBlockSleep, .value_ = 0});
    if (!status.ok()) co_return status;
    status = co_await storage.CompleteStorageStartup();
    if (!status.ok()) co_return status;
    const unsigned iterations =
        scenario == RetirementScenario::kRepeated ? 4 : 1;
    const std::size_t key_count =
        scenario == RetirementScenario::kPrefixClaim ? 513 : 1;
    for (unsigned iteration = 0; iteration < iterations; ++iteration) {
      std::vector<std::string> keys;
      for (std::size_t index = 0; index < key_count; ++index) {
        keys.push_back("retiring-old-value-" + std::to_string(iteration) + "-" +
                       std::to_string(index));
        auto wrote = co_await storage.Set(0, keys.back(), "must-not-resurrect");
        if (!wrote.ok()) co_return wrote.status();
      }
      auto& partition = impl->PartitionForKey(store, keys.front());
      auto found = co_await impl->FindVerifiedEntry(
          store, partition.indexes_[0], ComputeDigest(keys.front()),
          keys.front());
      if (!found.ok()) co_return found.status();
      if (*found == nullptr)
        co_return absl::FailedPreconditionError(
            "missing retirement fixture value");
      const auto block = impl->MaterializeIndexLocation(**found).block_id();
      status = co_await SealForSweep(storage);
      if (!status.ok()) co_return status;
      for (const auto& key : keys) {
        auto removed = co_await storage.Delete(0, key);
        if (!removed.ok()) co_return removed.status();
        if (!*removed)
          co_return absl::FailedPreconditionError(
              "missing retirement fixture tombstone");
      }
      // Superseded accounting settles only when the replacement is durable.
      // Keep permanent older values on disk in a separate, now-dead block.
      status = co_await SealForSweep(storage);
      if (!status.ok()) co_return status;
      auto* source = impl->FindBlockState(store, block);
      if (source == nullptr || source->live_bytes_ != 0)
        co_return absl::FailedPreconditionError(
            "old records block is not empty");
      auto& allocator =
          *impl->device_allocators_[impl->DeviceIndexForBlock(block)];
      std::size_t source_read = 0;
      std::size_t read_count = 0;
      impl->ForEachOwnedBlock(store, [&](std::uint64_t id, BlockState& state) {
        if (state.kind_ == BlockKind::kRecords &&
            state.committed_bytes_ > kBlockHeaderBytes &&
            impl->StagingFor(store, state) == nullptr) {
          ++read_count;
          if (id == block) source_read = read_count;
        }
      });
      if (source_read == 0)
        co_return absl::FailedPreconditionError("old value has no disk read");

      co_await allocator.mutex_.Lock();
      std::optional<bycorf::Task<absl::Status>> retirement;
      bycorf::AsyncNotification retired;
      struct ReaderPin {
        BlockState* state_ = nullptr;
        void Reset() {
          if (state_ != nullptr) {
            --state_->pins_;
            state_ = nullptr;
          }
        }
        ~ReaderPin() { Reset(); }
      } pin;
      const auto begin_retirement = [&]() -> absl::Status {
        if (retirement.has_value()) return absl::OkStatus();
        auto task = impl->CleanBlockLocked(store, block);
        auto handle = std::move(task).ReleaseHandle();
        retirement.emplace(handle);
        retirement->SetCompletionCallback(
            &retired, [](void* context, auto) noexcept {
              static_cast<bycorf::AsyncNotification*>(context)->NotifyAll(
                  *bycorf::ThisWorker().self_);
            });
        handle.resume();
        if (scenario == RetirementScenario::kPinned) return absl::OkStatus();
        // Real defrag removed BlockState, then parked at the allocator mutex.
        // Neither the pending counter nor the bitmap is changed by the test.
        if (retirement->done() ||
            impl->FindBlockState(store, block) != nullptr ||
            store.pending_record_block_retirements_ != 1 ||
            !impl->BitmapBit(allocator, LocalBlockId(block)))
          return absl::FailedPreconditionError(
              "defrag did not pause before durable retirement");
        return absl::OkStatus();
      };
      if (scenario == RetirementScenario::kPinned) {
        // Model a reader acquiring its owner-local pin after defrag's initial
        // pins==0 check but before its final store-mutex check. The production
        // ReleaseEmptyBlock path sets freeing and waits with pending==0.
        co_await store.store_state_mutex_.Lock();
        status = begin_retirement();
        if (status.ok() && !retirement->done()) {
          ++source->pins_;
          pin.state_ = source;
        } else if (status.ok()) {
          status = absl::FailedPreconditionError(
              "defrag did not wait for store mutex");
        }
        store.store_state_mutex_.Unlock(worker);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (status.ok() && !source->freeing_ &&
               std::chrono::steady_clock::now() < deadline) {
          status =
              co_await bycorf::SleepFor(worker, std::chrono::milliseconds(1));
        }
        if (status.ok() && (!source->freeing_ || source->pins_ != 1 ||
                            store.pending_record_block_retirements_ != 0))
          status = absl::FailedPreconditionError(
              "reader pin did not hold the freeing window");
      } else if (scenario != RetirementScenario::kAfterRead &&
                 scenario != RetirementScenario::kPrefixClaim) {
        status = begin_retirement();
      }
      read_count = 0;
      bool retirement_wait_reached = false;
      SetHook(storage, [&](Point point) -> bycorf::Task<absl::Status> {
        if (point == Point::kBeforeRetirementWait)
          retirement_wait_reached = true;
        if ((scenario == RetirementScenario::kAfterRead &&
             point == Point::kAfterSweepRead && ++read_count == source_read) ||
            (scenario == RetirementScenario::kPrefixClaim &&
             point == Point::kAfterClaimLookup))
          co_return begin_retirement();
        co_return absl::OkStatus();
      });

      const auto before = storage.TombRaiderStats();
      std::optional<bycorf::Task<absl::Status>> round;
      bycorf::AsyncNotification round_finished;
      if (status.ok()) {
        auto task = Run(storage);
        auto handle = std::move(task).ReleaseHandle();
        round.emplace(handle);
        round->SetCompletionCallback(
            &round_finished, [](void* context, auto) noexcept {
              static_cast<bycorf::AsyncNotification*>(context)->NotifyAll(
                  *bycorf::ThisWorker().self_);
            });
        handle.resume();
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        // Observe the actual sweep wait while the allocator mutex remains
        // locked, including waits before the block snapshot and for pins.
        while (status.ok() && !retirement_wait_reached && !round->done() &&
               std::chrono::steady_clock::now() < deadline) {
          status =
              co_await bycorf::SleepFor(worker, std::chrono::milliseconds(1));
        }
        if (status.ok() &&
            (!retirement_wait_reached || !retirement.has_value() ||
             round->done() || !storage.TombRaiderStats().running_ ||
             storage.TombRaiderStats().rounds_ != before.rounds_ ||
             storage.TombRaiderStats().reaped_ != before.reaped_))
          status = absl::FailedPreconditionError(
              "sweep did not wait for ordinary retirement");
      }
      if (status.ok()) {
        for (const auto& key : keys) {
          auto protected_key = co_await IsTombstone(storage, 0, key);
          if (!protected_key.ok()) {
            status = protected_key.status();
            break;
          }
          if (!*protected_key) {
            status = absl::FailedPreconditionError(
                "pending retirement lost a necessary tombstone");
            break;
          }
        }
      }
      const bool cancel_wait = scenario == RetirementScenario::kCancelledWait;
      if (status.ok() && cancel_wait) {
        auto change = co_await Begin(storage, 1000 + iteration);
        if (!change.ok())
          status = change.status();
        else {
          if (!round->done() ||
              storage.TombRaiderStats().rounds_ != before.rounds_)
            status = absl::FailedPreconditionError(
                "population drain did not cancel retirement wait");
          auto cancelled = co_await Cancel(storage, *change);
          if (status.ok()) status = cancelled;
        }
      }
      // Always release the real waiters before returning any fixture failure.
      pin.Reset();
      allocator.mutex_.Unlock(worker);
      if (retirement.has_value()) {
        while (!retirement->done()) co_await retired.Wait();
        auto result = std::move(*retirement).TakeResult();
        if (status.ok()) status = result;
      }
      if (round.has_value()) {
        while (!round->done()) co_await round_finished.Wait();
        auto result = std::move(*round).TakeResult();
        if (status.ok()) status = result;
      }
      SetHook(storage, {});
      if (!status.ok()) co_return status;
      if (store.pending_record_block_retirements_ != 0 ||
          impl->BitmapBit(allocator, LocalBlockId(block)))
        co_return absl::FailedPreconditionError(
            "bitmap retirement did not finish");
      if (storage.TombRaiderStats().rounds_ !=
          before.rounds_ + (cancel_wait ? 0 : 1))
        co_return absl::FailedPreconditionError(
            "ordinary retirement restarted the whole sweep");
      if (scenario == RetirementScenario::kPrefixClaim) {
        // The first 512 claims were already applied. They can conservatively
        // retain that prefix, but must not invalidate the round or its suffix.
        if (storage.TombRaiderStats().reaped_ != before.reaped_ + 1)
          co_return absl::FailedPreconditionError(
              "mid-decode retirement lost suffix progress");
      }
      if (cancel_wait || scenario == RetirementScenario::kPrefixClaim) {
        status = co_await Run(storage);
        if (!status.ok()) co_return status;
      }
      if (storage.TombRaiderStats().reaped_ != before.reaped_ + key_count)
        co_return absl::FailedPreconditionError(
            "durable retirement did not permit reclamation");
      for (const auto& key : keys) {
        auto remaining = co_await IsTombstone(storage, 0, key);
        if (!remaining.ok()) co_return remaining.status();
        if (*remaining)
          co_return absl::FailedPreconditionError(
              "retired block kept its tombstone indefinitely");
      }
    }
    co_return absl::OkStatus();
  }

  static bycorf::Task<absl::Status> FlushWhileReapWaitsForKey(
      StorageEngine& storage, std::uint8_t database, std::string_view key) {
    auto& worker = *bycorf::ThisWorker().self_;
    auto hold = co_await tx::CurrentTxShard().AcquireKey(
        database, tx::FingerprintOf(ComputeDigest(key)),
        tx::LockMode::kExclusive);
    bool at_lock = false;
    SetHook(storage, [&](Point point) -> bycorf::Task<absl::Status> {
      if (point == Point::kBeforeReapLock) at_lock = true;
      co_return absl::OkStatus();
    });
    auto task = Run(storage);
    auto handle = std::move(task).ReleaseHandle();
    task = bycorf::Task<absl::Status>(handle);
    bycorf::AsyncNotification completed;
    task.SetCompletionCallback(&completed, [](void* context, auto) noexcept {
      static_cast<bycorf::AsyncNotification*>(context)->NotifyAll(
          *bycorf::ThisWorker().self_);
    });
    handle.resume();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!at_lock && !task.done() &&
           std::chrono::steady_clock::now() < deadline) {
      (co_await bycorf::SleepFor(worker, std::chrono::milliseconds(1)))
          .IgnoreError();
    }
    absl::Status changed;
    if (at_lock && !task.done()) {
      changed = co_await storage.FlushDbDetach(database);
      if (changed.ok()) changed = co_await storage.FlushDbReclaim(true);
    } else {
      changed = absl::FailedPreconditionError(
          "reap did not wait behind the held key");
    }
    hold.Reset();
    while (!task.done()) co_await completed.Wait();
    SetHook(storage, {});
    auto reaped = std::move(task).TakeResult();
    co_return changed.ok() ? reaped : changed;
  }
#endif
};

}  // namespace lavik::storage

namespace {

using namespace std::chrono_literals;

constexpr std::uint64_t kMiB = 1024 * 1024;
// Fault cases deliberately seal partially filled blocks and pause defrag so
// disk-resident older values remain present throughout each proof sweep.
constexpr std::uint64_t kControllerDataBytes = 2048 * kMiB;

[[noreturn]] void Fail(std::string message) {
  throw std::runtime_error(std::move(message));
}

class RespClient {
 public:
  explicit RespClient(int fd) : fd_(fd) {}
  RespClient(const RespClient&) = delete;
  RespClient& operator=(const RespClient&) = delete;
  RespClient(RespClient&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
  RespClient& operator=(RespClient&&) = delete;
  ~RespClient() {
    if (fd_ >= 0) ::close(fd_);
  }

  std::string Command(const std::vector<std::string_view>& args) {
    std::string request = "*" + std::to_string(args.size()) + "\r\n";
    for (std::string_view arg : args) {
      request += "$" + std::to_string(arg.size()) + "\r\n";
      request.append(arg);
      request += "\r\n";
    }
    SendAll(request);
    const std::string line = ReadLine();
    if (!line.starts_with('$') || line == "$-1") {
      return line;
    }
    std::size_t size = 0;
    const char* begin = line.data() + 1;
    const char* end = line.data() + line.size();
    const auto [parsed_end, error] = std::from_chars(begin, end, size);
    if (error != std::errc{} || parsed_end != end) {
      Fail("malformed bulk reply length");
    }
    std::string payload(size + 2, '\0');
    ReadExact(payload.data(), payload.size());
    if (!payload.ends_with("\r\n")) {
      Fail("malformed bulk reply terminator");
    }
    payload.resize(size);
    return line + "\r\n" + payload;
  }

 private:
  void SendAll(std::string_view bytes) {
    while (!bytes.empty()) {
      const ssize_t sent =
          ::send(fd_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
      if (sent < 0) {
        if (errno == EINTR) continue;
        Fail("send failed: " + std::string(std::strerror(errno)));
      }
      if (sent == 0) Fail("send returned zero bytes");
      bytes.remove_prefix(static_cast<std::size_t>(sent));
    }
  }

  void ReadExact(char* output, std::size_t size) {
    while (size != 0) {
      const ssize_t received = ::recv(fd_, output, size, 0);
      if (received < 0) {
        if (errno == EINTR) continue;
        Fail("recv failed: " + std::string(std::strerror(errno)));
      }
      if (received == 0) Fail("server closed the connection");
      output += received;
      size -= static_cast<std::size_t>(received);
    }
  }

  std::string ReadLine() {
    std::string response;
    while (!response.ends_with("\r\n")) {
      char byte = 0;
      ReadExact(&byte, 1);
      response.push_back(byte);
      if (response.size() > 4096) Fail("unexpectedly long RESP line");
    }
    response.resize(response.size() - 2);
    return response;
  }

  int fd_ = -1;
};

std::uint16_t FindFreePort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0) Fail("socket failed while selecting a port");
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0) {
    ::close(fd);
    Fail("bind failed while selecting a port");
  }
  socklen_t bytes = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &bytes) != 0) {
    ::close(fd);
    Fail("getsockname failed while selecting a port");
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

void CreateDataFile(const std::string& path, std::uint64_t bytes) {
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) Fail("failed to create test data file");
  const int allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  const int close_error = ::close(fd);
  if (allocated != 0 || close_error != 0) Fail("failed to size data file");
}

class TombRaiderControllerService final : public bycorf::Service {
 public:
  TombRaiderControllerService(
      lavik::storage::StorageEngine* storage, bool recover_incomplete,
      std::optional<lavik::storage::TombRaiderTestPeer::RetirementScenario>
          retirement_scenario = std::nullopt)
      : storage_(storage),
        recover_incomplete_(recover_incomplete),
        retirement_scenario_(retirement_scenario) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) Fail("tomb raider controller requires one worker");
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    worker_ = &worker;
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_->InitializeWorker(worker);
    if (result_.ok()) result_ = co_await ExerciseSelectedScenario();
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  void FinalizeWorker(bycorf::Worker& worker) noexcept override {
    storage_->FinalizeWorker(worker);
  }

  const absl::Status& result() const noexcept { return result_; }

 private:
  using Peer = lavik::storage::TombRaiderTestPeer;
  using Action = lavik::storage::TombRaiderConfigAction;

  bycorf::Task<absl::Status> ExerciseSelectedScenario() {
#if LAVIK_FAULTS_ENABLED
    if (retirement_scenario_.has_value()) {
      co_return co_await Peer::VerifyPendingBlockRetirement(
          *storage_, *retirement_scenario_);
    }
#endif
    if (recover_incomplete_) co_return co_await ExerciseIncompleteRecovery();
    co_return co_await Exercise();
  }

  bycorf::Task<absl::Status> RequireBlocked(std::string_view context) {
    const auto rounds = storage_->TombRaiderStats().rounds_;
    auto status = co_await Peer::Run(*storage_);
    if (!status.ok()) co_return status;
    if (storage_->TombRaiderStats().running_ ||
        storage_->TombRaiderStats().rounds_ != rounds) {
      co_return absl::FailedPreconditionError(std::string(context) +
                                              " admitted a maintenance round");
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> RequireRound(std::string_view context) {
    const auto rounds = storage_->TombRaiderStats().rounds_;
    auto status = co_await Peer::Run(*storage_);
    if (!status.ok()) co_return status;
    if (storage_->TombRaiderStats().rounds_ != rounds + 1) {
      co_return absl::FailedPreconditionError(
          std::string(context) + " did not complete one maintenance round");
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseIncompleteRecovery() {
    auto status = co_await storage_->CompleteStorageStartup();
    if (!status.ok()) co_return status;
    if (!storage_->ReplicaRecoveryFenced())
      co_return absl::FailedPreconditionError(
          "lost durable incomplete FULL fence");
    status = co_await storage_->ConfigureTombRaider(
        {.action_ = Action::kInterval, .value_ = 1});
    if (!status.ok()) co_return status;
    co_return co_await RequireBlocked("restart after incomplete FULL");
  }

  bycorf::Task<absl::Status> Exercise() {
    auto status = co_await RequireBlocked("startup not yet complete");
    if (!status.ok()) co_return status;
    // Managed nodes and replicas begin without active-expiration authority.
    // Local physical maintenance must nevertheless accept its own settings.
    status = co_await storage_->ConfigureTombRaider(
        {.action_ = Action::kInterval, .value_ = 1});
    if (!status.ok()) co_return status;
    auto seeded = co_await storage_->Set(0, "quiesce-running-round", "v");
    if (!seeded.ok()) co_return seeded.status();
    status = co_await storage_->CompleteStorageStartup();
    if (!status.ok()) co_return status;

    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (!storage_->TombRaiderStats().running_) {
      if (std::chrono::steady_clock::now() >= deadline)
        co_return absl::DeadlineExceededError("tomb raider did not start");
      status = co_await bycorf::SleepFor(*worker_, 1ms);
      if (!status.ok()) co_return status;
    }
    status = co_await storage_->ConfigureTombRaider({.action_ = Action::kOff});
    if (!status.ok()) co_return status;
    if (storage_->TombRaiderStats().enabled_ ||
        !storage_->TombRaiderStats().running_) {
      co_return absl::FailedPreconditionError(
          "user OFF interrupted the admitted round");
    }
    const auto rounds = storage_->TombRaiderStats().rounds_;
    const auto began = std::chrono::steady_clock::now();
    auto hold11 = co_await Peer::Begin(*storage_, 11);
    if (!hold11.ok()) co_return hold11.status();
    if (std::chrono::steady_clock::now() - began > 2s ||
        storage_->TombRaiderStats().running_ ||
        storage_->TombRaiderStats().rounds_ != rounds ||
        storage_->TombRaiderStats().enabled_) {
      co_return absl::FailedPreconditionError(
          "population drain did not forfeit promptly while preserving OFF");
    }
    status = co_await storage_->ConfigureTombRaider(
        {.action_ = Action::kBlockSleep, .value_ = 0});
    if (!status.ok()) co_return status;
    auto hold12 = co_await Peer::Begin(*storage_, 12);
    if (!hold12.ok()) co_return hold12.status();
    auto repeated = co_await Peer::Begin(*storage_, 12);
    if (!repeated.ok()) co_return repeated.status();
    if (repeated->generation_ != hold12->generation_)
      co_return absl::FailedPreconditionError(
          "repeated active admission created a second population hold");
    status = co_await Peer::Complete(*storage_, *hold11);
    if (!absl::IsFailedPrecondition(status))
      co_return absl::FailedPreconditionError("stale completion was accepted");
    status = co_await RequireBlocked("overlapping newer population change");
    if (!status.ok()) co_return status;
    status = co_await Peer::Complete(*storage_, *hold12);
    if (!status.ok()) co_return status;
    status = co_await RequireRound("complete population without authority");
    if (!status.ok()) co_return status;

    auto hold13 = co_await Peer::Begin(*storage_, 13);
    if (!hold13.ok()) co_return hold13.status();
    (void)co_await Peer::Complete(*storage_, *hold11);
    status = co_await RequireBlocked("late callback after newer admission");
    if (!status.ok()) co_return status;
    status = co_await Peer::Cancel(*storage_, *hold13);
    if (!status.ok()) co_return status;
    status = co_await RequireRound("pre-destructive cancellation");
    if (!status.ok()) co_return status;

    // Wire session IDs can be reused after a source restarts. Neither a late
    // completion nor a late cancellation may release that new incarnation.
    auto reused = co_await Peer::Begin(*storage_, 13);
    if (!reused.ok()) co_return reused.status();
    (void)co_await Peer::Complete(*storage_, *hold13);
    (void)co_await Peer::Cancel(*storage_, *hold13);
    status = co_await RequireBlocked("reused session after stale callbacks");
    if (!status.ok()) co_return status;
    status = co_await Peer::Complete(*storage_, *reused);
    if (!status.ok()) co_return status;
    status = co_await RequireRound("new incarnation of reused session");
    if (!status.ok()) co_return status;

    auto earlier = co_await Peer::Begin(*storage_, 16);
    if (!earlier.ok()) co_return earlier.status();
    auto newer = co_await Peer::Begin(*storage_, 17);
    if (!newer.ok()) co_return newer.status();
    status = co_await Peer::Complete(*storage_, *newer);
    if (!status.ok()) co_return status;
    status =
        co_await RequireBlocked("new completion with an older active hold");
    if (!status.ok()) co_return status;
    status = co_await Peer::Cancel(*storage_, *earlier);
    if (!status.ok()) co_return status;
    status = co_await RequireRound("all overlapping holds released");
    if (!status.ok()) co_return status;

    status = co_await storage_->ConfigureTombRaider(
        {.action_ = Action::kInterval, .value_ = 60'000});
    if (!status.ok()) co_return status;
    auto hold14 = co_await Peer::Begin(*storage_, 14);
    if (!hold14.ok()) co_return hold14.status();
    if (!storage_->TombRaiderStats().enabled_)
      co_return absl::FailedPreconditionError("internal drain changed user ON");
    status = co_await Peer::Cancel(*storage_, *hold14);
    if (!status.ok()) co_return status;
    status = co_await storage_->ConfigureTombRaider({.action_ = Action::kOff});
    if (!status.ok()) co_return status;
    for (bool authority : {true, false, true, false}) {
      storage_->SetExpirationAuthority(authority);
      status = co_await RequireRound("expiration authority transition");
      if (!status.ok()) co_return status;
    }
    // Serving may be fenced while a recovered complete population waits for
    // its parent/Meta. That is independent of local cleanup eligibility.
    storage_->SetReplicaLoading(true);
    status =
        co_await RequireRound("complete population behind a serving fence");
    storage_->SetReplicaLoading(false);
    if (!status.ok()) co_return status;

#if LAVIK_FAULTS_ENABLED
    status = co_await ExerciseFlushRaces();
    if (!status.ok()) co_return status;
#endif
    // Storage completion precedes native all-flow cut installation. Only the
    // explicit final session completion may reopen physical maintenance.
    status = co_await storage_->BeginReplicaFullSync(20);
    if (!status.ok()) co_return status;
    status = co_await RequireBlocked("durable FULL invalidation");
    if (!status.ok()) co_return status;
    auto catalog = co_await storage_->CommitFunctionCatalog("test-catalog");
    if (!catalog.ok()) co_return catalog.status();
    status = co_await storage_->CompleteReplicaFullSync(
        20, {.generation_ = 20, .digest_ = 20});
    if (!status.ok()) co_return status;
    status = co_await RequireBlocked("FULL root before final cut");
    if (!status.ok()) co_return status;
    status = co_await storage_->FinalizeReplicaFullSync(20);
    if (!status.ok()) co_return status;
    status = co_await RequireRound("completed FULL final cut");
    if (!status.ok()) co_return status;

    status = co_await storage_->BeginReplicaFullSync(22);
    if (!status.ok()) co_return status;
    catalog = co_await storage_->CommitFunctionCatalog("old-finalizer-catalog");
    if (!catalog.ok()) co_return catalog.status();
    status = co_await storage_->CompleteReplicaFullSync(
        22, {.generation_ = 22, .digest_ = 22});
    if (!status.ok()) co_return status;
    // The public lifecycle method binds the current private generation when
    // called, before its task runs. Reusing a wire ID cannot retarget it.
    auto delayed_finalizer = storage_->FinalizeReplicaFullSync(22);
    status = co_await storage_->AbortReplicaRoot(22);
    if (!status.ok()) co_return status;
    status = co_await storage_->BeginReplicaFullSync(22);
    if (!status.ok()) co_return status;
    catalog = co_await storage_->CommitFunctionCatalog("new-finalizer-catalog");
    if (!catalog.ok()) co_return catalog.status();
    status = co_await storage_->CompleteReplicaFullSync(
        22, {.generation_ = 23, .digest_ = 23});
    if (!status.ok()) co_return status;
    status = co_await std::move(delayed_finalizer);
    if (!absl::IsFailedPrecondition(status))
      co_return absl::FailedPreconditionError(
          "delayed FULL finalizer accepted a reused session ID");
    status = co_await RequireBlocked("new session before its own final cut");
    if (!status.ok()) co_return status;
    status = co_await storage_->FinalizeReplicaFullSync(22);
    if (!status.ok()) co_return status;
    status =
        co_await RequireRound("reused FULL session after its own final cut");
    if (!status.ok()) co_return status;

    status = co_await storage_->BeginReplicaFullSync(21);
    if (!status.ok()) co_return status;
    status = co_await storage_->AbortReplicaRoot(21);
    if (!status.ok()) co_return status;
    co_return co_await RequireBlocked("cancelled destructive FULL");
  }

#if LAVIK_FAULTS_ENABLED
  enum class FlushKind {
    kDbSync,
    kDbAsync,
    kAllSync,
    kAllAsync,
    kReplayDb,
    kReplayAll,
    kPartitionReset
  };

  bycorf::Task<absl::Status> Flush(FlushKind kind) {
    constexpr std::uint8_t kDb = 3;
    if (kind == FlushKind::kPartitionReset) {
      // Redis Cluster FLUSH advances partition/index generations without
      // changing the global DB epoch. An epoch-only guard misses this case.
      const auto partition = static_cast<std::uint16_t>(
          lavik::storage::StorageShardForKey("{flush-raider}"));
      const auto before = storage_->DbEpoch(kDb);
      auto status =
          co_await storage_->ResetPartitionsDetach(std::span(&partition, 1));
      if (!status.ok()) co_return status;
      if (storage_->DbEpoch(kDb) != before)
        co_return absl::FailedPreconditionError(
            "partition reset changed DB epoch");
      co_return co_await storage_->FlushDbReclaim(true);
    }
    if (kind == FlushKind::kReplayDb) {
      co_return co_await storage_->ApplyReplicatedFlushDb(
          kDb, storage_->DbEpoch(kDb) + 1);
    }
    if (kind == FlushKind::kReplayAll) {
      std::array<std::uint64_t, lavik::storage::kLogicalDatabaseCount> epochs;
      for (std::size_t db = 0; db < epochs.size(); ++db)
        epochs[db] = storage_->DbEpoch(db) + 1;
      co_return co_await storage_->ApplyReplicatedFlushAll(epochs);
    }
    const bool all =
        kind == FlushKind::kAllSync || kind == FlushKind::kAllAsync;
    absl::Status status;
    if (all) {
      status = co_await storage_->FlushAllDetach();
    } else {
      status = co_await storage_->FlushDbDetach(kDb);
    }
    if (!status.ok()) co_return status;
    co_return co_await storage_->FlushDbReclaim(kind == FlushKind::kDbSync ||
                                                kind == FlushKind::kAllSync);
  }

  bycorf::Task<absl::Status> ExerciseFlushRaces() {
    using Point = Peer::Point;
    constexpr std::uint8_t kDb = 3;
    constexpr Point points[]{
        Point::kAfterMark,        Point::kAfterSweepRead,
        Point::kAfterClaimLookup, Point::kBeforeReapKeyLoad,
        Point::kAfterReapKeyLoad, Point::kBeforeReapLock};
    constexpr FlushKind kinds[]{
        FlushKind::kDbSync,        FlushKind::kDbAsync,  FlushKind::kAllSync,
        FlushKind::kAllAsync,      FlushKind::kReplayDb, FlushKind::kReplayAll,
        FlushKind::kPartitionReset};
    const std::string key = "{flush-raider}" + std::string(4096, 'k');
    const std::string survivor = key + "-survivor";
    for (Point point : points) {
      for (FlushKind kind : kinds) {
        auto status = co_await storage_->FlushAllDetach();
        if (!status.ok()) co_return status;
        status = co_await storage_->FlushDbReclaim(true);
        if (!status.ok()) co_return status;
        lavik::storage::SetOptions options;
        if (point != Point::kAfterClaimLookup) {
          options.expire_at_ms_ =
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch())
                  .count() +
              250;
        }
        auto wrote = co_await storage_->Set(kDb, key, "old-value", options);
        if (!wrote.ok()) co_return wrote.status();
        auto deleted = co_await storage_->Delete(kDb, key);
        if (!deleted.ok()) co_return deleted.status();
        if (!*deleted)
          co_return absl::FailedPreconditionError("fixture did not tombstone");
        status = co_await Peer::SealForSweep(*storage_);
        if (!status.ok()) co_return status;
        status = co_await bycorf::SleepFor(*worker_, 300ms);
        if (!status.ok()) co_return status;
        bool triggered = false;
        Peer::SetHook(
            *storage_, [&](Point actual) -> bycorf::Task<absl::Status> {
              if (actual != point || triggered) co_return absl::OkStatus();
              triggered = true;
              auto flushed = co_await Flush(kind);
              if (!flushed.ok()) co_return flushed;
              // Reuse the same long key in the new epoch, with a persistent
              // older value that makes this new tombstone necessary for
              // recovery.
              auto recreated = co_await storage_->Set(kDb, key, "new-value");
              if (!recreated.ok()) co_return recreated.status();
              auto removed = co_await storage_->Delete(kDb, key);
              if (!removed.ok()) co_return removed.status();
              auto live = co_await storage_->Set(kDb, survivor, "alive");
              if (!live.ok()) co_return live.status();
              co_return absl::OkStatus();
            });
        const auto before = storage_->TombRaiderStats();
        status = co_await Peer::Run(*storage_);
        Peer::SetHook(*storage_, {});
        if (!status.ok()) co_return status;
        if (!triggered)
          co_return absl::FailedPreconditionError(
              "FLUSH race did not reach point " +
              std::to_string(static_cast<unsigned>(point)));
        const auto after = storage_->TombRaiderStats();
        if (after.rounds_ != before.rounds_ || after.reaped_ != before.reaped_)
          co_return absl::FailedPreconditionError(
              "FLUSH invalidation completed/reaped the stale round");
        status = co_await RequireRound("fresh round after FLUSH cancellation");
        if (!status.ok()) co_return status;
        auto tombstone = co_await Peer::IsTombstone(*storage_, kDb, key);
        if (!tombstone.ok()) co_return tombstone.status();
        if (!*tombstone)
          co_return absl::FailedPreconditionError(
              "FLUSH race lost the new population's necessary tombstone");
        auto live = co_await storage_->Get(kDb, survivor);
        if (!live.ok()) co_return live.status();
      }
    }
    auto status = co_await storage_->FlushAllDetach();
    if (!status.ok()) co_return status;
    status = co_await storage_->FlushDbReclaim(true);
    if (!status.ok()) co_return status;
    const auto expiry = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count() +
                        250;
    auto wrote = co_await storage_->Set(
        kDb, key, "lock-wait",
        {.expire_at_ms_ = static_cast<std::uint64_t>(expiry)});
    if (!wrote.ok()) co_return wrote.status();
    auto removed = co_await storage_->Delete(kDb, key);
    if (!removed.ok()) co_return removed.status();
    status = co_await Peer::SealForSweep(*storage_);
    if (!status.ok()) co_return status;
    status = co_await bycorf::SleepFor(*worker_, 300ms);
    if (!status.ok()) co_return status;
    const auto before_wait = storage_->TombRaiderStats();
    status = co_await Peer::FlushWhileReapWaitsForKey(*storage_, kDb, key);
    if (!status.ok()) co_return status;
    if (storage_->TombRaiderStats().rounds_ != before_wait.rounds_ ||
        storage_->TombRaiderStats().reaped_ != before_wait.reaped_)
      co_return absl::FailedPreconditionError(
          "reap continued after FLUSH while waiting for its key lock");
    status = co_await RequireRound("fresh round after key-lock invalidation");
    if (!status.ok()) co_return status;

    Peer::SetHook(*storage_, [this](Point point) -> bycorf::Task<absl::Status> {
      if (point != Point::kAfterMark) co_return absl::OkStatus();
      auto flushed = co_await storage_->FlushDbDetach(kDb);
      if (!flushed.ok()) co_return flushed;
      // A concurrent generation change cannot mask an independent real
      // failure as normal round cancellation.
      co_return absl::DataLossError("injected real maintenance error");
    });
    auto failed = co_await Peer::Run(*storage_);
    Peer::SetHook(*storage_, {});
    if (!absl::IsDataLoss(failed))
      co_return absl::FailedPreconditionError(
          "maintenance error was swallowed as version cancellation");
    co_return absl::OkStatus();
  }
#endif

  lavik::storage::StorageEngine* storage_ = nullptr;
  bool recover_incomplete_ = false;
  std::optional<lavik::storage::TombRaiderTestPeer::RetirementScenario>
      retirement_scenario_;
  bycorf::Worker* worker_ = nullptr;
  absl::Status result_ = absl::UnknownError("controller service did not run");
};

void VerifyTombRaiderController(
    const std::string& path, bool recover_incomplete = false,
    std::optional<lavik::storage::TombRaiderTestPeer::RetirementScenario>
        retirement_scenario = std::nullopt) {
  lavik::storage::StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  options.expiration_authority_ = false;
  options.defrag_paused_ = true;
  options.tomb_raider_interval_ms_ = 1;
  // A normal round would remain asleep for a minute. Population drain must
  // exit at a bounded checkpoint while preserving the user's schedule.
  options.tomb_raider_sleep_ms_ = 60'000;
  lavik::storage::StorageEngine storage(std::move(options));
  lavik::InitWorkerMetrics(1);
  const absl::Status memory = lavik::InitMemoryLimit(512 * kMiB, 1);
  if (!memory.ok()) Fail(std::string(memory.message()));
  const absl::Status prepared = storage.Prepare(1);
  if (!prepared.ok()) Fail(std::string(prepared.message()));
  if (lavik::tx::TxRuntime::Get() == nullptr) lavik::tx::TxRuntime::Create(1);

  TombRaiderControllerService service(&storage, recover_incomplete,
                                      retirement_scenario);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  const absl::Status started = server.Start(runtime);
  if (!started.ok()) Fail(std::string(started.message()));
  server.WaitUntilStopped();
  if (!service.result().ok()) Fail(std::string(service.result().message()));
}

void VerifyRetirementRaces(const std::string& prefix) {
#if LAVIK_FAULTS_ENABLED
  using Scenario = lavik::storage::TombRaiderTestPeer::RetirementScenario;
  for (Scenario scenario : {Scenario::kBeforeSnapshot, Scenario::kAfterRead,
                            Scenario::kPinned, Scenario::kPrefixClaim,
                            Scenario::kRepeated, Scenario::kCancelledWait}) {
    const std::string path = prefix + ".retirement-" +
                             std::to_string(static_cast<unsigned>(scenario));
    struct Cleanup {
      const std::string& path_;
      ~Cleanup() { (void)::unlink(path_.c_str()); }
    } cleanup{path};
    CreateDataFile(path, 192 * kMiB);
    VerifyTombRaiderController(path, false, scenario);
  }
#endif
}

#if LAVIK_FAULTS_ENABLED
class TombRaiderMultiworkerService final : public bycorf::Service {
 public:
  explicit TombRaiderMultiworkerService(lavik::storage::StorageEngine& storage)
      : storage_(storage) {}
  void Prepare(unsigned workers) override {
    if (workers != 2)
      Fail("multiworker Tomb Raider fixture requires two workers");
  }
  void Stop() noexcept override {}
  void FinalizeWorker(bycorf::Worker& worker) noexcept override {
    storage_.FinalizeWorker(worker);
  }
  const absl::Status& result() const { return result_; }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    auto initialized = co_await storage_.InitializeWorker(worker);
    initialized_[worker.id()] = initialized;
    ready_.fetch_add(1, std::memory_order_release);
    if (worker.id() == 0) {
      while (ready_.load(std::memory_order_acquire) != 2)
        (co_await bycorf::SleepFor(worker, 1ms)).IgnoreError();
      result_ = initialized_[0].ok() ? initialized_[1] : initialized_[0];
      if (result_.ok()) result_ = co_await Exercise();
      finished_.store(true, std::memory_order_release);
    } else {
      while (!finished_.load(std::memory_order_acquire))
        (co_await bycorf::SleepFor(worker, 1ms)).IgnoreError();
    }
    worker.RequestStop();
    co_return absl::OkStatus();
  }

 private:
  using Peer = lavik::storage::TombRaiderTestPeer;
  using Point = Peer::Point;
  static constexpr std::uint8_t kDb = 3;

  bycorf::Task<absl::Status> Seed(bool expiring) {
    for (unsigned owner = 0; owner < 2; ++owner) {
      auto status = co_await bycorf::SubmitTaskTo(
          owner, [this, owner, expiring]() -> bycorf::Task<absl::Status> {
            lavik::storage::SetOptions options;
            if (expiring) {
              options.expire_at_ms_ =
                  std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count() +
                  250;
            }
            auto wrote =
                co_await storage_.Set(kDb, keys_[owner], "old", options);
            if (!wrote.ok()) co_return wrote.status();
            auto deleted = co_await storage_.Delete(kDb, keys_[owner]);
            if (!deleted.ok()) co_return deleted.status();
            co_return co_await Peer::SealForSweep(storage_);
          });
      if (!status.ok()) co_return status;
    }
    co_return co_await bycorf::SleepFor(*bycorf::ThisWorker().self_, 300ms);
  }

  bycorf::Task<absl::Status> Exercise() {
    for (unsigned owner = 0; owner < 2; ++owner) {
      for (unsigned suffix = 0;; ++suffix) {
        std::string key = "{raider-worker-" + std::to_string(suffix) + "}" +
                          std::string(4096, 'k');
        if (lavik::storage::StorageShardForKey(key) % 2 == owner) {
          keys_[owner] = std::move(key);
          break;
        }
      }
    }
    auto status = co_await storage_.CompleteStorageStartup();
    if (!status.ok()) co_return status;
    for (Point point : {Point::kAfterClaimLookup, Point::kBeforeReapKeyLoad}) {
      status = co_await storage_.FlushAllDetach();
      if (!status.ok()) co_return status;
      status = co_await storage_.FlushDbReclaim(true);
      if (!status.ok()) co_return status;
      status = co_await Seed(point != Point::kAfterClaimLookup);
      if (!status.ok()) co_return status;
      std::atomic<bool> triggered{false};
      Peer::SetHook(storage_, [&](Point actual) -> bycorf::Task<absl::Status> {
        if (actual != point || bycorf::ThisWorker().id_ != 1 ||
            triggered.exchange(true))
          co_return absl::OkStatus();
        // The coordinator is waiting for worker 1's claim/reap callback.
        // FLUSH hops back to worker 0, then detaches on both workers.
        auto flushed = co_await storage_.FlushDbDetach(kDb);
        if (!flushed.ok()) co_return flushed;
        flushed = co_await storage_.FlushDbReclaim(true);
        if (!flushed.ok()) co_return flushed;
        co_return co_await Seed(false);
      });
      const auto before = storage_.TombRaiderStats();
      status = co_await Peer::Run(storage_);
      Peer::SetHook(storage_, {});
      if (!status.ok()) co_return status;
      if (!triggered || storage_.TombRaiderStats().rounds_ != before.rounds_)
        co_return absl::FailedPreconditionError(
            "cross-worker FLUSH did not invalidate the whole round");
      if (point == Point::kBeforeReapKeyLoad &&
          storage_.TombRaiderStats().reaped_ != before.reaped_ + 1)
        co_return absl::FailedPreconditionError(
            "cancelled round lost worker zero's completed retirement count");
      status = co_await Peer::Run(storage_);
      if (!status.ok()) co_return status;
      if (storage_.TombRaiderStats().rounds_ != before.rounds_ + 1)
        co_return absl::FailedPreconditionError(
            "fresh cross-worker round failed");
      for (unsigned owner = 0; owner < 2; ++owner) {
        auto tombstone = co_await bycorf::SubmitTaskTo(owner, [this, owner] {
          return Peer::IsTombstone(storage_, kDb, keys_[owner]);
        });
        if (!tombstone.ok()) co_return tombstone.status();
        if (!*tombstone)
          co_return absl::FailedPreconditionError(
              "cross-worker FLUSH lost a necessary new tombstone");
      }
    }
    co_return absl::OkStatus();
  }

  lavik::storage::StorageEngine& storage_;
  std::array<std::string, 2> keys_;
  std::array<absl::Status, 2> initialized_;
  std::atomic<unsigned> ready_{0};
  std::atomic<bool> finished_{false};
  absl::Status result_ = absl::UnknownError("multiworker fixture did not run");
};

void VerifyMultiworkerTombRaider(const std::string& path) {
  lavik::storage::StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  options.expiration_authority_ = false;
  options.defrag_paused_ = true;
  options.tomb_raider_interval_ms_ = 0;
  options.tomb_raider_sleep_ms_ = 0;
  lavik::storage::StorageEngine storage(std::move(options));
  lavik::InitWorkerMetrics(2);
  auto status = lavik::InitMemoryLimit(1024 * kMiB, 2);
  if (!status.ok()) Fail(std::string(status.message()));
  status = storage.Prepare(2);
  if (!status.ok()) Fail(std::string(status.message()));
  lavik::tx::TxRuntime::Create(2);
  TombRaiderMultiworkerService service(storage);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 2;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  status = server.Start(runtime);
  if (!status.ok()) Fail(std::string(status.message()));
  server.WaitUntilStopped();
  if (!service.result().ok()) Fail(std::string(service.result().message()));
}
#endif

RespClient Connect(std::uint16_t port) {
  const auto deadline = std::chrono::steady_clock::now() + 20s;
  while (std::chrono::steady_clock::now() < deadline) {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) Fail("client socket failed");
    timeval timeout{.tv_sec = 30, .tv_usec = 0};
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                  sizeof(address)) == 0) {
      try {
        RespClient client(fd);
        if (client.Command({"PING"}) == "+PONG") return client;
      } catch (const std::exception&) {
        // The listener may be bound before storage recovery has installed the
        // worker services. That connection is reset during initialization;
        // reconnect until the command path itself is ready.
      }
      std::this_thread::sleep_for(10ms);
      continue;
    }
    ::close(fd);
    std::this_thread::sleep_for(10ms);
  }
  Fail("timed out connecting to Lavik");
}

class ServerProcess {
 public:
  ServerProcess(const std::string& binary, std::uint16_t port,
                const std::string& data_path, const std::string& log_path) {
    pid_ = ::fork();
    if (pid_ < 0) Fail("fork failed");
    if (pid_ == 0) {
      const int log_fd = ::open(
          log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
      if (log_fd >= 0) {
        (void)::dup2(log_fd, STDOUT_FILENO);
        (void)::dup2(log_fd, STDERR_FILENO);
        ::close(log_fd);
      }
      std::vector<std::string> arguments{
          binary,
          "--logtostderr",
          "--port",
          std::to_string(port),
          "--threads",
          "1",
          "--recv-buffers-per-worker",
          "0",
          "--flush-max-ms",
          "20",
          "--data-file",
          data_path,
          "--tomb-raider-interval-ms",
          "500",
      };
      std::vector<char*> child_argv;
      for (std::string& argument : arguments) {
        child_argv.push_back(argument.data());
      }
      child_argv.push_back(nullptr);
      ::execv(binary.c_str(), child_argv.data());
      _exit(127);
    }
  }

  ServerProcess(const ServerProcess&) = delete;
  ServerProcess& operator=(const ServerProcess&) = delete;
  ~ServerProcess() {
    if (pid_ > 0) {
      (void)::kill(pid_, SIGKILL);
      (void)::waitpid(pid_, nullptr, 0);
    }
  }

  void Stop() {
    if (pid_ <= 0) return;
    if (::kill(pid_, SIGINT) != 0 && errno != ESRCH) Fail("signal failed");
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      const pid_t result = ::waitpid(pid_, &status, WNOHANG);
      if (result == pid_) {
        pid_ = -1;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
          Fail("Lavik exited unsuccessfully");
        }
        return;
      }
      if (result < 0) Fail("waitpid failed");
      std::this_thread::sleep_for(10ms);
    }
    Fail("Lavik did not stop");
  }

 private:
  pid_t pid_ = -1;
};

void Expect(std::string_view actual, std::string_view expected,
            std::string_view operation) {
  if (actual != expected) {
    Fail(std::string(operation) + " returned '" + std::string(actual) +
         "', expected '" + std::string(expected) + "'");
  }
}

long long IntegerReply(std::string_view reply, std::string_view operation) {
  if (!reply.starts_with(':'))
    Fail(std::string(operation) + " was not integer");
  long long value = 0;
  const char* begin = reply.data() + 1;
  const char* end = reply.data() + reply.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, value);
  if (error != std::errc{} || parsed_end != end) {
    Fail(std::string(operation) + " was malformed");
  }
  return value;
}

void ExpectRange(long long actual, long long minimum, long long maximum,
                 std::string_view operation) {
  if (actual < minimum || actual > maximum) {
    Fail(std::string(operation) + " returned " + std::to_string(actual));
  }
}

std::string ReadFile(const std::string& path) {
  std::ifstream input(path);
  return std::string(std::istreambuf_iterator<char>(input),
                     std::istreambuf_iterator<char>());
}

long long InfoField(RespClient& client, std::string_view section,
                    std::string_view field) {
  const std::string info = client.Command({"INFO", section});
  const std::string needle = std::string(field) + ":";
  const std::size_t at = info.find(needle);
  if (at == std::string::npos) Fail("INFO missing " + std::string(field));
  long long value = 0;
  const char* begin = info.data() + at + needle.size();
  const auto [parsed_end, error] =
      std::from_chars(begin, info.data() + info.size(), value);
  if (error != std::errc{} || parsed_end == begin) {
    Fail("INFO field malformed: " + std::string(field));
  }
  return value;
}

long long StatField(RespClient& client, std::string_view field) {
  return InfoField(client, "stats", field);
}

void VerifyTtlReapingReducesMemory(RespClient& client) {
  // Fill the index with exactly 100 MiB of inline key bytes, independently
  // of value sizes, so reclamation must release both buckets and entry spans.
  const std::size_t key_bytes = 1024;
  const std::size_t expiring_keys = 100 * kMiB / key_bytes;
  const long long minimum_memory_growth = 100 * kMiB;
  const auto ttl = 15s;
  const auto reap_timeout = 300s;
  constexpr long long kRemainingAllowance = 8 * 1024;
  Expect(client.Command({"TOMBRAIDER", "OFF"}), "+OK",
         "memory test raider OFF");
  const auto stopped_deadline = std::chrono::steady_clock::now() + 10s;
  while (StatField(client, "tomb_raider_running") != 0) {
    if (std::chrono::steady_clock::now() > stopped_deadline) {
      Fail("memory test could not drain the previous tomb raider round");
    }
    std::this_thread::sleep_for(10ms);
  }
  Expect(client.Command({"DEFRAG", "PAUSE"}), "+OK",
         "memory test defrag PAUSE");
  const long long reaped_before = StatField(client, "tomb_raider_reaped");
  auto key_for = [key_bytes](std::size_t index) {
    constexpr std::string_view prefix = "{ttl-memory}:";
    const std::string suffix = std::to_string(index);
    return std::string(prefix) +
           std::string(key_bytes - prefix.size() - suffix.size(), '0') + suffix;
  };
  const std::string survivor = key_for(expiring_keys);
  Expect(client.Command({"SET", survivor, "alive"}), "+OK",
         "memory survivor SET");
  // INFO's retained-memory gauge is published by the 100 ms health loop.
  // Warm its first arena span before measuring growth; the survivor keeps a
  // span allocated after reaping. Defrag is paused so disk-block reclamation
  // cannot supply an unrelated memory drop. RSS is deliberately not an
  // assertion: mimalloc may retain freed pages even when Lavik releases
  // ownership.
  std::this_thread::sleep_for(300ms);
  const long long warm_memory = InfoField(client, "memory", "used_memory");
  const std::string ttl_ms = std::to_string(
      std::chrono::duration_cast<std::chrono::milliseconds>(ttl).count());
  std::cout << "TTL memory fixture: keys=" << expiring_keys
            << " key-bytes=" << key_bytes
            << " total-key-bytes=" << expiring_keys * key_bytes
            << " warm=" << warm_memory << std::endl;
  for (std::size_t i = 0; i < expiring_keys; ++i) {
    const std::string key = key_for(i);
    Expect(client.Command({"SET", key, "v", "PX", ttl_ms}), "+OK",
           "memory TTL SET");
    if ((i + 1) % 16384 == 0) {
      std::cout << "TTL memory fixture written=" << i + 1 << std::endl;
    }
  }
  // The same hash tag concentrates the bucket growth in one index.
  // Requiring it to return near its warm footprint catches a missing shrink
  // even if Erase still frees entries and individual overflow buckets.
  const auto grown_deadline = std::chrono::steady_clock::now() + 5s;
  long long populated_memory = 0;
  do {
    populated_memory = InfoField(client, "memory", "used_memory");
    if (populated_memory >= warm_memory + minimum_memory_growth) break;
    if (std::chrono::steady_clock::now() > grown_deadline) {
      Fail("TTL fixture did not grow retained memory: warm=" +
           std::to_string(warm_memory) +
           " populated=" + std::to_string(populated_memory));
    }
    std::this_thread::sleep_for(50ms);
  } while (true);
  std::cout << "TTL memory fixture populated=" << populated_memory << std::endl;

  const std::string last_key = key_for(expiring_keys - 1);
  const auto expired_deadline = std::chrono::steady_clock::now() + ttl + 5s;
  while (client.Command({"GET", last_key}) != "$-1") {
    if (std::chrono::steady_clock::now() > expired_deadline) {
      Fail("memory fixture TTL did not expire");
    }
    std::this_thread::sleep_for(50ms);
  }
  // Reads enqueue lazy-expiration candidates; active expiration writes their
  // tombstones. No explicit DEL or FLUSH may stand in for that lifecycle.
  for (std::size_t i = 0; i < expiring_keys; ++i) {
    const std::string key = key_for(i);
    Expect(client.Command({"GET", key}), "$-1", "memory expired GET");
  }
  if (StatField(client, "tomb_raider_reaped") != reaped_before) {
    Fail("memory fixture was reaped before tomb raider was enabled");
  }
  Expect(client.Command({"TOMBRAIDER", "BLOCK-SLEEP", "0"}), "+OK",
         "memory test raider pacing");
  Expect(client.Command({"TOMBRAIDER", "INTERVAL", "10"}), "+OK",
         "memory test raider interval");
  const auto reaped_deadline = std::chrono::steady_clock::now() + reap_timeout;
  auto next_progress = std::chrono::steady_clock::now();
  while (true) {
    const long long reaped =
        StatField(client, "tomb_raider_reaped") - reaped_before;
    if (reaped >= static_cast<long long>(expiring_keys)) break;
    if (std::chrono::steady_clock::now() > reaped_deadline) {
      Fail("memory fixture TTL tombstones were not all reaped: " +
           std::to_string(reaped) + "/" + std::to_string(expiring_keys));
    }
    if (std::chrono::steady_clock::now() >= next_progress) {
      std::cout << "TTL memory fixture reaped=" << reaped << "/"
                << expiring_keys
                << " used_memory=" << InfoField(client, "memory", "used_memory")
                << std::endl;
      next_progress = std::chrono::steady_clock::now() + 10s;
    }
    std::this_thread::sleep_for(50ms);
  }
  // Poll only administrative counters after reaping: no key lookup or write
  // should be needed to finish the final incremental shrink in the background.
  const auto reclaimed_deadline = std::chrono::steady_clock::now() + 30s;
  long long reclaimed_memory = 0;
  do {
    reclaimed_memory = InfoField(client, "memory", "used_memory");
    if (reclaimed_memory <= warm_memory + kRemainingAllowance &&
        reclaimed_memory <= populated_memory - minimum_memory_growth) {
      break;
    }
    if (std::chrono::steady_clock::now() > reclaimed_deadline) {
      Fail("TTL reaping did not reclaim index memory: warm=" +
           std::to_string(warm_memory) +
           " populated=" + std::to_string(populated_memory) +
           " reclaimed=" + std::to_string(reclaimed_memory));
    }
    std::this_thread::sleep_for(50ms);
  } while (true);
  Expect(client.Command({"GET", survivor}), "$5\r\nalive",
         "memory survivor GET");
  std::cout << "TTL tomb raider memory: warm=" << warm_memory
            << " populated=" << populated_memory
            << " reclaimed=" << reclaimed_memory << " bytes\n";
  Expect(client.Command({"DEFRAG", "RESUME"}), "+OK",
         "memory test defrag RESUME");
}

// Waits until tomb_raider_rounds advances past `floor`, so an assertion
// about reap totals is made only after a full round observed the state the
// test just arranged.
long long AwaitRoundBeyond(RespClient& client, long long floor) {
  const auto deadline = std::chrono::steady_clock::now() + 30s;
  while (std::chrono::steady_clock::now() < deadline) {
    const long long rounds = StatField(client, "tomb_raider_rounds");
    if (rounds > floor) return rounds;
    std::this_thread::sleep_for(50ms);
  }
  Fail("tomb raider round did not complete in time");
}

std::string LocalTimeAfter(std::chrono::seconds offset) {
  const std::time_t target = std::time(nullptr) + offset.count();
  std::tm local{};
  if (::localtime_r(&target, &local) == nullptr) {
    Fail("localtime_r failed");
  }
  char buffer[9]{};
  if (std::snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d", local.tm_hour,
                    local.tm_min, local.tm_sec) != 8) {
    Fail("daily time formatting failed");
  }
  return buffer;
}

}  // namespace

int main(int argc, char** argv) {
  const auto usage = [] {
    std::cerr << "usage: tomb_raider_e2e_test /path/to/lavik "
                 "[--controller-only | --multiworker-only]\n";
#if !LAVIK_FAULTS_ENABLED
    std::cerr << "--multiworker-only requires test fault support\n";
#endif
    return 2;
  };
  if (argc != 2 && argc != 3) {
    return usage();
  }
  const std::string_view mode = argc == 3 ? argv[2] : "";
  if (argc == 3 && mode != "--controller-only" && mode != "--multiworker-only")
    return usage();
#if !LAVIK_FAULTS_ENABLED
  if (mode == "--multiworker-only") return usage();
#endif
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-tombraider-" + std::to_string(::getpid()));
  const std::string data_path = prefix + ".data";
  const std::string quiesce_path = prefix + ".quiesce.data";
  const std::string log_path = prefix + ".log";
  (void)::unlink(data_path.c_str());
  (void)::unlink(quiesce_path.c_str());
  (void)::unlink(log_path.c_str());

  try {
#if LAVIK_FAULTS_ENABLED
    if (mode == "--multiworker-only") {
      CreateDataFile(quiesce_path, 256 * kMiB);
      VerifyMultiworkerTombRaider(quiesce_path);
      (void)::unlink(quiesce_path.c_str());
      std::cout << "tomb raider multiworker passed\n";
      return 0;
    }
#endif
    if (mode == "--controller-only") {
      CreateDataFile(quiesce_path, kControllerDataBytes);
      VerifyTombRaiderController(quiesce_path);
      VerifyTombRaiderController(quiesce_path, true);
      VerifyRetirementRaces(quiesce_path);
      (void)::unlink(quiesce_path.c_str());
      std::cout << "tomb raider controller passed\n";
      return 0;
    }
    const std::uint16_t port = FindFreePort();
    // The fixture needs room for both 100 MiB key populations: values
    // and their later tombstones, while defrag is deliberately paused.
    CreateDataFile(data_path, 1024 * kMiB);
    CreateDataFile(quiesce_path, kControllerDataBytes);
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"PING"}), "+PONG", "PING");
      Expect(client.Command(
                 {"CONFIG", "SET", "defrag-max-active-per-device", "1"}),
             "+OK", "CONFIG defrag max active");
      Expect(client.Command({"CONFIG", "SET", "defrag-sleep-ms", "25"}), "+OK",
             "CONFIG defrag block sleep");
      Expect(client.Command({"CONFIG", "SET", "defrag-record-sleep-us", "7"}),
             "+OK", "CONFIG defrag record sleep");
      Expect(client.Command({"CONFIG", "SET", "defrag-paused", "yes"}), "+OK",
             "CONFIG defrag paused");
      const std::string defrag_status = client.Command({"DEFRAG", "STATUS"});
      if (defrag_status.find("paused=1") == std::string::npos ||
          defrag_status.find("max_active_per_device=1") == std::string::npos ||
          defrag_status.find("block_sleep_ms=25") == std::string::npos ||
          defrag_status.find("record_sleep_us=7") == std::string::npos) {
        Fail("DEFRAG STATUS did not report runtime settings");
      }
      if (StatField(client, "defrag_max_active_per_device") != 1 ||
          StatField(client, "defrag_paused") != 1 ||
          StatField(client, "defrag_block_sleep_ms") != 25 ||
          StatField(client, "defrag_record_sleep_us") != 7) {
        Fail("INFO stats did not report defrag runtime settings");
      }
      Expect(client.Command({"DEFRAG", "RESUME"}), "+OK", "DEFRAG RESUME");
      Expect(client.Command({"DEFRAG", "MAX-ACTIVE", "0"}),
             "-ERR value is not an integer or out of range",
             "DEFRAG zero concurrency");
      Expect(client.Command({"DEFRAG", "INVALID", "1"}), "-ERR syntax error",
             "DEFRAG invalid setting");
      Expect(client.Command({"DEFRAG", "BLOCK-SLEEP-MS", "0"}), "+OK",
             "DEFRAG reset block sleep");
      Expect(client.Command({"DEFRAG", "RECORD-SLEEP-US", "0"}), "+OK",
             "DEFRAG reset record sleep");
      Expect(client.Command({"CONFIG", "SET", "tomb-raider-mode", "off"}),
             "+OK", "CONFIG tomb raider off");
      const long long disabled_rounds = StatField(client, "tomb_raider_rounds");
      std::this_thread::sleep_for(700ms);
      if (StatField(client, "tomb_raider_rounds") != disabled_rounds) {
        Fail("tomb raider ran while disabled");
      }
      if (StatField(client, "tomb_raider_enabled") != 0) {
        Fail("tomb raider did not report disabled");
      }
      if (client.Command({"TOMBRAIDER", "STATUS"}).find("mode=off") ==
          std::string::npos) {
        Fail("TOMBRAIDER STATUS did not report off mode");
      }
      if (StatField(client, "tomb_raider_eligible") != 0 ||
          client.Command({"TOMBRAIDER", "STATUS"})
                  .find("blocked_reason=user_off") == std::string::npos) {
        Fail("TOMBRAIDER did not distinguish user OFF from population gating");
      }
      Expect(client.Command({"TOMBRAIDER", "INVALID"}), "-ERR syntax error",
             "TOMBRAIDER invalid mode");
      Expect(client.Command({"TOMBRAIDER", "INTERVAL", "0"}),
             "-ERR value is not an integer or out of range",
             "TOMBRAIDER zero interval");
      Expect(client.Command({"CONFIG", "SET", "tomb-raider-sleep-ms", "0"}),
             "+OK", "CONFIG tomb raider block sleep");
      if (StatField(client, "tomb_raider_block_sleep_ms") != 0) {
        Fail("tomb raider did not update block sleep");
      }

      const std::string daily = LocalTimeAfter(2s);
      Expect(client.Command({"CONFIG", "SET", "tomb-raider-daily-time", daily}),
             "+OK", "CONFIG tomb raider daily");
      if (client.Command({"TOMBRAIDER", "STATUS"}).find("mode=daily") ==
          std::string::npos) {
        Fail("TOMBRAIDER STATUS did not report daily mode");
      }
      const long long daily_rounds = AwaitRoundBeyond(client, disabled_rounds);
      std::this_thread::sleep_for(1200ms);
      if (StatField(client, "tomb_raider_rounds") != daily_rounds) {
        Fail("daily tomb raider ran more than once");
      }
      Expect(client.Command({"TOMBRAIDER", "OFF"}), "+OK",
             "TOMBRAIDER daily OFF");
      Expect(client.Command({"TOMBRAIDER", "ON"}), "+OK", "TOMBRAIDER ON");
      if (client.Command({"TOMBRAIDER", "STATUS"}).find("mode=daily") ==
          std::string::npos) {
        Fail("TOMBRAIDER ON did not restore daily mode");
      }
      Expect(
          client.Command({"CONFIG", "SET", "tomb-raider-interval-ms", "500"}),
          "+OK", "CONFIG tomb raider interval");
      if (StatField(client, "tomb_raider_enabled") != 1) {
        Fail("tomb raider did not report enabled");
      }

      // Reapable: the only older record expires on its own, after which
      // nothing on disk needs the tombstone. Leave room for CI scheduling
      // delays between SET and DEL so expiration does not win the deletion.
      constexpr auto reapable_ttl = 5000ms;
      const std::string reapable_ttl_ms = std::to_string(reapable_ttl.count());
      Expect(client.Command({"SET", "reapable", "v", "PX", reapable_ttl_ms}),
             "+OK", "reapable SET");
      Expect(client.Command({"DEL", "reapable"}), ":1", "reapable DEL");
      // Not reapable: the buried value never expires, so the tombstone is
      // the only thing standing between it and resurrection.
      Expect(client.Command({"SET", "kept", "v"}), "+OK", "kept SET");
      Expect(client.Command({"DEL", "kept"}), ":1", "kept DEL");
      // Untouched live key, as a control across the restart below.
      Expect(client.Command({"SET", "control", "c"}), "+OK", "control SET");

      // Let the buried TTL lapse, then require a round that started after
      // that: its sweep must see the value as expired and reap exactly the
      // one tombstone.
      std::this_thread::sleep_for(reapable_ttl + 100ms);
      long long rounds = AwaitRoundBeyond(client, 0);
      rounds = AwaitRoundBeyond(client, rounds);
      const auto deadline = std::chrono::steady_clock::now() + 30s;
      while (StatField(client, "tomb_raider_reaped") < 1) {
        if (std::chrono::steady_clock::now() > deadline) {
          Fail("reapable tombstone was never reaped");
        }
        std::this_thread::sleep_for(50ms);
      }

      // Two more full rounds: the reaped total must stay at exactly one —
      // the permanent value keeps claiming its tombstone every sweep.
      rounds = AwaitRoundBeyond(client, rounds);
      (void)AwaitRoundBeyond(client, rounds);
      const long long reaped = StatField(client, "tomb_raider_reaped");
      if (reaped != 1) {
        Fail("expected exactly one reap, saw " + std::to_string(reaped));
      }

      Expect(client.Command({"GET", "reapable"}), "$-1", "reapable GET");
      Expect(client.Command({"GET", "kept"}), "$-1", "kept GET");
      Expect(client.Command({"GET", "control"}), "$1\r\nc", "control GET");
      VerifyTtlReapingReducesMemory(client);
      server.Stop();
    }

    // The kept tombstone must have survived to suppress the permanent
    // value across recovery; the reaped one must stay gone without it.
    {
      ServerProcess server(argv[1], port, data_path, log_path);
      RespClient client = Connect(port);
      Expect(client.Command({"GET", "kept"}), "$-1", "restart kept GET");
      Expect(client.Command({"GET", "reapable"}), "$-1",
             "restart reapable GET");
      Expect(client.Command({"GET", "control"}), "$1\r\nc",
             "restart control GET");
      server.Stop();
    }
    VerifyTombRaiderController(quiesce_path);
    VerifyTombRaiderController(quiesce_path, true);
    VerifyRetirementRaces(quiesce_path);
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    std::cerr << "--- Lavik log ---\n" << ReadFile(log_path);
    (void)::unlink(data_path.c_str());
    (void)::unlink(quiesce_path.c_str());
    (void)::unlink(log_path.c_str());
    return 1;
  }
  (void)::unlink(data_path.c_str());
  (void)::unlink(quiesce_path.c_str());
  (void)::unlink(log_path.c_str());
  std::cout << "tomb raider e2e passed\n";
  return 0;
}
