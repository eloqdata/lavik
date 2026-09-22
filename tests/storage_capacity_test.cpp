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
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
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

class ExpirationAuthorityTestPeer {
 public:
  using WorkerCache = StorageEngine::Impl::WorkerStore;

  static std::shared_ptr<const void> CachedGrant(const StorageEngine& storage,
                                                 WorkerCache& cache) {
    return storage.impl_->CurrentExpirationAuthority(cache);
  }

  static std::uint64_t PublicationVersion(const StorageEngine& storage) {
    return storage.impl_->expiration_authority_version_.load(
        std::memory_order_acquire);
  }

  static std::shared_ptr<const void> CurrentGrant(
      const StorageEngine& storage) {
    return std::static_pointer_cast<const void>(
        storage.impl_->active_expiration_authority_.load(
            std::memory_order_acquire));
  }

  static absl::Status ValidateGrant(const std::shared_ptr<const void>& grant) {
    return StorageEngine::Impl::ValidateExpirationAuthority(grant.get());
  }

  static bool IsCancellation(const absl::Status& status) {
    return StorageEngine::Impl::IsExpirationAuthorityCancellation(status);
  }

  static absl::Status RevokedGrantStatus() {
    StorageEngine::Impl::ExpirationAuthorityGrant grant(
        std::chrono::nanoseconds::max());
    grant.active_.store(false, std::memory_order_release);
    return StorageEngine::Impl::ValidateExpirationAuthority(&grant);
  }

  static absl::Status VerifyStaleQueueBudget(StorageEngine& storage) {
    auto* impl = storage.impl_.get();
    auto& store = impl->CurrentStore();
    if (!store.expired_candidates_.empty()) {
      return absl::FailedPreconditionError(
          "stale queue test did not start with an empty queue");
    }
    auto stale = impl->CurrentExpirationAuthority(store);
    if (stale == nullptr) {
      return absl::FailedPreconditionError(
          "stale queue test has no initial authority");
    }
    absl::Status replaced = storage.SetExpirationAuthorityUntil(
        std::chrono::nanoseconds::max() - std::chrono::nanoseconds(1));
    if (!replaced.ok()) return replaced;
    auto current = impl->CurrentExpirationAuthority(store);
    if (current == nullptr || current == stale) {
      return absl::FailedPreconditionError(
          "stale queue test did not replace its exact authority");
    }
    constexpr std::size_t kStaleCount = 2;
    for (std::size_t index = 0; index < kStaleCount; ++index) {
      StorageEngine::Impl::WorkerStore::ExpireCandidate candidate;
      candidate.expiration_authority_ = stale;
      store.expired_candidates_.push_back(std::move(candidate));
    }
    StorageEngine::Impl::WorkerStore::ExpireCandidate candidate;
    candidate.expiration_authority_ = current;
    store.expired_candidates_.push_back(std::move(candidate));

    if (impl->DiscardStaleExpirationCandidates(store, 0) != 0 ||
        store.expired_candidates_.size() != kStaleCount + 1) {
      store.expired_candidates_.clear();
      return absl::FailedPreconditionError(
          "zero remaining budget consumed a stale expiration candidate");
    }
    const std::size_t first = impl->DiscardStaleExpirationCandidates(store, 1);
    const bool retained_stale =
        store.expired_candidates_.size() == kStaleCount &&
        store.expired_candidates_.front().expiration_authority_ == stale;
    const std::size_t second = impl->DiscardStaleExpirationCandidates(store, 1);
    const bool preserved_current =
        store.expired_candidates_.size() == 1 &&
        store.expired_candidates_.front().expiration_authority_ == current;
    store.expired_candidates_.clear();
    if (first != 1 || !retained_stale || second != 1 || !preserved_current) {
      return absl::FailedPreconditionError(
          "stale exact grants did not consume the candidate budget");
    }
    return absl::OkStatus();
  }

#if LAVIK_FAULTS_ENABLED
  using Point = StorageEngine::Impl::ExpirationTestPoint;
  using Hook = StorageEngine::Impl::ExpirationTestHook;

  static void SetHook(StorageEngine& storage, Hook hook) {
    storage.impl_->expiration_test_hook_ = std::move(hook);
  }

  static bycorf::Task<absl::Status> ResumeAndExpireFront(
      StorageEngine& storage) {
    auto& store = storage.impl_->CurrentStore();
    if (store.expired_candidates_.empty()) {
      co_return absl::NotFoundError("no queued expiration candidate");
    }
    auto candidate = std::move(store.expired_candidates_.front());
    store.expired_candidates_.pop_front();
    storage.impl_->ResumeExpiration();
    co_return co_await storage.impl_->ExpireCandidate(store,
                                                      std::move(candidate));
  }

  static bycorf::Task<absl::Status> ExpireWhileKeyLocked(
      StorageEngine& storage, std::function<absl::Status()> transition) {
    auto* impl = storage.impl_.get();
    auto& store = impl->CurrentStore();
    if (store.expired_candidates_.empty())
      co_return absl::NotFoundError("no queued expiration candidate");
    auto candidate = std::move(store.expired_candidates_.front());
    store.expired_candidates_.pop_front();
    auto hold = co_await tx::CurrentTxShard().AcquireKey(
        candidate.db_id_, tx::FingerprintOf(candidate.digest_),
        tx::LockMode::kExclusive);
    auto task = impl->ExpireCandidate(store, std::move(candidate));
    auto handle = std::move(task).ReleaseHandle();
    task = bycorf::Task<absl::Status>(handle);
    bycorf::AsyncNotification completed;
    task.SetCompletionCallback(&completed, [](void* context, auto) noexcept {
      static_cast<bycorf::AsyncNotification*>(context)->NotifyAll(
          *bycorf::ThisWorker().self_);
    });
    // Start the real delete while holding its key. It must pass the early
    // authority check and suspend at AcquireKey before the transition runs.
    impl->ResumeExpiration();
    handle.resume();
    const bool suspended = !task.done();
    auto paused = co_await impl->QuiesceExpiration();
    auto changed = suspended
                       ? transition()
                       : absl::FailedPreconditionError(
                             "expiration did not suspend behind the held key");
    // Refresh the worker cache while the pending mutation retains the old
    // local control block. This must neither dangle nor reauthorize that work.
    (void)impl->CurrentExpirationAuthority(store);
    hold.Reset();
    while (!task.done()) co_await completed.Wait();
    if (!paused.ok()) co_return paused;
    if (!changed.ok()) co_return changed;
    co_return std::move(task).TakeResult();
  }
#endif
};

// Hold real generation leases while exercising the bounded staging pool. The
// peer advances only the generation number: no cleaner may retire these live
// transactions and accidentally make capacity available to the writer.
class WriteBufferPressureTestPeer {
 public:
  static bycorf::Task<absl::Status> ExerciseRotation(StorageEngine& storage) {
    auto& impl = *storage.impl_;
    TxShardWrites first;
    storage.InitializeTxWrites(StorageEngine::AllocateWriteTxid(),
                               std::span(&first, 1));
    auto written = co_await storage.SetLocked(0, "rotation-first",
                                              ComputeDigest("rotation-first"),
                                              "first", {}, &first);
    if (!written.ok()) co_return written.status();
    auto cleaned = co_await impl.RunTxCleaner();
    if (!cleaned.ok()) co_return cleaned;
    const auto next = impl.current_tx_generation_.load();
    if (next != first.generation_ + 1)
      co_return absl::FailedPreconditionError("first generation did not close");

    TxShardWrites second;
    storage.InitializeTxWrites(StorageEngine::AllocateWriteTxid(),
                               std::span(&second, 1));
    written = co_await storage.SetLocked(0, "rotation-second",
                                         ComputeDigest("rotation-second"),
                                         "second", {}, &second);
    if (!written.ok()) co_return written.status();
    for (unsigned round = 0; round < 8; ++round) {
      cleaned = co_await impl.RunTxCleaner();
      if (!cleaned.ok()) co_return cleaned;
      if (impl.current_tx_generation_.load() != next)
        co_return absl::FailedPreconditionError(
            "cleaner accumulated closed generations behind a live lease");
    }
    std::vector<TxShardWrites*> first_shards{&first};
    auto committed =
        co_await storage.CommitTxWrites(first.txid_, std::move(first_shards));
    if (!committed.ok()) co_return committed;
    first = {};
    cleaned = co_await impl.RunTxCleaner();
    if (!cleaned.ok()) co_return cleaned;
    if (impl.current_tx_generation_.load() != next + 1)
      co_return absl::FailedPreconditionError(
          "lease release did not reenable rotation");
    std::vector<TxShardWrites*> second_shards{&second};
    committed =
        co_await storage.CommitTxWrites(second.txid_, std::move(second_shards));
    if (!committed.ok()) co_return committed;
    second = {};
    cleaned = co_await impl.RunTxCleaner();
    if (!cleaned.ok()) co_return cleaned;
    for (const auto& key : {"rotation-first", "rotation-second"}) {
      auto value = co_await storage.Get(0, key);
      if (!value.ok()) co_return value.status();
      const auto bytes = value->value_bytes();
      const std::string_view expected = std::string_view(key).substr(9);
      if (std::string_view(reinterpret_cast<const char*>(bytes.data()),
                           bytes.size()) != expected)
        co_return absl::DataLossError(
            "generation rotation lost a committed value");
    }
    co_return absl::OkStatus();
  }

  static bycorf::Task<absl::Status> Exercise(StorageEngine& storage,
                                             bool extent) {
    using namespace std::chrono_literals;
    auto& impl = *storage.impl_;
    auto& store = impl.CurrentStore();
    std::vector<TxShardWrites> held(4);
    for (std::size_t i = 0; i < held.size(); ++i) {
      const auto txid = StorageEngine::AllocateWriteTxid();
      storage.InitializeTxWrites(txid, std::span(&held[i], 1));
      const std::string key = "pressure-held-" + std::to_string(i);
      auto written = co_await storage.SetLocked(0, key, ComputeDigest(key),
                                                "retained", {}, &held[i]);
      if (!written.ok()) co_return written.status();
      impl.current_tx_generation_.fetch_add(1, std::memory_order_seq_cst);
    }
    if (store.buffers_.available_write_buffers() != 0)
      co_return absl::FailedPreconditionError(
          "fixture did not exhaust staging");

    // First make every active tail durable. A later pressure seal must also
    // release an already-flushed buffer, without waiting for another append.
    impl.FlushActiveBlock(store);
    while (store.flush_running_) co_await bycorf::SleepFor(*store.worker_, 1ms);
    if (store.buffers_.available_write_buffers() != 0)
      co_return absl::FailedPreconditionError(
          "flush unexpectedly sealed a tail");

    // Keep one reader pin across the pressure seal. The other three buffers
    // suffice for progress, but this buffer must not be recycled yet.
    const auto block_id = store.active_tx_blocks_.begin()->second->block_id_;
    auto* pinned = impl.FindBlockState(store, block_id);
    ++pinned->pins_;
    const auto pinned_slot = pinned->staging_slot_;

    // Model a cleaner pass consuming earlier notifications. Only sealing the
    // already-durable transaction tails may re-arm it before any commit below.
    impl.tx_cleaner_dirty_.store(false, std::memory_order_release);
    const std::string value(extent ? 9 * 1024 * 1024 : 64, 'p');
    auto pending = storage.Set(0, "pressure-new", value);
    auto handle = std::move(pending).ReleaseHandle();
    pending = decltype(pending)(handle);
    handle.resume();
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!pending.done() && std::chrono::steady_clock::now() < deadline)
      co_await bycorf::SleepFor(*store.worker_, 1ms);
    const bool stalled = !pending.done();
    if (stalled) {
      // Unstick the original implementation so the negative test joins its
      // suspended writer and releases leases instead of hanging test teardown.
      co_await store.store_state_mutex_.Lock();
      impl.SealActiveBlocks(store);
      store.store_state_mutex_.Unlock(*store.worker_);
    }
    while (!pending.done()) co_await bycorf::SleepFor(*store.worker_, 1ms);
    auto written = std::move(pending).TakeResult();
    const bool pin_preserved = pinned->staging_slot_ == pinned_slot &&
                               pinned->release_pending_ && !pinned->in_memory_;
    --pinned->pins_;
    if (pinned->release_pending_) impl.ReleaseStagingBuffer(store, *pinned);
    if (!written.ok()) co_return written.status();
    if (stalled)
      co_return absl::DeadlineExceededError(
          "writer waited for buffers retained by live transaction generations");
    if (!pin_preserved)
      co_return absl::DataLossError("pressure seal recycled a pinned buffer");
    if (!impl.tx_cleaner_dirty_.load(std::memory_order_acquire))
      co_return absl::FailedPreconditionError(
          "durable transaction pressure seal did not re-arm cleaning");

    // The pressure seal must not revoke leases or lose their tagged data.
    // Appending the commit decisions may itself need another pressure seal.
    for (auto& tx : held) {
      std::vector<TxShardWrites*> shards{&tx};
      auto committed =
          co_await storage.CommitTxWrites(tx.txid_, std::move(shards));
      if (!committed.ok()) co_return committed;
    }
    for (std::size_t i = 0; i < held.size(); ++i) {
      const std::string key = "pressure-held-" + std::to_string(i);
      auto got = co_await storage.Get(0, key);
      if (!got.ok()) co_return got.status();
      const auto bytes = got->value_bytes();
      if (std::string_view(reinterpret_cast<const char*>(bytes.data()),
                           bytes.size()) != "retained")
        co_return absl::DataLossError("pressure seal changed a tagged value");
    }
    auto length = co_await storage.StringLength(0, "pressure-new");
    if (!length.ok()) co_return length.status();
    if (*length != value.size())
      co_return absl::DataLossError(
          "pressure writer returned the wrong length");
    co_return absl::OkStatus();
  }
};

}  // namespace lavik::storage

namespace {

constexpr std::uint64_t kMiB = 1024 * 1024;

#define ASSERT_CHECK(condition, message) ASSERT_TRUE(condition) << message

bool CreateFile(const std::string& path, std::uint64_t bytes) {
  const int fd =
      ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    return false;
  }
  const int allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  return ::close(fd) == 0 && allocated == 0;
}

bool GrowFile(const std::string& path, std::uint64_t bytes) {
  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return false;
  }
  const int allocated = ::posix_fallocate(fd, 0, static_cast<off_t>(bytes));
  const int close_error = ::close(fd);
  return allocated == 0 && close_error == 0;
}

std::uint64_t FileSize(const std::string& path) {
  struct stat info{};
  return ::stat(path.c_str(), &info) == 0
             ? static_cast<std::uint64_t>(info.st_size)
             : 0;
}

absl::Status Prepare(const std::vector<std::string>& paths,
                     bool reset = false) {
  lavik::storage::StorageEngineOptions options;
  options.data_files_ = paths;
  options.reset_data_files_ = reset;
  lavik::storage::StorageEngine engine(std::move(options));
  return engine.Prepare(1);
}

struct Cleanup {
  std::vector<std::string> paths_;
  ~Cleanup() {
    for (const std::string& path : paths_) {
      (void)::unlink(path.c_str());
    }
  }
};

constexpr std::chrono::nanoseconds FarFutureExpirationDeadline() {
  // Permanent authority is represented by max(); max()-1 remains a finite
  // capability without making deterministic tests depend on wall scheduling.
  return std::chrono::nanoseconds::max() - std::chrono::nanoseconds(1);
}

class FiniteExpirationAuthorityService final : public bycorf::Service {
 public:
  FiniteExpirationAuthorityService(lavik::storage::StorageEngine* storage,
                                   bycorf::Server* server)
      : storage_(storage), server_(server) {}

  void Prepare(unsigned thread_count) override {
    if (thread_count != 1) {
      result_ = absl::FailedPreconditionError(
          "finite expiration authority test requires one worker");
    }
  }

  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_->InitializeWorker(worker);
    if (!result_.ok()) co_return Finish();

    result_ = co_await storage_->QuiesceExpiration();
    if (!result_.ok()) co_return Finish();
    expiration_paused_ = true;

    result_ =
        storage_->SetExpirationAuthorityUntil(FarFutureExpirationDeadline());
    if (!result_.ok()) co_return Finish();
    const absl::Status tomb_raider = co_await storage_->ConfigureTombRaider(
        lavik::storage::TombRaiderConfigUpdate{
            .action_ = lavik::storage::TombRaiderConfigAction::kInterval,
            .value_ = 60'000});
    if (tomb_raider.code() != absl::StatusCode::kFailedPrecondition) {
      result_ = absl::FailedPreconditionError(
          "finite active-expiration authority changed Tomb Raider admission");
      co_return Finish();
    }
    // Cancellation cleanup obeys the same per-cycle work budget as actual and
    // failed deletion attempts; a zero remaining budget is a strict no-op.
    result_ =
        lavik::storage::ExpirationAuthorityTestPeer::VerifyStaleQueueBudget(
            *storage_);
    if (!result_.ok()) co_return Finish();
    result_ =
        storage_->SetExpirationAuthorityUntil(FarFutureExpirationDeadline());
    if (!result_.ok()) co_return Finish();
#if LAVIK_FAULTS_ENABLED
    result_ = co_await ExerciseDurableFinalPrecondition();
    if (!result_.ok()) co_return Finish();
    result_ = co_await ExerciseUnrelatedDurableFailure();
    if (!result_.ok()) co_return Finish();
    result_ = co_await ExerciseDiskFullFallbackPrecondition();
    if (!result_.ok()) co_return Finish();
    result_ = co_await ExerciseCurrentGrant();
    if (!result_.ok()) co_return Finish();
    result_ = co_await ExerciseCachedGrantTransitions();
#endif
    co_return Finish();
  }

  void Stop() noexcept override {}

  void FinalizeWorker(bycorf::Worker& worker) noexcept override {
    storage_->FinalizeWorker(worker);
  }

  const absl::Status& result() const noexcept { return result_; }

 private:
  bycorf::Task<absl::Status> SeedExpired(std::string_view key) {
    auto seeded = co_await storage_->Set(
        0, key, "value", lavik::storage::SetOptions{.expire_at_ms_ = 1});
    if (!seeded.ok()) co_return seeded.status();
    co_return co_await QueueExpired(key);
  }

#if LAVIK_FAULTS_ENABLED
  bycorf::Task<absl::Status> ExerciseDurableFinalPrecondition() {
    constexpr std::string_view kKey = "expiration-final-{foo}";
    absl::Status prepared = co_await SeedExpired(kKey);
    if (!prepared.ok()) co_return prepared;
    bool reached_final_append = false;
    lavik::storage::ExpirationAuthorityTestPeer::SetHook(
        *storage_, [&](auto point) -> std::optional<absl::Status> {
          if (point == lavik::storage::ExpirationAuthorityTestPeer::Point::
                           kBeforeDurableAppend) {
            reached_final_append = true;
            storage_->SetExpirationAuthority(false);
          }
          return std::nullopt;
        });
    expiration_paused_ = false;
    const absl::Status expired = co_await lavik::storage::
        ExpirationAuthorityTestPeer::ResumeAndExpireFront(*storage_);
    lavik::storage::ExpirationAuthorityTestPeer::SetHook(*storage_, {});
    absl::Status paused = co_await storage_->QuiesceExpiration();
    expiration_paused_ = paused.ok();
    if (!paused.ok()) co_return paused;
    if (!expired.ok() || !reached_final_append || storage_->LocalSize(0) != 1) {
      co_return absl::FailedPreconditionError(
          "revocation at the durable publication cut was not cancelled");
    }
    co_return storage_->SetExpirationAuthorityUntil(
        FarFutureExpirationDeadline());
  }

  bycorf::Task<absl::Status> ExerciseUnrelatedDurableFailure() {
    constexpr std::string_view kKey = "expiration-internal-{foo}";
    absl::Status prepared = co_await SeedExpired(kKey);
    if (!prepared.ok()) co_return prepared;
    bool injected = false;
    lavik::storage::ExpirationAuthorityTestPeer::SetHook(
        *storage_, [&](auto point) -> std::optional<absl::Status> {
          if (point != lavik::storage::ExpirationAuthorityTestPeer::Point::
                           kBeforeDurableAppend) {
            return std::nullopt;
          }
          injected = true;
          storage_->SetExpirationAuthority(false);
          return absl::InternalError("injected unrelated append failure");
        });
    expiration_paused_ = false;
    const absl::Status expired = co_await lavik::storage::
        ExpirationAuthorityTestPeer::ResumeAndExpireFront(*storage_);
    lavik::storage::ExpirationAuthorityTestPeer::SetHook(*storage_, {});
    absl::Status paused = co_await storage_->QuiesceExpiration();
    expiration_paused_ = paused.ok();
    if (!paused.ok()) co_return paused;
    if (!injected || expired.code() != absl::StatusCode::kInternal ||
        storage_->LocalSize(0) != 2) {
      co_return absl::FailedPreconditionError(
          "authority revocation swallowed an unrelated append failure");
    }
    co_return storage_->SetExpirationAuthorityUntil(
        FarFutureExpirationDeadline());
  }

  bycorf::Task<absl::Status> ExerciseDiskFullFallbackPrecondition() {
    constexpr std::string_view kKey = "expiration-fallback-{foo}";
    absl::Status prepared = co_await SeedExpired(kKey);
    if (!prepared.ok()) co_return prepared;
    bool forced_disk_full = false;
    bool reached_fallback = false;
    lavik::storage::ExpirationAuthorityTestPeer::SetHook(
        *storage_, [&](auto point) -> std::optional<absl::Status> {
          using Point = lavik::storage::ExpirationAuthorityTestPeer::Point;
          if (point == Point::kBeforeDurableAppend) {
            forced_disk_full = true;
            return absl::ResourceExhaustedError("injected full device");
          }
          reached_fallback = true;
          storage_->SetExpirationAuthority(false);
          return std::nullopt;
        });
    expiration_paused_ = false;
    const absl::Status expired = co_await lavik::storage::
        ExpirationAuthorityTestPeer::ResumeAndExpireFront(*storage_);
    lavik::storage::ExpirationAuthorityTestPeer::SetHook(*storage_, {});
    absl::Status paused = co_await storage_->QuiesceExpiration();
    expiration_paused_ = paused.ok();
    if (!paused.ok()) co_return paused;
    if (!expired.ok() || !forced_disk_full || !reached_fallback ||
        storage_->LocalSize(0) != 3) {
      co_return absl::FailedPreconditionError(
          "disk-full fallback did not recheck exact expiration authority");
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseCurrentGrant() {
    constexpr std::string_view kKey = "expiration-current-{foo}";
    absl::Status granted =
        storage_->SetExpirationAuthorityUntil(FarFutureExpirationDeadline());
    if (!granted.ok()) co_return granted;
    absl::Status prepared = co_await SeedExpired(kKey);
    if (!prepared.ok()) co_return prepared;
    expiration_paused_ = false;
    const absl::Status expired = co_await lavik::storage::
        ExpirationAuthorityTestPeer::ResumeAndExpireFront(*storage_);
    absl::Status paused = co_await storage_->QuiesceExpiration();
    expiration_paused_ = paused.ok();
    if (!paused.ok()) co_return paused;
    if (!expired.ok() || storage_->LocalSize(0) != 3) {
      co_return absl::FailedPreconditionError(
          "candidate carrying the current grant was not expired");
    }
    co_return absl::OkStatus();
  }

  bycorf::Task<absl::Status> ExerciseCachedGrantTransitions() {
    using Peer = lavik::storage::ExpirationAuthorityTestPeer;
    for (unsigned scenario = 0; scenario != 3; ++scenario) {
      const auto deadline =
          FarFutureExpirationDeadline() - std::chrono::seconds(1);
      auto lease = std::make_shared<lavik::LeaseDeadline>(deadline);
      auto granted = storage_->SetExpirationAuthorityUntil(lease);
      if (!granted.ok()) co_return granted;
      auto prepared = co_await SeedExpired("expiration-cache-{foo}-" +
                                           std::to_string(scenario));
      if (!prepared.ok()) co_return prepared;
      const auto before = storage_->LocalSize(0);
      absl::Status expired;
      if (scenario == 0) {
        // A queued local alias cannot survive authority revocation as a
        // permission to delete, even though it still owns the grant object.
        storage_->SetExpirationAuthority(false);
        expiration_paused_ = false;
        expired = co_await Peer::ResumeAndExpireFront(*storage_);
        auto paused = co_await storage_->QuiesceExpiration();
        expiration_paused_ = paused.ok();
        if (!paused.ok()) co_return paused;
      } else {
        expired = co_await Peer::ExpireWhileKeyLocked(*storage_, [&] {
          if (scenario == 1)
            return storage_->SetExpirationAuthorityUntil(
                FarFutureExpirationDeadline());
          return lease->Renew(deadline - std::chrono::seconds(1),
                              FarFutureExpirationDeadline())
                     ? absl::OkStatus()
                     : absl::FailedPreconditionError(
                           "shared lease renewal failed");
        });
      }
      if (!expired.ok()) co_return expired;
      const auto expected = before - (scenario == 2 ? 1 : 0);
      if (storage_->LocalSize(0) != expected)
        co_return absl::FailedPreconditionError(
            "cached expiration grant changed mutation authorization");
    }
    co_return absl::OkStatus();
  }
#endif

  bycorf::Task<absl::Status> QueueExpired(std::string_view key) {
    auto result = co_await storage_->Get(0, key);
    if (result.ok() || result.status().code() != absl::StatusCode::kNotFound) {
      co_return absl::FailedPreconditionError(
          "expired candidate was not logically absent");
    }
    co_return absl::OkStatus();
  }

  absl::Status Finish() {
#if LAVIK_FAULTS_ENABLED
    lavik::storage::ExpirationAuthorityTestPeer::SetHook(*storage_, {});
#endif
    ResumeExpiration();
    server_->RequestStop();
    return result_;
  }

  void ResumeExpiration() {
    if (!expiration_paused_) return;
    storage_->ResumeExpiration();
    expiration_paused_ = false;
  }

  lavik::storage::StorageEngine* storage_ = nullptr;
  bycorf::Server* server_ = nullptr;
  bool expiration_paused_ = false;
  absl::Status result_ =
      absl::UnknownError("finite expiration authority test did not run");
};

class WriteBufferPressureService final : public bycorf::Service {
 public:
  WriteBufferPressureService(lavik::storage::StorageEngine& storage,
                             bool extent, bool rotation = false)
      : storage_(storage), extent_(extent), rotation_(rotation) {}
  bycorf::Task<absl::Status> Run(bycorf::Worker& worker,
                                 bycorf::ServiceContext) override {
    lavik::BindMemoryAccountingShard(worker.id());
    lavik::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_.InitializeWorker(worker);
    if (result_.ok() && rotation_)
      result_ = co_await lavik::storage::WriteBufferPressureTestPeer::
          ExerciseRotation(storage_);
    else if (result_.ok())
      result_ = co_await lavik::storage::WriteBufferPressureTestPeer::Exercise(
          storage_, extent_);
    worker.RequestStop();
    co_return result_;
  }
  void Prepare(unsigned) override {}
  void Stop() noexcept override {}
  void FinalizeWorker(bycorf::Worker& worker) noexcept override {
    storage_.FinalizeWorker(worker);
  }
  absl::Status result_ = absl::UnknownError("pressure test did not run");

 private:
  lavik::storage::StorageEngine& storage_;
  bool extent_;
  bool rotation_;
};

}  // namespace

TEST(StorageReplicationTest, RejectsDisabledDatabaseCounts) {
  lavik::storage::StorageEngineOptions options;
  options.database_count_ = 1;
  lavik::storage::StorageEngine engine(std::move(options));

  // Reject an invalid range before touching worker/session state. In
  // particular, the standalone default must not silently become DB0-only.
  for (const std::uint8_t count : {0, 2, 16}) {
    const auto result = engine.BeginPartitionReplication(1, 0, count);
    EXPECT_EQ(result.status().code(), absl::StatusCode::kInvalidArgument);
  }
}

TEST(StorageEngineRuntimeFailureTest,
     PermanentRequestFenceAlsoPublishesTheMonitorLatch) {
  lavik::storage::StorageEngine engine({});
  EXPECT_FALSE(engine.ReplicaRecoveryFenced());
  EXPECT_FALSE(engine.RuntimeFailureLatched());

  engine.FenceRequestServingUntilRestart();

  EXPECT_TRUE(engine.ReplicaRecoveryFenced());
  EXPECT_TRUE(engine.RuntimeFailureLatched());
}

TEST(StorageExpirationAuthorityTest, RejectsElapsedFiniteAuthority) {
  lavik::storage::StorageEngine engine({});

  const absl::Status status =
      engine.SetExpirationAuthorityUntil(std::chrono::nanoseconds::zero());

  EXPECT_EQ(status.code(), absl::StatusCode::kDeadlineExceeded);
}

TEST(StorageExpirationAuthorityTest,
     RecognizesOnlyMarkedAuthorityCancellation) {
  const absl::Status cancelled =
      lavik::storage::ExpirationAuthorityTestPeer::RevokedGrantStatus();

  EXPECT_TRUE(
      lavik::storage::ExpirationAuthorityTestPeer::IsCancellation(cancelled));
  EXPECT_FALSE(lavik::storage::ExpirationAuthorityTestPeer::IsCancellation(
      absl::InternalError("unrelated storage failure")));
  EXPECT_FALSE(lavik::storage::ExpirationAuthorityTestPeer::IsCancellation(
      absl::DataLossError("unrelated storage corruption")));
  EXPECT_FALSE(lavik::storage::ExpirationAuthorityTestPeer::IsCancellation(
      absl::FailedPreconditionError("unmarked precondition")));
}

TEST(StorageExpirationAuthorityTest,
     SharedRenewalPreservesCapturedMutationCapability) {
  using namespace std::chrono_literals;
  using Peer = lavik::storage::ExpirationAuthorityTestPeer;
  lavik::storage::StorageEngine engine({});
  const auto deadline = FarFutureExpirationDeadline() - 1s;
  const auto lease = std::make_shared<lavik::LeaseDeadline>(deadline);
  ASSERT_TRUE(engine.SetExpirationAuthorityUntil(lease).ok());
  auto captured = Peer::CurrentGrant(engine);
  ASSERT_TRUE(Peer::ValidateGrant(captured).ok());
  EXPECT_TRUE(lease->Renew(deadline - 1s, deadline + 1s));
  EXPECT_EQ(captured, Peer::CurrentGrant(engine));
  EXPECT_TRUE(Peer::ValidateGrant(captured).ok());
  lease->Revoke();
  EXPECT_TRUE(Peer::IsCancellation(Peer::ValidateGrant(captured)));
  ASSERT_TRUE(
      engine.SetExpirationAuthorityUntil(FarFutureExpirationDeadline()).ok());
  EXPECT_TRUE(Peer::IsCancellation(Peer::ValidateGrant(captured)));
  EXPECT_TRUE(Peer::ValidateGrant(Peer::CurrentGrant(engine)).ok());
}

TEST(StorageExpirationAuthorityTest, LocalRoleLossRevokesSharedLease) {
  lavik::storage::StorageEngine engine({});
  const auto lease =
      std::make_shared<lavik::LeaseDeadline>(FarFutureExpirationDeadline());
  ASSERT_TRUE(engine.SetExpirationAuthorityUntil(lease).ok());
  engine.SetExpirationAuthority(false);
  EXPECT_FALSE(lease->valid_at(std::chrono::nanoseconds(1)));
  EXPECT_FALSE(
      lease->Renew(std::chrono::nanoseconds(1), FarFutureExpirationDeadline()));
}

TEST(StorageExpirationAuthorityTest, WorkersOwnSeparateCachedControlBlocks) {
  using Peer = lavik::storage::ExpirationAuthorityTestPeer;
  lavik::storage::StorageEngine engine({});
  Peer::WorkerCache worker0, worker1;
  auto global = Peer::CurrentGrant(engine);
  auto local0 = Peer::CachedGrant(engine, worker0);
  auto local1 = Peer::CachedGrant(engine, worker1);
  ASSERT_NE(local0, nullptr);
  EXPECT_EQ(local0.get(), global.get());
  EXPECT_EQ(local1.get(), global.get());
  EXPECT_TRUE(local0.owner_before(local1) || local1.owner_before(local0));
  EXPECT_TRUE(local0.owner_before(global) || global.owner_before(local0));
  const auto global_refs = global.use_count();
  std::vector<std::shared_ptr<const void>> candidates;
  for (unsigned i = 0; i != 64; ++i)
    candidates.push_back(Peer::CachedGrant(engine, worker0));
  EXPECT_EQ(global.use_count(), global_refs);
  EXPECT_FALSE(local0.owner_before(candidates.back()));
  EXPECT_FALSE(candidates.back().owner_before(local0));

  engine.SetExpirationAuthority(false);
  EXPECT_EQ(Peer::CachedGrant(engine, worker0), nullptr);
  EXPECT_EQ(Peer::CachedGrant(engine, worker1), nullptr);
  EXPECT_TRUE(Peer::IsCancellation(Peer::ValidateGrant(candidates.front())));
  engine.SetExpirationAuthority(true);
  auto replacement = Peer::CachedGrant(engine, worker0);
  ASSERT_NE(replacement, nullptr);
  EXPECT_NE(replacement.get(), local0.get());
  EXPECT_TRUE(Peer::IsCancellation(Peer::ValidateGrant(local0)));
  EXPECT_TRUE(Peer::ValidateGrant(replacement).ok());
}

TEST(StorageExpirationAuthorityTest,
     SharedRenewalKeepsWorkerCacheAndLiveChecks) {
  using namespace std::chrono_literals;
  using Peer = lavik::storage::ExpirationAuthorityTestPeer;
  lavik::storage::StorageEngineOptions options;
  options.expiration_authority_ = false;
  lavik::storage::StorageEngine engine(std::move(options));
  Peer::WorkerCache cache;
  EXPECT_EQ(Peer::CachedGrant(engine, cache), nullptr);
  const auto deadline = FarFutureExpirationDeadline() - 1s;
  auto lease = std::make_shared<lavik::LeaseDeadline>(deadline);
  ASSERT_TRUE(engine.SetExpirationAuthorityUntil(lease).ok());
  auto captured = Peer::CachedGrant(engine, cache);
  ASSERT_NE(captured, nullptr);
  const auto version = Peer::PublicationVersion(engine);
  ASSERT_TRUE(lease->Renew(deadline - 1s, deadline + 1s));
  EXPECT_EQ(Peer::PublicationVersion(engine), version);
  auto renewed = Peer::CachedGrant(engine, cache);
  EXPECT_EQ(renewed.get(), captured.get());
  EXPECT_FALSE(renewed.owner_before(captured));
  EXPECT_FALSE(captured.owner_before(renewed));
  // Even without cache invalidation, the captured epoch's terminal expiry
  // must be observed by both cache hits and already queued mutation guards.
  EXPECT_FALSE(lease->valid_at(FarFutureExpirationDeadline()));
  EXPECT_EQ(Peer::PublicationVersion(engine), version);
  EXPECT_EQ(Peer::CachedGrant(engine, cache), nullptr);
  EXPECT_TRUE(Peer::IsCancellation(Peer::ValidateGrant(captured)));
}

TEST(StorageExpirationAuthorityTest, LegacyPermanentGrantIsIdempotent) {
  lavik::storage::StorageEngineOptions options;
  options.expiration_authority_ = false;
  lavik::storage::StorageEngine engine(std::move(options));

  engine.SetExpirationAuthority(true);
  auto first =
      lavik::storage::ExpirationAuthorityTestPeer::CurrentGrant(engine);
  ASSERT_NE(first, nullptr);
  engine.SetExpirationAuthority(true);
  auto repeated =
      lavik::storage::ExpirationAuthorityTestPeer::CurrentGrant(engine);

  EXPECT_EQ(first.get(), repeated.get());

  engine.SetExpirationAuthority(false);
  engine.SetExpirationAuthority(true);
  auto reenabled =
      lavik::storage::ExpirationAuthorityTestPeer::CurrentGrant(engine);
  EXPECT_NE(first.get(), reenabled.get());

  ASSERT_TRUE(
      engine.SetExpirationAuthorityUntil(FarFutureExpirationDeadline()).ok());
  auto finite =
      lavik::storage::ExpirationAuthorityTestPeer::CurrentGrant(engine);
  engine.SetExpirationAuthority(true);
  auto permanent =
      lavik::storage::ExpirationAuthorityTestPeer::CurrentGrant(engine);
  EXPECT_NE(finite.get(), permanent.get());
}

TEST(StorageExpirationAuthorityTest,
     FiniteAuthorityIsExactCancellableAndIndependentOfTombRaider) {
  const std::string path = lavik::test::TestDataPath(
      "lavik-expiration-authority-" + std::to_string(::getpid()) + ".data");
  Cleanup cleanup{{path}};
  ASSERT_CHECK(CreateFile(path, 96 * kMiB),
               "failed to create expiration-authority storage file");

  lavik::storage::StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  options.expiration_authority_ = false;
  options.tomb_raider_interval_ms_ = 0;
  options.tx_cleaner_cooldown_ms_ = 0;
  lavik::storage::StorageEngine storage(std::move(options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());
  if (lavik::tx::TxRuntime::Get() == nullptr) lavik::tx::TxRuntime::Create(1);

  bycorf::Server server;
  FiniteExpirationAuthorityService service(&storage, &server);
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result().ok()) << service.result();
}

TEST(StorageCapacityTest, ValidatesAndPreservesDeviceCapacities) {
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-storage-capacity-" + std::to_string(::getpid()));
  Cleanup cleanup;

  const std::string unequal_a = prefix + "-unequal-a.data";
  const std::string unequal_b = prefix + "-unequal-b.data";
  cleanup.paths_.push_back(unequal_a);
  cleanup.paths_.push_back(unequal_b);
  ASSERT_CHECK(
      CreateFile(unequal_a, 88 * kMiB) && CreateFile(unequal_b, 96 * kMiB),
      "failed to create unequal-capacity files");
  ASSERT_CHECK(Prepare({unequal_a, unequal_b}).ok(),
               "unequal fresh device capacities were rejected");
  ASSERT_CHECK(
      FileSize(unequal_a) == 88 * kMiB && FileSize(unequal_b) == 96 * kMiB,
      "storage prepare changed regular-file sizes");

  ASSERT_CHECK(GrowFile(unequal_b, 104 * kMiB),
               "failed to grow initialized test file");
  ASSERT_CHECK(Prepare({unequal_a, unequal_b}).ok(),
               "larger backing file did not preserve labeled capacity");
  ASSERT_CHECK(
      ::truncate(unequal_a.c_str(), static_cast<off_t>(80 * kMiB)) == 0 &&
          !Prepare({unequal_a, unequal_b}).ok(),
      "backing file smaller than its label was accepted");

  const std::string too_small = prefix + "-small.data";
  cleanup.paths_.push_back(too_small);
  ASSERT_CHECK(CreateFile(too_small, 72 * kMiB) && !Prepare({too_small}).ok(),
               "single-device file with no foreground block was accepted");

  const std::string minimum = prefix + "-minimum.data";
  cleanup.paths_.push_back(minimum);
  ASSERT_CHECK(CreateFile(minimum, 80 * kMiB) && Prepare({minimum}).ok() &&
                   Prepare({minimum}, true).ok(),
               "80 MiB single-device minimum was rejected");

  const std::string unaligned = prefix + "-unaligned.data";
  cleanup.paths_.push_back(unaligned);
  ASSERT_CHECK(
      CreateFile(unaligned, 80 * kMiB + 4096) && !Prepare({unaligned}).ok(),
      "unaligned fresh regular file was accepted");

  const std::string missing = prefix + "-missing.data";
  ASSERT_CHECK(!Prepare({missing}).ok(), "missing storage path was created");
}

TEST(StorageCapacityTest, ExpandsAnInitializedStorageSet) {
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-storage-expansion-" + std::to_string(::getpid()));
  Cleanup cleanup;
  const std::string original = prefix + "-original.data";
  const std::string added = prefix + "-added.data";
  cleanup.paths_ = {original, added};

  ASSERT_CHECK(CreateFile(original, 88 * kMiB),
               "failed to create original storage file");
  ASSERT_CHECK(Prepare({original}).ok(),
               "failed to initialize original storage set");
  ASSERT_CHECK(CreateFile(added, 88 * kMiB),
               "failed to create added storage file");
  ASSERT_CHECK(Prepare({added, original}).ok(),
               "failed to expand initialized storage set");
  ASSERT_CHECK(Prepare({original, added}).ok(),
               "expanded storage set did not reopen in a new argument order");
  ASSERT_CHECK(!Prepare({original}).ok(),
               "expanded storage set reopened with a missing member");
}

TEST(StorageCapacityTest, RejectsForeignDeviceDuringExpansion) {
  const std::string prefix = lavik::test::TestDataPath(
      "lavik-storage-foreign-" + std::to_string(::getpid()));
  Cleanup cleanup;
  const std::string first = prefix + "-first.data";
  const std::string foreign = prefix + "-foreign.data";
  cleanup.paths_ = {first, foreign};

  ASSERT_CHECK(CreateFile(first, 88 * kMiB) && CreateFile(foreign, 88 * kMiB),
               "failed to create foreign-device test files");
  ASSERT_CHECK(Prepare({first}).ok() && Prepare({foreign}).ok(),
               "failed to initialize independent storage sets");
  ASSERT_CHECK(!Prepare({first, foreign}).ok(),
               "foreign initialized device was accepted as an expansion");
  ASSERT_CHECK(Prepare({first, foreign}, true).ok(),
               "explicit storage reset did not replace foreign device sets");
  ASSERT_CHECK(Prepare({foreign, first}).ok(),
               "reset storage set could not be reopened");
}

TEST(StorageCapacityTest, LiveTransactionGenerationsCannotStarveStaging) {
  for (bool extent : {false, true}) {
    SCOPED_TRACE(extent ? "extent writer" : "inline writer");
    const std::string path = lavik::test::TestDataPath(
        "lavik-buffer-pressure-" + std::to_string(::getpid()) +
        (extent ? "-extent.data" : "-inline.data"));
    Cleanup cleanup{{path}};
    ASSERT_TRUE(CreateFile(path, 256 * kMiB));
    lavik::storage::StorageEngineOptions options;
    options.data_files_ = {path};
    options.buffers_.registered_bytes_ = 64 * kMiB;
    options.buffers_.storage_write_buffer_count_ = 4;
    options.tx_cleaner_cooldown_ms_ = 0;
    options.expiration_authority_ = false;
    lavik::storage::StorageEngine storage(std::move(options));
    lavik::InitWorkerMetrics(1);
    ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
    ASSERT_TRUE(storage.Prepare(1).ok());
    if (lavik::tx::TxRuntime::Get() == nullptr) lavik::tx::TxRuntime::Create(1);
    WriteBufferPressureService service(storage, extent);
    bycorf::Server server;
    server.AddService(&service);
    bycorf::ServerOptions runtime;
    runtime.thread_count_ = 1;
    runtime.pin_workers_ = false;
    runtime.recv_buffer_count_ = 0;
    ASSERT_TRUE(server.Start(runtime).ok());
    server.WaitUntilStopped();
    EXPECT_TRUE(service.result_.ok()) << service.result_;
  }
}

TEST(StorageCapacityTest, CleanerBoundsGenerationsWhileCommitIsOutstanding) {
  const std::string path = lavik::test::TestDataPath(
      "lavik-generation-rotation-" + std::to_string(::getpid()) + ".data");
  Cleanup cleanup{{path}};
  ASSERT_TRUE(CreateFile(path, 128 * kMiB));
  lavik::storage::StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  options.tx_cleaner_cooldown_ms_ = 0;  // The peer drives exact cleaner rounds.
  options.expiration_authority_ = false;
  lavik::storage::StorageEngine storage(std::move(options));
  lavik::InitWorkerMetrics(1);
  ASSERT_TRUE(lavik::InitMemoryLimit(512 * kMiB, 1).ok());
  ASSERT_TRUE(storage.Prepare(1).ok());
  if (lavik::tx::TxRuntime::Get() == nullptr) lavik::tx::TxRuntime::Create(1);
  WriteBufferPressureService service(storage, false, true);
  bycorf::Server server;
  server.AddService(&service);
  bycorf::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  ASSERT_TRUE(server.Start(runtime).ok());
  server.WaitUntilStopped();
  EXPECT_TRUE(service.result_.ok()) << service.result_;
}
