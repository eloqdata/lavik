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
  static std::shared_ptr<const void> CurrentGrant(
      const StorageEngine& storage) {
    return std::static_pointer_cast<const void>(
        storage.impl_->active_expiration_authority_.load(
            std::memory_order_acquire));
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
    auto stale = impl->CurrentExpirationAuthority();
    if (stale == nullptr) {
      return absl::FailedPreconditionError(
          "stale queue test has no initial authority");
    }
    absl::Status replaced = storage.SetExpirationAuthorityUntil(
        std::chrono::nanoseconds::max() - std::chrono::nanoseconds(1));
    if (!replaced.ok()) return replaced;
    auto current = impl->CurrentExpirationAuthority();
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
#endif
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
  lavik::tx::TxRuntime::Create(1);

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
