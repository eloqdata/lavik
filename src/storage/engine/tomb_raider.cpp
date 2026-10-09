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

#include <ctime>
#include <optional>

#include "impl.h"
#include "lavik/status_macros.h"

namespace lavik::storage {

namespace {

constexpr auto kMaxScheduleSleep = std::chrono::seconds(1);
// Long operator-configured pacing sleeps stay cheap while replica transition
// latency remains independent of that setting.
constexpr auto kMaxForfeitCheckpointSleep = std::chrono::milliseconds(100);

template <typename Duration>
std::chrono::milliseconds ScheduleSleep(Duration remaining) {
  auto result =
      std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
  if (result <= std::chrono::milliseconds::zero()) {
    return std::chrono::milliseconds(1);
  }
  return std::min(result, std::chrono::duration_cast<std::chrono::milliseconds>(
                              kMaxScheduleSleep));
}

std::optional<std::chrono::system_clock::time_point> NextDailyTime(
    std::uint32_t daily_second) {
  const auto now = std::chrono::system_clock::now();
  const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
  std::tm local{};
  if (::localtime_r(&now_time, &local) == nullptr) {
    return std::nullopt;
  }
  local.tm_hour = static_cast<int>(daily_second / 3600);
  local.tm_min = static_cast<int>((daily_second % 3600) / 60);
  local.tm_sec = static_cast<int>(daily_second % 60);
  local.tm_isdst = -1;
  std::time_t scheduled = std::mktime(&local);
  if (scheduled == static_cast<std::time_t>(-1)) {
    return std::nullopt;
  }
  auto due = std::chrono::system_clock::from_time_t(scheduled);
  if (due <= now) {
    ++local.tm_mday;
    local.tm_isdst = -1;
    scheduled = std::mktime(&local);
    if (scheduled == static_cast<std::time_t>(-1)) {
      return std::nullopt;
    }
    due = std::chrono::system_clock::from_time_t(scheduled);
  }
  return due;
}

}  // namespace

// A tombstone (and the index entry pinning it) is dead weight once no older
// record of its key survives on disk: recovery would conclude "absent" with
// or without it. The raider proves that by scanning every allocated records
// block once — the only authority on what actually survives — in three
// phases: mark every candidate entry unclaimed, sweep all blocks clearing
// the bit for entries a surviving dangerous record still needs, then reap
// what stayed unclaimed. Everything is memory-state; a crash mid-round just
// forfeits the round, and recovery rebuilds both tombstone entries and
// shielding bits exactly from the surviving records.

Task<absl::Status> StorageEngine::Impl::CompleteStorageStartup() {
  co_return co_await bycorf::SubmitTo(0, [this] {
    // Startup completion is a local recovery fact, not serving readiness.
    // Do not reset population completeness on a repeated notification.
    tomb_raider_startup_complete_.store(true, std::memory_order_release);
    return absl::OkStatus();
  });
}

Task<absl::Status> StorageEngine::Impl::ConfigureTombRaider(
    TombRaiderConfigUpdate update) {
  co_return co_await bycorf::SubmitTo(
      0, [this, update] { return ApplyTombRaiderConfig(update); });
}

Task<absl::StatusOr<StorageEngine::Impl::PopulationChangeToken>>
StorageEngine::Impl::BeginPopulationChange(std::uint64_t session_id) {
  if (session_id == 0)
    co_return absl::InvalidArgumentError("invalid population change session");
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, session_id] { return BeginPopulationChange(session_id); });
  }
  auto [it, inserted] =
      tomb_raider_population_changes_.try_emplace(session_id, 0);
  if (inserted) {
    it->second = tomb_raider_population_generation_.fetch_add(
                     1, std::memory_order_acq_rel) +
                 1;
    tomb_raider_population_holds_.store(tomb_raider_population_changes_.size(),
                                        std::memory_order_release);
  }
  const auto generation = it->second;
  while (tomb_raider_running_.load(std::memory_order_acquire)) {
    co_await tomb_raider_round_finished_.Wait();
  }
  // Cancellation/supersession can run while the round drains. Never revive
  // a removed hold or authorize the old caller to start a destructive reset.
  it = tomb_raider_population_changes_.find(session_id);
  if (it == tomb_raider_population_changes_.end() || it->second != generation) {
    co_return absl::CancelledError("population change was superseded");
  }
  co_return PopulationChangeToken{session_id, generation};
}

Task<absl::Status> StorageEngine::Impl::CompletePopulationChange(
    PopulationChangeToken token) {
  co_return co_await bycorf::SubmitTo(0, [this, token] {
    auto it = tomb_raider_population_changes_.find(token.session_id_);
    if (it == tomb_raider_population_changes_.end() ||
        it->second != token.generation_)
      return absl::FailedPreconditionError("population change is not active");
    const bool current = it->second == tomb_raider_population_generation_.load(
                                           std::memory_order_acquire);
    if (current && (ReplicaRecoveryFenced() || RuntimeFailureLatched())) {
      return absl::FailedPreconditionError("population is still incomplete");
    }
    tomb_raider_population_changes_.erase(it);
    if (current)
      tomb_raider_population_complete_.store(true, std::memory_order_release);
    tomb_raider_population_holds_.store(tomb_raider_population_changes_.size(),
                                        std::memory_order_release);
    return current
               ? absl::OkStatus()
               : absl::FailedPreconditionError("stale population completion");
  });
}

Task<absl::Status> StorageEngine::Impl::CancelPopulationChange(
    PopulationChangeToken token) {
  co_return co_await bycorf::SubmitTo(0, [this, token] {
    auto it = tomb_raider_population_changes_.find(token.session_id_);
    if (it != tomb_raider_population_changes_.end() &&
        it->second == token.generation_)
      tomb_raider_population_changes_.erase(it);
    tomb_raider_population_holds_.store(tomb_raider_population_changes_.size(),
                                        std::memory_order_release);
    return absl::OkStatus();
  });
}

std::string_view StorageEngine::Impl::TombRaiderBlockedReason() const noexcept {
  if (shutdown_flush_requested_.load(std::memory_order_acquire))
    return "shutdown";
  if (RuntimeFailureLatched()) return "storage_failure";
  if (!tomb_raider_startup_complete_.load(std::memory_order_acquire))
    return "startup";
  if (tomb_raider_population_holds_.load(std::memory_order_acquire) != 0)
    return "population_change";
  if (!tomb_raider_population_complete_.load(std::memory_order_acquire) ||
      ReplicaRecoveryFenced())
    return "incomplete_population";
  return "none";
}

bool StorageEngine::Impl::TombRaiderRoundValid(
    TombRaiderRound& round, const WorkerStore* store) const noexcept {
  bool valid =
      !round.cancelled_.load(std::memory_order_acquire) &&
      !TombRaiderShouldForfeit() &&
      round.population_generation_ ==
          tomb_raider_population_generation_.load(std::memory_order_acquire) &&
      round.index_generation_ ==
          tomb_raider_index_generation_.load(std::memory_order_acquire);
  for (std::uint8_t db = 0; valid && db < options_.database_count_; ++db) {
    valid = round.db_epochs_[db] == DbEpoch(db) &&
            (store == nullptr || round.indexes_[store->worker_->id()][db] ==
                                     store->index_generations_[db]);
  }
  if (!valid) round.cancelled_.store(true, std::memory_order_release);
  return valid;
}

absl::Status StorageEngine::Impl::ApplyTombRaiderConfig(
    TombRaiderConfigUpdate update) {
  TombRaiderMode mode =
      tomb_raider_config_.mode_.load(std::memory_order_relaxed);
  bool reschedule = false;
  auto begin_reschedule = [this] {
    tomb_raider_config_.generation_.fetch_add(1, std::memory_order_acq_rel);
  };
  switch (update.action_) {
    case TombRaiderConfigAction::kOff:
      if (mode == TombRaiderMode::kOff) {
        return absl::OkStatus();
      }
      begin_reschedule();
      tomb_raider_config_.last_mode_.store(mode, std::memory_order_relaxed);
      mode = TombRaiderMode::kOff;
      reschedule = true;
      break;
    case TombRaiderConfigAction::kOn:
      if (mode != TombRaiderMode::kOff) {
        return absl::OkStatus();
      }
      mode = tomb_raider_config_.last_mode_.load(std::memory_order_relaxed);
      if (mode == TombRaiderMode::kInterval &&
          tomb_raider_config_.interval_ms_.load(std::memory_order_relaxed) ==
              0) {
        return absl::Status(absl::StatusCode::kFailedPrecondition,
                            "no previous tomb raider schedule");
      }
      begin_reschedule();
      reschedule = true;
      break;
    case TombRaiderConfigAction::kInterval:
      if (update.value_ == 0 ||
          update.value_ > static_cast<std::uint64_t>(
                              std::chrono::milliseconds::max().count())) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "invalid tomb raider interval");
      }
      begin_reschedule();
      tomb_raider_config_.interval_ms_.store(update.value_,
                                             std::memory_order_relaxed);
      mode = TombRaiderMode::kInterval;
      tomb_raider_config_.last_mode_.store(mode, std::memory_order_relaxed);
      reschedule = true;
      break;
    case TombRaiderConfigAction::kBlockSleep:
      if (update.value_ > std::numeric_limits<std::uint32_t>::max()) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "invalid tomb raider block sleep");
      }
      tomb_raider_config_.block_sleep_ms_.store(
          static_cast<std::uint32_t>(update.value_), std::memory_order_release);
      return absl::OkStatus();
    case TombRaiderConfigAction::kDaily:
      if (update.value_ >= 24 * 60 * 60) {
        return absl::Status(absl::StatusCode::kInvalidArgument,
                            "invalid tomb raider daily time");
      }
      begin_reschedule();
      tomb_raider_config_.daily_second_.store(
          static_cast<std::uint32_t>(update.value_), std::memory_order_relaxed);
      mode = TombRaiderMode::kDaily;
      tomb_raider_config_.last_mode_.store(mode, std::memory_order_relaxed);
      reschedule = true;
      break;
  }

  if (!reschedule) {
    return absl::OkStatus();
  }
  tomb_raider_config_.mode_.store(mode, std::memory_order_relaxed);
  tomb_raider_config_.generation_.fetch_add(1, std::memory_order_release);
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombRaiderLoop(WorkerStore* store) {
  std::uint64_t generation = 0;
  auto interval_due = std::chrono::steady_clock::now();
  std::optional<std::chrono::system_clock::time_point> daily_due;
  while (!store->worker_->stop_requested() &&
         !shutdown_flush_requested_.load(std::memory_order_acquire)) {
    const auto configured =
        tomb_raider_config_.generation_.load(std::memory_order_acquire);
    const auto mode = tomb_raider_config_.mode_.load(std::memory_order_relaxed);
    if (configured != generation) {
      generation = configured;
      interval_due =
          std::chrono::steady_clock::now() +
          std::chrono::milliseconds(
              tomb_raider_config_.interval_ms_.load(std::memory_order_relaxed));
      daily_due = mode == TombRaiderMode::kDaily
                      ? NextDailyTime(tomb_raider_config_.daily_second_.load(
                            std::memory_order_relaxed))
                      : std::nullopt;
    }
    bool due = mode == TombRaiderMode::kInterval &&
               std::chrono::steady_clock::now() >= interval_due;
    if (mode == TombRaiderMode::kDaily && daily_due &&
        std::chrono::system_clock::now() >= *daily_due) {
      due = true;
      // A blocked daily occurrence is skipped, never queued for catch-up.
      daily_due = NextDailyTime(
          tomb_raider_config_.daily_second_.load(std::memory_order_relaxed));
    }
    if (due && !TombRaiderShouldForfeit()) {
      const auto result = co_await RunTombRaider();
      if (!result.ok()) {
        spdlog::error("tomb raider round failed: {}", result.message());
        // Memory pressure is retryable. Data/IO faults must not silently turn
        // a failed sweep into permission to reap or repeatedly scan bad media.
        if (!absl::IsResourceExhausted(result) && !absl::IsCancelled(result))
          LatchRuntimeFailure(*store);
      }
      interval_due =
          std::chrono::steady_clock::now() +
          std::chrono::milliseconds(
              tomb_raider_config_.interval_ms_.load(std::memory_order_relaxed));
      if (mode == TombRaiderMode::kDaily)
        daily_due = NextDailyTime(
            tomb_raider_config_.daily_second_.load(std::memory_order_relaxed));
      // A configuration update while the round ran restarts its new schedule
      // at this completion on the next iteration.
      continue;
    }
    auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
        kMaxScheduleSleep);
    if (mode == TombRaiderMode::kInterval && !due)
      delay = ScheduleSleep(interval_due - std::chrono::steady_clock::now());
    if (mode == TombRaiderMode::kDaily && daily_due)
      delay = ScheduleSleep(*daily_due - std::chrono::system_clock::now());
    LAVIK_CO_RETURN_IF_ERROR(co_await bycorf::SleepFor(*store->worker_, delay));
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RunTombRaider() {
  if (TombRaiderShouldForfeit()) {
    co_return absl::OkStatus();
  }
  bool expected = false;
  if (!tomb_raider_running_.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel)) {
    co_return absl::OkStatus();
  }
  // The round counts as a settlement: its frames park on cross-worker hops,
  // so the shutdown drain must not finish under it. In exchange, every
  // phase aborts at its next block/batch boundary once shutdown or population
  // replacement requests forfeiture. A later round rebuilds the proof.
  active_settlements_.fetch_add(1, std::memory_order_acq_rel);
  struct RoundGuard {
    std::atomic<bool>* running_;
    std::atomic<std::uint32_t>* settlements_;
    AsyncNotification* finished_;
    Worker* worker_;
    ~RoundGuard() {
      settlements_->fetch_sub(1, std::memory_order_acq_rel);
      running_->store(false, std::memory_order_release);
      finished_->NotifyAll(*worker_);
    }
  } round_guard{&tomb_raider_running_, &active_settlements_,
                &tomb_raider_round_finished_, bycorf::ThisWorker().self_};

  if (TombRaiderShouldForfeit()) {
    co_return absl::OkStatus();
  }

  TombRaiderRound round;
  round.population_generation_ =
      tomb_raider_population_generation_.load(std::memory_order_acquire);
  round.index_generation_ =
      tomb_raider_index_generation_.load(std::memory_order_acquire);
  for (std::uint8_t db = 0; db < options_.database_count_; ++db)
    round.db_epochs_[db] = DbEpoch(db);
  round.indexes_.resize(worker_count_);
  for (unsigned target = 0; target < worker_count_; ++target) {
    co_await bycorf::SubmitTo(target, [this, target, &round] {
      round.indexes_[target] = stores_[target]->index_generations_;
      return true;
    });
    if (!TombRaiderRoundValid(round)) co_return absl::OkStatus();
  }

  // The reap must not start until every worker's sweep has finished: the
  // record that still needs a candidate may sit in the last unswept block.
  for (unsigned target = 0; target < worker_count_; ++target) {
    LAVIK_CO_RETURN_IF_ERROR(co_await bycorf::SubmitTaskTo(
        target, [this, target, &round]() -> Task<absl::Status> {
          co_return co_await TombMarkLocal(*stores_[target], round);
        }));
    if (!TombRaiderRoundValid(round)) {
      co_return absl::OkStatus();
    }
  }
#if LAVIK_FAULTS_ENABLED
  if (tomb_raider_test_hook_) {
    LAVIK_CO_RETURN_IF_ERROR(
        co_await tomb_raider_test_hook_(TombRaiderTestPoint::kAfterMark));
    if (!TombRaiderRoundValid(round)) co_return absl::OkStatus();
  }
#endif
  for (unsigned target = 0; target < worker_count_; ++target) {
    LAVIK_CO_RETURN_IF_ERROR(co_await bycorf::SubmitTaskTo(
        target, [this, target, &round]() -> Task<absl::Status> {
          co_return co_await TombSweepLocal(*stores_[target], round);
        }));
    if (!TombRaiderRoundValid(round)) {
      co_return absl::OkStatus();
    }
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    LAVIK_CO_RETURN_IF_ERROR(co_await bycorf::SubmitTaskTo(
        target, [this, target, &round]() -> Task<absl::Status> {
          co_return co_await TombReapLocal(*stores_[target], round);
        }));
    if (!TombRaiderRoundValid(round)) {
      co_return absl::OkStatus();
    }
  }
  tomb_raider_rounds_.fetch_add(1, std::memory_order_relaxed);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombMarkLocal(WorkerStore& store,
                                                      TombRaiderRound& round) {
  std::size_t steps = 0;
  for (auto& partition : store.partitions_) {
    for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
      auto& index = partition.indexes_[db_id];
      if (index.empty()) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        if (!TombRaiderRoundValid(round, &store)) {
          co_return absl::OkStatus();  // forfeit the round
        }
        cursor = index.Scan(cursor, [](RecordIndex::Entry& entry) {
          if (entry.value_.kind() == RecordKind::kTombstone ||
              (entry.value_.kind() == RecordKind::kValue &&
               entry.value_.shielding())) {
            entry.value_.set_unclaimed(true);
          }
        });
        if (++steps % 256 == 0) {
          co_await bycorf::Yield(*store.worker_);
        }
      } while (cursor != 0);
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombClaimLocal(
    WorkerStore& store, TombRaiderRound& round, std::vector<TombClaim> claims) {
  std::size_t handled = 0;
  for (const TombClaim& claim : claims) {
    if (!TombRaiderRoundValid(round, &store)) {
      co_return absl::OkStatus();
    }
    auto& partition = PartitionForKey(store, claim.key_);
    if (partition.replication_epoch_ == claim.replication_epoch_) {
      auto resolved = co_await FindVerifiedEntry(
          store, partition.indexes_[claim.db_id_], claim.digest_, claim.key_);
#if LAVIK_FAULTS_ENABLED
      if (tomb_raider_test_hook_) {
        LAVIK_CO_RETURN_IF_ERROR(resolved);
        LAVIK_CO_RETURN_IF_ERROR(co_await tomb_raider_test_hook_(
            TombRaiderTestPoint::kAfterClaimLookup));
        // A fault hook is itself an async boundary. Ordinary writes need not
        // advance a population generation, so never retain its raw result.
        if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
        resolved = co_await FindVerifiedEntry(
            store, partition.indexes_[claim.db_id_], claim.digest_, claim.key_);
      }
#endif
      if (!resolved.ok()) {
        if (absl::IsAborted(resolved.status())) {
          round.cancelled_.store(true, std::memory_order_release);
          co_return absl::OkStatus();
        }

        co_return resolved.status();
      }
      if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
      if (partition.replication_epoch_ != claim.replication_epoch_) {
        round.cancelled_.store(true, std::memory_order_release);
        co_return absl::OkStatus();
      }
      auto* entry = *resolved;
      if (entry != nullptr && entry->value_.unclaimed() &&
          claim.mutation_sequence_ < entry->value_.mutation_sequence_) {
        entry->value_.set_unclaimed(false);
      }
    }
    if (++handled % 256 == 0) {
      co_await bycorf::Yield(*store.worker_);
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombSweepLocal(WorkerStore& store,
                                                       TombRaiderRound& round) {
  struct SweepBuffer {
    RegisteredBufferPool* pool_ = nullptr;
    std::uint16_t buffer_id_ = 0;
    std::byte* heap_data_ = nullptr;
    FixedBuffer buffer_{};

    ~SweepBuffer() {
      if (buffer_id_ != 0) {
        pool_->ReleaseWriteBuffer(buffer_id_);
      } else if (heap_data_ != nullptr) {
        pool_->ReleaseHeapWriteBuffer(heap_data_);
      }
    }

    bool registered() const noexcept {
      return buffer_id_ != 0 && pool_->buffers_registered();
    }
  } sweep{.pool_ = &store.buffers_};
  if (store.buffers_.TryAcquireWriteBuffer(&sweep.buffer_id_)) {
    sweep.buffer_ = store.buffers_.write_buffer(sweep.buffer_id_);
  } else if (store.buffers_.TryAcquireHeapWriteBuffer(&sweep.heap_data_)) {
    sweep.buffer_ = FixedBuffer{
        .data_ = sweep.heap_data_,
        .size_ = options_.buffers_.write_buffer_bytes_,
        .index_ = 0,
    };
  } else {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "failed to allocate a tomb raider sweep buffer");
  }
  if (sweep.buffer_.size_ < kStorageBlockBytes) {
    co_return absl::Status(absl::StatusCode::kResourceExhausted,
                           "tomb raider sweep buffer is smaller than a block");
  }
  sweep.buffer_.size_ = kStorageBlockBytes;

  struct BlockSnapshot {
    std::uint64_t block_id_ = 0;
    std::uint64_t allocation_epoch_ = 0;
  };
  // Blocks allocated after this snapshot cannot matter: new appends carry
  // higher sequences, and relocations only move index-current records — for
  // a key whose entry is a candidate, every older record is dead to the
  // index and is dropped by salvage, never moved.
  // A retiring block can already be absent from this runtime walk while
  // recovery still sees its allocated bitmap bit. Wait for its durable
  // retirement before taking the snapshot, without discarding this round's
  // marks. No worker-local state changes between the final check and the walk.
  while (store.pending_record_block_retirements_ != 0) {
    if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
#if LAVIK_FAULTS_ENABLED
    if (tomb_raider_test_hook_) {
      LAVIK_CO_RETURN_IF_ERROR(co_await tomb_raider_test_hook_(
          TombRaiderTestPoint::kBeforeRetirementWait));
    }
#endif
    LAVIK_CO_RETURN_IF_ERROR(co_await bycorf::SleepFor(
        *store.worker_, std::chrono::milliseconds(1)));
  }
  if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
  std::vector<BlockSnapshot> blocks;
  ForEachOwnedBlock(store, [&](std::uint64_t block_id, BlockState& state) {
    if (state.kind_ == BlockKind::kRecords &&
        state.committed_bytes_ > kBlockHeaderBytes) {
      blocks.push_back(BlockSnapshot{
          .block_id_ = block_id,
          .allocation_epoch_ = state.allocation_epoch_,
      });
    }
  });

  const auto present = [&](const BlockSnapshot& snapshot) {
    const auto* state = FindBlockState(store, snapshot.block_id_);
    return state != nullptr &&
           state->allocation_epoch_ == snapshot.allocation_epoch_ &&
           state->kind_ == BlockKind::kRecords && !state->freeing_;
  };
  // Defrag may retire any snapshot block while we yield. Preserve the scan's
  // progress and wait only for the durability evidence needed to skip it.
  // No source pin or storage/key/allocator lock is held across this wait.
  const auto await_block =
      [&](const BlockSnapshot& snapshot) -> Task<absl::StatusOr<bool>> {
    while (TombRaiderRoundValid(round, &store)) {
      const auto* state = FindBlockState(store, snapshot.block_id_);
      if (state != nullptr) {
        // Reuse is published only after the old bitmap clear is durable.
        if (state->allocation_epoch_ != snapshot.allocation_epoch_)
          co_return false;
        if (state->kind_ != BlockKind::kRecords) {
          // A kind change without a new allocation cannot prove retirement.
          round.cancelled_.store(true, std::memory_order_release);
          co_return false;
        }
        if (!state->freeing_) co_return true;
        // Freeing can wait for readers before incrementing the retirement
        // counter. A zero counter alone cannot certify this allocation gone.
      } else if (store.pending_record_block_retirements_ == 0) {
        co_return false;
      }
#if LAVIK_FAULTS_ENABLED
      if (tomb_raider_test_hook_) {
        LAVIK_CO_RETURN_IF_ERROR(co_await tomb_raider_test_hook_(
            TombRaiderTestPoint::kBeforeRetirementWait));
      }
#endif
      LAVIK_CO_RETURN_IF_ERROR(co_await bycorf::SleepFor(
          *store.worker_, std::chrono::milliseconds(1)));
    }
    co_return false;
  };

  std::vector<std::vector<TombClaim>> pending(worker_count_);
  auto flush_claims = [&](unsigned owner) -> Task<absl::Status> {
    if (!TombRaiderRoundValid(round, &store)) {
      pending[owner].clear();
      co_return absl::OkStatus();
    }
    std::vector<TombClaim> batch = std::move(pending[owner]);
    pending[owner].clear();
    if (batch.empty()) {
      co_return absl::OkStatus();
    }
    if (owner == store.worker_->id()) {
      co_return co_await TombClaimLocal(store, round, std::move(batch));
    }
    co_return co_await bycorf::SubmitTaskTo(
        owner,
        [this, owner, &round,
         batch = std::move(batch)]() mutable -> Task<absl::Status> {
          co_return co_await TombClaimLocal(*stores_[owner], round,
                                            std::move(batch));
        });
  };

  for (const BlockSnapshot& snapshot : blocks) {
    if (!TombRaiderRoundValid(round, &store)) {
      co_return absl::OkStatus();  // forfeit the round
    }
    bool retired = false;
    while (!present(snapshot)) {
      auto available = co_await await_block(snapshot);
      LAVIK_CO_RETURN_IF_ERROR(available);
      if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
      if (!*available) {
        retired = true;
        break;
      }
    }
    if (retired) continue;
    BlockState* state = FindBlockState(store, snapshot.block_id_);
    const std::uint32_t committed = state->committed_bytes_;
    if (committed <= kBlockHeaderBytes) {
      continue;
    }
    StagingSlot* slot = StagingFor(store, *state);
    if (slot != nullptr) {
      // Unflushed records exist only in the staging buffer; missing them
      // would let a still-dangerous key reap its tombstone. The copy runs
      // without suspending, so the captured prefix is consistent and the
      // slot cannot be released underneath it.
      FixedBuffer staged =
          slot->write_buffer_id_ != 0
              ? store.buffers_.write_buffer(slot->write_buffer_id_)
              : FixedBuffer{.data_ = slot->heap_data_,
                            .size_ = slot->heap_data_size_,
                            .index_ = 0};
      if (staged.data_ == nullptr || staged.size_ < committed) {
        continue;
      }
      std::memcpy(sweep.buffer_.data_, staged.data_, committed);
    } else {
      // Slotless blocks are fully flushed and never appended to again, so
      // the on-disk image below `committed` is final.
      const auto [file_id, block_offset] = FileOffset(snapshot.block_id_);
      auto read = co_await ReadStorageBuffer(
          *store.worker_, store.files_[file_id], sweep.buffer_,
          sweep.registered(), block_offset);
      LAVIK_CO_RETURN_IF_ERROR(read);
      if (*read != kStorageBlockBytes) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "short block read during tomb raider sweep");
      }
#if LAVIK_FAULTS_ENABLED
      if (tomb_raider_test_hook_) {
        LAVIK_CO_RETURN_IF_ERROR(co_await tomb_raider_test_hook_(
            TombRaiderTestPoint::kAfterSweepRead));
      }
#endif
      if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
      // The decode loop below rechecks physical retirement before touching
      // this copied image, just as it does after every asynchronous claim.
    }

    const std::uint64_t now_ms = UnixTimeMillis();
    std::uint32_t record_offset = kBlockHeaderBytes;
    std::size_t decoded = 0;
    while (record_offset < committed) {
      if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
      // The copied bytes may outlive their physical block across a claim/key
      // lookup. Its UUID registry may already be gone after reclamation.
      if (!present(snapshot)) {
        auto available = co_await await_block(snapshot);
        LAVIK_CO_RETURN_IF_ERROR(available);
        if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
        // Already issued prefix claims only keep tombstones alive. Retaining
        // them after durable block retirement is conservative; the unscanned
        // suffix is now unreachable by recovery and needs no further claims.
        if (!*available) break;
        continue;  // Re-resolve after the wait before decoding this record.
      }
      const std::optional<std::uint32_t> next =
          NextRecordOffset(sweep.buffer_.data_, record_offset, committed);
      if (!next.has_value()) {
        break;  // torn or foreign bytes: nothing decodable remains
      }
      if (*next != record_offset) {
        record_offset = *next;
        continue;
      }
      RecordHeader record{};
      std::string_view disk_key;
      std::span<const std::byte> record_bytes(
          sweep.buffer_.data_ + record_offset, committed - record_offset);
      if (!DecodeRecordHeader(record_bytes, &record, &disk_key) ||
          record.allocation_epoch_ != snapshot.allocation_epoch_ ||
          record.total_disk_bytes_ == 0 ||
          record_offset + record.total_disk_bytes_ > committed) {
        break;
      }
      record_offset += record.total_disk_bytes_;
      // Only a value that could still be alive claims its key's candidate:
      // expired values are suppressed by their own timestamp, tombstones
      // and commit records suppress nothing worth keeping a marker for,
      // and records from a flushed database epoch are already condemned.
      if (record.kind_ == RecordKind::kValue &&
          record.db_id_ < options_.database_count_ &&
          record.db_epoch_ == round.db_epochs_[record.db_id_] &&
          (record.expire_at_ms_ == 0 || record.expire_at_ms_ > now_ms)) {
        std::string loaded_key;
        if (record.key_indirect_) [[unlikely]] {
          // Resolve on this physical owner before yielding. The per-block
          // reference owns the handle; looking up its UUID on another worker
          // could race retirement of this already-copied source block.
          const auto refs = store.indirect_key_references_.find(
              {snapshot.block_id_, snapshot.allocation_epoch_});
          if (refs == store.indirect_key_references_.end())
            co_return absl::DataLossError(
                "indirect key block has no references");
          const auto ref =
              refs->second.find(record_offset - record.total_disk_bytes_);
          if (ref == refs->second.end())
            co_return absl::DataLossError("indirect key record has no UUID");
          auto original = co_await LoadIndirectKey(ref->second);
          if (!original.ok()) {
            if (absl::IsAborted(original.status())) {
              round.cancelled_.store(true, std::memory_order_release);
              co_return absl::OkStatus();
            }
            co_return original.status();
          }
          if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
          loaded_key = std::move(*original);
          if (loaded_key.size() != record.key_bytes_)
            co_return absl::DataLossError("indirect key length mismatch");
          disk_key = loaded_key;
        }

        const unsigned key_owner = OwnerForKey(disk_key);
        pending[key_owner].push_back(TombClaim{
            .mutation_sequence_ = record.mutation_sequence_,
            .replication_epoch_ = record.replication_epoch_,
            .digest_ = ComputeDigest(disk_key),
            .key_ = std::string(disk_key),
            .db_id_ = record.db_id_,
        });
        if (pending[key_owner].size() >= 512) {
          LAVIK_CO_RETURN_IF_ERROR(co_await flush_claims(key_owner));
        }
      }
      if (++decoded % 256 == 0) {
        if (!TombRaiderRoundValid(round, &store)) {
          co_return absl::OkStatus();
        }
        co_await bycorf::Yield(*store.worker_);
      }
    }
    // Throttle: one block per sleep bounds the sweep's disk-bandwidth and
    // CPU share, so a full-disk round never crowds out online traffic.
    const std::uint32_t block_sleep_ms =
        tomb_raider_config_.block_sleep_ms_.load(std::memory_order_relaxed);
    auto sleep_remaining = std::chrono::milliseconds(block_sleep_ms);
    while (sleep_remaining > std::chrono::milliseconds::zero()) {
      if (!TombRaiderRoundValid(round, &store)) {
        co_return absl::OkStatus();
      }
      const auto sleep_chunk =
          std::min(sleep_remaining, kMaxForfeitCheckpointSleep);
      LAVIK_CO_RETURN_IF_ERROR(
          co_await bycorf::SleepFor(*store.worker_, sleep_chunk));
      sleep_remaining -= sleep_chunk;
    }
  }
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    if (!TombRaiderRoundValid(round, &store)) {
      co_return absl::OkStatus();
    }
    LAVIK_CO_RETURN_IF_ERROR(co_await flush_claims(owner));
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombReapLocal(WorkerStore& store,
                                                      TombRaiderRound& round) {
  struct Candidate {
    Digest digest_{};
    std::string key_;
    RecordLocation location_{};
    std::uint32_t key_bytes_ = 0;
    std::uint8_t db_id_ = 0;
    std::uint64_t replication_epoch_ = 0;
  };
  std::vector<Candidate> tombs;
  std::uint64_t refreshed = 0;
  std::uint64_t reaped = 0;
  struct PublishTotals {
    Impl* impl_;
    const std::uint64_t& refreshed_;
    const std::uint64_t& reaped_;
    ~PublishTotals() {
      impl_->tomb_raider_refreshed_.fetch_add(refreshed_,
                                              std::memory_order_relaxed);
      impl_->tomb_raider_reaped_.fetch_add(reaped_, std::memory_order_relaxed);
    }
  } totals{this, refreshed, reaped};
  std::size_t steps = 0;
  for (auto& partition : store.partitions_) {
    for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
      auto& index = partition.indexes_[db_id];
      if (index.empty()) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        if (!TombRaiderRoundValid(round, &store)) {
          co_return absl::OkStatus();
        }
        cursor = index.Scan(cursor, [&](RecordIndex::Entry& entry) {
          if (!entry.value_.unclaimed()) {
            return;
          }
          if (entry.value_.kind() == RecordKind::kValue) {
            // The sweep found nothing this value was shielding: the sticky
            // bit outlived whatever it once protected. Its next expiry can
            // take the in-memory path.
            entry.value_.set_unclaimed(false);
            if (entry.value_.shielding()) {
              entry.value_.set_shielding(false);
              ++refreshed;
            }
          } else if (entry.value_.kind() == RecordKind::kTombstone) {
            tombs.push_back(Candidate{
                .digest_ = entry.key_complete() ? ComputeDigest(entry.key())
                                                : entry.external_key_digest(),
                .key_ = entry.key_complete() ? std::string(entry.key())
                                             : std::string{},
                .location_ = MaterializeIndexLocation(entry),
                .key_bytes_ = entry.logical_key_size(),
                .db_id_ = db_id,
                .replication_epoch_ = partition.replication_epoch_,
            });
          }
        });
        if (++steps % 256 == 0) {
          co_await bycorf::Yield(*store.worker_);
        }
      } while (cursor != 0);
    }
  }

  for (Candidate& candidate : tombs) {
    if (!TombRaiderRoundValid(round, &store)) {
      break;  // forfeit the rest; totals below still publish
    }
    if (candidate.key_.empty() && candidate.key_bytes_ != 0) [[unlikely]] {
#if LAVIK_FAULTS_ENABLED
      if (tomb_raider_test_hook_) {
        LAVIK_CO_RETURN_IF_ERROR(co_await tomb_raider_test_hook_(
            TombRaiderTestPoint::kBeforeReapKeyLoad));
      }
#endif
      if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
      auto key = co_await LoadOutOfIndexKey(store, candidate.location_,
                                            candidate.key_bytes_);
#if LAVIK_FAULTS_ENABLED
      if (tomb_raider_test_hook_) {
        LAVIK_CO_RETURN_IF_ERROR(co_await tomb_raider_test_hook_(
            TombRaiderTestPoint::kAfterReapKeyLoad));
      }
#endif
      if (!key.ok()) {
        if (absl::IsAborted(key.status())) {
          round.cancelled_.store(true, std::memory_order_release);
          co_return absl::OkStatus();
        }

        co_return key.status();
      }
      if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
      candidate.key_ = std::move(*key);
    }
#if LAVIK_FAULTS_ENABLED
    if (tomb_raider_test_hook_) {
      LAVIK_CO_RETURN_IF_ERROR(co_await tomb_raider_test_hook_(
          TombRaiderTestPoint::kBeforeReapLock));
    }
#endif
    if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        candidate.db_id_, tx::FingerprintOf(candidate.digest_),
        tx::LockMode::kExclusive);
    if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
    auto& partition = PartitionForKey(store, candidate.key_);
    auto resolved =
        co_await FindVerifiedEntry(store, partition.indexes_[candidate.db_id_],
                                   candidate.digest_, candidate.key_);
    LAVIK_CO_RETURN_IF_ERROR(resolved);
    if (!TombRaiderRoundValid(round, &store)) co_return absl::OkStatus();
    if (partition.replication_epoch_ != candidate.replication_epoch_) {
      round.cancelled_.store(true, std::memory_order_release);
      co_return absl::OkStatus();
    }
    auto* entry = *resolved;
    if (entry == nullptr || entry->value_.kind() != RecordKind::kTombstone ||
        !entry->value_.unclaimed() ||
        !MaterializeIndexLocation(*entry).SamePhysicalRecord(
            candidate.location_)) {
      continue;  // rewritten or claimed since collection
    }
    const RecordLocation dropped = MaterializeIndexLocation(*entry);
    const ExtentManifest dropped_dependent_extents = ExtentManifest{};
    // No watcher or replica cares: erasing a tombstone changes nothing a
    // reader can observe. A staged physical record retains only address bits;
    // flush completion rejects them after Erase removes the live bucket slot.
    store.external_manifests_.erase(entry);
    const std::size_t logical_key_bytes = entry->logical_key_size();
    const bool erased = partition.indexes_[candidate.db_id_].Erase(entry);
    assert(erased);
    if (erased) {
      RemoveFullSyncCoverageEntry(partition, candidate.db_id_,
                                  logical_key_bytes);
    }
    // Erase transferred retirement ownership to us. Finish this settlement
    // even if FLUSH advances an epoch while the cross-owner hop is pending.
    absl::Status dead = co_await MarkRecordDead(
        RetiredRecordOf(dropped, dropped_dependent_extents));
    if (!dead.ok()) {
      LatchRuntimeFailure(store);
      co_return dead;
    }
    ++reaped;
    co_await bycorf::Yield(*store.worker_);
  }
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
