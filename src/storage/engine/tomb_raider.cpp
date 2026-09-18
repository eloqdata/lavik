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

namespace lavik::storage {

namespace {

constexpr auto kMaxScheduleSleep = std::chrono::minutes(1);
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

Task<absl::Status> StorageEngine::QuiesceTombRaiderForReplica() {
  return impl_->QuiesceTombRaiderForReplica();
}

Task<absl::Status> StorageEngine::Impl::ConfigureTombRaider(
    TombRaiderConfigUpdate update) {
  co_return co_await bycorf::SubmitTo(
      0, [this, update] { return ApplyTombRaiderConfig(*stores_[0], update); });
}

Task<absl::Status> StorageEngine::Impl::QuiesceTombRaiderForReplica() {
  co_return co_await bycorf::SubmitTaskTo(0, [this]() -> Task<absl::Status> {
    WorkerStore& coordinator = *stores_[0];
    while (tomb_raider_quiescing_) {
      absl::Status waited = co_await bycorf::SleepFor(
          *coordinator.worker_, std::chrono::milliseconds(1));
      if (!waited.ok()) co_return waited;
    }

    tomb_raider_quiescing_ = true;
    tomb_raider_forfeit_requested_.store(true, std::memory_order_release);
    struct QuiesceGuard {
      bool* quiescing_;
      std::atomic<bool>* forfeit_requested_;
      ~QuiesceGuard() {
        forfeit_requested_->store(false, std::memory_order_release);
        *quiescing_ = false;
      }
    } quiesce_guard{&tomb_raider_quiescing_, &tomb_raider_forfeit_requested_};

    absl::Status disabled = ApplyTombRaiderConfig(
        coordinator,
        TombRaiderConfigUpdate{.action_ = TombRaiderConfigAction::kOff});
    if (!disabled.ok()) co_return disabled;
    while (tomb_raider_running_.load(std::memory_order_acquire)) {
      co_await tomb_raider_round_finished_.Wait();
    }
    co_return absl::OkStatus();
  });
}

absl::Status StorageEngine::Impl::ApplyTombRaiderConfig(
    WorkerStore& coordinator, TombRaiderConfigUpdate update) {
  const bool needs_authority =
      update.action_ == TombRaiderConfigAction::kOn ||
      update.action_ == TombRaiderConfigAction::kInterval ||
      update.action_ == TombRaiderConfigAction::kDaily;
  if (needs_authority && tomb_raider_quiescing_) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "tomb raider is quiescing for replica reset");
  }
  if (needs_authority &&
      !expiration_authority_.load(std::memory_order_acquire)) {
    return absl::Status(absl::StatusCode::kFailedPrecondition,
                        "tomb raider is unavailable on this server");
  }

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
  const std::uint64_t generation =
      tomb_raider_config_.generation_.fetch_add(1, std::memory_order_acq_rel) +
      1;
  if (mode != TombRaiderMode::kOff) {
    coordinator.worker_->SpawnBackground(
        TombRaiderLoop(&coordinator, generation));
  }
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombRaiderLoop(
    WorkerStore* store, std::uint64_t generation) {
  while (!store->worker_->stop_requested()) {
    if (store->worker_->stop_requested() ||
        shutdown_flush_requested_.load(std::memory_order_acquire)) {
      break;
    }
    if (generation !=
        tomb_raider_config_.generation_.load(std::memory_order_acquire)) {
      break;
    }
    const TombRaiderMode mode =
        tomb_raider_config_.mode_.load(std::memory_order_relaxed);
    if (mode == TombRaiderMode::kOff) {
      break;
    }
    if (tomb_raider_running_.load(std::memory_order_acquire)) {
      co_await tomb_raider_round_finished_.Wait();
      continue;
    }

    if (mode == TombRaiderMode::kInterval) {
      const std::uint64_t interval =
          tomb_raider_config_.interval_ms_.load(std::memory_order_relaxed);
      if (interval == 0 ||
          interval > static_cast<std::uint64_t>(
                         std::chrono::milliseconds::max().count())) {
        co_return absl::Status(absl::StatusCode::kInvalidArgument,
                               "invalid tomb raider interval");
      }
      const auto due = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(interval);
      while (std::chrono::steady_clock::now() < due) {
        absl::Status waited = co_await bycorf::SleepFor(
            *store->worker_,
            ScheduleSleep(due - std::chrono::steady_clock::now()));
        if (!waited.ok()) {
          co_return waited;
        }
        if (generation !=
            tomb_raider_config_.generation_.load(std::memory_order_acquire)) {
          co_return absl::OkStatus();
        }
      }
    } else {
      const auto due = NextDailyTime(
          tomb_raider_config_.daily_second_.load(std::memory_order_relaxed));
      if (!due.has_value()) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "failed to calculate tomb raider daily time");
      }
      while (std::chrono::system_clock::now() < *due) {
        absl::Status waited = co_await bycorf::SleepFor(
            *store->worker_,
            ScheduleSleep(*due - std::chrono::system_clock::now()));
        if (!waited.ok()) {
          co_return waited;
        }
        if (generation !=
            tomb_raider_config_.generation_.load(std::memory_order_acquire)) {
          co_return absl::OkStatus();
        }
      }
    }

    if (generation !=
        tomb_raider_config_.generation_.load(std::memory_order_acquire)) {
      break;
    }
    absl::Status round = co_await RunTombRaider();
    if (!round.ok()) {
      spdlog::error("tomb raider round failed: {}", round.message());
      co_return round;
    }
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
  // phase aborts at its next block/batch boundary once shutdown or replica
  // quiesce requests forfeiture. A forfeited round costs nothing; a later
  // primary may explicitly configure a fresh schedule and redo it.
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

  // The reap must not start until every worker's sweep has finished: the
  // record that still needs a candidate may sit in the last unswept block.
  for (unsigned target = 0; target < worker_count_; ++target) {
    absl::Status marked = co_await bycorf::SubmitTaskTo(
        target, [this, target]() -> Task<absl::Status> {
          co_return co_await TombMarkLocal(*stores_[target]);
        });
    if (!marked.ok()) {
      co_return marked;
    }
    if (TombRaiderShouldForfeit()) {
      co_return absl::OkStatus();
    }
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    absl::Status swept = co_await bycorf::SubmitTaskTo(
        target, [this, target]() -> Task<absl::Status> {
          co_return co_await TombSweepLocal(*stores_[target]);
        });
    if (!swept.ok()) {
      co_return swept;
    }
    if (TombRaiderShouldForfeit()) {
      co_return absl::OkStatus();
    }
  }
  for (unsigned target = 0; target < worker_count_; ++target) {
    absl::Status reaped = co_await bycorf::SubmitTaskTo(
        target, [this, target]() -> Task<absl::Status> {
          co_return co_await TombReapLocal(*stores_[target]);
        });
    if (!reaped.ok()) {
      co_return reaped;
    }
    if (TombRaiderShouldForfeit()) {
      co_return absl::OkStatus();
    }
  }
  tomb_raider_rounds_.fetch_add(1, std::memory_order_relaxed);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombMarkLocal(WorkerStore& store) {
  std::size_t steps = 0;
  for (auto& partition : store.partitions_) {
    for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
      auto& index = partition.indexes_[db_id];
      if (index.empty()) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        if (TombRaiderShouldForfeit()) {
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
    WorkerStore& store, std::vector<TombClaim> claims) {
  std::size_t handled = 0;
  for (const TombClaim& claim : claims) {
    if (TombRaiderShouldForfeit()) {
      co_return absl::OkStatus();
    }
    auto& partition = PartitionForKey(store, claim.key_);
    if (partition.replication_epoch_ == claim.replication_epoch_) {
      auto resolved = co_await FindVerifiedEntry(
          store, partition.indexes_[claim.db_id_], claim.digest_, claim.key_);
      if (!resolved.ok()) {
        co_return resolved.status();
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

Task<absl::Status> StorageEngine::Impl::TombSweepLocal(WorkerStore& store) {
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

  std::vector<std::vector<TombClaim>> pending(worker_count_);
  auto flush_claims = [&](unsigned owner) -> Task<absl::Status> {
    if (TombRaiderShouldForfeit()) {
      pending[owner].clear();
      co_return absl::OkStatus();
    }
    std::vector<TombClaim> batch = std::move(pending[owner]);
    pending[owner].clear();
    if (batch.empty()) {
      co_return absl::OkStatus();
    }
    if (owner == store.worker_->id()) {
      co_return co_await TombClaimLocal(store, std::move(batch));
    }
    co_return co_await bycorf::SubmitTaskTo(
        owner,
        [this, owner,
         batch = std::move(batch)]() mutable -> Task<absl::Status> {
          co_return co_await TombClaimLocal(*stores_[owner], std::move(batch));
        });
  };

  for (const BlockSnapshot& snapshot : blocks) {
    if (TombRaiderShouldForfeit()) {
      co_return absl::OkStatus();  // forfeit the round
    }
    BlockState* state = FindBlockState(store, snapshot.block_id_);
    if (state == nullptr || !state->allocated_ ||
        state->allocation_epoch_ != snapshot.allocation_epoch_ ||
        state->kind_ != BlockKind::kRecords) {
      continue;  // freed or reused: its old records left the disk with it
    }
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
      if (!read.ok()) {
        co_return read.status();
      }
      if (*read != kStorageBlockBytes) {
        co_return absl::Status(absl::StatusCode::kInternal,
                               "short block read during tomb raider sweep");
      }
      BlockState* current = FindBlockState(store, snapshot.block_id_);
      if (current == nullptr || !current->allocated_ ||
          current->allocation_epoch_ != snapshot.allocation_epoch_) {
        continue;  // reclaimed while the read was in flight
      }
    }

    const std::uint64_t now_ms = UnixTimeMillis();
    std::uint32_t record_offset = kBlockHeaderBytes;
    std::size_t decoded = 0;
    while (record_offset < committed) {
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
          record.db_epoch_ == DbEpoch(record.db_id_) &&
          (record.expire_at_ms_ == 0 || record.expire_at_ms_ > now_ms)) {
        const std::byte* payload_data = sweep.buffer_.data_ + record_offset -
                                        record.total_disk_bytes_ +
                                        record.header_bytes_;
        const auto payload =
            std::span<const std::byte>(payload_data, record.payload_bytes_);
        std::string loaded_key;
        if (record.key_external_) [[unlikely]] {
          if (record.external_) {
            auto manifest =
                DecodeManifest(payload,
                               static_cast<std::uint64_t>(record.key_bytes_) +
                                   record.logical_size_,
                               record.value_type_ == ValueType::kString);
            if (!manifest.ok()) {
              co_return manifest.status();
            }
            auto external_key =
                co_await LoadExternalKey(store, *manifest, record.key_bytes_);
            if (!external_key.ok()) {
              co_return external_key.status();
            }
            loaded_key = std::move(*external_key);
            disk_key = loaded_key;
          } else {
            if (record.payload_bytes_ < record.key_bytes_) {
              co_return absl::Status(absl::StatusCode::kInternal,
                                     "inline external key is truncated");
            }
            disk_key = std::string_view(
                reinterpret_cast<const char*>(payload_data), record.key_bytes_);
          }
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
          absl::Status flushed = co_await flush_claims(key_owner);
          if (!flushed.ok()) {
            co_return flushed;
          }
        }
      }
      if (++decoded % 256 == 0) {
        if (TombRaiderShouldForfeit()) {
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
      if (TombRaiderShouldForfeit()) {
        co_return absl::OkStatus();
      }
      const auto sleep_chunk =
          std::min(sleep_remaining, kMaxForfeitCheckpointSleep);
      absl::Status slept =
          co_await bycorf::SleepFor(*store.worker_, sleep_chunk);
      if (!slept.ok()) {
        co_return slept;
      }
      sleep_remaining -= sleep_chunk;
    }
  }
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    if (TombRaiderShouldForfeit()) {
      co_return absl::OkStatus();
    }
    absl::Status flushed = co_await flush_claims(owner);
    if (!flushed.ok()) {
      co_return flushed;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::TombReapLocal(WorkerStore& store) {
  struct Candidate {
    Digest digest_{};
    std::string key_;
    ExtentManifest extents_;
    RecordLocation location_{};
    std::uint32_t key_bytes_ = 0;
    std::uint8_t db_id_ = 0;
  };
  std::vector<Candidate> tombs;
  std::uint64_t refreshed = 0;
  std::size_t steps = 0;
  for (auto& partition : store.partitions_) {
    for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
      auto& index = partition.indexes_[db_id];
      if (index.empty()) {
        continue;
      }
      std::uint64_t cursor = 0;
      do {
        if (TombRaiderShouldForfeit()) {
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
                .extents_ = DependentExtentsFor(store, &entry),
                .location_ = MaterializeIndexLocation(entry),
                .key_bytes_ = entry.logical_key_size(),
                .db_id_ = db_id,
            });
          }
        });
        if (++steps % 256 == 0) {
          co_await bycorf::Yield(*store.worker_);
        }
      } while (cursor != 0);
    }
  }

  std::uint64_t reaped = 0;
  for (Candidate& candidate : tombs) {
    if (TombRaiderShouldForfeit()) {
      break;  // forfeit the rest; totals below still publish
    }
    if (candidate.key_.empty() && candidate.key_bytes_ != 0) [[unlikely]] {
      auto key = co_await LoadOutOfIndexKey(
          store, candidate.location_, candidate.extents_, candidate.key_bytes_);
      if (!key.ok()) {
        co_return key.status();
      }
      candidate.key_ = std::move(*key);
    }
    auto key_lock = co_await tx::CurrentTxShard().AcquireKey(
        candidate.db_id_, tx::FingerprintOf(candidate.digest_),
        tx::LockMode::kExclusive);
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
    auto& partition = PartitionForKey(store, candidate.key_);
    auto resolved =
        co_await FindVerifiedEntry(store, partition.indexes_[candidate.db_id_],
                                   candidate.digest_, candidate.key_);
    if (!resolved.ok()) {
      co_return resolved.status();
    }
    auto* entry = *resolved;
    if (entry == nullptr || entry->value_.kind() != RecordKind::kTombstone ||
        !entry->value_.unclaimed()) {
      continue;  // rewritten or claimed since collection
    }
    const RecordLocation dropped = MaterializeIndexLocation(*entry);
    const ExtentManifest dropped_dependent_extents =
        DependentExtentsFor(store, entry);
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
    absl::Status dead = co_await MarkRecordDead(
        RetiredRecordOf(dropped, dropped_dependent_extents));
    if (!dead.ok()) {
      LatchRuntimeFailure(store);
      co_return dead;
    }
    ++reaped;
    co_await bycorf::Yield(*store.worker_);
  }
  if (reaped != 0) {
    tomb_raider_reaped_.fetch_add(reaped, std::memory_order_relaxed);
  }
  if (refreshed != 0) {
    tomb_raider_refreshed_.fetch_add(refreshed, std::memory_order_relaxed);
  }
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
