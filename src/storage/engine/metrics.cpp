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

#include <sys/statvfs.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "impl.h"

namespace lavik::storage {

std::uint64_t StorageEngine::Impl::LocalDirtyStagingBytes() const {
  std::uint64_t dirty = 0;
  // Run without suspension on the owner, avoiding store locks and additional
  // accounting on the append/flush paths. Released slots do not contribute.
  for (const StagingSlot& slot : CurrentStore().staging_slots_) {
    if (slot.write_buffer_id_ == 0) continue;
    if (slot.committed_bytes_ >= slot.durable_bytes_) {
      dirty += slot.committed_bytes_ - slot.durable_bytes_;
    }
  }
  return dirty;
}

void StorageEngine::Impl::UpdateDurabilityPending(
    StorageDurabilityStats& stats) const {
  // Sample before and after visiting workers. A registered commit may finish
  // and append on an already-visited worker; retaining the earlier nonzero
  // count keeps the observation conservative until the next dirty-byte scan.
  stats.flushes_pending_ = std::max(
      stats.flushes_pending_, active_flushes_.load(std::memory_order_acquire));
  stats.tx_commits_pending_ =
      std::max(stats.tx_commits_pending_,
               active_tx_commits_.load(std::memory_order_acquire));
}

Task<StorageDurabilityStats> StorageEngine::Impl::DurabilityStats() const {
  StorageDurabilityStats result;
  UpdateDurabilityPending(result);
  for (unsigned target = 0; target < worker_count_; ++target) {
    result.dirty_staging_bytes_ += co_await bycorf::SubmitTo(
        target, [this] { return LocalDirtyStagingBytes(); });
  }
  UpdateDurabilityPending(result);
  co_return result;
}

Task<StorageDiagnosticsSnapshot> StorageEngine::Impl::CollectDiagnostics(
    std::vector<StorageReplicationLogMetrics>* replication_logs) const {
  StorageDiagnosticsSnapshot result;
  result.tx_cleaner_ = TxCleanerStats();
  result.tx_commit_batches_ = TxCommitBatchStats();
  if (replication_logs != nullptr) {
    replication_logs->clear();
    replication_logs->reserve(worker_count_);
  }
  UpdateDurabilityPending(result.durability_);
  for (unsigned target = 0; target < worker_count_; ++target) {
    // The collector exclusively owns this output until SubmitTo completes.
    // Reserve before visiting workers and never mutate the vector while its
    // element is borrowed, so the callback can fill it without another copy.
    auto* replication = replication_logs == nullptr
                            ? nullptr
                            : &replication_logs->emplace_back();
    result.durability_.dirty_staging_bytes_ +=
        co_await bycorf::SubmitTo(target, [this, target, replication] {
          const std::uint64_t dirty = LocalDirtyStagingBytes();
          if (replication != nullptr) {
            const ReplicationLogInfo log = LocalReplicationLogInfo();
            *replication = StorageReplicationLogMetrics{
                .worker_id_ = target,
                .floor_lsn_ = log.floor_lsn_,
                .tail_lsn_ = log.tail_lsn_,
                .coverage_revocations_ = log.coverage_revocations_,
                .backpressure_waits_ = log.backpressure_waits_,
                .chunk_count_ = log.block_count_,
                .capacity_bytes_ = log.capacity_bytes_,
                .publish_queue_bytes_ = log.publish_queue_bytes_,
                .publish_queue_capacity_bytes_ =
                    log.publish_queue_capacity_bytes_,
                .fullsync_publish_queue_bytes_ =
                    log.fullsync_publish_queue_bytes_,
                .fullsync_publisher_admitted_bytes_ =
                    log.fullsync_publisher_admitted_bytes_,
                .fullsync_publish_queue_capacity_bytes_ =
                    log.fullsync_publish_queue_capacity_bytes_,
                .fullsync_session_count_ = log.fullsync_session_count_,
                .pinned_cursors_ = log.retained_cursor_count_,
                .fullsync_backpressure_waits_ =
                    log.fullsync_backpressure_waits_,
                .active_ = log.state_ == ReplicationLogState::kActive,
                .backpressured_ = log.capacity_backpressured_,
            };
          }
          return dirty;
        });
  }
  UpdateDurabilityPending(result.durability_);
  result.expiration_pause_count_ = ExpirationPauseCount();
  co_return result;
}

Task<StorageMetricsSnapshot> StorageEngine::Impl::CollectMetrics() const {
  struct AllocatorMetrics {
    std::uint64_t available_blocks_ = 0;
  };

  StorageMetricsSnapshot result;
  result.diagnostics_ = co_await CollectDiagnostics(&result.replication_logs_);
  result.devices_.reserve(devices_.size());
  for (std::size_t index = 0; index < devices_.size(); ++index) {
    const StorageDevice& device = devices_[index];
    const bycorf::WorkerId owner = device_allocators_[index]->owner_;
    const AllocatorMetrics allocator =
        co_await bycorf::SubmitTo(owner, [this, index] {
          const DeviceAllocator& state = *device_allocators_[index];
          const StorageDevice& device = devices_[index];
          const std::uint64_t pristine =
              state.next_pristine_ < device.capacity_blocks_
                  ? device.capacity_blocks_ - state.next_pristine_
                  : 0;
          return AllocatorMetrics{
              .available_blocks_ = pristine + state.ready_blocks_.size() +
                                   state.cold_free_.size(),
          };
        });
    const std::uint64_t reserve = DefragReserveForDevice(index);
    const std::uint64_t foreground_blocks =
        allocator.available_blocks_ > reserve
            ? allocator.available_blocks_ - reserve
            : 0;
    StorageDeviceMetrics metrics{
        .path_ = device.path_,
        .device_id_ = device.id_,
        .capacity_bytes_ = device.data_block_count_ * kStorageBlockBytes,
        .available_bytes_ = foreground_blocks * kStorageBlockBytes,
        .filesystem_available_bytes_ = std::nullopt,
    };
    if (!device.is_block_device_) {
      struct statvfs filesystem{};
      if (::statvfs(device.path_.c_str(), &filesystem) == 0) {
        metrics.filesystem_available_bytes_ =
            static_cast<std::uint64_t>(filesystem.f_bavail) *
            static_cast<std::uint64_t>(filesystem.f_frsize);
      }
    }
    result.devices_.push_back(std::move(metrics));
  }
  co_return result;
}

}  // namespace lavik::storage
