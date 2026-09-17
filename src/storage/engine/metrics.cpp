#include <sys/statvfs.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "impl.h"

namespace keylane::storage {

Task<StorageDurabilityStats> StorageEngine::Impl::DurabilityStats() const {
  StorageDurabilityStats result{
      .dirty_staging_bytes_ = 0,
      .flushes_pending_ = active_flushes_.load(std::memory_order_acquire),
      // Sample before visiting workers. A commit chain registered before the
      // INFO request may finish and append on a worker already visited; the
      // initial nonzero sample keeps this observation conservative, and the
      // following poll will see the appended record's dirty bytes.
      .tx_commits_pending_ = active_tx_commits_.load(std::memory_order_acquire),
  };
  for (unsigned target = 0; target < worker_count_; ++target) {
    result.dirty_staging_bytes_ +=
        co_await bycorf::SubmitTo(target, [this, target] {
          const WorkerStore& store = *stores_[target];
          std::uint64_t dirty = 0;
          // This non-suspending callback runs on the owning worker, so no
          // store mutex or hot-path atomics are needed for the local snapshot.
          for (const StagingSlot& slot : store.staging_slots_) {
            const bool allocated =
                slot.write_buffer_id_ != 0 || slot.heap_data_ != nullptr;
            if (!allocated) continue;
            if (slot.committed_bytes_ >= slot.durable_bytes_) {
              dirty += slot.committed_bytes_ - slot.durable_bytes_;
            }
          }
          return dirty;
        });
  }
  result.flushes_pending_ = std::max(
      result.flushes_pending_, active_flushes_.load(std::memory_order_acquire));
  result.tx_commits_pending_ =
      std::max(result.tx_commits_pending_,
               active_tx_commits_.load(std::memory_order_acquire));
  co_return result;
}

Task<StorageMetricsSnapshot> StorageEngine::Impl::CollectMetrics() const {
  struct AllocatorMetrics {
    std::uint64_t available_blocks_ = 0;
  };

  StorageMetricsSnapshot result;
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
  result.replication_logs_.reserve(worker_count_);
  for (unsigned target = 0; target < worker_count_; ++target) {
    result.replication_logs_.push_back(
        co_await bycorf::SubmitTo(target, [this, target] {
          const ReplicationLogInfo log = LocalReplicationLogInfo();
          return StorageReplicationLogMetrics{
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
              .fullsync_backpressure_waits_ = log.fullsync_backpressure_waits_,
              .active_ = log.state_ == ReplicationLogState::kActive,
              .backpressured_ = log.capacity_backpressured_,
          };
        }));
  }
  co_return result;
}

}  // namespace keylane::storage
