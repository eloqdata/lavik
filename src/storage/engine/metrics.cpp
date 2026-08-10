#include <sys/statvfs.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "impl.h"

namespace keylane::storage {

Task<StorageMetricsSnapshot> StorageEngine::Impl::CollectMetrics() const {
  struct AllocatorMetrics {
    std::uint64_t available_blocks_ = 0;
  };

  StorageMetricsSnapshot result;
  result.devices_.reserve(devices_.size());
  for (std::size_t index = 0; index < devices_.size(); ++index) {
    const StorageDevice& device = devices_[index];
    const celer::WorkerId owner = device_allocators_[index]->owner_;
    const AllocatorMetrics allocator =
        co_await celer::SubmitTo(owner, [this, index] {
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
      struct statvfs filesystem {};
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

}  // namespace keylane::storage
