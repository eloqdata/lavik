#include "celer/io/backend_options.h"

#include <atomic>
#include <mutex>

namespace celer {
namespace detail {
IoBackends io_backends;
}
namespace {
std::mutex options_mutex;
std::atomic<bool> frozen{false};
}  // namespace
absl::Status ConfigureIoBackends(IoBackends options) {
  std::lock_guard lock(options_mutex);
#ifndef CELER_WITH_DPDK
  if (options.dpdk_network)
    return absl::UnimplementedError("DPDK network support is not compiled in");
#endif
#ifndef CELER_WITH_SPDK_STORAGE
  if (options.spdk_storage)
    return absl::UnimplementedError("SPDK storage support is not compiled in");
#endif
  if (frozen.load(std::memory_order_relaxed)) {
    if (options == detail::io_backends) return absl::OkStatus();
    return absl::FailedPreconditionError(
        "I/O backends are fixed for the process lifetime");
  }
  detail::io_backends = options;
  return absl::OkStatus();
}
void FreezeIoBackends() {
  // Buffer allocation can occur on an I/O path. Once published, selection is
  // immutable: acquire its visibility without contending on a global mutex.
  if (frozen.load(std::memory_order_acquire)) return;
  std::lock_guard lock(options_mutex);
  frozen.store(true, std::memory_order_release);
}
}  // namespace celer
