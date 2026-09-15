#ifndef CELER_IO_BACKEND_OPTIONS_H_
#define CELER_IO_BACKEND_OPTIONS_H_

#include "absl/status/status.h"

namespace celer {
struct IoBackends {
  bool dpdk_network = false;
  bool spdk_storage = false;
  bool operator==(const IoBackends&) const = default;
};

// Select before creating workers or allocating storage buffers. The selection
// is process-wide and freezes on first I/O use: DMA allocation/free pairs,
// EAL device discovery and worker-owned TCP state cannot change underneath I/O.
// Compiled-in support does not activate a backend. Unsupported requests fail.
absl::Status ConfigureIoBackends(IoBackends options);
// Publish the selected options before I/O; repeated calls are lock-free.
void FreezeIoBackends();
namespace detail {
extern IoBackends io_backends;
}
inline bool DpdkNetworkEnabled() noexcept {
#ifdef CELER_WITH_DPDK
  return detail::io_backends.dpdk_network;
#else
  return false;
#endif
}
inline bool SpdkStorageEnabled() noexcept {
#ifdef CELER_WITH_SPDK_STORAGE
  return detail::io_backends.spdk_storage;
#else
  return false;
#endif
}
}  // namespace celer
#endif
