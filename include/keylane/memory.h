#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "absl/status/status.h"

namespace keylane {

struct MemoryStats {
  std::uint64_t used_bytes_ = 0;
  std::uint64_t rss_bytes_ = 0;
  std::uint64_t committed_bytes_ = 0;
  std::uint64_t reserved_bytes_ = 0;
  std::uint64_t peak_used_bytes_ = 0;
  std::uint64_t max_bytes_ = 0;
  std::uint64_t fullsync_reserved_bytes_ = 0;
  std::uint64_t rejected_commands_ = 0;
};

// A configured value of zero selects 80% of the host or process-cgroup memory
// capacity, whichever is smaller.
absl::Status InitMemoryLimit(std::uint64_t configured_max_bytes,
                             unsigned worker_count);

// Allocation hooks call this with the allocator's usable-size delta. Worker
// threads bind once so their updates land on independent cache lines.
void AccountMemoryAllocation(std::int64_t delta) noexcept;
void BindMemoryAccountingShard(unsigned worker_id) noexcept;

// Periodically publishes the cheap per-worker allocation-counter sum.
void RefreshMemoryStats() noexcept;
// Explicit INFO/metrics path: refreshes RSS and allocator-wide diagnostics.
void RefreshMemoryDiagnostics() noexcept;
MemoryStats GetMemoryStats() noexcept;

// Conservative preflight for commands that may increase retained memory.
// It never calls into mimalloc and performs only relaxed atomic loads.
bool WouldExceedMemoryLimit(std::size_t additional_bytes) noexcept;
// Reserves process-memory headroom for a full-sync coverage map. The
// reservation is logical: each partition releases its actual scan structures
// after handoff, while ordinary writes cannot consume the reusable headroom
// until the session ends.
bool TryReserveFullSyncMemory(std::size_t bytes) noexcept;
void ReleaseFullSyncMemory(std::size_t bytes) noexcept;
void RecordMemoryRejection() noexcept;

std::string HumanReadableMemory(std::uint64_t bytes);

}  // namespace keylane
