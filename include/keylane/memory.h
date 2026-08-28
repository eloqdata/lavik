#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "absl/status/status.h"

namespace keylane {

// Worker IDs occupy ten bits in storage's runtime record-location index. Keep
// memory admission and that representation on the same process-wide limit.
inline constexpr unsigned kMaxMemoryWorkers = 1024;

struct MemoryStats {
  std::uint64_t used_bytes_ = 0;
  std::uint64_t rss_bytes_ = 0;
  std::uint64_t committed_bytes_ = 0;
  std::uint64_t reserved_bytes_ = 0;
  std::uint64_t peak_used_bytes_ = 0;
  std::uint64_t max_bytes_ = 0;
  std::uint64_t fullsync_reserved_bytes_ = 0;
  std::uint64_t admission_pending_bytes_ = 0;
  std::uint64_t rejected_commands_ = 0;
};

// Holds process-memory headroom while an allocation is crossing the gap
// between admission and the allocator hook publishing its usable size. It is
// intentionally short-lived: once the allocation call returns, allocator
// accounting owns the bytes and this reservation should leave scope.
class MemoryReservation {
 public:
  MemoryReservation() noexcept = default;
  MemoryReservation(const MemoryReservation&) = delete;
  MemoryReservation& operator=(const MemoryReservation&) = delete;
  MemoryReservation(MemoryReservation&& other) noexcept;
  MemoryReservation& operator=(MemoryReservation&& other) noexcept;
  ~MemoryReservation();

  explicit operator bool() const noexcept { return admitted_; }
  std::size_t bytes() const noexcept { return bytes_; }

 private:
  friend std::optional<MemoryReservation> TryReserveMemory(
      std::size_t bytes) noexcept;
  MemoryReservation(std::size_t bytes, unsigned shard,
                    bool admitted) noexcept
      : bytes_(bytes), shard_(shard), admitted_(admitted) {}

  void Release() noexcept;

  std::size_t bytes_ = 0;
  unsigned shard_ = 0;
  bool admitted_ = false;
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
// Reserves headroom in the calling worker's fixed share. Worker reservations
// touch only that worker's cache line; they never contend for a process-global
// balance. A configured limit of zero (before InitMemoryLimit) is treated as
// unlimited so allocator-backed containers remain usable in isolated tests.
std::optional<MemoryReservation> TryReserveMemory(
    std::size_t bytes) noexcept;
// Reserves the usable-size class mimalloc will charge for one ordinary
// unaligned allocation request. Keep the permit only across the allocation;
// the global new hook publishes the actual usable size before it returns.
std::optional<MemoryReservation> TryReserveMemoryAllocation(
    std::size_t requested_bytes) noexcept;
// Reserves process-memory headroom for a full-sync coverage map. The
// reservation is logical: each partition releases its actual scan structures
// after handoff, while ordinary writes cannot consume the reusable headroom
// until the session ends.
bool TryReserveFullSyncMemory(std::size_t bytes) noexcept;
void ReleaseFullSyncMemory(std::size_t bytes) noexcept;
void RecordMemoryRejection() noexcept;

std::string HumanReadableMemory(std::uint64_t bytes);

}  // namespace keylane
