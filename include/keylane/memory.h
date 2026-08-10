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
  std::uint64_t rejected_commands_ = 0;
};

// A configured value of zero selects 80% of the host or process-cgroup memory
// capacity, whichever is smaller.
absl::Status InitMemoryLimit(std::uint64_t configured_max_bytes);

// Refreshes allocator and RSS values. This is intentionally a background-path
// operation; request processing reads only the cached atomics below.
void RefreshMemoryStats() noexcept;
MemoryStats GetMemoryStats() noexcept;

// Conservative preflight for commands that may increase retained memory.
// It never calls into mimalloc and performs only relaxed atomic loads.
bool WouldExceedMemoryLimit(std::size_t additional_bytes) noexcept;
void RecordMemoryRejection() noexcept;

std::string HumanReadableMemory(std::uint64_t bytes);

}  // namespace keylane
