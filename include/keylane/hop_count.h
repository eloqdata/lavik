#pragma once

#include <atomic>
#include <cstdint>

#ifndef KEYLANE_ENABLE_CROSS_CORE_HOP_COUNT
#define KEYLANE_ENABLE_CROSS_CORE_HOP_COUNT 0
#endif

namespace keylane {

#if KEYLANE_ENABLE_CROSS_CORE_HOP_COUNT
// Cross-core transfers made by command dispatch: the owner dispatch itself,
// and replication publisher admission's acquire and release.
//
// This is deliberately absent from production builds. Hop count is an
// implementation detail and an operator has no use for it, but it is also the
// only direct evidence that a single-key write reaches its key owner once
// rather than three times -- everything else that changes is latency, which no
// test can assert. INFO STATS reports it as command_cross_core_hops in a build
// configured with -DKEYLANE_ENABLE_CROSS_CORE_HOP_COUNT=ON.
inline std::atomic<std::uint64_t> g_command_cross_core_hops{0};

inline void CountCommandCrossCoreHop() noexcept {
  g_command_cross_core_hops.fetch_add(1, std::memory_order_relaxed);
}

inline std::uint64_t CommandCrossCoreHops() noexcept {
  return g_command_cross_core_hops.load(std::memory_order_relaxed);
}
#else
inline constexpr void CountCommandCrossCoreHop() noexcept {}
#endif

}  // namespace keylane
