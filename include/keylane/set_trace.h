#pragma once

#include <chrono>
#include <cstdint>

#ifndef KEYLANE_ENABLE_SET_LATENCY_TRACE
#define KEYLANE_ENABLE_SET_LATENCY_TRACE 0
#endif

namespace keylane {

#if KEYLANE_ENABLE_SET_LATENCY_TRACE
inline std::uint64_t SetTraceNowNanos() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
#else
inline constexpr std::uint64_t SetTraceNowNanos() noexcept { return 0; }
#endif

struct SetLatencyTrace {
  std::uint64_t request_start_ns_ = 0;
  std::uint64_t owner_start_ns_ = 0;
  std::uint64_t key_lock_start_ns_ = 0;
  std::uint64_t key_lock_acquired_ns_ = 0;
  std::uint64_t store_lock_start_ns_ = 0;
  std::uint64_t store_lock_acquired_ns_ = 0;
  std::uint64_t lookup_done_ns_ = 0;
  std::uint64_t append_start_ns_ = 0;
  std::uint64_t block_wait_start_ns_ = 0;
  std::uint64_t block_ready_ns_ = 0;
  std::uint64_t encode_done_ns_ = 0;
  std::uint64_t index_done_ns_ = 0;
  std::uint64_t append_done_ns_ = 0;
  std::uint64_t replication_done_ns_ = 0;
  std::uint64_t owner_done_ns_ = 0;
  std::uint64_t origin_resume_ns_ = 0;
  std::uint64_t send_start_ns_ = 0;
  std::uint64_t send_complete_ns_ = 0;
  bool remote_ = false;
  bool replication_ = false;
  bool allocated_block_ = false;
};

}  // namespace keylane
