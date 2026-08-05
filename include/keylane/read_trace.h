#pragma once

#include <chrono>
#include <cstdint>

#ifndef KEYLANE_ENABLE_READ_LATENCY_TRACE
#define KEYLANE_ENABLE_READ_LATENCY_TRACE 0
#endif

namespace keylane {

#if KEYLANE_ENABLE_READ_LATENCY_TRACE
inline std::uint64_t ReadTraceNowNanos() noexcept {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
#else
inline constexpr std::uint64_t ReadTraceNowNanos() noexcept { return 0; }
#endif

struct ReadLatencyTrace {
  std::uint64_t request_start_ns = 0;
  std::uint64_t owner_start_ns = 0;
  std::uint64_t lookup_done_ns = 0;
  std::uint64_t buffer_acquire_start_ns = 0;
  std::uint64_t buffer_acquired_ns = 0;
  std::uint64_t io_submit_ns = 0;
  std::uint64_t io_complete_ns = 0;
  std::uint64_t decode_done_ns = 0;
  std::uint64_t owner_done_ns = 0;
  std::uint64_t origin_resume_ns = 0;
  std::uint64_t send_start_ns = 0;
  std::uint64_t send_complete_ns = 0;
  bool remote = false;
  bool hit = false;
  bool disk_read = false;
  bool heap_read_buffer = false;
};

}  // namespace keylane
