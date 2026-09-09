#pragma once

#include <chrono>
#include <cstdint>

namespace keylane::cluster {

// Lease deadlines need elapsed time to advance while the host is suspended.
// std::chrono::steady_clock maps to CLOCK_MONOTONIC on Linux and therefore
// pauses across suspend. Keep the existing steady-clock representation so
// callers can inject deterministic time points in tests, but obtain every
// production lease timestamp through LeaseClockNow(), which uses
// CLOCK_BOOTTIME. Keylane is Linux-only; there is intentionally no fallback
// to a clock that could let an old authority survive suspend/resume.
using LeaseTime = std::chrono::steady_clock::time_point;
using LeaseDuration = std::chrono::steady_clock::duration;

LeaseTime LeaseClockNow() noexcept;

inline std::int64_t LeaseClockMillis() noexcept {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             LeaseClockNow().time_since_epoch())
      .count();
}

}  // namespace keylane::cluster
