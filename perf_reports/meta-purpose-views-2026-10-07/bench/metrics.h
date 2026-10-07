/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>

namespace measurement {
using Clock = std::chrono::steady_clock;
inline double Ns(Clock::time_point start, Clock::time_point end) {
  return std::chrono::duration<double, std::nano>(end - start).count();
}
struct Counters {
  std::uint64_t allocations = 0, bytes = 0, frees = 0, freed_usable_bytes = 0;
  double wait_ns = 0, hold_ns = 0;
  Counters& operator+=(const Counters& other) {
    allocations += other.allocations;
    bytes += other.bytes;
    frees += other.frees;
    freed_usable_bytes += other.freed_usable_bytes;
    wait_ns += other.wait_ns;
    hold_ns += other.hold_ns;
    return *this;
  }
};
inline thread_local bool tracking = false;
inline thread_local Counters counters;
inline void Start() {
  counters = {};
  tracking = true;
}
inline Counters Stop() {
  tracking = false;
  return counters;
}

// Only task-built state-machine objects use this guard. No production hook.
struct Lock {
  std::mutex& mutex;
  bool measured;
  Clock::time_point acquired;
  explicit Lock(std::mutex& mu) : mutex(mu), measured(tracking) {
    const auto start = Clock::now();
    mutex.lock();
    acquired = Clock::now();
    if (measured) counters.wait_ns += Ns(start, acquired);
  }
  ~Lock() {
    if (measured) counters.hold_ns += Ns(acquired, Clock::now());
    mutex.unlock();
  }
};
}  // namespace measurement
