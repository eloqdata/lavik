/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

#ifndef LAVIK_ENABLE_TRACE
#define LAVIK_ENABLE_TRACE 0
#endif

namespace lavik {

#if !LAVIK_ENABLE_TRACE
namespace detail {

template <typename T, std::size_t Tag>
struct DisabledSetTraceField {
  constexpr DisabledSetTraceField() noexcept = default;
  constexpr DisabledSetTraceField(T) noexcept {}
  constexpr DisabledSetTraceField& operator=(T) noexcept { return *this; }
  constexpr operator T() const noexcept { return T{}; }
};

}  // namespace detail
#endif

#if LAVIK_ENABLE_TRACE
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
#if LAVIK_ENABLE_TRACE
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
  bool standby_block_ = false;
#else
#define LAVIK_DISABLED_SET_TRACE_FIELD(type, name, tag) \
  [[no_unique_address]] detail::DisabledSetTraceField<type, tag> name
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, request_start_ns_, 0);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, owner_start_ns_, 1);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, key_lock_start_ns_, 2);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, key_lock_acquired_ns_, 3);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, store_lock_start_ns_, 4);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, store_lock_acquired_ns_, 5);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, lookup_done_ns_, 6);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, append_start_ns_, 7);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, block_wait_start_ns_, 8);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, block_ready_ns_, 9);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, encode_done_ns_, 10);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, index_done_ns_, 11);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, append_done_ns_, 12);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, replication_done_ns_, 13);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, owner_done_ns_, 14);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, origin_resume_ns_, 15);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, send_start_ns_, 16);
  LAVIK_DISABLED_SET_TRACE_FIELD(std::uint64_t, send_complete_ns_, 17);
  LAVIK_DISABLED_SET_TRACE_FIELD(bool, remote_, 18);
  LAVIK_DISABLED_SET_TRACE_FIELD(bool, replication_, 19);
  LAVIK_DISABLED_SET_TRACE_FIELD(bool, allocated_block_, 20);
  LAVIK_DISABLED_SET_TRACE_FIELD(bool, standby_block_, 21);
#undef LAVIK_DISABLED_SET_TRACE_FIELD
#endif
};

#if !LAVIK_ENABLE_TRACE
static_assert(sizeof(SetLatencyTrace) == 1);
#endif

}  // namespace lavik
