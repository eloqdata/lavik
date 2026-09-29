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

// Disabled trace fields preserve source compatibility for instrumentation
// sites without making every CommandReply carry the diagnostic payload. Each
// tag is a distinct empty type so [[no_unique_address]] may overlap all fields;
// reads remain zero and writes intentionally disappear in non-trace builds.
template <typename T, std::size_t Tag>
struct DisabledReadTraceField {
  constexpr DisabledReadTraceField() noexcept = default;
  constexpr DisabledReadTraceField(T) noexcept {}
  constexpr DisabledReadTraceField& operator=(T) noexcept { return *this; }
  constexpr operator T() const noexcept { return T{}; }
};

}  // namespace detail
#endif

#if LAVIK_ENABLE_TRACE
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
#if LAVIK_ENABLE_TRACE
  std::uint64_t request_start_ns_ = 0;
  std::uint64_t owner_start_ns_ = 0;
  std::uint64_t lookup_done_ns_ = 0;
  std::uint64_t buffer_acquire_start_ns_ = 0;
  std::uint64_t buffer_acquired_ns_ = 0;
  std::uint64_t io_submit_ns_ = 0;
  std::uint64_t io_complete_ns_ = 0;
  std::uint64_t decode_done_ns_ = 0;
  std::uint64_t owner_done_ns_ = 0;
  std::uint64_t origin_resume_ns_ = 0;
  std::uint64_t send_start_ns_ = 0;
  std::uint64_t send_complete_ns_ = 0;
  bool remote_ = false;
  bool hit_ = false;
  bool disk_read_ = false;
  bool heap_read_buffer_ = false;
#else
#define LAVIK_DISABLED_READ_TRACE_FIELD(type, name, tag) \
  [[no_unique_address]] detail::DisabledReadTraceField<type, tag> name
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, request_start_ns_, 0);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, owner_start_ns_, 1);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, lookup_done_ns_, 2);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, buffer_acquire_start_ns_, 3);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, buffer_acquired_ns_, 4);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, io_submit_ns_, 5);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, io_complete_ns_, 6);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, decode_done_ns_, 7);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, owner_done_ns_, 8);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, origin_resume_ns_, 9);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, send_start_ns_, 10);
  LAVIK_DISABLED_READ_TRACE_FIELD(std::uint64_t, send_complete_ns_, 11);
  LAVIK_DISABLED_READ_TRACE_FIELD(bool, remote_, 12);
  LAVIK_DISABLED_READ_TRACE_FIELD(bool, hit_, 13);
  LAVIK_DISABLED_READ_TRACE_FIELD(bool, disk_read_, 14);
  LAVIK_DISABLED_READ_TRACE_FIELD(bool, heap_read_buffer_, 15);
#undef LAVIK_DISABLED_READ_TRACE_FIELD
#endif
};

#if !LAVIK_ENABLE_TRACE
static_assert(sizeof(ReadLatencyTrace) == 1);
#endif

}  // namespace lavik
