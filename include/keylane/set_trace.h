#pragma once

#include <cstddef>
#include <chrono>
#include <cstdint>

#ifndef KEYLANE_ENABLE_SET_LATENCY_TRACE
#define KEYLANE_ENABLE_SET_LATENCY_TRACE 0
#endif

namespace keylane {

#if !KEYLANE_ENABLE_SET_LATENCY_TRACE
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
#if KEYLANE_ENABLE_SET_LATENCY_TRACE
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
#define KEYLANE_DISABLED_SET_TRACE_FIELD(type, name, tag) \
  [[no_unique_address]] detail::DisabledSetTraceField<type, tag> name
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, request_start_ns_, 0);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, owner_start_ns_, 1);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, key_lock_start_ns_, 2);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, key_lock_acquired_ns_, 3);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, store_lock_start_ns_, 4);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, store_lock_acquired_ns_, 5);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, lookup_done_ns_, 6);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, append_start_ns_, 7);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, block_wait_start_ns_, 8);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, block_ready_ns_, 9);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, encode_done_ns_, 10);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, index_done_ns_, 11);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, append_done_ns_, 12);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, replication_done_ns_, 13);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, owner_done_ns_, 14);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, origin_resume_ns_, 15);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, send_start_ns_, 16);
  KEYLANE_DISABLED_SET_TRACE_FIELD(std::uint64_t, send_complete_ns_, 17);
  KEYLANE_DISABLED_SET_TRACE_FIELD(bool, remote_, 18);
  KEYLANE_DISABLED_SET_TRACE_FIELD(bool, replication_, 19);
  KEYLANE_DISABLED_SET_TRACE_FIELD(bool, allocated_block_, 20);
  KEYLANE_DISABLED_SET_TRACE_FIELD(bool, standby_block_, 21);
#undef KEYLANE_DISABLED_SET_TRACE_FIELD
#endif
};

#if !KEYLANE_ENABLE_SET_LATENCY_TRACE
static_assert(sizeof(SetLatencyTrace) == 1);
#endif

}  // namespace keylane
