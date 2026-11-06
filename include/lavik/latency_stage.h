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
#include <cstdint>

namespace lavik {
struct OnlineLatencyTrace;

// This is one shared vocabulary for actual operations, not a stage matrix
// emitted for every command. Each stage records an observed operation.
enum class TraceStage : std::uint8_t {
  kRequestQueue,
  kRouteToOwner,
  kRouteBack,
  kKeyLock,
  kStoreLock,
  kLookup,
  kReadBuffer,
  kReadIo,
  kAppendCapacity,
  kDurabilityWait,
  kResponseQueue,
  kSocketSend,
  kDecode,
  kValueProcess,
  kAdmissionWait,
  kTxSchedule,
  kCommitQueueWait,
  kBlockingWait,
  kWakeResume,
  kReplicationFence,
  kReplicaAckWait,
  kWatchCheck,
  kScriptPrepare,
  kPublishFanout,
  kSnapshotPrepare,
  kCount
};

// The borrowed root must outlive the awaited operation. Every worker writes
// only its own segment; the root freezes segments after its children join.
// No worker-wide "current trace" survives co_await, and unsampled calls read
// no clock or allocate a segment. Detached jobs must not borrow this scope.
class TraceSpanScope {
 public:
  TraceSpanScope(OnlineLatencyTrace* trace, TraceStage stage) noexcept {
    if (trace) [[unlikely]]
      Begin(trace, stage);
  }
  TraceSpanScope(const TraceSpanScope&) = delete;
  TraceSpanScope& operator=(const TraceSpanScope&) = delete;
  ~TraceSpanScope() { End(); }
  void End() noexcept {
    if (recorder_) [[unlikely]]
      Finish();
  }

 private:
  // Sampling is exceptional on request paths. Keep clock reads and recording
  // out of those instruction streams even when the caller is built with LTO.
  [[gnu::cold, gnu::noinline]] void Begin(OnlineLatencyTrace* trace,
                                          TraceStage stage) noexcept;
  [[gnu::cold, gnu::noinline]] void Finish() noexcept;
  void* recorder_ = nullptr;
  // Only the sampled path initializes or reads these fields.
  std::uint64_t start_ns_;
  TraceStage stage_;
};

// Worker-local TSC time anchored to the common monotonic epoch. A null root
// reads no clock; absolute values from awaited workers can be merged safely.
std::uint64_t TraceNowNanos(OnlineLatencyTrace* trace) noexcept;
// Record one already-timed interval on the caller's worker. The borrowed root
// is valid only through the awaited request; no detached operation inherits it.
void RecordTraceInterval(OnlineLatencyTrace* trace, TraceStage stage,
                         std::uint64_t start, std::uint64_t end) noexcept;
}  // namespace lavik
