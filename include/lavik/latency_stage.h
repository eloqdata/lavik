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
// emitted for every command. Summary spans contain work measured below them.
enum class TraceStage : std::uint8_t {
  kRequestQueue,
  kDispatch,
  kRouteToOwner,
  kRouteBack,
  kKeyLock,
  kStoreLock,
  kLookup,
  kReadBuffer,
  kReadIo,
  kAppendCapacity,
  kAppend,
  kDurabilityWait,
  kResponseQueue,
  kSocketSend,
  kServerTotal,
  kDecode,
  kValueProcess,
  kAdmissionWait,
  kReplyBuild,
  kTxSchedule,
  kTxRound,
  kTxFinish,
  kCommitQueueWait,
  kBlockingWait,
  kWakeResume,
  kReplicationFence,
  kReplicaAckWait,
  kWatchCheck,
  kChildExecute,
  kScriptPrepare,
  kScriptRun,
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
  TraceSpanScope(OnlineLatencyTrace* trace, TraceStage stage) noexcept;
  TraceSpanScope(const TraceSpanScope&) = delete;
  TraceSpanScope& operator=(const TraceSpanScope&) = delete;
  ~TraceSpanScope() { End(); }
  void End() noexcept;

 private:
  void* recorder_ = nullptr;
  std::uint64_t start_ns_ = 0;
  TraceStage stage_{};
};

// Worker-local TSC time anchored to the common monotonic epoch. A null root
// reads no clock; absolute values from awaited workers can be merged safely.
std::uint64_t TraceNowNanos(OnlineLatencyTrace* trace) noexcept;
// Preserve one logical child's envelope after its shard fragments join.
// Only canonical identities enter the bounded detail; arguments are omitted.
void RecordTraceChild(OnlineLatencyTrace* trace, std::uint16_t command,
                      std::uint64_t start, std::uint64_t end) noexcept;

// Record one already-timed interval on the caller's worker. The borrowed root
// is valid only through the awaited request; no detached operation inherits it.
void RecordTraceInterval(OnlineLatencyTrace* trace, TraceStage stage,
                         std::uint64_t start, std::uint64_t end) noexcept;
}  // namespace lavik
