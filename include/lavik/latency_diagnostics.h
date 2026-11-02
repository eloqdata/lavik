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

#include <array>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "bycorf/runtime/task.h"
#include "lavik/command_args.h"
#include "lavik/latency_stage.h"
#include "lavik/read_trace.h"
#include "lavik/set_trace.h"

namespace lavik {
struct LoggingOptions;
struct CommandRequest;
struct TraceSession;
struct TraceTimingData;

// Histograms use a fixed family vocabulary; individual canonical command
// identities belong in bounded trace records, not a dense command/stage grid.
enum class TraceCommandFamily : std::uint8_t {
  kGet,
  kSet,
  kRead,
  kWrite,
  kMultiKey,
  kTransaction,
  kScript,
  kBlocking,
  kReplication,
  kPubSub,
  kAdmin,
  kOther,
  kCount
};

// These resources measure actual suspension, including maintenance callers.
// Ownership remains on the worker that suspended; no shared hot-path lock.
enum class LatencyResource {
  kKeyLock,
  kStoreLock,
  kReadBuffer,
  kWriteBuffer,
  kDurability,
  kCount
};
void BeginLatencyWait(LatencyResource resource) noexcept;
void EndLatencyWait(LatencyResource resource) noexcept;
class LatencyWait {
 public:
  void Begin(LatencyResource resource) noexcept {
    resource_ = resource;
    BeginLatencyWait(resource);
    active_ = true;
  }
  void End() noexcept {
    if (std::exchange(active_, false)) EndLatencyWait(resource_);
  }
  ~LatencyWait() { End(); }

 private:
  LatencyResource resource_{};
  bool active_ = false;
};

// Decorates the existing awaitable; never calls await_ready twice to probe it.
template <typename Awaiter>
class LatencyWaitAwaiter {
 public:
  LatencyWaitAwaiter(Awaiter awaiter, LatencyResource resource)
      : awaiter_(std::move(awaiter)), resource_(resource) {}
  bool await_ready() { return awaiter_.await_ready(); }
  bool await_suspend(std::coroutine_handle<> handle) {
    const bool suspended = awaiter_.await_suspend(handle);
    if (suspended) wait_.Begin(resource_);
    return suspended;
  }
  decltype(auto) await_resume() {
    wait_.End();
    return awaiter_.await_resume();
  }

 private:
  Awaiter awaiter_;
  LatencyResource resource_;
  LatencyWait wait_;
};

struct OnlineLatencyTrace {
  ReadLatencyTrace read_;
  SetLatencyTrace set_;
  std::uint64_t id_ = 0;
  std::uint64_t timestamp_ns_ = 0;
  std::uint64_t ready_ns_ = 0;
  std::uint64_t dispatch_ns_ = 0;
  std::uint64_t dispatch_done_ns_ = 0;
  std::uint64_t send_ns_ = 0;
  std::uint64_t done_ns_ = 0;
  std::uint64_t threshold_us_ = 0;
  std::uint32_t sample_every_ = 0;
  unsigned origin_ = 0;
  unsigned slot_ = 0;
  std::uint16_t command_id_ = 0;
  TraceCommandFamily family_ = TraceCommandFamily::kOther;
  bool is_set_ = false;
  bool write_ok_ = false;
  bool queued_ = false;
  // Active writers are partitioned by worker. Retained records hold only
  // frozen, immutable timings shared with the asynchronous JSON consumer.
  std::shared_ptr<TraceSession> session_;
  std::shared_ptr<const TraceTimingData> timings_;
};

// A worker-local pool lease. Its connection owns it through send completion;
// a borrowed pointer may cross workers only through awaited request routing.
class OnlineTraceHandle {
 public:
  OnlineTraceHandle() = default;
  explicit OnlineTraceHandle(OnlineLatencyTrace* trace) : trace_(trace) {}
  OnlineTraceHandle(const OnlineTraceHandle&) = delete;
  OnlineTraceHandle& operator=(const OnlineTraceHandle&) = delete;
  OnlineTraceHandle(OnlineTraceHandle&& other) noexcept
      : trace_(std::exchange(other.trace_, nullptr)) {}
  OnlineTraceHandle& operator=(OnlineTraceHandle&& other) noexcept {
    if (this != &other) {
      if (trace_) Cancel();
      trace_ = std::exchange(other.trace_, nullptr);
    }
    return *this;
  }
  ~OnlineTraceHandle() {
    if (trace_) Cancel();
  }
  OnlineLatencyTrace* get() const { return trace_; }
  explicit operator bool() const { return trace_ != nullptr; }
  void Complete(bool write_ok) {
    if (trace_) Finish(write_ok);
  }

 private:
  void Cancel() noexcept;
  void Finish(bool write_ok);
  OnlineLatencyTrace* trace_ = nullptr;
};

// Initialize before workers start; stop the log consumer after workers stop.
// Diagnostic JSON uses its own rotating sink in log_dir_, even when ordinary
// logging targets stderr. Options are copied before the consumer starts.
void InitLatencyDiagnostics(unsigned workers,
                            const LoggingOptions* logging = nullptr);
void StopLatencyDiagnostics();
std::uint64_t LatencyMonotonicNanos() noexcept;
// Diagnostic control requests are excluded to avoid tracing the commands
// used to inspect/reset collection. Selection never retains argument text.
OnlineTraceHandle SelectOnlineTrace(CommandArgsView args);
void BindOnlineTraceCommand(OnlineLatencyTrace* trace,
                            const CommandRequest& request);
void AdjustRequestQueue(std::int64_t delta) noexcept;
void AdjustResponseWrites(std::int64_t delta) noexcept;
// CONFIG is runtime-only: restart always disables collection. Re-enabling
// starts a fresh bounded time window. In-flight samples retain their config.
absl::Status ConfigureLatencyDiagnostics(std::string_view name,
                                         std::string_view value);
std::string LatencyDiagnosticsConfig(std::string_view name);
bycorf::Task<std::vector<std::string>> GetLatencyTraces(std::size_t count,
                                                        std::uint64_t id = 0);
bycorf::Task<absl::Status> ResetLatencyTraces();
bycorf::Task<std::string> RenderLatencyDiagnosticsMetrics();

}  // namespace lavik
