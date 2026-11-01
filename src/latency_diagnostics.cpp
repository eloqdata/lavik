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

#include "lavik/latency_diagnostics.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"
#include "lavik/logging.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"

namespace lavik {
namespace {
constexpr unsigned kCapacity = 256;
constexpr unsigned kHistory = 128;
constexpr unsigned kLogCapacity = 64;
constexpr std::array<std::string_view, 5> kResources{
    "key_lock", "store_lock", "read_buffer", "write_buffer", "durability"};
enum Stage {
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
  kStageCount
};
constexpr std::array<std::string_view, kStageCount> kStages{
    "request_queue",  "dispatch",        "route_to_owner", "route_back",
    "key_lock",       "store_lock",      "lookup",         "read_buffer",
    "read_io",        "append_capacity", "append",         "durability_wait",
    "response_queue", "socket_send",     "server_total",   "decode"};
constexpr std::array<std::uint64_t, 15> kBoundsUs{
    1,     4,      16,      64,      256,      1000,     4000,     16000,
    64000, 256000, 1000000, 4000000, 16000000, 64000000, 256000000};
struct Histogram {
  std::array<std::uint64_t, kBoundsUs.size() + 1> bins{};
  std::uint64_t ns = 0;
  void Add(std::uint64_t value) {
    const auto us = (value + 999) / 1000;
    ++bins[std::lower_bound(kBoundsUs.begin(), kBoundsUs.end(), us) -
           kBoundsUs.begin()];
    ns += value;
  }
};
struct Metrics {
  std::int64_t queued = 0, writes = 0;
  std::array<std::uint64_t, 5> events{}, waiters{};
  std::uint64_t eligible = 0, selected = 0, pool_dropped = 0, filtered = 0,
                retained = 0, overwritten = 0, cancelled = 0, log_dropped = 0;
  std::array<std::array<Histogram, kStages.size()>, 2> hist{};
};
struct Shard {
  Metrics metrics;
  std::array<OnlineLatencyTrace, kCapacity> pool{};
  std::array<unsigned, kCapacity> free{};
  unsigned free_count = kCapacity;
  std::array<OnlineLatencyTrace, kHistory> history{};
  unsigned history_next = 0, history_size = 0;
  std::uint64_t sequence = 0, random = 1, log_epoch = 0;
  unsigned log_count = 0;
  // SPSC: the worker publishes complete fixed-size copies; only the logger
  // thread consumes them. A blocked sink cannot retain a request's pool lease.
  std::array<OnlineLatencyTrace, kLogCapacity> logs{};
  alignas(64) std::atomic<std::uint64_t> log_head{0};
  alignas(64) std::atomic<std::uint64_t> log_tail{0};
};
std::unique_ptr<Shard[]> g_shards;
unsigned g_count = 0;
std::atomic<std::uint64_t> g_deadline_ns{0};
std::atomic<std::uint32_t> g_duration{300}, g_sample{64};
std::atomic<std::uint64_t> g_threshold{10000};
std::jthread g_logger;
std::atomic<std::uint64_t> g_sink_dropped{0};
std::atomic<bool> g_log_enabled{true};
std::atomic<std::uint32_t> g_log_limit{20};
Shard* Local() {
  if (!g_shards || bycorf::ThisWorker().id_ >= g_count) return nullptr;
  return &g_shards[bycorf::ThisWorker().id_];
}
bool Enabled() {
  auto deadline = g_deadline_ns.load(std::memory_order_relaxed);
  if (deadline == 0) return false;
  if (LatencyMonotonicNanos() < deadline) return true;
  // Expiry restores the clock-free disabled fast path. A concurrent enable
  // installs a different deadline and must not be cleared by this observer.
  g_deadline_ns.compare_exchange_strong(deadline, 0, std::memory_order_relaxed);
  return false;
}

struct Span {
  std::uint64_t start = 0, end = 0;
  const LatencyIntervals* timing = nullptr;
};
std::uint64_t Duration(const Span& span) {
  return span.timing ? span.timing->total_ns : span.end - span.start;
}
std::array<Span, kStages.size()> Spans(const OnlineLatencyTrace& t) {
  std::array<Span, kStages.size()> s{};
  s[kRequestQueue] = {t.ready_ns_, t.dispatch_ns_};
  s[kDispatch] = {t.dispatch_ns_, t.dispatch_done_ns_};
  if (t.is_set_) {
    const auto& v = t.set_;
    if (v.remote_) {
      s[kRouteToOwner] = {t.dispatch_ns_, v.owner_start_ns_};
      s[kRouteBack] = {v.owner_done_ns_, t.dispatch_done_ns_};
    }
    s[kKeyLock] = {v.key_lock_start_ns_, v.key_lock_acquired_ns_};
    s[kStoreLock] = {v.store_lock_start_ns_, v.store_lock_acquired_ns_};
    s[kLookup] = {v.store_lock_acquired_ns_, v.lookup_done_ns_};
    s[kAppendCapacity] = {v.block_wait_start_ns_, v.block_ready_ns_};
    s[kAppend] = {v.append_start_ns_, v.append_done_ns_};
    s[kDurabilityWait] = {v.durability_start_ns_, v.durability_done_ns_};
  } else {
    const auto& v = t.read_;
    // Direct remote reads execute I/O on the connection worker; ownership
    // alone must not create a fictitious outbound or return routing span.
    if (v.remote_) {
      s[kRouteToOwner] = {t.dispatch_ns_, v.owner_start_ns_};
      s[kRouteBack] = {v.owner_done_ns_, t.dispatch_done_ns_};
    }
    s[kKeyLock] = {v.key_lock_start_ns_, v.key_lock_acquired_ns_};
    s[kLookup] = {v.owner_start_ns_, v.lookup_done_ns_};
    s[kReadBuffer] = {v.buffer_acquire_start_ns_, v.buffer_acquired_ns_};
    if (v.disk_read_) s[kReadIo] = {v.io_submit_ns_, v.io_complete_ns_};
    if (v.buffer_timing_.count)
      s[kReadBuffer] = {v.buffer_timing_.first_ns, v.buffer_timing_.last_ns,
                        &v.buffer_timing_};
    if (v.io_timing_.count)
      s[kReadIo] = {v.io_timing_.first_ns, v.io_timing_.last_ns, &v.io_timing_};
    s[kDecode] = {v.io_complete_ns_, v.decode_done_ns_};
  }
  s[kResponseQueue] = {t.dispatch_done_ns_, t.send_ns_};
  s[kSocketSend] = {t.send_ns_, t.done_ns_};
  s[kServerTotal] = {t.ready_ns_, t.done_ns_};
  return s;
}
bool Valid(const Span& s) { return s.start && s.end >= s.start; }
std::string Json(const OnlineLatencyTrace& t) {
  std::string out = absl::StrCat(
      "{\"event\":\"command_latency_trace\",\"level\":\"info\",\"timestamp_"
      "unix_ns\":\"",
      t.timestamp_ns_, "\",\"trace_id\":\"", t.id_, "\",\"command\":\"",
      t.is_set_ ? "SET" : "GET", "\",\"worker\":", t.origin_,
      ",\"total_us\":", (t.done_ns_ - t.ready_ns_) / 1000,
      ",\"threshold_us\":", t.threshold_us_,
      ",\"sample_every\":", t.sample_every_,
      ",\"write_ok\":", t.write_ok_ ? "true" : "false", ",\"stages\":[");
  const auto spans = Spans(t);
  bool first = true;
  for (unsigned i = 0; i < spans.size(); ++i) {
    const auto& span = spans[i];
    if (!Valid(span) || span.start < t.ready_ns_) continue;
    if (!first) out += ',';
    first = false;
    absl::StrAppend(&out, "{\"name\":\"", kStages[i],
                    "\",\"start_us\":", (span.start - t.ready_ns_) / 1000,
                    ",\"duration_us\":", Duration(span) / 1000);
    if (span.timing && span.timing->count > 1) {
      absl::StrAppend(&out, ",\"observations\":", span.timing->count,
                      ",\"span_us\":", (span.end - span.start) / 1000,
                      ",\"intervals\":[");
      const auto count = std::min<std::uint64_t>(span.timing->count,
                                                 span.timing->intervals.size());
      for (unsigned n = 0; n < count; ++n) {
        const auto& interval = span.timing->intervals[n];
        if (n) out += ',';
        absl::StrAppend(
            &out, "{\"start_us\":", (interval.start_ns - t.ready_ns_) / 1000,
            ",\"duration_us\":", (interval.end_ns - interval.start_ns) / 1000,
            "}");
      }
      absl::StrAppend(&out,
                      "],\"intervals_dropped\":", span.timing->count - count);
    }
    out += '}';
  }
  out += "]}";
  return out;
}
void Release(OnlineLatencyTrace* t, bool cancelled) {
  auto& shard = g_shards[t->origin_];
  if (cancelled) ++shard.metrics.cancelled;
  shard.free[shard.free_count++] = t->slot_;
}
void Retain(OnlineLatencyTrace& t) {
  auto& s = g_shards[t.origin_];
  if (t.done_ns_ - t.ready_ns_ < t.threshold_us_ * 1000) {
    ++s.metrics.filtered;
    return;
  }
  ++s.metrics.retained;
  t.timestamp_ns_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
  const auto spans = Spans(t);
  for (unsigned i = 0; i < spans.size(); ++i)
    if (Valid(spans[i])) s.metrics.hist[t.is_set_][i].Add(Duration(spans[i]));
  if (s.history_size == kHistory)
    ++s.metrics.overwritten;
  else
    ++s.history_size;
  s.history[s.history_next++ % kHistory] = t;
  if (!g_log_enabled.load(std::memory_order_relaxed)) return;
  if (t.done_ns_ - s.log_epoch >= 1000000000) {
    s.log_epoch = t.done_ns_;
    s.log_count = 0;
  }
  const auto head = s.log_head.load(std::memory_order_relaxed);
  const auto limit = g_log_limit.load(std::memory_order_relaxed);
  if ((limit != 0 && s.log_count >= limit) ||
      head - s.log_tail.load(std::memory_order_acquire) == kLogCapacity) {
    ++s.metrics.log_dropped;
    return;
  }
  ++s.log_count;
  s.logs[head % kLogCapacity] = t;
  s.log_head.store(head + 1, std::memory_order_release);
}
}  // namespace

std::uint64_t LatencyMonotonicNanos() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}
void InitLatencyDiagnostics(unsigned workers, const LoggingOptions* options) {
  StopLatencyDiagnostics();
  g_count = workers;
  g_shards = std::make_unique<Shard[]>(workers);
  g_deadline_ns = 0;
  for (unsigned w = 0; w < workers; ++w) {
    auto& s = g_shards[w];
    s.random = 0x9e3779b97f4a7c15ULL ^ (w + 1);
    for (unsigned i = 0; i < kCapacity; ++i) s.free[i] = i;
  }
  const auto settings = options ? *options : LoggingOptions{};
  g_sink_dropped = 0;
  g_logger = std::jthread([settings](std::stop_token stop) {
    std::shared_ptr<spdlog::logger> logger;
    std::uint64_t retry_ns = 0;
    // This sink is deliberately not registered as the default logger and
    // shares no sink mutex or stderr descriptor with ordinary worker logging.
    const auto open_logger = [&] {
      const auto path =
          std::filesystem::path(settings.log_dir_) / "latency-diagnostics.log";
      auto sink = std::make_shared<spdlog::sinks::rotating_file_sink_st>(
          path.string(), settings.max_log_size_mb_ * 1024 * 1024,
          settings.max_log_files_ - 1, false);
      logger = std::make_shared<spdlog::logger>("latency-diagnostics", sink);
      logger->set_pattern("%v");
      logger->set_error_handler([](const std::string&) { ++g_sink_dropped; });
    };
    while (!stop.stop_requested()) {
      for (unsigned w = 0; w < g_count; ++w) {
        auto& s = g_shards[w];
        auto tail = s.log_tail.load(std::memory_order_relaxed);
        const auto head = s.log_head.load(std::memory_order_acquire);
        while (tail != head && !stop.stop_requested()) {
          const auto entry = s.logs[tail % kLogCapacity];
          s.log_tail.store(++tail, std::memory_order_release);
          if (!logger && LatencyMonotonicNanos() >= retry_ns) {
            try {
              open_logger();
            } catch (const spdlog::spdlog_ex&) {
              retry_ns = LatencyMonotonicNanos() + 1000000000;
            }
          }
          if (logger)
            logger->info("{}", Json(entry));
          else
            ++g_sink_dropped;
        }
      }
      if (logger) logger->flush();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  });
}
void StopLatencyDiagnostics() {
  if (g_logger.joinable()) {
    g_logger.request_stop();
    g_logger.join();
  }
}
OnlineTraceHandle SelectOnlineTrace(std::string_view command) {
  if (!Enabled()) return {};
  const bool set = absl::EqualsIgnoreCase(command, "SET");
  if (!set && !absl::EqualsIgnoreCase(command, "GET")) return {};
  auto* s = Local();
  if (!s) return {};
  ++s->metrics.eligible;
  const auto n = g_sample.load(std::memory_order_relaxed);
  if (n != 1) {
    s->random ^= s->random << 13;
    s->random ^= s->random >> 7;
    s->random ^= s->random << 17;
    if (s->random % n != 0) return {};
  }
  ++s->metrics.selected;
  if (!s->free_count) {
    ++s->metrics.pool_dropped;
    return {};
  }
  const auto slot = s->free[--s->free_count];
  auto& t = s->pool[slot];
  t = {};
  t.origin_ = bycorf::ThisWorker().id_;
  t.slot_ = slot;
  t.id_ = (++s->sequence << 16) | t.origin_;
  t.ready_ns_ = LatencyMonotonicNanos();
  t.is_set_ = set;
  t.threshold_us_ = g_threshold.load(std::memory_order_relaxed);
  t.sample_every_ = n;
  return OnlineTraceHandle(&t);
}
OnlineTraceHandle& OnlineTraceHandle::operator=(
    OnlineTraceHandle&& other) noexcept {
  if (trace_) Release(trace_, true);
  trace_ = std::exchange(other.trace_, nullptr);
  return *this;
}
OnlineTraceHandle::~OnlineTraceHandle() {
  if (trace_) Release(trace_, true);
}
void OnlineTraceHandle::Complete(bool ok) {
  if (!trace_) return;
  trace_->done_ns_ = LatencyMonotonicNanos();
  trace_->write_ok_ = ok;
  Retain(*trace_);
  Release(std::exchange(trace_, nullptr), false);
}
void AdjustRequestQueue(std::int64_t delta) noexcept {
  if (auto* s = Local()) s->metrics.queued += delta;
}
void AdjustResponseWrites(std::int64_t delta) noexcept {
  if (auto* s = Local()) s->metrics.writes += delta;
}
void BeginLatencyWait(LatencyResource resource) noexcept {
  if (auto* s = Local()) {
    const auto i = static_cast<unsigned>(resource);
    ++s->metrics.events[i];
    ++s->metrics.waiters[i];
  }
}
void EndLatencyWait(LatencyResource resource) noexcept {
  if (auto* s = Local()) --s->metrics.waiters[static_cast<unsigned>(resource)];
}
absl::Status ConfigureLatencyDiagnostics(std::string_view name,
                                         std::string_view value) {
  if (name == "latency-diagnostics-log-enabled") {
    if (!absl::EqualsIgnoreCase(value, "yes") &&
        !absl::EqualsIgnoreCase(value, "no"))
      return absl::InvalidArgumentError("expected yes or no");
    g_log_enabled = absl::EqualsIgnoreCase(value, "yes");
    return absl::OkStatus();
  }
  if (name == "latency-diagnostics-enabled") {
    if (!absl::EqualsIgnoreCase(value, "yes") &&
        !absl::EqualsIgnoreCase(value, "no"))
      return absl::InvalidArgumentError("expected yes or no");
    g_deadline_ns.store(
        absl::EqualsIgnoreCase(value, "yes")
            ? LatencyMonotonicNanos() +
                  static_cast<std::uint64_t>(g_duration.load()) * 1000000000
            : 0);
    return absl::OkStatus();
  }
  std::uint64_t n = 0;
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), n);
  if (error != std::errc{} || end != value.data() + value.size())
    return absl::InvalidArgumentError(
        "value is not an integer or out of range");
  if (name == "latency-diagnostics-log-max-per-second" && n <= 1000000)
    g_log_limit = n;
  else if (name == "latency-diagnostics-duration-seconds" && n >= 1 &&
           n <= 3600)
    g_duration = n;
  else if (name == "latency-diagnostics-sample-every" && n >= 1 && n <= 1000000)
    g_sample = n;
  else if (name == "latency-diagnostics-slow-threshold-us" &&
           n <= 3600000000ULL)
    g_threshold = n;
  else
    return absl::InvalidArgumentError("value out of range");
  return absl::OkStatus();
}
std::string LatencyDiagnosticsConfig(std::string_view name) {
  if (name == "latency-diagnostics-log-max-per-second")
    return std::to_string(g_log_limit.load());
  if (name == "latency-diagnostics-log-enabled")
    return g_log_enabled.load() ? "yes" : "no";
  if (name == "latency-diagnostics-enabled") return Enabled() ? "yes" : "no";
  if (name == "latency-diagnostics-duration-seconds")
    return std::to_string(g_duration.load());
  if (name == "latency-diagnostics-sample-every")
    return std::to_string(g_sample.load());
  return std::to_string(g_threshold.load());
}
bycorf::Task<std::vector<std::string>> GetLatencyTraces(std::size_t count,
                                                        std::uint64_t id) {
  std::vector<OnlineLatencyTrace> entries;
  for (unsigned w = 0; w < g_count; ++w) {
    auto part = co_await bycorf::SubmitTo(w, [w, id] {
      std::vector<OnlineLatencyTrace> result;
      const auto& s = g_shards[w];
      for (unsigned i = 0; i < s.history_size; ++i) {
        const auto& t = s.history[(s.history_next - 1 - i) % kHistory];
        if (!id || t.id_ == id) result.push_back(t);
      }
      return result;
    });
    entries.insert(entries.end(), part.begin(), part.end());
  }
  std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
    return a.done_ns_ > b.done_ns_;
  });
  std::vector<std::string> result;
  for (unsigned i = 0; i < std::min(count, entries.size()); ++i)
    result.push_back(Json(entries[i]));
  co_return result;
}
bycorf::Task<absl::Status> ResetLatencyTraces() {
  for (unsigned w = 0; w < g_count; ++w)
    co_await bycorf::SubmitTo(w, [w] {
      g_shards[w].history_size = 0;
      return true;
    });
  co_return absl::OkStatus();
}
bycorf::Task<std::string> RenderLatencyDiagnosticsMetrics() {
  Metrics total;
  unsigned inflight = 0;
  for (unsigned w = 0; w < g_count; ++w) {
    const auto copy = co_await bycorf::SubmitTo(w, [w] {
      return std::pair(g_shards[w].metrics, kCapacity - g_shards[w].free_count);
    });
    const auto& m = copy.first;
    inflight += copy.second;
    total.queued += m.queued;
    total.writes += m.writes;
    for (unsigned i = 0; i < kResources.size(); ++i) {
      total.events[i] += m.events[i];
      total.waiters[i] += m.waiters[i];
    }
    total.eligible += m.eligible;
    total.selected += m.selected;
    total.pool_dropped += m.pool_dropped;
    total.filtered += m.filtered;
    total.retained += m.retained;
    total.overwritten += m.overwritten;
    total.cancelled += m.cancelled;
    total.log_dropped += m.log_dropped;
    for (unsigned c = 0; c < 2; ++c)
      for (unsigned i = 0; i < kStages.size(); ++i) {
        auto& h = total.hist[c][i];
        const auto& v = m.hist[c][i];
        h.ns += v.ns;
        for (unsigned b = 0; b < h.bins.size(); ++b) h.bins[b] += v.bins[b];
      }
  }
  total.log_dropped += g_sink_dropped.load(std::memory_order_relaxed);
  std::string out;
  const auto gauge = [&](std::string_view name, auto value,
                         std::string_view help) {
    absl::StrAppend(&out, "# HELP ", name, " ", help, "\n# TYPE ", name,
                    " gauge\n", name, " ", value, "\n");
  };
  gauge(
      "lavik_request_queue_length", total.queued,
      "Complete parsed commands awaiting dispatch; excludes MULTI residency.");
  gauge("lavik_response_writes_inflight", total.writes,
        "Ordinary Redis socket writes currently awaiting completion.");
  gauge("lavik_latency_diagnostics_log_max_per_second", g_log_limit.load(),
        "Per-worker diagnostic record enqueue limit; zero removes the rate "
        "limit but not the bounded queue.");
  gauge("lavik_latency_diagnostics_log_enabled", g_log_enabled.load() ? 1 : 0,
        "Whether retained diagnostic records are offered to the asynchronous "
        "log sink.");
  gauge("lavik_latency_diagnostics_enabled", Enabled() ? 1 : 0,
        "Whether new eligible commands can be sampled.");
  gauge("lavik_latency_diagnostics_sample_every", g_sample.load(),
        "Selection probability denominator; one means all eligible commands.");
  gauge("lavik_latency_diagnostics_threshold_seconds", g_threshold.load() / 1e6,
        "Server-total threshold applied after send completion.");
  gauge("lavik_latency_diagnostics_inflight", inflight,
        "Leased trace contexts; capacity 256 per data worker.");
  const auto deadline = g_deadline_ns.load();
  const auto now = LatencyMonotonicNanos();
  gauge("lavik_latency_diagnostics_remaining_seconds",
        deadline > now ? (deadline - now) / 1e9 : 0.0,
        "Remaining collection window.");
  absl::StrAppend(&out,
                  "# HELP lavik_wait_events_total Actual coroutine suspensions "
                  "by resource.\n# TYPE lavik_wait_events_total counter\n# "
                  "HELP lavik_waiters Suspended resource waiters, including "
                  "maintenance.\n# TYPE lavik_waiters gauge\n");
  for (unsigned i = 0; i < kResources.size(); ++i)
    absl::StrAppend(&out, "lavik_wait_events_total{resource=\"", kResources[i],
                    "\"} ", total.events[i], "\n", "lavik_waiters{resource=\"",
                    kResources[i], "\"} ", total.waiters[i], "\n");
  absl::StrAppend(
      &out,
      "# HELP lavik_latency_diagnostics_samples_total Diagnostic selection and "
      "disposition counts; not full command counts.\n# TYPE "
      "lavik_latency_diagnostics_samples_total counter\n");
  const std::array<std::pair<std::string_view, std::uint64_t>, 8> counters{
      {{"eligible", total.eligible},
       {"selected", total.selected},
       {"pool_dropped", total.pool_dropped},
       {"filtered", total.filtered},
       {"retained", total.retained},
       {"overwritten", total.overwritten},
       {"cancelled", total.cancelled},
       {"log_dropped", total.log_dropped}}};
  for (auto [name, value] : counters)
    absl::StrAppend(&out, "lavik_latency_diagnostics_samples_total{result=\"",
                    name, "\"} ", value, "\n");
  absl::StrAppend(&out,
                  "# HELP lavik_latency_diagnostics_stage_seconds Stage "
                  "durations of selected commands above the server-total "
                  "threshold; overlapping spans are not additive.\n# TYPE "
                  "lavik_latency_diagnostics_stage_seconds histogram\n");
  for (unsigned c = 0; c < 2; ++c)
    for (unsigned i = 0; i < kStages.size(); ++i) {
      const auto& h = total.hist[c][i];
      const auto labels = absl::StrCat("command=\"", c ? "SET" : "GET",
                                       "\",stage=\"", kStages[i], "\"");
      std::uint64_t cumulative = 0;
      for (unsigned b = 0; b < h.bins.size(); ++b) {
        cumulative += h.bins[b];
        const auto bound = b == kBoundsUs.size()
                               ? std::string("+Inf")
                               : absl::StrCat(kBoundsUs[b] / 1e6);
        absl::StrAppend(&out, "lavik_latency_diagnostics_stage_seconds_bucket{",
                        labels, ",le=\"", bound, "\"} ", cumulative, "\n");
      }
      absl::StrAppend(&out, "lavik_latency_diagnostics_stage_seconds_sum{",
                      labels, "} ", h.ns / 1e9, "\n",
                      "lavik_latency_diagnostics_stage_seconds_count{", labels,
                      "} ", cumulative, "\n");
    }
  co_return out;
}
}  // namespace lavik
