#include "keylane/slowlog.h"

#include <sys/time.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "celer/runtime/cross_core.h"
#include "celer/runtime/cycle_clock.h"

namespace keylane {
namespace {

constexpr std::size_t kMaximumArgumentCount = 32;
constexpr std::size_t kMaximumArgumentLength = 128;

class SlowLogShard {
 public:
  void Configure(std::int64_t threshold_micros, std::size_t max_len) {
    SetThreshold(threshold_micros);
    SetCapacity(max_len);
  }

  void SetThreshold(std::int64_t threshold_micros) noexcept {
    threshold_micros_ = threshold_micros;
    if (threshold_micros < 0) {
      threshold_ticks_ = std::numeric_limits<std::uint64_t>::max();
      return;
    }
    const long double ticks = static_cast<long double>(counter_frequency_) *
                              static_cast<long double>(threshold_micros) /
                              1'000'000.0L;
    if (ticks >=
        static_cast<long double>(std::numeric_limits<std::uint64_t>::max())) {
      threshold_ticks_ = std::numeric_limits<std::uint64_t>::max();
    } else {
      threshold_ticks_ = static_cast<std::uint64_t>(std::ceil(ticks));
    }
  }

  void SetCapacity(std::size_t capacity) {
    const std::size_t keep = std::min(capacity, entries_.size());
    std::vector<SlowLogEntry> replacement;
    replacement.reserve(keep);
    const std::size_t first = entries_.size() - keep;
    for (std::size_t offset = first; offset < entries_.size(); ++offset) {
      replacement.push_back(std::move(entries_[PhysicalIndex(offset)]));
    }
    entries_.swap(replacement);
    head_ = 0;
    capacity_ = capacity;
  }

  bool ShouldRecord(std::uint64_t elapsed_ticks,
                    bool may_block) const noexcept {
    return !may_block && threshold_micros_ >= 0 && capacity_ != 0 &&
           elapsed_ticks >= threshold_ticks_;
  }

  void Push(SlowLogEntry entry) {
    assert(capacity_ != 0);
    if (entries_.size() < capacity_) {
      entries_.push_back(std::move(entry));
      return;
    }
    entries_[head_] = std::move(entry);
    if (++head_ == capacity_) head_ = 0;
  }

  std::vector<SlowLogEntry> Snapshot() const {
    std::vector<SlowLogEntry> result;
    result.reserve(entries_.size());
    for (std::size_t offset = 0; offset < entries_.size(); ++offset) {
      result.push_back(entries_[PhysicalIndex(offset)]);
    }
    return result;
  }

  void Reset() {
    entries_.clear();
    head_ = 0;
  }
  std::size_t size() const noexcept { return entries_.size(); }
  std::size_t capacity() const noexcept { return capacity_; }
  std::int64_t threshold_micros() const noexcept { return threshold_micros_; }
  double counter_frequency() const noexcept { return counter_frequency_; }

 private:
  std::size_t PhysicalIndex(std::size_t offset) const noexcept {
    assert(offset < entries_.size());
    const std::size_t tail = entries_.size() - head_;
    return offset < tail ? head_ + offset : offset - tail;
  }

  std::vector<SlowLogEntry> entries_;
  std::size_t head_ = 0;
  std::size_t capacity_ = 0;
  std::int64_t threshold_micros_ = -1;
  std::uint64_t threshold_ticks_ = std::numeric_limits<std::uint64_t>::max();
  double counter_frequency_ = std::max(1.0, celer::CycleCounterFrequency());
};

std::unique_ptr<SlowLogShard[]> g_slowlog_shards;
unsigned g_slowlog_worker_count = 0;
std::atomic<std::uint64_t> g_next_slowlog_id{0};

SlowLogShard* LocalShard() noexcept {
  if (g_slowlog_shards == nullptr) return nullptr;
  const unsigned worker = celer::ThisWorker().id_;
  if (worker >= g_slowlog_worker_count) return nullptr;
  return &g_slowlog_shards[worker];
}

bool ShouldRedactArgument(std::span<const std::string> args,
                          std::size_t index) {
  if (args.empty() || index == 0) return false;
  if (absl::EqualsIgnoreCase(args.front(), "AUTH")) return true;
  if (absl::EqualsIgnoreCase(args.front(), "HELLO")) {
    for (std::size_t option = 1; option < args.size(); ++option) {
      if (absl::EqualsIgnoreCase(args[option], "AUTH")) {
        return index == option + 1 || index == option + 2;
      }
    }
  }
  if (absl::EqualsIgnoreCase(args.front(), "CONFIG") && args.size() >= 4 &&
      absl::EqualsIgnoreCase(args[1], "SET") &&
      (absl::EqualsIgnoreCase(args[2], "requirepass") ||
       absl::EqualsIgnoreCase(args[2], "masterauth"))) {
    return index == 3;
  }
  return false;
}

std::string TruncateArgument(std::string_view argument) {
  if (argument.size() <= kMaximumArgumentLength) return std::string(argument);
  const std::size_t omitted = argument.size() - kMaximumArgumentLength;
  const std::string suffix = absl::StrCat("... (", omitted, " more bytes)");
  const std::size_t prefix = suffix.size() < kMaximumArgumentLength
                                 ? kMaximumArgumentLength - suffix.size()
                                 : 0;
  return absl::StrCat(argument.substr(0, prefix), suffix);
}

std::vector<std::string> CopyArguments(std::span<const std::string> args) {
  std::vector<std::string> result;
  const bool truncated = args.size() > kMaximumArgumentCount;
  const std::size_t copied =
      truncated ? kMaximumArgumentCount - 1 : args.size();
  result.reserve(copied + static_cast<std::size_t>(truncated));
  for (std::size_t index = 0; index < copied; ++index) {
    result.push_back(ShouldRedactArgument(args, index)
                         ? "(redacted)"
                         : TruncateArgument(args[index]));
  }
  if (truncated) {
    result.push_back(
        absl::StrCat("... (", args.size() - copied, " more arguments)"));
  }
  return result;
}

}  // namespace

void InitSlowLog(unsigned worker_count, std::int64_t threshold_micros,
                 std::size_t max_len) {
  g_slowlog_shards = std::make_unique<SlowLogShard[]>(worker_count);
  g_slowlog_worker_count = worker_count;
  for (unsigned worker = 0; worker < worker_count; ++worker) {
    g_slowlog_shards[worker].Configure(threshold_micros, max_len);
  }
}

void MaybeRecordSlowCommand(std::span<const std::string> args,
                            std::string_view client_address,
                            std::string_view client_name,
                            std::uint64_t elapsed_ticks, bool may_block) {
  SlowLogShard* shard = LocalShard();
  if (shard == nullptr || !shard->ShouldRecord(elapsed_ticks, may_block) ||
      args.empty()) [[likely]] {
    return;
  }

  timeval now{};
  (void)::gettimeofday(&now, nullptr);
  const long double micros =
      static_cast<long double>(elapsed_ticks) * 1'000'000.0L /
      static_cast<long double>(shard->counter_frequency());
  const std::uint64_t duration =
      micros >= static_cast<long double>(
                    std::numeric_limits<std::uint64_t>::max())
          ? std::numeric_limits<std::uint64_t>::max()
          : static_cast<std::uint64_t>(micros);
  shard->Push(SlowLogEntry{
      .id_ = g_next_slowlog_id.fetch_add(1, std::memory_order_relaxed),
      .unix_time_seconds_ = now.tv_sec,
      .duration_micros_ = duration,
      .args_ = CopyArguments(args),
      .client_address_ = std::string(client_address),
      .client_name_ = std::string(client_name),
  });
}

celer::Task<std::vector<SlowLogEntry>> CollectSlowLog(std::size_t count) {
  std::vector<SlowLogEntry> merged;
  for (unsigned worker = 0; worker < g_slowlog_worker_count; ++worker) {
    std::vector<SlowLogEntry> entries = co_await celer::SubmitTo(
        worker, [worker] { return g_slowlog_shards[worker].Snapshot(); });
    std::move(entries.begin(), entries.end(), std::back_inserter(merged));
  }
  std::sort(merged.begin(), merged.end(),
            [](const SlowLogEntry& left, const SlowLogEntry& right) {
              return left.id_ > right.id_;
            });
  const std::size_t max_len = SlowLogMaxLen();
  if (merged.size() > max_len) merged.resize(max_len);
  if (merged.size() > count) merged.resize(count);
  co_return merged;
}

celer::Task<std::size_t> SlowLogLength() {
  std::size_t length = 0;
  const std::size_t max_len = SlowLogMaxLen();
  for (unsigned worker = 0; worker < g_slowlog_worker_count; ++worker) {
    const std::size_t local = co_await celer::SubmitTo(
        worker, [worker] { return g_slowlog_shards[worker].size(); });
    if (local >= max_len - length) co_return max_len;
    length += local;
  }
  co_return length;
}

celer::Task<absl::Status> ResetSlowLog() {
  for (unsigned worker = 0; worker < g_slowlog_worker_count; ++worker) {
    (void)co_await celer::SubmitTo(worker, [worker] {
      g_slowlog_shards[worker].Reset();
      return true;
    });
  }
  co_return absl::OkStatus();
}

celer::Task<absl::Status> ConfigureSlowLogThreshold(
    std::int64_t threshold_micros) {
  for (unsigned worker = 0; worker < g_slowlog_worker_count; ++worker) {
    (void)co_await celer::SubmitTo(worker, [worker, threshold_micros] {
      g_slowlog_shards[worker].SetThreshold(threshold_micros);
      return true;
    });
  }
  co_return absl::OkStatus();
}

celer::Task<absl::Status> ConfigureSlowLogMaxLen(std::size_t max_len) {
  for (unsigned worker = 0; worker < g_slowlog_worker_count; ++worker) {
    (void)co_await celer::SubmitTo(worker, [worker, max_len] {
      g_slowlog_shards[worker].SetCapacity(max_len);
      return true;
    });
  }
  co_return absl::OkStatus();
}

std::int64_t SlowLogThresholdMicros() noexcept {
  SlowLogShard* shard = LocalShard();
  return shard == nullptr ? -1 : shard->threshold_micros();
}

std::size_t SlowLogMaxLen() noexcept {
  SlowLogShard* shard = LocalShard();
  return shard == nullptr ? 0 : shard->capacity();
}

}  // namespace keylane
