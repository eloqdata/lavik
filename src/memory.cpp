#include "keylane/memory.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "mimalloc-stats.h"

namespace keylane {
namespace {

struct alignas(64) MemoryGaugeCache {
  std::atomic<std::uint64_t> used_bytes_{0};
  std::atomic<std::uint64_t> rss_bytes_{0};
  std::atomic<std::uint64_t> committed_bytes_{0};
  std::atomic<std::uint64_t> reserved_bytes_{0};
  std::atomic<std::uint64_t> peak_used_bytes_{0};
  std::atomic<std::uint64_t> max_bytes_{0};
};

struct alignas(64) MemoryCounterCache {
  std::atomic<std::uint64_t> rejected_commands_{0};
};

struct alignas(64) AllocationShard {
  std::atomic<std::int64_t> bytes_{0};
};

constexpr unsigned kMaxMemoryWorkers = 1024;

static_assert(sizeof(MemoryGaugeCache) == 64);
static_assert(sizeof(MemoryCounterCache) == 64);
static_assert(sizeof(AllocationShard) == 64);

MemoryGaugeCache g_memory_gauges;
MemoryCounterCache g_memory_counters;
// Slot zero collects allocations made outside a bound worker. Worker N uses
// slot N+1, so workers never update the same cache line.
std::array<AllocationShard, kMaxMemoryWorkers + 1> g_allocation_shards;
std::atomic<unsigned> g_accounted_workers{0};
thread_local unsigned g_allocation_shard = 0;
thread_local std::int64_t g_local_allocated_bytes = 0;

std::string ReadSmallFile(const char* path) {
  const int fd = ::open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return {};
  }
  std::array<char, 16 * 1024> buffer{};
  const ssize_t bytes = ::read(fd, buffer.data(), buffer.size() - 1);
  (void)::close(fd);
  if (bytes <= 0) {
    return {};
  }
  return std::string(buffer.data(), static_cast<std::size_t>(bytes));
}

std::uint64_t ParseUnsigned(std::string_view text) noexcept {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
    text.remove_prefix(1);
  }
  std::uint64_t value = 0;
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  return error == std::errc{} && end != text.data() ? value : 0;
}

std::uint64_t HostMemoryCapacity() {
  const std::string meminfo = ReadSmallFile("/proc/meminfo");
  constexpr std::string_view prefix = "MemTotal:";
  const std::size_t begin = meminfo.find(prefix);
  if (begin == std::string::npos) {
    return 0;
  }
  const std::uint64_t kib =
      ParseUnsigned(std::string_view(meminfo).substr(begin + prefix.size()));
  return kib > std::numeric_limits<std::uint64_t>::max() / 1024
             ? std::numeric_limits<std::uint64_t>::max()
             : kib * 1024;
}

std::uint64_t CgroupMemoryCapacityAt(const char* limit_path) {
  const std::string limit_text = ReadSmallFile(limit_path);
  if (limit_text.empty() || limit_text.starts_with("max")) {
    return 0;
  }
  return ParseUnsigned(limit_text);
}

std::uint64_t MemoryCapacity() {
  const std::uint64_t host = HostMemoryCapacity();
  std::uint64_t cgroup = CgroupMemoryCapacityAt("/sys/fs/cgroup/memory.max");
  if (cgroup == 0) {
    cgroup =
        CgroupMemoryCapacityAt("/sys/fs/cgroup/memory/memory.limit_in_bytes");
  }
  if (host == 0) {
    return cgroup;
  }
  return cgroup == 0 ? host : std::min(host, cgroup);
}

std::uint64_t ProcessRss() noexcept {
  // Metrics and INFO may be scraped repeatedly. Keep the procfs descriptor
  // for the process lifetime and use pread so callers share no file offset.
  static const int fd = ::open("/proc/self/statm", O_RDONLY | O_CLOEXEC);
  static const long page_size = ::sysconf(_SC_PAGESIZE);
  if (fd < 0) {
    return 0;
  }
  std::array<char, 128> buffer{};
  const ssize_t bytes = ::pread(fd, buffer.data(), buffer.size(), 0);
  if (bytes <= 0) {
    return 0;
  }
  const std::string_view statm(buffer.data(), static_cast<std::size_t>(bytes));
  const std::size_t separator = statm.find_first_of(" \t");
  if (separator == std::string::npos) {
    return 0;
  }
  const std::size_t resident = statm.find_first_not_of(" \t", separator);
  if (resident == std::string::npos) {
    return 0;
  }
  const std::uint64_t pages =
      ParseUnsigned(std::string_view(statm).substr(resident));
  if (page_size <= 0 || pages > std::numeric_limits<std::uint64_t>::max() /
                                    static_cast<std::uint64_t>(page_size)) {
    return 0;
  }
  return pages * static_cast<std::uint64_t>(page_size);
}

std::uint64_t AllocatorUsed() noexcept {
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  std::int64_t total = 0;
  for (unsigned index = 0; index <= workers; ++index) {
    const std::int64_t shard =
        g_allocation_shards[index].bytes_.load(std::memory_order_relaxed);
    if (shard > 0 && total > std::numeric_limits<std::int64_t>::max() - shard) {
      return std::numeric_limits<std::uint64_t>::max();
    }
    if (shard < 0 && total < std::numeric_limits<std::int64_t>::min() - shard) {
      return 0;
    }
    total += shard;
  }
  return total > 0 ? static_cast<std::uint64_t>(total) : 0;
}

void UpdatePeak(std::uint64_t current) noexcept {
  std::uint64_t peak =
      g_memory_gauges.peak_used_bytes_.load(std::memory_order_relaxed);
  while (peak < current &&
         !g_memory_gauges.peak_used_bytes_.compare_exchange_weak(
             peak, current, std::memory_order_relaxed)) {
  }
}

}  // namespace

absl::Status InitMemoryLimit(std::uint64_t configured_max_bytes,
                             unsigned worker_count) {
  if (worker_count == 0 || worker_count > kMaxMemoryWorkers) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "worker count exceeds memory accounting capacity");
  }
  g_accounted_workers.store(worker_count, std::memory_order_release);
  std::uint64_t maximum = configured_max_bytes;
  if (maximum == 0) {
    const std::uint64_t capacity = MemoryCapacity();
    if (capacity == 0) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "cannot determine system memory capacity; set "
                          "--max-memory explicitly");
    }
    maximum = capacity - capacity / 5;
  }
  if (maximum == 0) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "max memory must be greater than zero");
  }
  g_memory_gauges.max_bytes_.store(maximum, std::memory_order_relaxed);
  RefreshMemoryStats();
  RefreshMemoryDiagnostics();
  return absl::OkStatus();
}

void AccountMemoryAllocation(std::int64_t delta) noexcept {
  if (g_allocation_shard == 0) {
    // Startup and miscellaneous non-worker threads share the fallback shard;
    // their allocation rate is not part of the command hot path.
    g_allocation_shards[0].bytes_.fetch_add(delta, std::memory_order_relaxed);
    return;
  }
  // One thread owns every non-zero shard. Publish with a plain relaxed atomic
  // store (a normal store on the supported CPUs), avoiding a locked RMW on
  // every allocation/free while worker 0 remains able to sample safely.
  g_local_allocated_bytes += delta;
  g_allocation_shards[g_allocation_shard].bytes_.store(
      g_local_allocated_bytes, std::memory_order_relaxed);
}

void BindMemoryAccountingShard(unsigned worker_id) noexcept {
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  const unsigned next = worker_id < workers ? worker_id + 1 : 0;
  if (next == g_allocation_shard) {
    return;
  }
  g_allocation_shard = next;
  g_local_allocated_bytes =
      g_allocation_shards[next].bytes_.load(std::memory_order_relaxed);
}

void RefreshMemoryStats() noexcept {
  const std::uint64_t used = AllocatorUsed();
  g_memory_gauges.used_bytes_.store(used, std::memory_order_relaxed);
  UpdatePeak(used);
}

void RefreshMemoryDiagnostics() noexcept {
  const std::uint64_t rss = ProcessRss();
  if (rss != 0) {
    g_memory_gauges.rss_bytes_.store(rss, std::memory_order_relaxed);
  }
  mi_stats_t_decl(stats);
  if (mi_stats_get(&stats)) {
    g_memory_gauges.committed_bytes_.store(
        static_cast<std::uint64_t>(
            std::max<std::int64_t>(0, stats.committed.current)),
        std::memory_order_relaxed);
    g_memory_gauges.reserved_bytes_.store(
        static_cast<std::uint64_t>(
            std::max<std::int64_t>(0, stats.reserved.current)),
        std::memory_order_relaxed);
  }
}

MemoryStats GetMemoryStats() noexcept {
  return MemoryStats{
      .used_bytes_ =
          g_memory_gauges.used_bytes_.load(std::memory_order_relaxed),
      .rss_bytes_ = g_memory_gauges.rss_bytes_.load(std::memory_order_relaxed),
      .committed_bytes_ =
          g_memory_gauges.committed_bytes_.load(std::memory_order_relaxed),
      .reserved_bytes_ =
          g_memory_gauges.reserved_bytes_.load(std::memory_order_relaxed),
      .peak_used_bytes_ =
          g_memory_gauges.peak_used_bytes_.load(std::memory_order_relaxed),
      .max_bytes_ = g_memory_gauges.max_bytes_.load(std::memory_order_relaxed),
      .rejected_commands_ =
          g_memory_counters.rejected_commands_.load(std::memory_order_relaxed),
  };
}

bool WouldExceedMemoryLimit(std::size_t additional_bytes) noexcept {
  const std::uint64_t maximum =
      g_memory_gauges.max_bytes_.load(std::memory_order_relaxed);
  const std::uint64_t used =
      g_memory_gauges.used_bytes_.load(std::memory_order_relaxed);
  return used >= maximum || additional_bytes > maximum - used;
}

void RecordMemoryRejection() noexcept {
  g_memory_counters.rejected_commands_.fetch_add(1, std::memory_order_relaxed);
}

std::string HumanReadableMemory(std::uint64_t bytes) {
  constexpr std::array<std::string_view, 7> units{"B", "K", "M", "G",
                                                  "T", "P", "E"};
  long double value = static_cast<long double>(bytes);
  std::size_t unit = 0;
  while (value >= 1024.0L && unit + 1 < units.size()) {
    value /= 1024.0L;
    ++unit;
  }
  const std::uint64_t hundredths =
      static_cast<std::uint64_t>(value * 100.0L + 0.5L);
  return absl::StrCat(hundredths / 100, ".", hundredths % 100 < 10 ? "0" : "",
                      hundredths % 100, units[unit]);
}

}  // namespace keylane
