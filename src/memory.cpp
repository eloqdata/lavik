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
#if KEYLANE_USE_MIMALLOC
#include "mimalloc-stats.h"
#endif

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

static_assert(sizeof(MemoryGaugeCache) == 64);
static_assert(sizeof(MemoryCounterCache) == 64);

MemoryGaugeCache g_memory_gauges;
MemoryCounterCache g_memory_counters;

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
  const int fd = ::open("/proc/self/statm", O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return 0;
  }
  std::array<char, 128> buffer{};
  const ssize_t bytes = ::read(fd, buffer.data(), buffer.size());
  (void)::close(fd);
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
  const long page_size = ::sysconf(_SC_PAGESIZE);
  if (page_size <= 0 || pages > std::numeric_limits<std::uint64_t>::max() /
                                    static_cast<std::uint64_t>(page_size)) {
    return 0;
  }
  return pages * static_cast<std::uint64_t>(page_size);
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

absl::Status InitMemoryLimit(std::uint64_t configured_max_bytes) {
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
  return absl::OkStatus();
}

void RefreshMemoryStats() noexcept {
  std::uint64_t used = 0;
  std::uint64_t committed = 0;
  std::uint64_t reserved = 0;
#if KEYLANE_USE_MIMALLOC
  mi_stats_t_decl(stats);
  if (mi_stats_get(&stats)) {
    used = static_cast<std::uint64_t>(
        std::max<std::int64_t>(0, stats.malloc_normal.current) +
        std::max<std::int64_t>(0, stats.malloc_huge.current));
    committed = static_cast<std::uint64_t>(
        std::max<std::int64_t>(0, stats.committed.current));
    reserved = static_cast<std::uint64_t>(
        std::max<std::int64_t>(0, stats.reserved.current));
  }
#endif
  const std::uint64_t rss = ProcessRss();
#if !KEYLANE_USE_MIMALLOC
  used = rss;
  committed = rss;
  reserved = rss;
#endif
  g_memory_gauges.used_bytes_.store(used, std::memory_order_relaxed);
  g_memory_gauges.rss_bytes_.store(rss, std::memory_order_relaxed);
  g_memory_gauges.committed_bytes_.store(committed, std::memory_order_relaxed);
  g_memory_gauges.reserved_bytes_.store(reserved, std::memory_order_relaxed);
  UpdatePeak(used);
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
  const std::uint64_t rss =
      g_memory_gauges.rss_bytes_.load(std::memory_order_relaxed);
  const std::uint64_t observed = std::max(used, rss);
  return observed >= maximum || additional_bytes > maximum - observed;
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
