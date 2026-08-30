#include "keylane/memory.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "mimalloc-stats.h"
#include "mimalloc.h"

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
  // Retained allocations are much less frequent than ordinary new/delete, so
  // a direct RMW keeps cross-worker destruction correct without a pointer
  // owner table or an owner-thread folding protocol.
  std::atomic<std::uint64_t> retained_bytes_{0};
  std::atomic<std::uint64_t> admission_pending_bytes_{0};
  std::atomic<std::uint64_t> fullsync_reserved_bytes_{0};
  std::atomic<std::uint64_t> client_buffered_bytes_{0};
};

static_assert(sizeof(MemoryGaugeCache) == 64);
static_assert(sizeof(MemoryCounterCache) == 64);
static_assert(sizeof(AllocationShard) == 64);

MemoryGaugeCache g_memory_gauges;
MemoryCounterCache g_memory_counters;
std::atomic<std::uint64_t> g_client_buffer_limit_value{5};
std::atomic<bool> g_client_buffer_limit_percentage{true};
// Slot zero collects allocations made outside a bound worker. Worker N uses
// slot N+1, so workers never update the same cache line.
std::array<AllocationShard, kMaxMemoryWorkers + 1> g_allocation_shards;
std::atomic<unsigned> g_accounted_workers{0};
thread_local unsigned g_allocation_shard = 0;
thread_local std::uint64_t g_local_admission_pending_bytes = 0;
thread_local std::uint64_t g_local_fullsync_reserved_bytes = 0;
thread_local std::uint64_t g_local_client_buffered_bytes = 0;

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

std::uint64_t RetainedUsed() noexcept {
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  std::uint64_t total = 0;
  for (unsigned index = 0; index <= workers; ++index) {
    const std::uint64_t shard = g_allocation_shards[index].retained_bytes_.load(
        std::memory_order_relaxed);
    if (shard > std::numeric_limits<std::uint64_t>::max() - total) {
      return std::numeric_limits<std::uint64_t>::max();
    }
    total += shard;
  }
  return total;
}

std::uint64_t SumAdmissionPending() noexcept {
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  std::uint64_t total = 0;
  for (unsigned index = 0; index <= workers; ++index) {
    const std::uint64_t value =
        g_allocation_shards[index].admission_pending_bytes_.load(
            std::memory_order_relaxed);
    total = value > std::numeric_limits<std::uint64_t>::max() - total
                ? std::numeric_limits<std::uint64_t>::max()
                : total + value;
  }
  return total;
}

std::uint64_t SumFullSyncReserved() noexcept {
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  std::uint64_t total = 0;
  for (unsigned index = 0; index <= workers; ++index) {
    const std::uint64_t value =
        g_allocation_shards[index].fullsync_reserved_bytes_.load(
            std::memory_order_relaxed);
    total = value > std::numeric_limits<std::uint64_t>::max() - total
                ? std::numeric_limits<std::uint64_t>::max()
                : total + value;
  }
  return total;
}

std::uint64_t SumClientBuffered() noexcept {
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  std::uint64_t total = 0;
  for (unsigned index = 0; index <= workers; ++index) {
    const std::uint64_t value =
        g_allocation_shards[index].client_buffered_bytes_.load(
            std::memory_order_relaxed);
    total = value > std::numeric_limits<std::uint64_t>::max() - total
                ? std::numeric_limits<std::uint64_t>::max()
                : total + value;
  }
  return total;
}

std::uint64_t DistributedShare(std::uint64_t total, unsigned worker_id,
                               unsigned workers) noexcept {
  return total / workers + (worker_id < total % workers ? 1 : 0);
}

bool WorkerWouldExceed(std::uint64_t maximum, std::size_t additional_bytes,
                       std::uint64_t pending_bytes,
                       std::uint64_t fullsync_bytes) noexcept {
  if (maximum == 0) return false;
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  if (g_allocation_shard == 0 || workers == 0) {
    const std::uint64_t used = RetainedUsed();
    const std::uint64_t reserved = SumFullSyncReserved();
    const std::uint64_t pending = SumAdmissionPending();
    return used >= maximum || reserved > maximum - used ||
           pending > maximum - used - reserved ||
           additional_bytes > maximum - used - reserved - pending;
  }

  const unsigned worker_id = g_allocation_shard - 1;
  const std::uint64_t capacity = DistributedShare(maximum, worker_id, workers);
  const std::uint64_t fallback =
      g_allocation_shards[0].retained_bytes_.load(std::memory_order_relaxed);
  const std::uint64_t shared = DistributedShare(fallback, worker_id, workers);
  const std::uint64_t owned =
      g_allocation_shards[g_allocation_shard].retained_bytes_.load(
          std::memory_order_relaxed);
  return shared >= capacity || owned > capacity - shared ||
         fullsync_bytes > capacity - shared - owned ||
         pending_bytes > capacity - shared - owned - fullsync_bytes ||
         additional_bytes >
             capacity - shared - owned - fullsync_bytes - pending_bytes;
}

std::uint64_t SteadyMemoryLimit() noexcept {
  const std::uint64_t maximum =
      g_memory_gauges.max_bytes_.load(std::memory_order_relaxed);
  // Retained state stops at 90% of the user-visible limit. Client buffers have
  // a separate 5% quota by default, leaving another 5% for ordinary allocator,
  // IO, and request-time peaks even when both admitted classes are full.
  // Temporary allocations are intentionally not admitted one by one.
  return maximum - maximum / 10;
}

constexpr std::uint64_t kMinimumClientBufferBytes = 128 * 1024;

std::uint64_t EffectiveClientBufferLimit(std::uint64_t maximum) noexcept {
  const std::uint64_t configured =
      g_client_buffer_limit_value.load(std::memory_order_relaxed);
  if (configured == 0) return 0;
  std::uint64_t bytes = configured;
  if (g_client_buffer_limit_percentage.load(std::memory_order_relaxed)) {
    // configured is at most 100, so splitting the multiplication preserves
    // the exact floor(maximum * percentage / 100) without overflow.
    bytes = maximum / 100 * configured + maximum % 100 * configured / 100;
  }
  return std::max(bytes, kMinimumClientBufferBytes);
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
                             unsigned worker_count,
                             ClientBufferLimit client_buffer_limit) {
  if (worker_count == 0 || worker_count > kMaxMemoryWorkers) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "worker count exceeds memory accounting capacity");
  }
  if (client_buffer_limit.percentage_ && client_buffer_limit.value_ > 100) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "maxmemory-clients percentage must be between 0% and 100%");
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
  g_client_buffer_limit_value.store(client_buffer_limit.value_,
                                    std::memory_order_relaxed);
  g_client_buffer_limit_percentage.store(client_buffer_limit.percentage_,
                                         std::memory_order_relaxed);
  RefreshMemoryStats();
  RefreshMemoryDiagnostics();
  return absl::OkStatus();
}

void AccountRetainedMemory(unsigned owner_shard, std::size_t bytes) noexcept {
  assert(owner_shard < g_allocation_shards.size());
  if (bytes == 0) return;
  const std::uint64_t previous =
      g_allocation_shards[owner_shard].retained_bytes_.fetch_add(
          bytes, std::memory_order_relaxed);
  // A wrapped accounting total cannot describe a valid process allocation.
  // Fail closed instead of later admitting writes against a small value.
  if (bytes > std::numeric_limits<std::uint64_t>::max() - previous) {
    std::abort();
  }
}

void ReleaseRetainedMemory(unsigned owner_shard, std::size_t bytes) noexcept {
  assert(owner_shard < g_allocation_shards.size());
  if (bytes == 0) return;
  const std::uint64_t previous =
      g_allocation_shards[owner_shard].retained_bytes_.fetch_sub(
          bytes, std::memory_order_relaxed);
  if (previous < bytes) {
    // Continuing after underflow would make this worker appear to own nearly
    // UINT64_MAX bytes and permanently poison admission and diagnostics.
    std::abort();
  }
}

void BindMemoryAccountingShard(unsigned worker_id) noexcept {
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  const unsigned next = worker_id < workers ? worker_id + 1 : 0;
  if (next == g_allocation_shard) return;
  g_allocation_shard = next;
  g_local_admission_pending_bytes =
      g_allocation_shards[next].admission_pending_bytes_.load(
          std::memory_order_relaxed);
  g_local_fullsync_reserved_bytes =
      g_allocation_shards[next].fullsync_reserved_bytes_.load(
          std::memory_order_relaxed);
  g_local_client_buffered_bytes =
      g_allocation_shards[next].client_buffered_bytes_.load(
          std::memory_order_relaxed);
}

unsigned CurrentMemoryAccountingShard() noexcept { return g_allocation_shard; }

std::int64_t WorkerMemoryAccountingBytes(unsigned worker_id) noexcept {
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  if (worker_id >= workers) return 0;
  const std::uint64_t bytes =
      g_allocation_shards[worker_id + 1].retained_bytes_.load(
          std::memory_order_relaxed);
  return bytes > static_cast<std::uint64_t>(
                     std::numeric_limits<std::int64_t>::max())
             ? std::numeric_limits<std::int64_t>::max()
             : static_cast<std::int64_t>(bytes);
}

void RefreshMemoryStats() noexcept {
  const std::uint64_t used = RetainedUsed();
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
      .client_buffer_limit_bytes_ = EffectiveClientBufferLimit(
          g_memory_gauges.max_bytes_.load(std::memory_order_relaxed)),
      .client_buffered_bytes_ = SumClientBuffered(),
      .fullsync_reserved_bytes_ = SumFullSyncReserved(),
      .admission_pending_bytes_ = SumAdmissionPending(),
      .rejected_commands_ =
          g_memory_counters.rejected_commands_.load(std::memory_order_relaxed),
  };
}

bool WouldExceedMemoryLimit(std::size_t additional_bytes) noexcept {
  return WorkerWouldExceed(SteadyMemoryLimit(), additional_bytes,
                           g_local_admission_pending_bytes,
                           g_local_fullsync_reserved_bytes);
}

ClientBufferReservation::ClientBufferReservation() noexcept
    : shard_(g_allocation_shard) {}

ClientBufferReservation::~ClientBufferReservation() { Release(bytes_); }

bool ClientBufferReservation::TryAcquire(std::size_t bytes) noexcept {
  assert(shard_ == g_allocation_shard &&
         "client buffers must stay on their connection worker");
  if (bytes == 0) return true;
  if (bytes > std::numeric_limits<std::size_t>::max() - bytes_) return false;

  const std::uint64_t maximum =
      g_memory_gauges.max_bytes_.load(std::memory_order_relaxed);
  if (maximum == 0) {
    bytes_ += bytes;
    if (shard_ == 0) {
      g_allocation_shards[0].client_buffered_bytes_.fetch_add(
          bytes, std::memory_order_relaxed);
    } else {
      g_local_client_buffered_bytes += bytes;
      g_allocation_shards[shard_].client_buffered_bytes_.store(
          g_local_client_buffered_bytes, std::memory_order_relaxed);
    }
    return true;
  }
  const unsigned workers = g_accounted_workers.load(std::memory_order_acquire);
  if (shard_ == 0 || workers == 0) return false;

  const std::uint64_t total_capacity = EffectiveClientBufferLimit(maximum);
  if (total_capacity == 0) {
    bytes_ += bytes;
    g_local_client_buffered_bytes += bytes;
    g_allocation_shards[shard_].client_buffered_bytes_.store(
        g_local_client_buffered_bytes, std::memory_order_relaxed);
    return true;
  }
  const unsigned worker_id = shard_ - 1;
  const std::uint64_t capacity =
      DistributedShare(total_capacity, worker_id, workers);
  if (g_local_client_buffered_bytes > capacity ||
      bytes > capacity - g_local_client_buffered_bytes) {
    return false;
  }
  bytes_ += bytes;
  g_local_client_buffered_bytes += bytes;
  g_allocation_shards[shard_].client_buffered_bytes_.store(
      g_local_client_buffered_bytes, std::memory_order_relaxed);
  return true;
}

void ClientBufferReservation::Release(std::size_t bytes) noexcept {
  assert(shard_ == g_allocation_shard &&
         "client buffers must stay on their connection worker");
  assert(bytes <= bytes_);
  bytes_ -= bytes;
  if (bytes == 0) return;
  if (shard_ == 0) {
    const std::uint64_t previous =
        g_allocation_shards[0].client_buffered_bytes_.fetch_sub(
            bytes, std::memory_order_relaxed);
    (void)previous;
    assert(previous >= bytes);
    return;
  }
  assert(g_local_client_buffered_bytes >= bytes);
  g_local_client_buffered_bytes -= bytes;
  g_allocation_shards[shard_].client_buffered_bytes_.store(
      g_local_client_buffered_bytes, std::memory_order_relaxed);
}

MemoryReservation::MemoryReservation(MemoryReservation&& other) noexcept
    : bytes_(std::exchange(other.bytes_, 0)),
      shard_(std::exchange(other.shard_, 0)),
      admitted_(std::exchange(other.admitted_, false)) {}

MemoryReservation& MemoryReservation::operator=(
    MemoryReservation&& other) noexcept {
  if (this != &other) {
    Release();
    bytes_ = std::exchange(other.bytes_, 0);
    shard_ = std::exchange(other.shard_, 0);
    admitted_ = std::exchange(other.admitted_, false);
  }
  return *this;
}

MemoryReservation::~MemoryReservation() { Release(); }

void MemoryReservation::Commit(std::size_t retained_bytes) noexcept {
  assert(admitted_ && "only an admitted reservation can be committed");
  const unsigned owner_shard = shard_;
  Release();
  AccountRetainedMemory(owner_shard, retained_bytes);
}

void MemoryReservation::Release() noexcept {
  if (!admitted_) return;
  admitted_ = false;
  if (bytes_ == 0) return;
  assert(shard_ == g_allocation_shard &&
         "memory reservations must be released on their owning worker");
  if (shard_ == 0) {
    const std::uint64_t previous =
        g_allocation_shards[0].admission_pending_bytes_.fetch_sub(
            bytes_, std::memory_order_relaxed);
    (void)previous;
    assert(previous >= bytes_);
    bytes_ = 0;
    return;
  }
  assert(g_local_admission_pending_bytes >= bytes_);
  g_local_admission_pending_bytes -= bytes_;
  g_allocation_shards[shard_].admission_pending_bytes_.store(
      g_local_admission_pending_bytes, std::memory_order_relaxed);
  bytes_ = 0;
}

RetainedMemoryCharge::RetainedMemoryCharge(
    RetainedMemoryCharge&& other) noexcept
    : owner_shard_(std::exchange(other.owner_shard_, 0)),
      bytes_(std::exchange(other.bytes_, 0)) {}

RetainedMemoryCharge& RetainedMemoryCharge::operator=(
    RetainedMemoryCharge&& other) noexcept {
  if (this != &other) {
    Reset();
    owner_shard_ = std::exchange(other.owner_shard_, 0);
    bytes_ = std::exchange(other.bytes_, 0);
  }
  return *this;
}

RetainedMemoryCharge::~RetainedMemoryCharge() { Reset(); }

void RetainedMemoryCharge::Adopt(MemoryReservation* reservation,
                                 std::size_t retained_bytes) noexcept {
  assert(reservation != nullptr && reservation->admitted_);
  Reset();
  owner_shard_ = reservation->shard_;
  bytes_ = retained_bytes;
  reservation->Commit(retained_bytes);
}

void RetainedMemoryCharge::Account(unsigned owner_shard,
                                   std::size_t retained_bytes) noexcept {
  Reset();
  owner_shard_ = owner_shard;
  bytes_ = retained_bytes;
  AccountRetainedMemory(owner_shard_, bytes_);
}

void RetainedMemoryCharge::Resize(std::size_t retained_bytes) noexcept {
  if (retained_bytes > bytes_) {
    AccountRetainedMemory(owner_shard_, retained_bytes - bytes_);
  } else if (retained_bytes < bytes_) {
    ReleaseRetainedMemory(owner_shard_, bytes_ - retained_bytes);
  }
  bytes_ = retained_bytes;
}

void RetainedMemoryCharge::Reset() noexcept {
  if (bytes_ == 0) return;
  ReleaseRetainedMemory(owner_shard_, bytes_);
  bytes_ = 0;
  owner_shard_ = 0;
}

std::optional<MemoryReservation> TryReserveMemory(std::size_t bytes) noexcept {
  const std::uint64_t maximum = SteadyMemoryLimit();
  if (bytes == 0 || maximum == 0) {
    return MemoryReservation(0, g_allocation_shard, true);
  }
  if (WorkerWouldExceed(maximum, bytes, g_local_admission_pending_bytes,
                        g_local_fullsync_reserved_bytes)) {
    return std::nullopt;
  }
  if (g_allocation_shard == 0) {
    g_allocation_shards[0].admission_pending_bytes_.fetch_add(
        bytes, std::memory_order_relaxed);
    return MemoryReservation(bytes, 0, true);
  }
  g_local_admission_pending_bytes += bytes;
  g_allocation_shards[g_allocation_shard].admission_pending_bytes_.store(
      g_local_admission_pending_bytes, std::memory_order_relaxed);
  return MemoryReservation(bytes, g_allocation_shard, true);
}

std::size_t AllocatorUsableSizeForRequest(
    std::size_t requested_bytes) noexcept {
  if (requested_bytes == 0) return 0;
  const std::size_t usable = mi_good_size(requested_bytes);
  // Treat an allocator result smaller than the request as an overflow or
  // unsupported-size signal. Admission must fail closed before new is called.
  return usable >= requested_bytes ? usable
                                   : std::numeric_limits<std::size_t>::max();
}

bool TryReserveFullSyncMemory(std::size_t bytes) noexcept {
  if (bytes == 0) return true;
  const std::uint64_t maximum = SteadyMemoryLimit();
  if (WorkerWouldExceed(maximum, bytes, g_local_admission_pending_bytes,
                        g_local_fullsync_reserved_bytes)) {
    return false;
  }
  g_local_fullsync_reserved_bytes += bytes;
  g_allocation_shards[g_allocation_shard].fullsync_reserved_bytes_.store(
      g_local_fullsync_reserved_bytes, std::memory_order_relaxed);
  return true;
}

void ConsumeFullSyncMemory(std::size_t bytes) noexcept {
  if (bytes == 0) return;
  assert(g_local_fullsync_reserved_bytes >= bytes);
  g_local_fullsync_reserved_bytes -= bytes;
  g_allocation_shards[g_allocation_shard].fullsync_reserved_bytes_.store(
      g_local_fullsync_reserved_bytes, std::memory_order_relaxed);
  // Full-sync coverage uses ordinary STL containers, but the session computes
  // and reserves their conservative retained footprint before capture. Move
  // consumed credit into retained accounting instead of instrumenting every
  // temporary node allocation.
  AccountRetainedMemory(g_allocation_shard, bytes);
}

void RestoreFullSyncMemory(std::size_t bytes) noexcept {
  if (bytes == 0) return;
  ReleaseRetainedMemory(g_allocation_shard, bytes);
  assert(bytes <= std::numeric_limits<std::uint64_t>::max() -
                      g_local_fullsync_reserved_bytes);
  g_local_fullsync_reserved_bytes += bytes;
  g_allocation_shards[g_allocation_shard].fullsync_reserved_bytes_.store(
      g_local_fullsync_reserved_bytes, std::memory_order_relaxed);
}

void ReleaseFullSyncMemory(std::size_t bytes) noexcept {
  if (bytes == 0) return;
  assert(g_local_fullsync_reserved_bytes >= bytes);
  g_local_fullsync_reserved_bytes -= bytes;
  g_allocation_shards[g_allocation_shard].fullsync_reserved_bytes_.store(
      g_local_fullsync_reserved_bytes, std::memory_order_relaxed);
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
