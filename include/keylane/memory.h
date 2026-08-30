#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "absl/status/status.h"

namespace keylane {

class RetainedMemoryCharge;

// Worker IDs occupy ten bits in storage's runtime record-location index. Keep
// memory admission and that representation on the same process-wide limit.
inline constexpr unsigned kMaxMemoryWorkers = 1024;

// User-facing maxmemory-clients value. Percentage values are resolved against
// the effective maxmemory after automatic host/cgroup sizing; byte values are
// absolute. Zero disables the ordinary-client request-buffer limit.
struct ClientBufferLimit {
  std::uint64_t value_ = 5;
  bool percentage_ = true;
};

struct MemoryStats {
  std::uint64_t used_bytes_ = 0;
  std::uint64_t rss_bytes_ = 0;
  std::uint64_t committed_bytes_ = 0;
  std::uint64_t reserved_bytes_ = 0;
  std::uint64_t peak_used_bytes_ = 0;
  std::uint64_t max_bytes_ = 0;
  std::uint64_t client_buffer_limit_bytes_ = 0;
  std::uint64_t client_buffered_bytes_ = 0;
  std::uint64_t fullsync_reserved_bytes_ = 0;
  std::uint64_t admission_pending_bytes_ = 0;
  std::uint64_t rejected_commands_ = 0;
};

// Snapshot of the accounting inputs used by one worker's memory gates.
// Retained bytes include that worker's deterministic share of allocations
// created outside a bound worker, so retained + pending + full-sync reserved
// reconciles with the worker-local admission decision. Temporary allocations
// and RSS remain process-wide diagnostics and are intentionally absent.
struct WorkerMemoryStats {
  std::uint64_t retained_bytes_ = 0;
  std::uint64_t admission_pending_bytes_ = 0;
  std::uint64_t fullsync_reserved_bytes_ = 0;
  std::uint64_t client_buffered_bytes_ = 0;
  std::uint64_t retained_limit_bytes_ = 0;
};

// Holds retained-memory headroom while a long-lived allocation is being
// constructed. It is intentionally short-lived: the retained owner takes over
// the actual usable bytes once allocation succeeds.
class MemoryReservation {
 public:
  MemoryReservation() noexcept = default;
  MemoryReservation(const MemoryReservation&) = delete;
  MemoryReservation& operator=(const MemoryReservation&) = delete;
  MemoryReservation(MemoryReservation&& other) noexcept;
  MemoryReservation& operator=(MemoryReservation&& other) noexcept;
  ~MemoryReservation();

  explicit operator bool() const noexcept { return admitted_; }
  std::size_t bytes() const noexcept { return bytes_; }

  // Converts this pending admission into live retained-memory accounting.
  // The retained owner must later return the same byte count with
  // ReleaseRetainedMemory.
  void Commit(std::size_t retained_bytes) noexcept;
  // Releases an unconsumed reservation on its owner worker. Shared publisher
  // tokens call this before they return to a coordinating worker; the later
  // destructor is then inert and cannot corrupt another worker's local cache.
  void Release() noexcept;

 private:
  friend class RetainedMemoryCharge;
  friend std::optional<MemoryReservation> TryReserveMemory(
      std::size_t bytes) noexcept;
  MemoryReservation(std::size_t bytes, unsigned shard, bool admitted) noexcept
      : bytes_(bytes), shard_(shard), admitted_(admitted) {}

  std::size_t bytes_ = 0;
  unsigned shard_ = 0;
  bool admitted_ = false;
};

// Move-only ownership of an explicitly accounted retained-memory estimate.
// It is suitable for queue items and staging objects whose destructor may run
// on a worker other than the allocator origin.
class RetainedMemoryCharge {
 public:
  RetainedMemoryCharge() noexcept = default;
  RetainedMemoryCharge(const RetainedMemoryCharge&) = delete;
  RetainedMemoryCharge& operator=(const RetainedMemoryCharge&) = delete;
  RetainedMemoryCharge(RetainedMemoryCharge&& other) noexcept;
  RetainedMemoryCharge& operator=(RetainedMemoryCharge&& other) noexcept;
  ~RetainedMemoryCharge();

  // Takes over an already admitted reservation after its allocation succeeds.
  void Adopt(MemoryReservation* reservation,
             std::size_t retained_bytes) noexcept;
  // Accounts an existing logical retained owner. Callers that require
  // admission must reserve before using this operation.
  void Account(unsigned owner_shard, std::size_t retained_bytes) noexcept;
  // Adjusts an existing charge owned by the same shard. Growth must already
  // be covered by an enclosing admission.
  void Resize(std::size_t retained_bytes) noexcept;
  void Reset() noexcept;

  std::size_t bytes() const noexcept { return bytes_; }

 private:
  unsigned owner_shard_ = 0;
  std::size_t bytes_ = 0;
};

// Accounts request bytes read from ordinary client connections until their
// command finishes. One instance belongs to one connection and must remain on
// the worker where it was created; this keeps acquire/release operations local
// to that worker's cache line.
class ClientBufferReservation {
 public:
  ClientBufferReservation() noexcept;
  ClientBufferReservation(const ClientBufferReservation&) = delete;
  ClientBufferReservation& operator=(const ClientBufferReservation&) = delete;
  ~ClientBufferReservation();

  // Acquires bytes from the calling worker's share of the 5% client-buffer
  // budget. The check happens once after a socket read, not for every parser
  // append. Failure leaves the reservation unchanged.
  [[nodiscard]] bool TryAcquire(std::size_t bytes) noexcept;
  // Releases bytes after parser-discarded input or a completed command. The
  // caller must not release more than it has acquired.
  void Release(std::size_t bytes) noexcept;
  [[nodiscard]] std::size_t bytes() const noexcept { return bytes_; }

 private:
  std::size_t bytes_ = 0;
  unsigned shard_ = 0;
};

// A configured value of zero selects 80% of the host or process-cgroup memory
// capacity, whichever is smaller.
absl::Status InitMemoryLimit(std::uint64_t configured_max_bytes,
                             unsigned worker_count,
                             ClientBufferLimit client_buffer_limit = {});

// Associates subsequent admission and retained allocations with one worker.
// This does not replace the thread's default mimalloc heap: ordinary C++
// allocations use mimalloc without participating in maxmemory accounting.
void BindMemoryAccountingShard(unsigned worker_id) noexcept;

// Returns the process accounting slot bound to this thread. Slot zero is the
// fallback for startup and non-worker threads; worker N owns slot N+1.
unsigned CurrentMemoryAccountingShard() noexcept;

// Explicit retained-memory ownership. These functions are intentionally used
// only by long-lived data structures, not by global new/delete. The owner slot
// travels with the allocation domain, so cross-worker destruction returns
// bytes to the origin without a pointer-to-owner index.
void AccountRetainedMemory(unsigned owner_shard, std::size_t bytes) noexcept;
void ReleaseRetainedMemory(unsigned owner_shard, std::size_t bytes) noexcept;

// Returns retained bytes owned by one worker. Ordinary C++ and request-
// temporary allocations remain visible through RSS and allocator diagnostics.
std::int64_t WorkerMemoryAccountingBytes(unsigned worker_id) noexcept;

// Exposes bounded, scrape-time worker snapshots without adding counters to the
// command path. The returned worker count is stable after server startup; an
// out-of-range worker ID returns an empty snapshot.
unsigned MemoryAccountingWorkerCount() noexcept;
WorkerMemoryStats GetWorkerMemoryStats(unsigned worker_id) noexcept;

// Periodically publishes the cheap per-worker retained-counter sum.
void RefreshMemoryStats() noexcept;
// Explicit INFO/metrics path: refreshes RSS and allocator-wide diagnostics.
void RefreshMemoryDiagnostics() noexcept;
MemoryStats GetMemoryStats() noexcept;

// Conservative preflight for retained allocations. Ten percent of configured
// maxmemory is withheld from retained state; the default client-buffer quota
// may consume five percentage points, leaving five for allocator, IO, and
// request-time peaks. Ordinary temporary allocations do not participate in
// admission; this boundary controls state that can accumulate. It never calls
// into mimalloc and performs only relaxed atomic loads.
bool WouldExceedMemoryLimit(std::size_t additional_bytes) noexcept;
// Reserves headroom in the calling worker's fixed share. Worker reservations
// touch only that worker's cache line; they never contend for a process-global
// balance. A configured limit of zero (before InitMemoryLimit) is treated as
// unlimited so allocator-backed containers remain usable in isolated tests.
std::optional<MemoryReservation> TryReserveMemory(std::size_t bytes) noexcept;
// Returns mimalloc's usable size for an ordinary allocation, or SIZE_MAX when
// the request cannot be represented safely.
std::size_t AllocatorUsableSizeForRequest(std::size_t requested_bytes) noexcept;
// Reserves retained-memory headroom for a full-sync coverage map. The
// reservation is logical until ConsumeFullSyncMemory converts slices into
// parent-accounted capture ownership; cleared partitions restore those slices
// so the same largest-partition budget can be reused at the next handoff.
bool TryReserveFullSyncMemory(std::size_t bytes) noexcept;
// Converts logical full-sync headroom into retained bytes immediately before a
// coverage structure grows. The coverage allocators are marked externally
// accounted, so this conservative parent credit is the sole charge. Restoring
// is used only after the corresponding structures have been cleared.
void ConsumeFullSyncMemory(std::size_t bytes) noexcept;
void RestoreFullSyncMemory(std::size_t bytes) noexcept;
void ReleaseFullSyncMemory(std::size_t bytes) noexcept;
void RecordMemoryRejection() noexcept;

std::string HumanReadableMemory(std::uint64_t bytes);

}  // namespace keylane
