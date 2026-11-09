/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "absl/hash/hash.h"
#include "bycorf/io/storage.h"
#include "lavik/storage/buffer_pool.h"

namespace lavik::storage {

// File slots remain open for the engine/worker lifetime. The registry is scoped
// to that lifetime; reopening a file would require a new incarnation fence.
// Equality, not the hash, establishes exact same-record/same-population
// identity.
struct ReadFlightKey {
  std::uintptr_t engine = 0;
  std::uint64_t block = 0, allocation_epoch = 0, mutation_sequence = 0;
  std::uint64_t aligned_offset = 0, read_bytes = 0, db_epoch = 0;
  std::uint64_t replication_epoch = 0, population_generation = 0;
  std::uint32_t file = 0, record_offset = 0, record_bytes = 0;
  std::uint16_t block_owner = 0;
  std::uint8_t db = 0;
  bool operator==(const ReadFlightKey&) const = default;
  template <typename H>
  friend H AbslHashValue(H h, const ReadFlightKey& key) {
    // Physical location and mutation already provide hash entropy. Population
    // fences still participate in full equality, including on hash collision;
    // hashing every usually constant fence adds work to every disjoint read.
    return H::combine(std::move(h), key.block, key.record_offset,
                      key.mutation_sequence);
  }
};

class ReadFlightRegistry;
struct ReadFlight;

// Intrusive owner-local waiter embedded in the existing request awaitable.
// Destroying an unqueued waiter detaches it without cancelling the producer.
// A queued continuation keeps the runtime's existing frame-lifetime contract.
class ReadFlightWaiter : public bycorf::OneShotIoAwaitable {
 public:
  ~ReadFlightWaiter() override;
  bool flight_completed() const noexcept { return delivered_; }
  bool flight_attached() const noexcept { return flight_ != nullptr; }

 protected:
  // The callback consumes the offered handle synchronously. Borrowing its
  // rvalue avoids another owner-carrier move/destructor at virtual dispatch;
  // no reference to this stack argument may survive the callback.
  virtual void ReceiveRead(bycorf::Worker& worker, ReadResultBuffer&& buffer,
                           int result, unsigned flags) = 0;
  bool delivered_ = false;

 private:
  // Fit both flags into the inherited completion's tail padding instead of
  // extending every request carrier by another alignment unit.
  bool known_missing_ = false;
  friend class ReadFlightRegistry;
  ReadFlight* flight_ = nullptr;
  ReadFlightWaiter* next_ = nullptr;
  std::uint64_t miss_epoch_ = 0;
};

struct ReadFlightStats {
  std::uint64_t leaders = 0, joins = 0, cancellations = 0, rejected = 0;
  std::uint64_t table_full = 0, waiter_full = 0, shared_budget = 0;
};

// All access is confined to the actual submitting worker. An accepted producer
// owns its pin, mutable DMA lease and stable completion tag independently of
// every waiter. Completed results are never kept in this index.
// A stop request alone does not close admission. The runtime drains I/O and
// destroys all request frames before finalization; TryJoin and Start must not
// be called after that barrier.
class ReadFlightRegistry {
 public:
  using ReleasePin = void (*)(void*, std::uint64_t, unsigned, bool) noexcept;
  static constexpr std::size_t kCapacity = 1024;
  static constexpr unsigned kMaxConsumers = 32;
  static std::unique_ptr<ReadFlightRegistry> TryCreate(
      bycorf::Worker& worker, RegisteredBufferPool& pool, void* pin_context,
      ReleasePin release_pin, std::size_t capacity = kCapacity) noexcept;
  ~ReadFlightRegistry();
  ReadFlightRegistry(const ReadFlightRegistry&) = delete;
  ReadFlightRegistry& operator=(const ReadFlightRegistry&) = delete;

  // Probe before acquiring a payload buffer. A full sharing quota bypasses;
  // it never waits for optimization capacity or disturbs the existing producer.
  bool TryJoin(const ReadFlightKey& key, std::size_t hash,
               ReadFlightWaiter& waiter);
  enum class Outcome { kStarted, kJoined, kBypass };
  struct Admission {
    Outcome outcome;
    bycorf::IoCompletion* tag = nullptr;
  };
  // Rechecks after buffer acquisition. kStarted transfers the lease and pin
  // and publishes the tag; the caller MUST submit immediately without yielding.
  // On synchronous rejection call Reject before returning or suspending.
  Admission Start(const ReadFlightKey& key, std::size_t hash,
                  ReadBufferLease& lease, bool& owns_pin,
                  ReadFlightWaiter& waiter);
  void Reject(bycorf::IoCompletion* tag, ReadBufferLease& lease,
              bool& owns_pin);
  // After backend quiescence and the all-worker frame-reclamation barrier only.
  // No callbacks, traces, wakeups or maintenance notifications are emitted.
  void FinalizeQuiesced() noexcept;
  std::size_t size() const noexcept { return active_; }
  // Attached requests, including producers; excludes delivered results still
  // retained by consumers. Owner-only diagnostics scan bounded stable slots
  // instead of maintaining another counter on every request/completion.
  [[gnu::cold]] std::size_t pending_waiters() const noexcept;
  std::size_t map_allocations() const noexcept { return allocations_.calls; }
  const ReadFlightStats& stats() const noexcept { return stats_; }

 private:
  friend class ReadFlightWaiter;
  friend struct ReadFlight;
  struct Lookup {
    const ReadFlightKey& key;
    std::size_t hash;
  };
  struct Hash {
    using is_transparent = void;
    std::size_t operator()(const ReadFlight* flight) const noexcept;
    std::size_t operator()(const Lookup& lookup) const noexcept {
      return lookup.hash;
    }
  };
  struct Equal {
    using is_transparent = void;
    bool operator()(const ReadFlight* a, const ReadFlight* b) const noexcept;
    bool operator()(const ReadFlight* a, const Lookup& b) const noexcept;
    bool operator()(const Lookup& a, const ReadFlight* b) const noexcept;
  };
  struct Allocations {
    std::size_t calls = 0, bytes = 0;
  };
  template <typename T>
  struct Allocator {
    using value_type = T;
    Allocations* state;
    explicit Allocator(Allocations* s) : state(s) {}
    template <typename U>
    Allocator(const Allocator<U>& other) : state(other.state) {}
    T* allocate(std::size_t count) {
      auto* p = std::allocator<T>{}.allocate(count);
      ++state->calls;
      state->bytes += count * sizeof(T);
      return p;
    }
    void deallocate(T* p, std::size_t count) {
      state->bytes -= count * sizeof(T);
      std::allocator<T>{}.deallocate(p, count);
    }
    template <typename U>
    bool operator==(const Allocator<U>& other) const {
      return state == other.state;
    }
  };
  using Map =
      absl::flat_hash_set<ReadFlight*, Hash, Equal, Allocator<ReadFlight*>>;
  ReadFlightRegistry(bycorf::Worker&, RegisteredBufferPool&, void*, ReleasePin,
                     std::size_t capacity);
  ReadFlight* Find(const ReadFlightKey& key, std::size_t hash) const;
  void Erase(ReadFlight& flight);
  bool Join(ReadFlight& flight, ReadFlightWaiter& waiter);
  void Attach(ReadFlight& flight, ReadFlightWaiter& waiter);
  void Detach(ReadFlightWaiter& waiter) noexcept;
  void Complete(ReadFlight& flight, int result, unsigned flags);
  // Keep the fanout scratch array off the single-consumer completion stack.
  [[gnu::noinline]] void CompleteShared(ReadFlight& flight, int result,
                                        unsigned flags);
  void Recycle(ReadFlight& flight) noexcept;

  bycorf::Worker& worker_;
  RegisteredBufferPool& pool_;
  void* pin_context_;
  ReleasePin release_pin_;
  std::size_t capacity_;
  RetainedMemoryCharge metadata_memory_;
  std::unique_ptr<ReadFlight[]> flights_;
  ReadFlight* free_ = nullptr;
  Allocations allocations_;
  // Most workers have few simultaneous physical reads. A direct pointer slot
  // avoids Swiss-table insertion/erasure for the non-colliding case. Collisions
  // retain the bounded table and full identity comparison; a vacant direct
  // slot never proves absence while an older colliding producer remains there.
  static constexpr std::size_t kDirectSlots = 256;
  std::array<ReadFlight*, kDirectSlots> direct_{};
  Map map_;
  std::size_t active_ = 0;
  ReadFlightStats stats_;
  bool finalized_ = false;
};

}  // namespace lavik::storage
