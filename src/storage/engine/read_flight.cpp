/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
#include "lavik/storage/read_flight.h"

#include <array>
#include <bit>
#include <cassert>
#include <new>

#include "bycorf/runtime/worker.h"

namespace lavik::storage {
struct ReadFlight final : bycorf::IoCompletion {
  void Complete(bycorf::Worker&, int result, unsigned flags) override {
    registry->Complete(*this, result, flags);
  }
  ReadFlightRegistry* registry = nullptr;
  ReadFlightKey key;
  std::size_t hash = 0;
  ReadBufferLease lease;
  SharedReadReservation sharing;
  ReadFlightWaiter* head = nullptr;
  ReadFlight* next_free = nullptr;
  unsigned consumers = 0;
  bool owns_pin = false;
  bool active = false;
};

ReadFlightWaiter::~ReadFlightWaiter() {
  if (flight_) flight_->registry->Detach(*this);
}
std::size_t ReadFlightRegistry::Hash::operator()(
    const ReadFlight* f) const noexcept {
  return f->hash;
}
bool ReadFlightRegistry::Equal::operator()(const ReadFlight* a,
                                           const ReadFlight* b) const noexcept {
  return a == b || a->key == b->key;
}
bool ReadFlightRegistry::Equal::operator()(const ReadFlight* a,
                                           const Lookup& b) const noexcept {
  return a->key == b.key;
}
bool ReadFlightRegistry::Equal::operator()(const Lookup& a,
                                           const ReadFlight* b) const noexcept {
  return a.key == b->key;
}

ReadFlightRegistry::ReadFlightRegistry(bycorf::Worker& worker,
                                       RegisteredBufferPool& pool,
                                       void* context, ReleasePin unpin,
                                       std::size_t capacity)
    : worker_(worker),
      pool_(pool),
      pin_context_(context),
      release_pin_(unpin),
      capacity_(capacity),
      flights_(std::make_unique<ReadFlight[]>(capacity)),
      map_(0, Hash{}, Equal{}, Allocator<ReadFlight*>(&allocations_)) {
  // reserve(n) alone only covers the initial inserts. At high occupancy,
  // tombstone churn can make the pinned Abseil grow instead of rehash in place
  // (its cutoff is 25/32 of bucket capacity). Rounding the live-flight limit
  // up to a power of two keeps larger tables at most about half full. The
  // default 1024-flight registry already has this headroom. Sanitizer iterator
  // instrumentation may still deliberately reallocate to detect stale uses.
  map_.reserve(std::bit_ceil(capacity));
  for (std::size_t i = 0; i < capacity; ++i) {
    flights_[i].registry = this;
    flights_[i].next_free = free_;
    free_ = &flights_[i];
  }
}

std::unique_ptr<ReadFlightRegistry> ReadFlightRegistry::TryCreate(
    bycorf::Worker& worker, RegisteredBufferPool& pool, void* context,
    ReleasePin unpin, std::size_t capacity) noexcept {
  if (capacity > kCapacity) return {};
  // Reserve conservatively for the pointer table's power-of-two backing, then
  // charge its observed allocation. Failure only disables this optimization.
  const auto bound =
      sizeof(ReadFlightRegistry) + capacity * (sizeof(ReadFlight) + 64) + 128;
  auto reservation = TryReserveMemory(bound);
  if (!reservation) return {};
  try {
    auto result = std::unique_ptr<ReadFlightRegistry>(
        new ReadFlightRegistry(worker, pool, context, unpin, capacity));
    const auto bytes = sizeof(ReadFlightRegistry) +
                       capacity * sizeof(ReadFlight) +
                       result->allocations_.bytes;
    assert(bytes <= bound);
    result->metadata_memory_.Adopt(&*reservation, bytes);
    return result;
  } catch (const std::bad_alloc&) {
    return {};
  }
}
ReadFlightRegistry::~ReadFlightRegistry() { FinalizeQuiesced(); }

void ReadFlightRegistry::Attach(ReadFlight& flight, ReadFlightWaiter& waiter) {
  assert(!waiter.flight_ && !waiter.delivered_);
  waiter.flight_ = &flight;
  waiter.known_missing_ = false;
  waiter.next_ = flight.head;
  flight.head = &waiter;
  ++flight.consumers;
}

bool ReadFlightRegistry::Join(ReadFlight& flight, ReadFlightWaiter& waiter) {
  if (flight.consumers == kMaxConsumers) {
    ++stats_.waiter_full;
    return false;
  }
  if (!flight.sharing.valid()) {
    flight.sharing = pool_.TryReserveSharedRead(flight.lease.bytes().size());
    if (!flight.sharing.valid()) {
      ++stats_.shared_budget;
      return false;
    }
  }
  Attach(flight, waiter);
  ++stats_.joins;
  return true;
}

bool ReadFlightRegistry::TryJoin(const ReadFlightKey& key, std::size_t hash,
                                 ReadFlightWaiter& waiter) {
  assert(!finalized_);
  const auto found = map_.find(Lookup{key, hash});
  if (found != map_.end()) {
    waiter.known_missing_ = false;
    return Join(**found, waiter);
  }
  waiter.miss_epoch_ = stats_.leaders;
  waiter.known_missing_ = true;
  return false;
}

ReadFlightRegistry::Admission ReadFlightRegistry::Start(
    const ReadFlightKey& key, std::size_t hash, ReadBufferLease& lease,
    bool& owns_pin, ReadFlightWaiter& waiter) {
  assert(!finalized_);
  // A miss remains valid while no producer has been inserted. Completions
  // only erase entries. This avoids a redundant lookup for immediate awaiting,
  // while retained requests recheck after any intervening publication. Never
  // retain a table iterator across acquisition or a possible suspension.
  // A retained miss must not span a full 2^64 producer-insertion cycle.
  // Ordinary unsigned wrap needs only equality and keeps this shortcut valid.
  if (!waiter.known_missing_ || waiter.miss_epoch_ != stats_.leaders) {
    if (auto found = map_.find(Lookup{key, hash}); found != map_.end()) {
      return {Join(**found, waiter) ? Outcome::kJoined : Outcome::kBypass};
    }
  }
  if (!free_) {
    ++stats_.table_full;
    return {Outcome::kBypass};
  }
  assert(lease.valid() && owns_pin);
  auto* flight = free_;
  flight->key = key;
  flight->hash = hash;
  const auto [it, inserted] = map_.insert(flight);
  assert(inserted);
  (void)it;
  (void)inserted;
  free_ = flight->next_free;
  flight->active = true;
  flight->lease = std::move(lease);
  flight->owns_pin = std::exchange(owns_pin, false);
  Attach(*flight, waiter);
  ++stats_.leaders;
  return {Outcome::kStarted, flight};
}

void ReadFlightRegistry::Detach(ReadFlightWaiter& waiter) noexcept {
  auto* flight = waiter.flight_;
  assert(flight && flight->active && flight->consumers);
  // Cancellation is exceptional and the list has at most 32 members. A
  // bounded scan saves a back-link in every ordinary request's hot carrier.
  auto** link = &flight->head;
  while (*link != &waiter) {
    assert(*link);
    link = &(*link)->next_;
  }
  *link = waiter.next_;
  waiter.flight_ = nullptr;
  waiter.next_ = nullptr;
  --flight->consumers;
  if (flight->consumers < 2) flight->sharing.Reset();
  ++stats_.cancellations;
}

void ReadFlightRegistry::Recycle(ReadFlight& flight) noexcept {
  assert(!flight.head && !flight.consumers && !flight.lease.valid() &&
         !flight.owns_pin);
  flight.sharing.Reset();
  flight.active = false;
  flight.next_free = free_;
  free_ = &flight;
}

void ReadFlightRegistry::Reject(bycorf::IoCompletion* tag,
                                ReadBufferLease& lease, bool& owns_pin) {
  auto& flight = *static_cast<ReadFlight*>(tag);
  // Publication and submission never suspend, so rejection has one waiter.
  assert(flight.active && flight.consumers == 1 && !lease.valid() && !owns_pin);
  map_.erase(&flight);
  auto* waiter = flight.head;
  waiter->flight_ = nullptr;
  waiter->next_ = nullptr;
  flight.head = nullptr;
  flight.consumers = 0;
  lease = std::move(flight.lease);
  owns_pin = std::exchange(flight.owns_pin, false);
  ++stats_.rejected;
  Recycle(flight);
}

void ReadFlightRegistry::Complete(ReadFlight& flight, int result,
                                  unsigned flags) {
  assert(flight.active);
  map_.erase(&flight);  // Close joining at physical completion, never at send
                        // completion.
  if (result >= 0) worker_.RecordStorageReadCompletion(result);
  if (flight.consumers <= 1) {
    auto* waiter = flight.head;
    assert(flight.consumers == (waiter ? 1 : 0));
    if (waiter) {
      waiter->flight_ = nullptr;
      waiter->next_ = nullptr;
      waiter->delivered_ = true;
    }
    flight.head = nullptr;
    flight.consumers = 0;
    ReadResultBuffer buffer(std::move(flight.lease));
    if (flight.owns_pin) {
      flight.owns_pin = false;
      release_pin_(pin_context_, flight.key.block, flight.key.block_owner,
                   false);
    }
    Recycle(flight);
    if (waiter) waiter->ReceiveRead(worker_, std::move(buffer), result, flags);
    return;
  }
  CompleteShared(flight, result, flags);
}

void ReadFlightRegistry::CompleteShared(ReadFlight& flight, int result,
                                        unsigned flags) {
  const unsigned count = flight.consumers;
  std::array<ReadFlightWaiter*, kMaxConsumers> waiters;
  auto* waiter = flight.head;
  for (unsigned i = 0; i < count; ++i) {
    assert(waiter);
    waiters[i] = waiter;
    auto* next = waiter->next_;
    waiter->flight_ = nullptr;
    waiter->next_ = nullptr;
    waiter->delivered_ = true;
    waiter = next;
  }
  assert(!waiter);
  flight.head = nullptr;
  flight.consumers = 0;
  assert(count > 1);
  ReadResultBuffer buffer =
      std::move(flight.sharing).Publish(std::move(flight.lease));
  if (flight.owns_pin) {
    flight.owns_pin = false;
    release_pin_(pin_context_, flight.key.block, flight.key.block_owner, false);
  }
  Recycle(flight);
  // Each callback stores its handle and enqueues its continuation. Keep the
  // original alive through fanout and give it to the final receiver, avoiding
  // a 33rd temporary reference when all consumer tickets are needed.
  for (unsigned i = 0; i < count; ++i) {
    ReadResultBuffer result_buffer;
    if (i + 1 == count)
      result_buffer = std::move(buffer);
    else {
      auto shared = buffer.CloneOnOwner();
      assert(shared);
      result_buffer = std::move(*shared);
    }
    waiters[i]->ReceiveRead(worker_, std::move(result_buffer), result, flags);
  }
}

void ReadFlightRegistry::FinalizeQuiesced() noexcept {
  if (finalized_) return;
  finalized_ = true;
  for (std::size_t i = 0; i < capacity_; ++i) {
    auto& flight = flights_[i];
    if (!flight.active) continue;
    // Request destructors detached every waiter before the all-worker barrier.
    assert(!flight.head && !flight.consumers);
    if (flight.owns_pin) {
      release_pin_(pin_context_, flight.key.block, flight.key.block_owner,
                   true);
      flight.owns_pin = false;
    }
    pool_.ReclaimReadAfterQuiesce(std::move(flight.lease));
    flight.sharing.Reset();
    flight.active = false;
  }
  map_.clear();
}
}  // namespace lavik::storage
