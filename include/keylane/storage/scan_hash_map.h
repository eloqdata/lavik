#pragma once

/*
 * ScanHashMap is derived from Valkey's src/hashtable.c at commit
 * 21c0d49a0 (Valkey 8.0.8-180-g21c0d49a0).
 *
 * Copyright (c) 2024-present, Valkey contributors
 * Copyright (c) 2006-2020, Redis Ltd.
 * Copyright (c) 2026, Keylane contributors
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * The complete license text is in third_party/valkey/COPYING. This C++
 * specialization retains Valkey's cache-line bucket layout, incremental
 * two-table expansion, and stateless reverse-bit scan algorithm. It replaces
 * Valkey runtime dependencies and generic callbacks with Keylane-owned entries.
 */

#include <array>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "keylane/storage/format.h"

namespace keylane::storage {

template <typename Value>
class ScanHashMap {
 public:
  struct Entry {
    static constexpr std::uint32_t kExternalKeyMask = std::uint32_t{1} << 31;

    std::uint64_t hash_ = 0;
    Value value_{};
    std::uint32_t key_size_ = 0;

    bool key_complete() const noexcept {
      return (key_size_ & kExternalKeyMask) == 0;
    }

    std::uint32_t logical_key_size() const noexcept {
      return key_size_ & ~kExternalKeyMask;
    }

    std::string_view key() const noexcept {
      return key_complete()
                 ? std::string_view(reinterpret_cast<const char*>(this + 1),
                                    logical_key_size())
                 : std::string_view{};
    }

    Digest external_key_digest() const noexcept {
      Digest digest;
      if (!key_complete()) {
        std::memcpy(digest.bytes_.data(), this + 1, digest.bytes_.size());
      }
      return digest;
    }

    static Entry* Create(const Digest& digest, std::string_view key,
                         const Value& value, bool key_complete = true) {
      if (key.size() >= kExternalKeyMask) {
        throw std::bad_alloc();
      }
      const std::size_t tail_bytes =
          key_complete ? key.size() : digest.bytes_.size();
      if (tail_bytes >
          std::numeric_limits<std::size_t>::max() - sizeof(Entry)) {
        throw std::bad_alloc();
      }
      static_assert(alignof(Entry) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
      void* storage = ::operator new(sizeof(Entry) + tail_bytes);
      Entry* entry = nullptr;
      try {
        entry = new (storage) Entry(Hash(digest),
                                    static_cast<std::uint32_t>(key.size()) |
                                        (key_complete ? 0 : kExternalKeyMask),
                                    value);
      } catch (...) {
        ::operator delete(storage);
        throw;
      }
      if (key_complete && !key.empty()) {
        std::memcpy(entry + 1, key.data(), key.size());
      } else if (!key_complete) {
        std::memcpy(entry + 1, digest.bytes_.data(), digest.bytes_.size());
      }
      return entry;
    }

    static void Destroy(Entry* entry) noexcept {
      if (entry == nullptr) {
        return;
      }
      entry->~Entry();
      ::operator delete(entry);
    }

   private:
    Entry(std::uint64_t hash, std::uint32_t key_size, const Value& value)
        : hash_(hash), value_(value), key_size_(key_size) {}
  };

  struct InsertResult {
    Entry* entry_ = nullptr;
    bool inserted_ = false;
  };

  ScanHashMap() = default;
  ScanHashMap(const ScanHashMap&) = delete;
  ScanHashMap& operator=(const ScanHashMap&) = delete;

  ScanHashMap(ScanHashMap&& other) noexcept { MoveFrom(std::move(other)); }

  ScanHashMap& operator=(ScanHashMap&& other) noexcept {
    if (this != &other) {
      Clear();
      MoveFrom(std::move(other));
    }
    return *this;
  }

  ~ScanHashMap() { Clear(); }

  std::size_t size() const noexcept {
    return tables_[0].used_ + tables_[1].used_;
  }

  bool empty() const noexcept { return size() == 0; }

  bool has_allocated_storage() const noexcept {
    return tables_[0].buckets_ != nullptr || tables_[1].buckets_ != nullptr;
  }

  Entry* Find(const Digest& digest, std::string_view key) {
    RehashStep();
    return FindWithoutStep(digest, key);
  }

  const Entry* Find(const Digest& digest, std::string_view key) const {
    return FindWithoutStep(digest, key);
  }

  std::vector<Entry*> FindCandidates(const Digest& digest,
                                     std::string_view key) {
    RehashStep();
    std::vector<Entry*> result;
    const std::uint64_t hash = Hash(digest);
    AppendCandidates(tables_[0], digest, key, hash, &result);
    if (Rehashing()) {
      AppendCandidates(tables_[1], digest, key, hash, &result);
    }
    return result;
  }

  bool Contains(const Entry* entry, std::uint64_t hash) const noexcept {
    if (entry == nullptr) {
      return false;
    }
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      const Table& table = tables_[t];
      if (table.buckets_ == nullptr) {
        continue;
      }
      const Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
      while (bucket != nullptr) {
        const std::size_t slots =
            Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
        for (std::size_t slot = 0; slot < slots; ++slot) {
          if (Occupied(*bucket, slot) && bucket->entries_[slot] == entry) {
            return true;
          }
        }
        bucket = Chained(*bucket) ? Child(bucket) : nullptr;
      }
    }
    return false;
  }

  InsertResult InsertOrAssign(const Digest& digest, std::string_view key,
                              const Value& value, bool key_complete = true) {
    if (Entry* existing = Find(digest, key); existing != nullptr) {
      existing->value_ = value;
      return {existing, false};
    }

    EnsureTable();
    MaybeStartExpansion();
    std::unique_ptr<Entry, void (*)(Entry*)> entry(
        Entry::Create(digest, key, value, key_complete), &Entry::Destroy);
    Entry* raw = entry.get();
    AddToTable(Rehashing() ? tables_[1] : tables_[0], raw);
    entry.release();
    return {raw, true};
  }

  Entry* InsertNew(const Digest& digest, std::string_view key,
                   const Value& value, bool key_complete = true) {
    EnsureTable();
    MaybeStartExpansion();
    std::unique_ptr<Entry, void (*)(Entry*)> entry(
        Entry::Create(digest, key, value, key_complete), &Entry::Destroy);
    Entry* raw = entry.get();
    AddToTable(Rehashing() ? tables_[1] : tables_[0], raw);
    entry.release();
    return raw;
  }

  // Deletes the matching entry, compacting its bucket chain the way Valkey's
  // hashtablePop does so chains stay dense and emptied child buckets are
  // freed. Compaction moves entries only within their own chain, and Scan
  // emits a whole chain per cursor position, so a concurrent scan never
  // misses an entry that existed throughout. The table itself never shrinks
  // (the port dropped shrinking with deletion); slots are reused by later
  // inserts, so footprint is bounded by the peak live count.
  bool Erase(const Digest& digest, std::string_view key) {
    RehashStep();
    const std::uint64_t hash = Hash(digest);
    const std::uint8_t tag = HashTag(hash);
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (table.buckets_ == nullptr) {
        continue;
      }
      Bucket* top = &table.buckets_[hash & BucketMask(table)];
      for (Bucket* bucket = top; bucket != nullptr;
           bucket = Chained(*bucket) ? Child(bucket) : nullptr) {
        const std::size_t slots =
            Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
        for (std::size_t slot = 0; slot < slots; ++slot) {
          if (Occupied(*bucket, slot) && bucket->hashes_[slot] == tag &&
              KeyEquals(*bucket->entries_[slot], digest, key)) {
            Entry::Destroy(bucket->entries_[slot]);
            bucket->entries_[slot] = nullptr;
            ClearOccupied(bucket, slot);
            --table.used_;
            FillBucketHole(top, bucket, slot);
            return true;
          }
        }
      }
    }
    return false;
  }

  bool Erase(Entry* entry) {
    if (entry == nullptr) {
      return false;
    }
    RehashStep();
    const std::uint64_t hash = entry->hash_;
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (table.buckets_ == nullptr) {
        continue;
      }
      Bucket* top = &table.buckets_[hash & BucketMask(table)];
      for (Bucket* bucket = top; bucket != nullptr;
           bucket = Chained(*bucket) ? Child(bucket) : nullptr) {
        const std::size_t slots =
            Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
        for (std::size_t slot = 0; slot < slots; ++slot) {
          if (Occupied(*bucket, slot) && bucket->entries_[slot] == entry) {
            Entry::Destroy(entry);
            bucket->entries_[slot] = nullptr;
            ClearOccupied(bucket, slot);
            --table.used_;
            FillBucketHole(top, bucket, slot);
            return true;
          }
        }
      }
    }
    return false;
  }

  template <typename Fn>
  void ForEach(Fn&& fn) {
    ForEachTable(tables_[0], fn);
    ForEachTable(tables_[1], fn);
  }

  // Visits entries until the callback returns false. Returns true when both
  // tables were exhausted and false when the callback stopped traversal.
  template <typename Fn>
  bool ForEachWhile(Fn&& fn) {
    return ForEachTableWhile(tables_[0], fn) &&
           ForEachTableWhile(tables_[1], fn);
  }

  // A cursor of zero starts and completes a full scan. The callback may be
  // invoked more than once for an entry if the table changes between calls.
  template <typename Fn>
  std::uint64_t Scan(std::uint64_t cursor, Fn&& fn) const {
    if (empty()) {
      return 0;
    }

    if (!Rehashing()) {
      const std::uint64_t mask = BucketMask(tables_[0]);
      EmitBucket(tables_[0], cursor & mask, fn);
      return NextCursor(cursor, mask);
    }

    const Table& small = tables_[0];
    const Table& large = tables_[1];
    const std::uint64_t small_mask = BucketMask(small);
    const std::uint64_t large_mask = BucketMask(large);

    const std::uint64_t small_index = cursor & small_mask;
    if (small_index >= rehash_index_) {
      EmitBucket(small, small_index, fn);
    }

    do {
      EmitBucket(large, cursor & large_mask, fn);
      cursor = NextCursor(cursor, large_mask);
    } while ((cursor & (small_mask ^ large_mask)) != 0);

    return cursor;
  }

  void Clear() noexcept {
    DestroyTable(tables_[0], true);
    DestroyTable(tables_[1], true);
    rehash_index_ = kNotRehashing;
  }

  // Moves every entry out into the returned map and leaves *this empty and
  // immediately usable. Destroying the returned map is what actually frees the
  // entries, so a caller can hand it to a background task and keep serving
  // reads from *this. O(1) — no entry is touched here.
  //
  // Entry addresses are stable for as long as their map lives (expansion moves
  // bucket pointers, never entries), so callers may cache a raw Entry*. Such a
  // caller must be able to tell that a detach happened before dereferencing;
  // this class does not track that, since the useful granularity is whatever
  // set of maps the caller detaches together.
  ScanHashMap Detach() noexcept {
    ScanHashMap detached;
    detached.tables_ = std::exchange(tables_, {});
    detached.rehash_index_ = std::exchange(rehash_index_, kNotRehashing);
    return detached;
  }

 private:
  static constexpr std::size_t kEntriesPerBucket = 7;
  static constexpr std::uint8_t kChainedBit = 0x80;
  static constexpr std::size_t kChildSlot = kEntriesPerBucket - 1;
  static constexpr std::size_t kTargetEntriesPerBucket = 6;
  static constexpr std::size_t kNotRehashing =
      std::numeric_limits<std::size_t>::max();

  struct alignas(64) Bucket {
    std::uint8_t presence_ = 0;
    std::array<std::uint8_t, kEntriesPerBucket> hashes_{};
    std::array<Entry*, kEntriesPerBucket> entries_{};
  };

  static_assert(sizeof(Bucket) == 64);

  struct Table {
    std::unique_ptr<Bucket[]> buckets_;
    std::uint8_t exponent_ = 0;
    std::size_t used_ = 0;
  };

  static bool Chained(const Bucket& bucket) noexcept {
    return (bucket.presence_ & kChainedBit) != 0;
  }

  static bool Occupied(const Bucket& bucket, std::size_t slot) noexcept {
    return (bucket.presence_ & (std::uint8_t{1} << slot)) != 0;
  }

  static void SetOccupied(Bucket* bucket, std::size_t slot) noexcept {
    bucket->presence_ |= std::uint8_t{1} << slot;
  }

  static void ClearOccupied(Bucket* bucket, std::size_t slot) noexcept {
    bucket->presence_ &= ~(std::uint8_t{1} << slot);
  }

  static Bucket* Child(Bucket* bucket) noexcept {
    assert(Chained(*bucket));
    return reinterpret_cast<Bucket*>(bucket->entries_[kChildSlot]);
  }

  static const Bucket* Child(const Bucket* bucket) noexcept {
    assert(Chained(*bucket));
    return reinterpret_cast<const Bucket*>(bucket->entries_[kChildSlot]);
  }

  static void SetChild(Bucket* bucket, Bucket* child) noexcept {
    bucket->presence_ |= kChainedBit;
    bucket->entries_[kChildSlot] = reinterpret_cast<Entry*>(child);
  }

  static std::size_t BucketCount(const Table& table) noexcept {
    return table.buckets_ == nullptr ? 0 : std::size_t{1} << table.exponent_;
  }

  static std::uint64_t BucketMask(const Table& table) noexcept {
    return static_cast<std::uint64_t>(BucketCount(table) - 1);
  }

  static std::uint64_t Hash(const Digest& digest) noexcept {
    std::uint64_t first = 0;
    std::uint64_t second = 0;
    std::uint32_t tail = 0;
    std::memcpy(&first, digest.bytes_.data(), sizeof(first));
    std::memcpy(&second, digest.bytes_.data() + sizeof(first), sizeof(second));
    std::memcpy(&tail, digest.bytes_.data() + 2 * sizeof(first), sizeof(tail));
    first ^= second + 0x9e3779b97f4a7c15ULL + (first << 6) + (first >> 2);
    first ^= static_cast<std::uint64_t>(tail) * 0xbf58476d1ce4e5b9ULL;
    first ^= first >> 30;
    first *= 0xbf58476d1ce4e5b9ULL;
    first ^= first >> 27;
    first *= 0x94d049bb133111ebULL;
    return first ^ (first >> 31);
  }

  static std::uint8_t HashTag(std::uint64_t hash) noexcept {
    return static_cast<std::uint8_t>(hash >> 56);
  }

  static bool KeyEquals(const Entry& entry, const Digest& digest,
                        std::string_view key) noexcept {
    if (entry.key_complete()) [[likely]] {
      return entry.key() == key;
    }
    return entry.logical_key_size() == key.size() &&
           entry.external_key_digest() == digest;
  }

  static std::uint64_t ReverseBits(std::uint64_t value) noexcept {
    value = ((value >> 1) & 0x5555555555555555ULL) |
            ((value & 0x5555555555555555ULL) << 1);
    value = ((value >> 2) & 0x3333333333333333ULL) |
            ((value & 0x3333333333333333ULL) << 2);
    value = ((value >> 4) & 0x0f0f0f0f0f0f0f0fULL) |
            ((value & 0x0f0f0f0f0f0f0f0fULL) << 4);
    return std::byteswap(value);
  }

  // Pieter Noordhuis' stateless scan cursor algorithm, as used by Valkey.
  static std::uint64_t NextCursor(std::uint64_t cursor,
                                  std::uint64_t mask) noexcept {
    cursor |= ~mask;
    cursor = ReverseBits(cursor);
    ++cursor;
    return ReverseBits(cursor);
  }

  bool Rehashing() const noexcept { return rehash_index_ != kNotRehashing; }

  void EnsureTable() {
    if (tables_[0].buckets_ == nullptr) {
      tables_[0].buckets_ = std::make_unique<Bucket[]>(1);
      tables_[0].exponent_ = 0;
    }
  }

  void MaybeStartExpansion() {
    if (Rehashing()) {
      return;
    }
    const std::size_t buckets = BucketCount(tables_[0]);
    if (tables_[0].used_ + 1 <= buckets * kTargetEntriesPerBucket) {
      return;
    }
    assert(tables_[0].exponent_ < 63);
    tables_[1].exponent_ = tables_[0].exponent_ + 1;
    tables_[1].buckets_ =
        std::make_unique<Bucket[]>(std::size_t{1} << tables_[1].exponent_);
    tables_[1].used_ = 0;
    rehash_index_ = 0;
  }

  void RehashStep() {
    if (!Rehashing()) {
      return;
    }

    MoveBucket(&tables_[0].buckets_[rehash_index_], &tables_[1]);
    ++rehash_index_;
    if (rehash_index_ == BucketCount(tables_[0])) {
      assert(tables_[0].used_ == 0);
      tables_[0] = std::move(tables_[1]);
      tables_[1] = Table{};
      rehash_index_ = kNotRehashing;
    }
  }

  static Entry* FindInTable(Table& table, const Digest& digest,
                            std::string_view key, std::uint64_t hash) {
    if (table.buckets_ == nullptr) {
      return nullptr;
    }
    Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    const std::uint8_t tag = HashTag(hash);
    while (bucket != nullptr) {
      const std::size_t slots =
          Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
      for (std::size_t slot = 0; slot < slots; ++slot) {
        if (Occupied(*bucket, slot) && bucket->hashes_[slot] == tag &&
            KeyEquals(*bucket->entries_[slot], digest, key)) {
          return bucket->entries_[slot];
        }
      }
      bucket = Chained(*bucket) ? Child(bucket) : nullptr;
    }
    return nullptr;
  }

  static const Entry* FindInTable(const Table& table, const Digest& digest,
                                  std::string_view key, std::uint64_t hash) {
    return FindInTable(const_cast<Table&>(table), digest, key, hash);
  }

  static void AppendCandidates(Table& table, const Digest& digest,
                               std::string_view key, std::uint64_t hash,
                               std::vector<Entry*>* result) {
    if (table.buckets_ == nullptr) {
      return;
    }
    Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    const std::uint8_t tag = HashTag(hash);
    while (bucket != nullptr) {
      const std::size_t slots =
          Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
      for (std::size_t slot = 0; slot < slots; ++slot) {
        if (Occupied(*bucket, slot) && bucket->hashes_[slot] == tag &&
            KeyEquals(*bucket->entries_[slot], digest, key)) {
          result->push_back(bucket->entries_[slot]);
        }
      }
      bucket = Chained(*bucket) ? Child(bucket) : nullptr;
    }
  }

  Entry* FindWithoutStep(const Digest& digest, std::string_view key) {
    const std::uint64_t hash = Hash(digest);
    if (Entry* found = FindInTable(tables_[0], digest, key, hash);
        found != nullptr) {
      return found;
    }
    return Rehashing() ? FindInTable(tables_[1], digest, key, hash) : nullptr;
  }

  const Entry* FindWithoutStep(const Digest& digest,
                               std::string_view key) const {
    const std::uint64_t hash = Hash(digest);
    if (const Entry* found = FindInTable(tables_[0], digest, key, hash);
        found != nullptr) {
      return found;
    }
    return Rehashing() ? FindInTable(tables_[1], digest, key, hash) : nullptr;
  }

  static void AddToTable(Table& table, Entry* entry) {
    const std::uint64_t hash = entry->hash_;
    const std::uint8_t tag = HashTag(hash);
    Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
    while (true) {
      const std::size_t slots =
          Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
      for (std::size_t slot = 0; slot < slots; ++slot) {
        if (!Occupied(*bucket, slot)) {
          bucket->entries_[slot] = entry;
          bucket->hashes_[slot] = tag;
          SetOccupied(bucket, slot);
          ++table.used_;
          return;
        }
      }
      if (Chained(*bucket)) {
        bucket = Child(bucket);
        continue;
      }

      auto* child = new Bucket();
      Entry* displaced = bucket->entries_[kChildSlot];
      const std::uint8_t displaced_hash = bucket->hashes_[kChildSlot];
      assert(Occupied(*bucket, kChildSlot));
      ClearOccupied(bucket, kChildSlot);
      SetChild(bucket, child);
      child->entries_[0] = displaced;
      child->hashes_[0] = displaced_hash;
      SetOccupied(child, 0);
      bucket = child;
    }
  }

  // Moves the last entry of the chain into the freed slot and unlinks the
  // tail bucket once it empties. Only meaningful for chained tops: holes in
  // an unchained bucket are reused by AddToTable's slot scan.
  static void FillBucketHole(Bucket* top, Bucket* holed,
                             std::size_t hole_slot) {
    if (!Chained(*top)) {
      return;
    }
    Bucket* parent = nullptr;
    Bucket* tail = top;
    while (Chained(*tail)) {
      parent = tail;
      tail = Child(tail);
    }
    std::size_t last = kEntriesPerBucket;
    for (std::size_t slot = kEntriesPerBucket; slot-- > 0;) {
      if (Occupied(*tail, slot)) {
        last = slot;
        break;
      }
    }
    if (last != kEntriesPerBucket && !(tail == holed && last == hole_slot)) {
      holed->entries_[hole_slot] = tail->entries_[last];
      holed->hashes_[hole_slot] = tail->hashes_[last];
      SetOccupied(holed, hole_slot);
      tail->entries_[last] = nullptr;
      ClearOccupied(tail, last);
    }
    if (tail->presence_ == 0) {
      parent->presence_ &= ~kChainedBit;
      parent->entries_[kChildSlot] = nullptr;
      delete tail;
    }
  }

  static void MoveBucketEntries(Bucket* top, Table* target) {
    Bucket* bucket = top;
    while (bucket != nullptr) {
      const bool chained = Chained(*bucket);
      Bucket* next = chained ? Child(bucket) : nullptr;
      const std::size_t slots = chained ? kChildSlot : kEntriesPerBucket;
      for (std::size_t slot = 0; slot < slots; ++slot) {
        if (Occupied(*bucket, slot)) {
          AddToTable(*target, bucket->entries_[slot]);
          ClearOccupied(bucket, slot);
        }
      }
      if (bucket != top) {
        delete bucket;
      }
      bucket = next;
    }
    top->presence_ = 0;
    top->entries_.fill(nullptr);
    top->hashes_.fill(0);
  }

  void MoveBucket(Bucket* top, Table* target) {
    const std::size_t before = target->used_;
    MoveBucketEntries(top, target);
    const std::size_t moved = target->used_ - before;
    assert(tables_[0].used_ >= moved);
    tables_[0].used_ -= moved;
  }

  template <typename Fn>
  static void EmitBucket(const Table& table, std::uint64_t index, Fn& fn) {
    if (table.buckets_ == nullptr) {
      return;
    }
    const Bucket* bucket = &table.buckets_[index];
    while (bucket != nullptr) {
      const std::size_t slots =
          Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
      for (std::size_t slot = 0; slot < slots; ++slot) {
        if (Occupied(*bucket, slot)) {
          fn(*bucket->entries_[slot]);
        }
      }
      bucket = Chained(*bucket) ? Child(bucket) : nullptr;
    }
  }

  template <typename Fn>
  static void ForEachTable(Table& table, Fn& fn) {
    const std::size_t count = BucketCount(table);
    for (std::size_t index = 0; index < count; ++index) {
      EmitBucket(table, index, fn);
    }
  }

  template <typename Fn>
  static bool EmitBucketWhile(const Table& table, std::uint64_t index, Fn& fn) {
    if (table.buckets_ == nullptr) return true;
    const Bucket* bucket = &table.buckets_[index];
    while (bucket != nullptr) {
      const std::size_t slots =
          Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
      for (std::size_t slot = 0; slot < slots; ++slot) {
        if (Occupied(*bucket, slot) && !fn(*bucket->entries_[slot])) {
          return false;
        }
      }
      bucket = Chained(*bucket) ? Child(bucket) : nullptr;
    }
    return true;
  }

  template <typename Fn>
  static bool ForEachTableWhile(Table& table, Fn& fn) {
    const std::size_t count = BucketCount(table);
    for (std::size_t index = 0; index < count; ++index) {
      if (!EmitBucketWhile(table, index, fn)) return false;
    }
    return true;
  }

  static void DestroyTable(Table& table, bool destroy_entries) noexcept {
    const std::size_t count = BucketCount(table);
    for (std::size_t index = 0; index < count; ++index) {
      Bucket* top = &table.buckets_[index];
      Bucket* bucket = top;
      while (bucket != nullptr) {
        const bool chained = Chained(*bucket);
        Bucket* next = chained ? Child(bucket) : nullptr;
        const std::size_t slots = chained ? kChildSlot : kEntriesPerBucket;
        if (destroy_entries) {
          for (std::size_t slot = 0; slot < slots; ++slot) {
            if (Occupied(*bucket, slot)) {
              Entry::Destroy(bucket->entries_[slot]);
            }
          }
        }
        if (bucket != top) {
          delete bucket;
        }
        bucket = next;
      }
    }
    table = Table{};
  }

  void MoveFrom(ScanHashMap&& other) noexcept {
    tables_ = std::move(other.tables_);
    rehash_index_ = std::exchange(other.rehash_index_, kNotRehashing);
    other.tables_ = {};
  }

  std::array<Table, 2> tables_{};
  std::size_t rehash_index_ = kNotRehashing;
};

}  // namespace keylane::storage
