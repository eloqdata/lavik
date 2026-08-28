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

// The default policy stores every value directly in Entry. Specialized maps
// may split a value into a common hot-path prefix and an optional derived
// payload while retaining the same hash-table implementation.
template <typename Value>
struct ScanHashMapInlineEntryPolicy {
  using StoredValue = Value;
  struct Extra {};

  static constexpr bool HasExtraValue(const Value&) noexcept { return false; }
  static constexpr bool HasExtraStored(const StoredValue&) noexcept {
    return false;
  }
  static StoredValue Store(const Value& value) { return value; }
  static Extra StoreExtra(const Value&) noexcept { return {}; }
  static Value Load(const StoredValue& value, const Extra*) { return value; }
  static void Assign(StoredValue* stored, Extra*, const Value& value) {
    *stored = value;
  }
};

// Cached hashes retain the 32 address bits needed by the largest direct bucket
// table. MaxBucketExponent is configurable so tests can exercise saturation
// without allocating 2^32 buckets; production uses the full uint32_t range.
template <typename Value,
          unsigned MaxBucketExponent =
              std::numeric_limits<std::uint32_t>::digits,
          typename EntryPolicy = ScanHashMapInlineEntryPolicy<Value>>
class ScanHashMap {
 public:
  // Digest arguments for inline keys must equal ComputeDigest(key). Entries do
  // not retain a bucket hash, so rehash and pointer-only mutation reconstruct
  // it from the live key. External entries retain the supplied digest tail.
  static_assert(MaxBucketExponent <=
                std::numeric_limits<std::uint32_t>::digits);
  static_assert(MaxBucketExponent < std::numeric_limits<std::size_t>::digits);

  using StoredValue = typename EntryPolicy::StoredValue;
  using EntryExtra = typename EntryPolicy::Extra;

  struct ExtendedEntry;

  struct Entry {
    static constexpr std::uint32_t kMaxLogicalKeySize =
        (std::uint32_t{1} << 31) - 1;

    // Key length and inline/external representation live in a varint directly
    // before the key tail. Keeping the aligned value first lets common entries
    // use exactly the value's footprint instead of reserving another aligned
    // word for fixed-width key metadata and a cached bucket hash.
    StoredValue value_{};

    bool has_extra() const noexcept {
      return EntryPolicy::HasExtraStored(value_);
    }

    EntryExtra* optional_extra() noexcept {
      return has_extra() ? extra() : nullptr;
    }
    const EntryExtra* optional_extra() const noexcept {
      return has_extra() ? extra() : nullptr;
    }

    Value value() const { return EntryPolicy::Load(value_, optional_extra()); }

    bool can_assign(const Value& value) const noexcept {
      return has_extra() == EntryPolicy::HasExtraValue(value);
    }

    void assign(const Value& value) {
      assert(can_assign(value));
      EntryPolicy::Assign(&value_, optional_extra(), value);
    }

    bool key_complete() const noexcept {
      return DecodeKeyMetadata(tail()).key_complete_;
    }

    std::uint32_t logical_key_size() const noexcept {
      return DecodeKeyMetadata(tail()).logical_size_;
    }

    // Returns bytes occupied by the tail key-metadata varint. Normal callers
    // should use key(), key_complete(), and logical_key_size().
    std::uint8_t key_metadata_bytes() const noexcept {
      return DecodeKeyMetadata(tail()).encoded_bytes_;
    }

    // Returns the varint width without constructing an Entry. The external
    // representation bit shares the same varint with the logical length.
    static std::uint8_t KeyMetadataBytesFor(std::uint32_t logical_size,
                                            bool key_complete) noexcept {
      assert(logical_size <= kMaxLogicalKeySize);
      return EncodedKeyMetadataBytes((logical_size << 1) |
                                     static_cast<std::uint32_t>(!key_complete));
    }

    std::string_view key() const noexcept {
      const KeyMetadata metadata = DecodeKeyMetadata(tail());
      return metadata.key_complete_
                 ? std::string_view(reinterpret_cast<const char*>(
                                        tail() + metadata.encoded_bytes_),
                                    metadata.logical_size_)
                 : std::string_view{};
    }

    Digest external_key_digest() const noexcept {
      Digest digest;
      const KeyMetadata metadata = DecodeKeyMetadata(tail());
      if (!metadata.key_complete_) {
        std::memcpy(&digest, tail() + metadata.encoded_bytes_, sizeof(digest));
      }
      return digest;
    }

    static Entry* Create(const Digest& digest, std::string_view key,
                         const Value& value, bool key_complete = true);
    static Entry* CreateReplacement(const Entry& source, const Value& value);

    static void Destroy(Entry* entry) noexcept;

   private:
    friend struct ExtendedEntry;

    struct KeyMetadata {
      std::uint32_t logical_size_ = 0;
      std::uint8_t encoded_bytes_ = 0;
      bool key_complete_ = true;
    };

    explicit Entry(const StoredValue& value) : value_(value) {}

    static std::uint8_t EncodedKeyMetadataBytes(std::uint32_t encoded) noexcept;
    static std::uint8_t EncodeKeyMetadata(std::byte* output,
                                          std::uint32_t logical_size,
                                          bool key_complete) noexcept;
    static KeyMetadata DecodeKeyMetadata(const std::byte* input) noexcept;
    std::size_t tail_bytes() const noexcept;

    EntryExtra* extra() noexcept;
    const EntryExtra* extra() const noexcept;
    std::byte* tail() noexcept;
    const std::byte* tail() const noexcept;
  };

  struct ExtendedEntry final : Entry {
    EntryExtra extra_{};

   private:
    friend struct Entry;

    ExtendedEntry(const StoredValue& value, const EntryExtra& extra)
        : Entry(value), extra_(extra) {}
  };

  struct InsertResult {
    Entry* entry_ = nullptr;
    bool inserted_ = false;
  };

  // Resumable traversal for callers that can keep the map stable between
  // batches. Unlike Scan's stateless cursor, this cursor remembers an exact
  // entry position, so pausing never repeats a bucket chain. A cursor belongs
  // to one map and becomes invalid after any insertion, erase, or rehash.
  class StableScanCursor {
   public:
    bool finished() const noexcept { return table_ == 2; }

   private:
    friend class ScanHashMap;

    std::uint8_t table_ = 0;
    std::size_t bucket_ = 0;
    std::size_t chain_ = 0;
    std::size_t slot_ = 0;
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

  // Includes both tables during incremental expansion. Primarily useful for
  // capacity diagnostics and saturation tests.
  std::size_t allocated_bucket_count() const noexcept {
    return BucketCount(tables_[0]) + BucketCount(tables_[1]);
  }

  // Returns the low bucket-address bits callers must retain with an Entry
  // address across suspension. It is computed while the Entry is known live;
  // FindAddress can then validate membership without dereferencing stale
  // storage.
  static std::uint32_t AddressHash(const Digest& digest) noexcept {
    return static_cast<std::uint32_t>(Hash(digest));
  }
  static std::uint32_t AddressHash(const Entry& entry) noexcept {
    return static_cast<std::uint32_t>(EntryHash(entry));
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

  bool Contains(const Entry* entry, std::uint32_t hash) const noexcept {
    return FindAddress(reinterpret_cast<std::uintptr_t>(entry), hash) !=
           nullptr;
  }

  Entry* FindAddress(std::uintptr_t address, std::uint32_t hash) noexcept {
    return const_cast<Entry*>(std::as_const(*this).FindAddress(address, hash));
  }

  // Resolves an address cached by an asynchronous owner without interpreting
  // it as an Entry first. The object may have been erased while that owner was
  // suspended; comparing integer addresses with live bucket slots makes the
  // membership check safe before any object lifetime is assumed. Callers must
  // use the returned live pointer rather than reconstructing one from address.
  const Entry* FindAddress(std::uintptr_t address,
                           std::uint32_t hash) const noexcept {
    if (address == 0) {
      return nullptr;
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
          if (Occupied(*bucket, slot) &&
              reinterpret_cast<std::uintptr_t>(bucket->entries_[slot]) ==
                  address) {
            return bucket->entries_[slot];
          }
        }
        bucket = Chained(*bucket) ? Child(bucket) : nullptr;
      }
    }
    return nullptr;
  }

  // Updates an entry without changing its address when the policy-selected
  // concrete type stays the same. If the representation changes, swaps a new
  // object into the existing bucket slot and returns the detached old object
  // through `replaced`. Before destroying it, the caller must migrate caches
  // that assume a live object; asynchronous address-only caches can instead
  // use FindAddress to validate membership before dereferencing.
  Entry* ReplaceValue(Entry* existing, const Value& value, const Digest& digest,
                      Entry** replaced) {
    assert(existing != nullptr);
    assert(replaced != nullptr);
    *replaced = nullptr;
    if (existing->can_assign(value)) {
      existing->assign(value);
      return existing;
    }

    std::unique_ptr<Entry, void (*)(Entry*)> replacement(
        Entry::CreateReplacement(*existing, value), &Entry::Destroy);
    const std::uint64_t hash = Hash(digest);
    const int tables = Rehashing() ? 2 : 1;
    for (int t = 0; t < tables; ++t) {
      Table& table = tables_[t];
      if (table.buckets_ == nullptr) {
        continue;
      }
      Bucket* bucket = &table.buckets_[hash & BucketMask(table)];
      while (bucket != nullptr) {
        const std::size_t slots =
            Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
        for (std::size_t slot = 0; slot < slots; ++slot) {
          if (Occupied(*bucket, slot) && bucket->entries_[slot] == existing) {
            bucket->entries_[slot] = replacement.get();
            *replaced = existing;
            return replacement.release();
          }
        }
        bucket = Chained(*bucket) ? Child(bucket) : nullptr;
      }
    }
    assert(false && "replacement target must belong to this map");
    return nullptr;
  }

  InsertResult InsertOrAssign(const Digest& digest, std::string_view key,
                              const Value& value, bool key_complete = true) {
    if (Entry* existing = Find(digest, key); existing != nullptr) {
      // The inline policy never changes representation. Specialized policies
      // use ReplaceValue when a new value changes the concrete entry type.
      assert(existing->can_assign(value));
      existing->assign(value);
      return {existing, false};
    }

    EnsureTable();
    MaybeStartExpansion();
    std::unique_ptr<Entry, void (*)(Entry*)> entry(
        Entry::Create(digest, key, value, key_complete), &Entry::Destroy);
    Entry* raw = entry.get();
    AddToTable(Rehashing() ? tables_[1] : tables_[0], raw, Hash(digest));
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
    AddToTable(Rehashing() ? tables_[1] : tables_[0], raw, Hash(digest));
    entry.release();
    return raw;
  }

  // Deletes the matching entry, compacting its bucket chain the way Valkey's
  // hashtablePop does so chains stay dense and emptied child buckets are
  // freed. Compaction moves entries only within their own chain, and Scan
  // emits a whole chain per cursor position, so an owner-serialized cursor
  // scan does not miss an entry that exists throughout while erases occur
  // between Scan calls. Scan and mutation are not thread-safe concurrently.
  // The table itself never shrinks (the port dropped shrinking with deletion);
  // slots are reused by later inserts, so footprint is bounded by the peak
  // live count.
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
    const std::uint64_t hash = EntryHash(*entry);
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

  // Visits a stable map from cursor's exact position. Returning false from
  // the callback pauses after the current entry; a later call with the same
  // cursor resumes at the following entry. Returns true only after both
  // tables are exhausted. Mutation between calls is unsupported because it
  // can move entries between tables or compact a bucket chain.
  template <typename Fn>
  bool ScanStableWhile(StableScanCursor* cursor, Fn&& fn) const {
    assert(cursor != nullptr);
    while (cursor->table_ < 2) {
      const Table& table = tables_[cursor->table_];
      const std::size_t bucket_count = BucketCount(table);
      if (cursor->bucket_ >= bucket_count) {
        ++cursor->table_;
        cursor->bucket_ = 0;
        cursor->chain_ = 0;
        cursor->slot_ = 0;
        continue;
      }

      const Bucket* bucket = &table.buckets_[cursor->bucket_];
      for (std::size_t chain = 0; chain < cursor->chain_; ++chain) {
        assert(Chained(*bucket));
        bucket = Child(bucket);
      }
      const std::size_t slots =
          Chained(*bucket) ? kChildSlot : kEntriesPerBucket;
      while (cursor->slot_ < slots) {
        const std::size_t slot = cursor->slot_++;
        if (Occupied(*bucket, slot) && !fn(*bucket->entries_[slot])) {
          return false;
        }
      }

      cursor->slot_ = 0;
      if (Chained(*bucket)) {
        ++cursor->chain_;
      } else {
        ++cursor->bucket_;
        cursor->chain_ = 0;
      }
    }
    return true;
  }

  // Samples a short range of buckets and chooses uniformly from the sampled
  // entries, matching the bounded-work shape of Valkey's fair random lookup.
  // The caller may reject unsuitable records and retry with fresh entropy.
  Entry* FairRandomEntry(std::uint64_t entropy) {
    if (empty()) return nullptr;
    constexpr std::size_t kSampleEntries = 15;
    constexpr std::size_t kMaximumScanSteps = 32;
    std::array<Entry*, kSampleEntries> sampled{};
    std::size_t count = 0;
    std::uint64_t cursor = entropy;
    for (std::size_t step = 0;
         step < kMaximumScanSteps && count < sampled.size(); ++step) {
      cursor = Scan(cursor, [&](Entry& entry) {
        if (count < sampled.size()) sampled[count++] = &entry;
      });
      if (cursor == 0) break;
    }
    if (count == 0) return nullptr;
    // SplitMix64 finalizer keeps the sample choice independent from the bits
    // consumed by the scan cursor.
    entropy ^= entropy >> 30;
    entropy *= 0xbf58476d1ce4e5b9ULL;
    entropy ^= entropy >> 27;
    entropy *= 0x94d049bb133111ebULL;
    entropy ^= entropy >> 31;
    return sampled[entropy % count];
  }

  // A cursor of zero starts and completes a full scan. The owner may mutate,
  // rehash, or compact the map between Scan calls, and the callback may then
  // be invoked more than once for an entry. Mutation from another thread
  // during this call, or from inside the callback, is unsupported.
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
  // Entry addresses are stable across expansion and ordinary assignment.
  // ReplaceValue deliberately returns the detached old object when a storage
  // policy changes concrete type, making the exceptional invalidation
  // explicit to its caller. A raw-pointer cache must likewise detect detach
  // before dereferencing; this class does not track that, since the useful
  // granularity is whatever set of maps the caller detaches together.
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
    return digest.value_;
  }

  static std::uint8_t HashTag(std::uint64_t hash) noexcept {
    return static_cast<std::uint8_t>(hash >> 56);
  }

  static std::uint64_t EntryHash(const Entry& entry) noexcept {
    return Hash(entry.key_complete() ? ComputeDigest(entry.key())
                                     : entry.external_key_digest());
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
    // Once every cached address bit is in use, retaining the current direct
    // table is safer than overflowing the entry hash. AddToTable continues to
    // accept entries through bucket chains, so saturation is a performance
    // boundary rather than a capacity or correctness failure.
    if (tables_[0].exponent_ >= MaxBucketExponent) {
      return;
    }
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

  static void AddToTable(Table& table, Entry* entry, std::uint64_t hash) {
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
          // Entries no longer retain a bucket hash. Recompute it while the
          // source object is known live; external keys already carry their
          // digest, while inline keys trade rehash CPU for the smaller steady
          // state representation.
          AddToTable(*target, bucket->entries_[slot],
                     EntryHash(*bucket->entries_[slot]));
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

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
std::uint8_t ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::
    EncodedKeyMetadataBytes(std::uint32_t encoded) noexcept {
  std::uint8_t bytes = 1;
  while (encoded >= 0x80) {
    encoded >>= 7;
    ++bytes;
  }
  return bytes;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
std::uint8_t
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::EncodeKeyMetadata(
    std::byte* output, std::uint32_t logical_size, bool key_complete) noexcept {
  assert(output != nullptr);
  assert(logical_size <= kMaxLogicalKeySize);
  std::uint32_t encoded =
      (logical_size << 1) | static_cast<std::uint32_t>(!key_complete);
  const std::uint8_t bytes = EncodedKeyMetadataBytes(encoded);
  for (std::uint8_t index = 0; index < bytes; ++index) {
    std::uint8_t byte = static_cast<std::uint8_t>(encoded & 0x7f);
    encoded >>= 7;
    if (encoded != 0) {
      byte |= 0x80;
    }
    output[index] = static_cast<std::byte>(byte);
  }
  return bytes;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::KeyMetadata
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::DecodeKeyMetadata(
    const std::byte* input) noexcept {
  assert(input != nullptr);
  std::uint32_t encoded = 0;
  for (std::uint8_t index = 0; index < 5; ++index) {
    const std::uint8_t byte = std::to_integer<std::uint8_t>(input[index]);
    if (index == 4) {
      assert((byte & 0xf0) == 0);
    }
    encoded |= static_cast<std::uint32_t>(byte & 0x7f) << (index * 7);
    if ((byte & 0x80) == 0) {
      return KeyMetadata{
          .logical_size_ = encoded >> 1,
          .encoded_bytes_ = static_cast<std::uint8_t>(index + 1),
          .key_complete_ = (encoded & 1) == 0,
      };
    }
  }
  assert(false && "unterminated key metadata varint");
  return {};
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
std::size_t ScanHashMap<Value, MaxBucketExponent,
                        EntryPolicy>::Entry::tail_bytes() const noexcept {
  const KeyMetadata metadata = DecodeKeyMetadata(tail());
  return metadata.encoded_bytes_ +
         (metadata.key_complete_ ? metadata.logical_size_ : sizeof(Digest));
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry*
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::Create(
    const Digest& digest, std::string_view key, const Value& value,
    bool key_complete) {
  if (key.size() > kMaxLogicalKeySize) {
    throw std::bad_alloc();
  }
  const std::uint32_t logical_size = static_cast<std::uint32_t>(key.size());
  const std::uint32_t encoded_metadata =
      (logical_size << 1) | static_cast<std::uint32_t>(!key_complete);
  const std::size_t metadata_bytes = EncodedKeyMetadataBytes(encoded_metadata);
  const std::size_t payload_bytes = key_complete ? key.size() : sizeof(digest);
  const std::size_t tail_bytes = metadata_bytes + payload_bytes;
  const bool extended = EntryPolicy::HasExtraValue(value);
  const std::size_t header_bytes =
      extended ? sizeof(ExtendedEntry) : sizeof(Entry);
  if (tail_bytes > std::numeric_limits<std::size_t>::max() - header_bytes) {
    throw std::bad_alloc();
  }
  static_assert(alignof(Entry) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  static_assert(alignof(ExtendedEntry) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__);
  void* storage = ::operator new(header_bytes + tail_bytes);
  Entry* entry = nullptr;
  try {
    if (extended) {
      entry = new (storage) ExtendedEntry(EntryPolicy::Store(value),
                                          EntryPolicy::StoreExtra(value));
    } else {
      entry = new (storage) Entry(EntryPolicy::Store(value));
    }
  } catch (...) {
    ::operator delete(storage);
    throw;
  }
  const std::uint8_t written_metadata =
      EncodeKeyMetadata(entry->tail(), logical_size, key_complete);
  assert(written_metadata == metadata_bytes);
  std::byte* payload = entry->tail() + written_metadata;
  if (key_complete && !key.empty()) {
    std::memcpy(payload, key.data(), key.size());
  } else if (!key_complete) {
    std::memcpy(payload, &digest, sizeof(digest));
  }
  return entry;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry*
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::CreateReplacement(
    const Entry& source, const Value& value) {
  const std::size_t tail_bytes = source.tail_bytes();
  const bool extended = EntryPolicy::HasExtraValue(value);
  const std::size_t header_bytes =
      extended ? sizeof(ExtendedEntry) : sizeof(Entry);
  if (tail_bytes > std::numeric_limits<std::size_t>::max() - header_bytes) {
    throw std::bad_alloc();
  }
  void* storage = ::operator new(header_bytes + tail_bytes);
  Entry* entry = nullptr;
  try {
    if (extended) {
      entry = new (storage) ExtendedEntry(EntryPolicy::Store(value),
                                          EntryPolicy::StoreExtra(value));
    } else {
      entry = new (storage) Entry(EntryPolicy::Store(value));
    }
  } catch (...) {
    ::operator delete(storage);
    throw;
  }
  if (tail_bytes != 0) {
    std::memcpy(entry->tail(), source.tail(), tail_bytes);
  }
  return entry;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
void ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::Destroy(
    Entry* entry) noexcept {
  if (entry == nullptr) {
    return;
  }
  if (entry->has_extra()) {
    static_cast<ExtendedEntry*>(entry)->~ExtendedEntry();
  } else {
    entry->~Entry();
  }
  ::operator delete(entry);
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::EntryExtra*
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::extra() noexcept {
  return &static_cast<ExtendedEntry*>(this)->extra_;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
const typename ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::EntryExtra*
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::extra()
    const noexcept {
  return &static_cast<const ExtendedEntry*>(this)->extra_;
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
std::byte*
ScanHashMap<Value, MaxBucketExponent, EntryPolicy>::Entry::tail() noexcept {
  return reinterpret_cast<std::byte*>(this) +
         (has_extra() ? sizeof(ExtendedEntry) : sizeof(Entry));
}

template <typename Value, unsigned MaxBucketExponent, typename EntryPolicy>
const std::byte* ScanHashMap<Value, MaxBucketExponent,
                             EntryPolicy>::Entry::tail() const noexcept {
  return reinterpret_cast<const std::byte*>(this) +
         (has_extra() ? sizeof(ExtendedEntry) : sizeof(Entry));
}

}  // namespace keylane::storage
