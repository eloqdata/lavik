#include <charconv>
#include <cmath>
#include <cstdlib>
#include <random>
#include <set>

#include "impl.h"
#include "keylane/glob.h"
#include "keylane/memory.h"
#include "keylane/random_sample.h"
#include "keylane/redis_parse.h"
#include "keylane/storage/detail/grouped_scratch.h"

namespace keylane::storage {

namespace {

bool DigestLess(const Digest& left, const Digest& right) {
  return left.value_ < right.value_;
}

bool EntryLess(const HashEntry& left, const HashEntry& right) {
  if (left.digest_ != right.digest_)
    return DigestLess(left.digest_, right.digest_);
  return left.field_ < right.field_;
}

bool IsWrite(const HashOperation& operation) {
  return operation.kind_ == HashOperationKind::kSet ||
         operation.kind_ == HashOperationKind::kReplaceOnly ||
         operation.kind_ == HashOperationKind::kSetIfAbsent ||
         operation.kind_ == HashOperationKind::kDelete ||
         operation.kind_ == HashOperationKind::kPopRandom ||
         operation.kind_ == HashOperationKind::kIncrementInteger ||
         operation.kind_ == HashOperationKind::kIncrementFloat;
}

bool IsPointOperation(HashOperationKind kind) {
  switch (kind) {
    case HashOperationKind::kSet:
    case HashOperationKind::kSetIfAbsent:
    case HashOperationKind::kDelete:
    case HashOperationKind::kIncrementInteger:
    case HashOperationKind::kIncrementFloat:
    case HashOperationKind::kGet:
    case HashOperationKind::kGetMany:
    case HashOperationKind::kExists:
    case HashOperationKind::kStringLength:
      return true;
    default:
      return false;
  }
}

bool NeedsGroupedHash(const HashValue& value) {
  // Small hashes keep the compact path. Crossing the threshold publishes a
  // complete grouped graph atomically; incremental writes never demote it.
  // An explicit whole-Hash replacement may choose a compact new value.
  std::uint64_t bytes = kHashValueHeaderBytes;
  for (const auto& entry : value.entries_) {
    bytes += 8 + entry.field_.size() + entry.value_.size();
    if (bytes >= kGroupedHashPromotionBytes) return true;
  }
  return false;
}

absl::StatusOr<HashResult> ReadCompactHashResult(std::string_view payload,
                                                 HashOperationKind kind,
                                                 std::uint64_t count) {
  auto reader = HashValueReader::Open(payload);
  if (!reader.ok()) return reader.status();
  if (reader->size() != count) {
    return absl::InternalError(
        "Hash element count does not match record metadata");
  }
  const bool fields = kind != HashOperationKind::kValues;
  const bool values = kind != HashOperationKind::kKeys;
  const std::size_t width = kind == HashOperationKind::kGetAll ? 2 : 1;
  std::size_t retained = sizeof(HashResult);
  auto add_bytes = [&](std::size_t bytes) {
    if (bytes > std::numeric_limits<std::size_t>::max() - retained)
      return false;
    retained += bytes;
    return true;
  };
  // Validate and admit before copying any strings. Full reads do not need
  // lookup digests or a mutable HashEntry array. The second traversal copies
  // only selected strings directly into the owned result; no view escapes the
  // LoadedValue lease. Use the same conservative SSO accounting as other Hash
  // results, and verify actual allocator capacities before ownership handoff.
  const std::size_t inline_capacity = std::string{}.capacity();
  auto inspect = *reader;
  for (std::size_t i = 0; i < count; ++i) {
    auto entry = inspect.Next();
    if (!entry.ok()) return entry.status();
    if (!add_bytes(width * sizeof(std::optional<std::string>)) ||
        (fields &&
         !add_bytes(std::max(entry->field_.size(), inline_capacity) + 1)) ||
        (values &&
         !add_bytes(std::max(entry->value_.size(), inline_capacity) + 1))) {
      RecordMemoryRejection();
      return absl::ResourceExhaustedError("OOM Hash output is too large");
    }
  }
  auto reservation = TryReserveMemory(retained);
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM Hash output retention");
  }
  HashResult result;
  result.key_exists_ = true;
  result.length_ = count;
  result.values_.reserve(count * width);
  for (std::size_t i = 0; i < count; ++i) {
    auto entry = reader->Next();
    if (!entry.ok()) return entry.status();
    if (fields) result.values_.emplace_back(std::in_place, entry->field_);
    if (values) result.values_.emplace_back(std::in_place, entry->value_);
  }
  retained = sizeof(HashResult) +
             result.values_.capacity() * sizeof(result.values_[0]);
  for (const auto& value : result.values_) retained += value->capacity() + 1;
  if (retained > reservation->bytes()) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM Hash output retention");
  }
  result.retained_charge_.Adopt(&*reservation, retained);
  return result;
}

}  // namespace

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHashLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  return ExecuteHashLikeLocked(db_id, key, digest, operation, ValueType::kHash,
                               tx, replication, mutation_precondition);
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHashLikeLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, ValueType value_type, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  // Private preparation allocations must unwind here: Bycorf terminates on
  // exceptions escaping a coroutine body, even when its caller has a catch.
  try {
    assert(db_id < kLogicalDatabaseCount);
    const bool replace = operation.kind_ == HashOperationKind::kReplaceOnly;
    if (replace &&
        (value_type != ValueType::kHash || operation.fields_.empty() ||
         operation.fields_.size() != operation.values_.size())) {
      co_return absl::InvalidArgumentError("invalid Hash replacement fields");
    }
    if (operation.kind_ == HashOperationKind::kScan &&
        operation.scan_count_ == 0) {
      co_return absl::InvalidArgumentError(
          "Hash scan COUNT must be greater than zero");
    }

    WorkerStore& store = CurrentStore();
    auto& partition = PartitionForKey(store, key);
    co_await store.store_state_mutex_.Lock();
    UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

    auto& index = partition.indexes_[db_id];
    auto* found = index.Find(digest, key);
    if (found != nullptr && !found->key_complete()) [[unlikely]] {
      auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
      if (!resolved.ok()) co_return resolved.status();
      found = *resolved;
    }
    const bool stored_value =
        found != nullptr && found->value_.kind() == RecordKind::kValue;
    const std::uint64_t now_ms =
        operation.now_ms_ == 0 ? UnixTimeMillis() : operation.now_ms_;
    const bool exists = stored_value && !IsExpired(*found, now_ms);
    if (!exists && stored_value && found->value_.grouped()) {
      auto metadata = co_await ReadKeyMetadataLocked(db_id, key, digest);
      if (!metadata.ok()) co_return metadata.status();
    }
    if (exists && found->value_.value_type() != value_type) {
      co_return absl::InvalidArgumentError(
          "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    if (replace && !exists) co_return HashResult{};
    const bool read_only = !IsWrite(operation);
    // Exercise the command's storage-error reply path independently of the
    // outer maxmemory preflight. No mutation has been staged at this boundary.
    KEYLANE_FAULT_INJECT(
        if (KEYLANE_FAULT_MATCHES("KEYLANE_FAIL_HASH_ADMISSION_KEY", key) &&
            !read_only && value_type == ValueType::kHash) {
          co_return absl::ResourceExhaustedError(
              "OOM injected Hash storage admission failure");
        });
    const auto observed_write =
        CaptureCompactWriteSnapshot(store, partition, db_id);
    const auto observed_index_generation = observed_write.index_generation_;
    const auto observed_db_epoch = observed_write.db_epoch_;
    const auto observed_replication_epoch = observed_write.replication_epoch_;
    auto read_epoch_changed = [&]() {
      return read_only &&
             (store.index_generations_[db_id] != observed_index_generation ||
              EffectiveRecordDbEpoch(partition, db_id) != observed_db_epoch ||
              partition.replication_epoch_ != observed_replication_epoch);
    };
    const RecordLocation location =
        exists ? MaterializeIndexLocation(*found) : RecordLocation{};
    const ExtentManifest extents =
        exists ? ExtentsFor(store, found) : ExtentManifest{};
    const std::uint64_t expire_at_ms = exists ? location.expire_at_ms_ : 0;
    GroupedHashObject::Handle grouped;
    if (exists && location.grouped()) {
      auto view = partition.grouped_objects_[db_id].Lookup(
          key, GroupedObjectVersion{
                   .root_ = location,
                   .db_epoch_ = observed_db_epoch,
                   .replication_epoch_ = observed_replication_epoch,
                   .index_generation_ = partition.grouped_generations_[db_id],
               });
      if (!view.ok()) co_return view.status();
      grouped = std::move(*view);
      // Validate the preceding decision, but never inherit its fields/pages.
      // Append/CommitGroupedHashMutation still find and retire the old side
      // view; a null mutation predecessor creates a fresh incarnation.
      if (replace) grouped.reset();
    }

    HashResult result;
    result.key_exists_ = exists;
    result.length_ = exists ? location.logical_size_ : 0;

    // HLEN/SCARD are carried in the record header. Loading and decoding the
    // complete monolithic value here turns an O(1) metadata lookup into an
    // O(value-size) disk read and allocation.
    if (operation.kind_ == HashOperationKind::kLength) co_return result;

    // Ordinary small HSET/HMSET and SADD/SREM retain exclusive key intent in
    // their caller; normal command dispatch also retains database admission
    // through append's later allocation waits. Other keys need not wait for
    // this key's disk read or private decode/modify/encode work. Keep compact
    // external values, EXEC and native candidate/loading on their existing
    // paths; ordinary online replay has the same command guards and may use
    // this path too. Snapshot only value metadata before suspension, never an
    // index entry pointer. GC is allowed to relocate this same logical version
    // while the state mutex is released. Set multi-key mutation adapters always
    // provide their durable TxShardWrites; kPopRandom stays on its original
    // selection/replication path.
    const bool prepare_operation =
        (value_type == ValueType::kHash &&
         (operation.kind_ == HashOperationKind::kSet || replace)) ||
        (value_type == ValueType::kSet &&
         (operation.kind_ == HashOperationKind::kSet ||
          operation.kind_ == HashOperationKind::kDelete));
    const bool unlocked_compact_write =
        prepare_operation && exists &&
        CanPrepareCompactWriteUnlocked(store, partition, found, location, tx);
    const bool unlocked_create =
        !exists && !read_only && IsPointOperation(operation.kind_) &&
        operation.kind_ != HashOperationKind::kDelete &&
        CanPrepareCollectionCreateUnlocked(partition, tx);
    // Point writers retain their key hold even inside EXEC/Lua. Random pops use
    // a separate selection/publication adapter and retain its existing
    // contract.
    const bool unlocked_grouped_write =
        !read_only && grouped != nullptr &&
        operation.kind_ != HashOperationKind::kPopRandom &&
        CanPrepareGroupedWriteUnlocked(partition);
    if (read_only || unlocked_compact_write || unlocked_grouped_write ||
        unlocked_create) {
      found = nullptr;
      unlock.Unlock();
    }
    KEYLANE_FAULT_INJECT(if (unlocked_create) {
      KEYLANE_FAULT_BAD_ALLOC("KEYLANE_FAIL_COLLECTION_CREATE_PREPARE_KEY",
                              key);
    });
    KEYLANE_FAULT_INJECT(if (unlocked_grouped_write) {
      const auto paused =
          co_await PauseGroupedWriteForTest(*store.worker_, key, "prepare");
      if (!paused.ok()) co_return paused;
    });
    KEYLANE_FAULT_INJECT(if (unlocked_compact_write &&
                             value_type == ValueType::kHash) {
      const char* paused_key =
          std::getenv("KEYLANE_COMPACT_HASH_WRITE_PAUSE_KEY");
      const char* configured =
          std::getenv("KEYLANE_COMPACT_HASH_WRITE_PAUSE_MS");
      if (paused_key != nullptr && key == paused_key && configured != nullptr) {
        std::int64_t milliseconds = 0;
        const char* end = configured + std::strlen(configured);
        const auto parsed = std::from_chars(configured, end, milliseconds);
        if (parsed.ec == std::errc{} && parsed.ptr == end && milliseconds > 0) {
          spdlog::info("compact hash write pause armed key={} milliseconds={}",
                       key, milliseconds);
          auto paused = co_await bycorf::SleepFor(
              *store.worker_, std::chrono::milliseconds(milliseconds));
          if (!paused.ok()) co_return paused;
          spdlog::info("compact hash write pause complete key={}", key);
        }
      }
    });
    KEYLANE_FAULT_INJECT(if (unlocked_compact_write) {
      const auto paused =
          co_await PauseCompactWriteForTest(*store.worker_, key);
      if (!paused.ok()) co_return paused;
    });
    KEYLANE_FAULT_INJECT(if (read_only) {
      if (const char* configured = std::getenv("KEYLANE_HASH_READ_PAUSE_MS");
          configured != nullptr) {
        std::uint64_t milliseconds = 0;
        const char* end = configured + std::strlen(configured);
        const auto parsed = std::from_chars(configured, end, milliseconds);
        if (parsed.ec == std::errc{} && parsed.ptr == end &&
            milliseconds != 0) {
          absl::Status paused = co_await bycorf::SleepFor(
              *store.worker_, std::chrono::milliseconds(milliseconds));
          if (!paused.ok()) co_return paused;
        }
      }
    });

    if (grouped != nullptr &&
        (operation.kind_ == HashOperationKind::kRandomFields ||
         operation.kind_ == HashOperationKind::kPopRandom)) {
      co_return co_await ExecuteGroupedHashRandomLocked(
          store, partition, db_id, key, digest, operation, std::move(grouped),
          value_type, tx, replication, mutation_precondition);
    }
    if (grouped != nullptr && operation.kind_ == HashOperationKind::kScan) {
      // Cursors are field digests under the persisted routing seed, not ranks.
      // Deleting earlier fields or splitting a leaf cannot shift a later field
      // behind the cursor. COUNT remains a hint: one leaf bounds scratch and a
      // complete digest-collision cluster must be returned together.
      const auto& groups = grouped->directory().groups();
      const auto* route = groups.Floor(operation.cursor_);
      if (route == nullptr)
        co_return absl::DataLossError("grouped scan has no routing leaf");
      auto next = groups.find(route->id_.prefix_);
      ++next;
      const auto* entry = grouped->FindGroup(route->id_);
      if (entry == nullptr)
        co_return absl::DataLossError("grouped scan has no physical page");
      GroupedScratchBudget budget;
      const auto included = budget.AddGroup(
          entry->value_, grouped->ExtentsFor(route->id_), key.size());
      if (!included.ok()) co_return included;
      // Matching strings move into the reply; no second payload copy is made.
      // The physical read buffer has its own independent admission.
      auto scratch = budget.Reserve(1);
      if (!scratch.ok()) co_return scratch.status();
      auto loaded = co_await LoadHashGroupSnapshot(store, partition, db_id, key,
                                                   digest, grouped, route->id_);
      if (!loaded.ok()) {
        if (read_epoch_changed()) co_return HashResult{};
        co_return loaded.status();
      }
      auto& fields = loaded->snapshot_.value_.entries_;
      for (auto& field : fields)
        field.digest_ =
            ComputeDigest(field.field_, grouped->directory().root().seed_);
      std::sort(fields.begin(), fields.end(), EntryLess);
      const auto first =
          std::lower_bound(fields.begin(), fields.end(), operation.cursor_,
                           [](const HashEntry& field, std::uint64_t cursor) {
                             return field.digest_.value_ < cursor;
                           });
      const auto begin = static_cast<std::size_t>(first - fields.begin());
      auto end = begin + std::min<std::uint64_t>(operation.scan_count_,
                                                 fields.size() - begin);
      while (end < fields.size() && end != begin &&
             fields[end].digest_ == fields[end - 1].digest_)
        ++end;
      result.values_.reserve((end - begin) *
                             (value_type == ValueType::kHash ? 2 : 1));
      for (auto i = begin; i < end; ++i) {
        auto& field = fields[i];
        if (operation.match_ != "*" &&
            !keylane::RedisGlobMatch(operation.match_, field.field_))
          continue;
        result.values_.emplace_back(std::move(field.field_));
        if (value_type == ValueType::kHash)
          result.values_.emplace_back(std::move(field.value_));
      }
      result.cursor_ = end < fields.size()    ? fields[end].digest_.value_
                       : next != groups.end() ? next->first
                                              : 0;
      std::size_t retained = sizeof(HashResult) + result.values_.capacity() *
                                                      sizeof(result.values_[0]);
      for (const auto& value : result.values_)
        if (value) retained += value->capacity() + 1;
      if (retained > scratch->bytes())
        co_return absl::ResourceExhaustedError(
            "grouped scan output exceeds admission");
      result.retained_charge_.Adopt(&*scratch, retained);
      co_return result;
    }

    // A multi-field point operation keeps the selected leaves until planning
    // finishes; scanning operations keep the entire decoded value. Admit that
    // peak before any leaf allocations, including copies made by the planner.
    std::set<HashGroupId> selected;
    std::optional<MemoryReservation> grouped_scratch;
    if (replace || unlocked_create) {
      // Bound owned field copies, sorting and encoding before allocating them.
      // Request bytes have their separate client-buffer admission already.
      if (operation.fields_.size() != operation.values_.size())
        co_return absl::InvalidArgumentError("Hash field/value mismatch");
      GroupedScratchBudget budget;
      for (std::size_t i = 0; i < operation.fields_.size(); ++i) {
        if (operation.fields_[i].size() > kMaxStringBytes ||
            operation.values_[i].size() > kMaxStringBytes)
          co_return absl::OutOfRangeError(
              "Hash field or value exceeds 512 MiB");
        for (const auto bytes :
             {operation.fields_[i].size(), operation.values_[i].size(),
              std::size_t{256}}) {
          const auto added = budget.AddBytes(bytes);
          if (!added.ok()) co_return added;
        }
      }
      auto admitted = budget.Reserve(unlocked_create ? 2 : 4);
      if (!admitted.ok()) co_return admitted.status();
      grouped_scratch.emplace(std::move(*admitted));
    } else if (grouped != nullptr) {
      GroupedScratchBudget budget;
      if (IsPointOperation(operation.kind_)) {
        for (const auto field : operation.fields_) {
          const auto* route = grouped->directory().Find(field);
          if (route == nullptr)
            co_return absl::DataLossError("missing Hash field route");
          selected.insert(route->id_);
        }
      } else {
        for (const auto& [prefix, metadata] : grouped->directory().groups())
          selected.insert(metadata.id_);
      }
      for (const auto id : selected) {
        const auto* entry = grouped->FindGroup(id);
        if (entry == nullptr)
          co_return absl::DataLossError("missing Hash scratch page");
        const auto added =
            budget.AddGroup(entry->value_, grouped->ExtentsFor(id), key.size());
        if (!added.ok()) co_return added;
      }
      for (const auto field : operation.fields_) {
        const auto added = budget.AddBytes(field.size());
        if (!added.ok()) co_return added;
      }
      for (const auto value : operation.values_) {
        const auto added = budget.AddBytes(value.size());
        if (!added.ok()) co_return added;
      }
      auto admitted = budget.Reserve(4);
      if (!admitted.ok()) co_return admitted.status();
      grouped_scratch.emplace(std::move(*admitted));
    }
    HashValue compact;
    if (grouped != nullptr) {
      if (IsPointOperation(operation.kind_)) {
        // Loading a requested leaf once is enough even when HMGET/HSET mentions
        // several fields routed to that same leaf. Unrelated large field values
        // never enter this command's scratch unless they share the affected
        // group.
        for (const auto id : selected) {
          if (unlocked_grouped_write) co_await bycorf::Yield(*store.worker_);
          auto loaded = co_await LoadHashGroupSnapshot(
              store, partition, db_id, key, digest, grouped, id);
          if (!loaded.ok()) {
            if (read_epoch_changed()) co_return HashResult{};
            co_return loaded.status();
          }
          for (auto& entry : loaded->snapshot_.value_.entries_) {
            compact.entries_.push_back(std::move(entry));
          }
        }
      } else {
        auto loaded = co_await LoadGroupedHashValue(store, partition, db_id,
                                                    key, digest, grouped);
        if (!loaded.ok()) {
          if (read_epoch_changed()) co_return HashResult{};
          co_return loaded.status();
        }
        compact = std::move(*loaded);
      }
    } else if (exists && !replace) {
      auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                       location, extents);
      if (!loaded.ok()) {
        if (read_epoch_changed()) co_return HashResult{};
        co_return loaded.status();
      }
      const auto bytes = loaded->value();
      const std::string_view payload(
          reinterpret_cast<const char*>(bytes.data()), bytes.size());
      if (operation.kind_ == HashOperationKind::kGetAll ||
          operation.kind_ == HashOperationKind::kKeys ||
          operation.kind_ == HashOperationKind::kValues) {
        co_return ReadCompactHashResult(payload, operation.kind_,
                                        location.logical_size_);
      }
      auto decoded = DecodeHashValue(payload);
      if (!decoded.ok()) co_return decoded.status();
      compact = std::move(*decoded);
      if (compact.entries_.size() != location.logical_size_) {
        co_return absl::InternalError(
            "Hash element count does not match record metadata");
      }
    }

    const std::uint64_t selected_field_count = compact.entries_.size();
    std::set<HashGroupId> changed_groups;
    auto mark_group_changed = [&](std::string_view field) {
      if (grouped != nullptr) {
        const auto* route = grouped->directory().Find(field);
        assert(route != nullptr);
        changed_groups.insert(route->id_);
      }
    };

    auto find_entry = [&](std::string_view field) {
      const Digest field_digest = ComputeDigest(field);
      return std::find_if(compact.entries_.begin(), compact.entries_.end(),
                          [&](const HashEntry& entry) {
                            return entry.digest_ == field_digest &&
                                   entry.field_ == field;
                          });
    };
    auto lookup = [&](std::string_view field) -> HashEntry* {
      auto entry = find_entry(field);
      return entry == compact.entries_.end() ? nullptr : &*entry;
    };

    auto incremented_value = [&](std::optional<std::string_view> current)
        -> absl::StatusOr<std::string> {
      if (operation.fields_.size() != 1 || operation.values_.size() != 1) {
        return absl::InvalidArgumentError("invalid Hash increment operands");
      }
      if (operation.kind_ == HashOperationKind::kIncrementInteger) {
        std::int64_t previous = 0;
        std::int64_t increment = 0;
        if (current.has_value()) {
          const auto parsed = std::from_chars(
              current->data(), current->data() + current->size(), previous);
          if (parsed.ec != std::errc{} ||
              parsed.ptr != current->data() + current->size()) {
            return absl::InvalidArgumentError("hash value is not an integer");
          }
        }
        const std::string_view delta = operation.values_.front();
        const auto parsed = std::from_chars(
            delta.data(), delta.data() + delta.size(), increment);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != delta.data() + delta.size()) {
          return absl::InvalidArgumentError(
              "value is not an integer or out of range");
        }
        std::int64_t updated = 0;
        if (__builtin_add_overflow(previous, increment, &updated)) {
          return absl::OutOfRangeError("increment or decrement would overflow");
        }
        result.signed_integer_ = updated;
        return std::to_string(updated);
      }

      long double previous = 0;
      long double increment = 0;
      if (current.has_value() && !ParseRedisLongDouble(*current, &previous)) {
        return absl::InvalidArgumentError("hash value is not a float");
      }
      const std::string_view delta = operation.values_.front();
      std::string_view special = delta;
      if (!special.empty() &&
          (special.front() == '+' || special.front() == '-')) {
        special.remove_prefix(1);
      }
      std::string special_lower(special);
      std::transform(special_lower.begin(), special_lower.end(),
                     special_lower.begin(), [](unsigned char byte) {
                       return static_cast<char>(std::tolower(byte));
                     });
      if (special_lower == "inf" || special_lower == "infinity" ||
          special_lower == "nan") {
        return absl::InvalidArgumentError("value is NaN or Infinity");
      }
      if (!ParseRedisLongDouble(delta, &increment))
        return absl::InvalidArgumentError("value is not a valid float");
      const long double updated = previous + increment;
      if (!std::isfinite(updated))
        return absl::OutOfRangeError("increment would produce NaN or Infinity");
      if (!FormatRedisLongDouble(updated, &result.scalar_)) {
        return absl::InternalError("failed to format Hash float");
      }
      return result.scalar_;
    };

    auto requested_samples = [&]() -> absl::StatusOr<std::uint64_t> {
      if (!operation.count_provided_) return 1;
      if (operation.count_ == std::numeric_limits<std::int64_t>::min())
        return absl::OutOfRangeError("value is out of range");
      const std::uint64_t requested = static_cast<std::uint64_t>(
          operation.count_ < 0 ? -operation.count_ : operation.count_);
      return requested;
    };

    auto append_random = [&]() -> absl::Status {
      if (compact.entries_.empty()) return absl::OkStatus();
      auto requested = requested_samples();
      if (!requested.ok()) return requested.status();
      if (operation.count_provided_ && operation.count_ < 0) {
        for (std::uint64_t i = 0; i < *requested; ++i) {
          const HashEntry& entry = compact.entries_[RandomRank(
              compact.entries_.size(), RandomSampleGenerator())];
          result.values_.push_back(entry.field_);
          if (operation.with_values_) result.values_.push_back(entry.value_);
        }
        return absl::OkStatus();
      }
      if (*requested >= compact.entries_.size()) {
        for (const HashEntry& entry : compact.entries_) {
          result.values_.push_back(entry.field_);
          if (operation.with_values_) result.values_.push_back(entry.value_);
        }
        return absl::OkStatus();
      }
      auto ranks = SampleUniqueRandomRanks(compact.entries_.size(), *requested,
                                           true, RandomSampleGenerator());
      for (std::uint64_t rank : ranks) {
        result.values_.push_back(compact.entries_[rank].field_);
        if (operation.with_values_)
          result.values_.push_back(compact.entries_[rank].value_);
      }
      return absl::OkStatus();
    };

    switch (operation.kind_) {
      case HashOperationKind::kReplaceOnly: {
        // Sort request positions, not owned values: duplicate fields use the
        // last argument, in O(n log n) without reading any old Hash payload.
        std::vector<std::size_t> order(operation.fields_.size());
        for (std::size_t i = 0; i < order.size(); ++i) order[i] = i;
        std::sort(order.begin(), order.end(), [&](auto a, auto b) {
          return operation.fields_[a] == operation.fields_[b]
                     ? a < b
                     : operation.fields_[a] < operation.fields_[b];
        });
        compact.entries_.reserve(order.size());
        for (std::size_t i = 0; i < order.size(); ++i) {
          const auto position = order[i];
          const auto field = operation.fields_[position];
          if (i + 1 < order.size() && field == operation.fields_[order[i + 1]])
            continue;
          compact.entries_.push_back(
              HashEntry{.digest_ = ComputeDigest(field),
                        .field_ = std::string(field),
                        .value_ = std::string(operation.values_[position])});
        }
        // Identical replacements still write and invalidate WATCH. Comparing
        // old values would defeat the explicit read-free replacement contract.
        result.changed_ = true;
        break;
      }
      case HashOperationKind::kSet:
      case HashOperationKind::kSetIfAbsent: {
        if (operation.fields_.size() != operation.values_.size())
          co_return absl::InvalidArgumentError("Hash field/value mismatch");
        for (std::size_t i = 0; i < operation.fields_.size(); ++i) {
          if (unlocked_create && i != 0 && i % 256 == 0)
            co_await bycorf::Yield(*store.worker_);
          if (operation.fields_[i].size() > kMaxStringBytes ||
              operation.values_[i].size() > kMaxStringBytes) {
            co_return absl::OutOfRangeError(
                "Hash field or value exceeds Redis-compatible 512 MiB limit");
          }
          HashEntry* current = lookup(operation.fields_[i]);
          if (current != nullptr) {
            if (operation.kind_ == HashOperationKind::kSetIfAbsent) continue;
            if (current->value_ != operation.values_[i]) {
              mark_group_changed(operation.fields_[i]);
              current->value_ = operation.values_[i];
              result.changed_ = true;
            }
          } else {
            mark_group_changed(operation.fields_[i]);
            compact.entries_.push_back(
                HashEntry{.digest_ = ComputeDigest(operation.fields_[i]),
                          .field_ = std::string(operation.fields_[i]),
                          .value_ = std::string(operation.values_[i])});
            ++result.integer_;
            result.changed_ = true;
          }
        }
        break;
      }
      case HashOperationKind::kDelete:
        for (std::string_view field : operation.fields_) {
          auto entry = find_entry(field);
          if (entry != compact.entries_.end()) {
            mark_group_changed(field);
            compact.entries_.erase(entry);
            ++result.integer_;
            result.changed_ = true;
          }
        }
        break;
      case HashOperationKind::kIncrementInteger:
      case HashOperationKind::kIncrementFloat: {
        if (operation.fields_.size() != 1 || operation.values_.size() != 1)
          co_return absl::InvalidArgumentError(
              "invalid Hash increment operands");
        const std::string_view field = operation.fields_.front();
        HashEntry* current = lookup(field);
        auto updated = incremented_value(
            current == nullptr
                ? std::optional<std::string_view>{}
                : std::optional<std::string_view>{current->value_});
        if (!updated.ok()) co_return updated.status();
        mark_group_changed(field);
        if (current == nullptr) {
          compact.entries_.push_back(HashEntry{.digest_ = ComputeDigest(field),
                                               .field_ = std::string(field),
                                               .value_ = std::move(*updated)});
        } else {
          current->value_ = std::move(*updated);
        }
        result.changed_ = true;
        break;
      }
      case HashOperationKind::kGet:
      case HashOperationKind::kGetMany:
        for (std::string_view field : operation.fields_) {
          HashEntry* entry = lookup(field);
          result.values_.push_back(
              entry == nullptr ? std::optional<std::string>{}
                               : std::optional<std::string>{entry->value_});
        }
        co_return result;
      case HashOperationKind::kExists:
        result.integer_ = !operation.fields_.empty() &&
                          lookup(operation.fields_.front()) != nullptr;
        co_return result;
      case HashOperationKind::kStringLength: {
        HashEntry* entry = operation.fields_.empty()
                               ? nullptr
                               : lookup(operation.fields_.front());
        result.integer_ = entry == nullptr ? 0 : entry->value_.size();
        co_return result;
      }
      case HashOperationKind::kGetAll:
      case HashOperationKind::kKeys:
      case HashOperationKind::kValues: {
        // These results can outlive this command-local decoded snapshot. Move
        // its strings into the result at this terminal read-only return, never
        // borrow their storage. Admission must cover the transferred capacity,
        // not just the size that a freshly copied string would allocate.
        // Grouped scratch remains reserved through the ownership handoff.
        const auto width =
            operation.kind_ == HashOperationKind::kGetAll ? 2 : 1;
        std::size_t retained = sizeof(HashResult);
        auto add_bytes = [&](std::size_t bytes) {
          if (bytes > std::numeric_limits<std::size_t>::max() - retained)
            return false;
          retained += bytes;
          return true;
        };
        for (const auto& entry : compact.entries_) {
          if (!add_bytes(width * sizeof(std::optional<std::string>)) ||
              (operation.kind_ != HashOperationKind::kValues &&
               !add_bytes(entry.field_.capacity() + 1)) ||
              (operation.kind_ != HashOperationKind::kKeys &&
               !add_bytes(entry.value_.capacity() + 1))) {
            RecordMemoryRejection();
            co_return absl::ResourceExhaustedError(
                "OOM Hash output is too large");
          }
        }
        std::optional<MemoryReservation> compact_output;
        auto* scratch = grouped_scratch ? &*grouped_scratch : nullptr;
        if (scratch == nullptr) {
          compact_output = TryReserveMemory(retained);
          if (!compact_output) {
            RecordMemoryRejection();
            co_return absl::ResourceExhaustedError("OOM Hash output retention");
          }
          scratch = &*compact_output;
        }
        if (retained > scratch->bytes()) {
          RecordMemoryRejection();
          co_return absl::ResourceExhaustedError("OOM Hash output retention");
        }
        try {
          result.values_.reserve(compact.entries_.size() * width);
          for (HashEntry& entry : compact.entries_) {
            if (operation.kind_ != HashOperationKind::kValues)
              result.values_.emplace_back(std::move(entry.field_));
            if (operation.kind_ != HashOperationKind::kKeys)
              result.values_.emplace_back(std::move(entry.value_));
          }
        } catch (const std::bad_alloc&) {
          RecordMemoryRejection();
          co_return absl::ResourceExhaustedError("OOM Hash output retention");
        }
        retained = sizeof(HashResult) +
                   result.values_.capacity() * sizeof(result.values_[0]);
        for (const auto& value : result.values_)
          retained += value->capacity() + 1;
        if (retained > scratch->bytes()) {
          RecordMemoryRejection();
          co_return absl::ResourceExhaustedError("OOM Hash output retention");
        }
        result.retained_charge_.Adopt(scratch, retained);
        co_return result;
      }
      case HashOperationKind::kRandomFields: {
        absl::Status sampled = append_random();
        if (!sampled.ok()) co_return sampled;
        co_return result;
      }
      case HashOperationKind::kPopRandom: {
        auto requested = requested_samples();
        if (!requested.ok()) co_return requested.status();
        auto ranks =
            SampleUniqueRandomRanks(compact.entries_.size(), *requested, false,
                                    RandomSampleGenerator());
        std::sort(ranks.begin(), ranks.end());
        for (std::uint64_t rank : ranks) {
          mark_group_changed(compact.entries_[rank].field_);
          result.values_.push_back(compact.entries_[rank].field_);
        }
        for (auto it = ranks.rbegin(); it != ranks.rend(); ++it)
          compact.entries_.erase(compact.entries_.begin() + *it);
        result.integer_ = ranks.size();
        result.changed_ = !ranks.empty();
        break;
      }
      case HashOperationKind::kScan: {
        // HSCAN cursors encode a digest prefix, so only this path requires
        // digest order. Full reads preserve the stored order and avoid sorting.
        std::sort(compact.entries_.begin(), compact.entries_.end(), EntryLess);
        const auto begin_it = std::lower_bound(
            compact.entries_.begin(), compact.entries_.end(), operation.cursor_,
            [](const HashEntry& entry, std::uint64_t cursor) {
              return ScanCursorPrefix(entry.digest_) < cursor;
            });
        if (begin_it == compact.entries_.end()) {
          result.cursor_ = 0;
          co_return result;
        }
        const std::size_t begin = begin_it - compact.entries_.begin();
        const std::size_t examined =
            static_cast<std::size_t>(std::min<std::uint64_t>(
                operation.scan_count_, compact.entries_.size() - begin));
        std::size_t end = begin + examined;
        while (end < compact.entries_.size() &&
               ScanCursorPrefix(compact.entries_[end].digest_) ==
                   ScanCursorPrefix(compact.entries_[end - 1].digest_)) {
          ++end;
        }
        for (std::size_t i = begin; i < end; ++i) {
          if (operation.match_ == "*" ||
              keylane::RedisGlobMatch(operation.match_,
                                      compact.entries_[i].field_)) {
            result.values_.push_back(compact.entries_[i].field_);
            if (value_type == ValueType::kHash)
              result.values_.push_back(compact.entries_[i].value_);
          }
        }
        result.cursor_ = end == compact.entries_.size()
                             ? 0
                             : ScanCursorPrefix(compact.entries_[end].digest_);
        co_return result;
      }
      case HashOperationKind::kLength:
        co_return result;
    }

    result.length_ = grouped == nullptr
                         ? compact.entries_.size()
                         : location.logical_size_ - selected_field_count +
                               compact.entries_.size();
    result.key_exists_ = result.length_ != 0;
    std::optional<HashGroupMutationPlan> grouped_plan;
    if (unlocked_grouped_write) {
      if (result.changed_ && result.key_exists_) {
        const std::vector<HashGroupId> changed(changed_groups.begin(),
                                               changed_groups.end());
        auto prepared =
            PrepareGroupedHashMutation(grouped, std::move(compact), changed,
                                       result.length_, grouped->revision());
        if (!prepared.ok()) co_return prepared.status();
        grouped_plan.emplace(std::move(*prepared));
      }
      co_await store.store_state_mutex_.Lock();
      unlock.Adopt();
      const auto valid = ValidateGroupedWriteSnapshot(
          store, partition, db_id, key, grouped, observed_write,
          mutation_precondition != nullptr
              ? mutation_precondition
              : (tx != nullptr ? &tx->mutation_precondition_ : nullptr));
      if (!valid.ok()) co_return valid;
    }
    std::optional<std::string> prepared_compact_payload;
    bool compact_write_promotes = false;
    if (unlocked_compact_write || unlocked_create) {
      if (result.changed_ && result.key_exists_) {
        compact_write_promotes = NeedsGroupedHash(compact);
        if (unlocked_create && compact_write_promotes) {
          auto prepared = PrepareGroupedHashMutation(
              nullptr, std::move(compact), {}, result.length_, 1);
          if (!prepared.ok()) co_return prepared.status();
          grouped_plan.emplace(std::move(*prepared));
        } else if (!compact_write_promotes) {
          auto encoded = EncodeHashValue(compact);
          if (!encoded.ok()) co_return encoded.status();
          prepared_compact_payload.emplace(std::move(*encoded));
        }
      }

      KEYLANE_FAULT_INJECT(if (unlocked_create) {
        const auto paused =
            co_await PauseGroupedWriteForTest(*store.worker_, key, "create");
        if (!paused.ok()) co_return paused;
      });
      co_await store.store_state_mutex_.Lock();
      unlock.Adopt();
      absl::Status valid;
      if (unlocked_create) {
        // External keys remain digest candidates even after verification;
        // re-resolve them rather than rejecting a valid expired predecessor.
        auto* current = index.Find(digest, key);
        if (current != nullptr && !current->key_complete()) {
          auto verified = co_await FindVerifiedEntry(store, index, digest, key);
          if (!verified.ok()) co_return verified.status();
          current = *verified;
        }
        valid = ValidateCollectionCreateSnapshot(
            store, partition, db_id, current, now_ms, observed_write,
            mutation_precondition);
      } else {
        valid = ValidateCompactWriteSnapshot(store, partition, db_id, key,
                                             digest, location, observed_write);
      }
      if (!valid.ok()) co_return valid;
    }
    if (!result.changed_) {
      if (value_type == ValueType::kHash &&
          operation.kind_ == HashOperationKind::kSet &&
          !operation.fields_.empty()) {
        const MutationPrecondition* effective_precondition =
            mutation_precondition != nullptr
                ? mutation_precondition
                : (tx != nullptr ? &tx->mutation_precondition_ : nullptr);
        if (effective_precondition != nullptr) {
          absl::Status valid = effective_precondition->Validate();
          if (!valid.ok()) co_return valid;
        }
        tx::CurrentTxShard().MarkWatched(db_id, tx::FingerprintOf(digest));
      }
      co_return result;
    }

    if (replication != nullptr && value_type == ValueType::kSet &&
        operation.kind_ == HashOperationKind::kPopRandom) {
      replication->args_.clear();
      replication->args_.reserve(result.values_.size() + 2);
      replication->args_.emplace_back("SREM");
      replication->args_.emplace_back(key);
      for (const std::optional<std::string>& member : result.values_) {
        if (member.has_value()) replication->args_.push_back(*member);
      }
    } else if (replication != nullptr && value_type == ValueType::kHash &&
               (operation.kind_ == HashOperationKind::kIncrementInteger ||
                operation.kind_ == HashOperationKind::kIncrementFloat)) {
      replication->args_ = {
          "HSET", std::string(key), std::string(operation.fields_.front()),
          operation.kind_ == HashOperationKind::kIncrementInteger
              ? std::to_string(result.signed_integer_)
              : result.scalar_};
    }
    if (result.key_exists_ &&
        (grouped != nullptr || ((unlocked_compact_write || unlocked_create)
                                    ? compact_write_promotes
                                    : NeedsGroupedHash(compact)))) {
      absl::Status written = co_await CommitGroupedHashMutationLocked(
          store, partition, db_id, key, digest, grouped, std::move(compact),
          std::vector<HashGroupId>(changed_groups.begin(),
                                   changed_groups.end()),
          result.length_, value_type, expire_at_ms, tx, replication,
          mutation_precondition, grouped_plan ? &*grouped_plan : nullptr);
      if (!written.ok()) co_return written;
      co_return result;
    }
    RecordKind kind = RecordKind::kValue;
    ValueType published_type = value_type;
    std::string payload;
    if (!result.key_exists_) {
      kind = RecordKind::kTombstone;
      published_type = ValueType::kNone;
    } else if (prepared_compact_payload.has_value()) {
      payload = std::move(*prepared_compact_payload);
    } else {
      auto encoded = EncodeHashValue(compact);
      if (!encoded.ok()) co_return encoded.status();
      payload = std::move(*encoded);
    }
    absl::Status written = co_await AppendLocked(
        store, partition, db_id, key, digest, payload, kind, published_type,
        kind == RecordKind::kValue ? expire_at_ms : 0, tx,
        kind == RecordKind::kValue ? compact.entries_.size() : 0, nullptr,
        nullptr, replication, nullptr, true, nullptr, mutation_precondition);
    if (!written.ok()) co_return written;
    co_return result;
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM preparing Hash/Set operation");
  }
}

}  // namespace keylane::storage
