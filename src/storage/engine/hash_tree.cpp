#include <charconv>
#include <cmath>
#include <random>

#include "impl.h"
#include "keylane/glob.h"
#include "keylane/memory.h"
#include "keylane/random_sample.h"
#include "keylane/redis_parse.h"

namespace keylane::storage {

namespace {

bool DigestLess(const Digest& left, const Digest& right) {
  return std::lexicographical_compare(left.bytes_.begin(), left.bytes_.end(),
                                      right.bytes_.begin(), right.bytes_.end());
}

bool EntryLess(const HashEntry& left, const HashEntry& right) {
  if (left.digest_ != right.digest_)
    return DigestLess(left.digest_, right.digest_);
  return left.field_ < right.field_;
}

bool IsWrite(const HashOperation& operation) {
  return operation.kind_ == HashOperationKind::kSet ||
         operation.kind_ == HashOperationKind::kSetIfAbsent ||
         operation.kind_ == HashOperationKind::kDelete ||
         operation.kind_ == HashOperationKind::kPopRandom ||
         operation.kind_ == HashOperationKind::kIncrementInteger ||
         operation.kind_ == HashOperationKind::kIncrementFloat;
}

}  // namespace

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHashLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication) {
  co_return co_await ExecuteHashLikeLocked(db_id, key, digest, operation,
                                           ValueType::kHash, tx, replication);
}

Task<absl::StatusOr<HashResult>> StorageEngine::Impl::ExecuteHashLikeLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const HashOperation& operation, ValueType value_type, TxShardWrites* tx,
    ReplicationCommandAppend* replication) {
  assert(db_id < kLogicalDatabaseCount);
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
      found != nullptr && found->value_.kind_ == RecordKind::kValue;
  const std::uint64_t now_ms =
      operation.now_ms_ == 0 ? UnixTimeMillis() : operation.now_ms_;
  const bool exists = stored_value && !IsExpired(found->value_, now_ms);
  if (exists && found->value_.value_type_ != value_type) {
    co_return absl::InvalidArgumentError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  const bool read_only = !IsWrite(operation);
  const std::uint64_t observed_index_generation =
      store.index_generations_[db_id];
  const std::uint64_t observed_db_epoch = DbEpoch(db_id);
  const std::uint64_t observed_replication_epoch =
      partition.replication_epoch_;
  auto read_epoch_changed = [&]() {
    return read_only &&
           (store.index_generations_[db_id] != observed_index_generation ||
            DbEpoch(db_id) != observed_db_epoch ||
            partition.replication_epoch_ != observed_replication_epoch);
  };
  const RecordLocation location = exists ? found->value_ : RecordLocation{};
  const ExtentManifest extents = exists ? ExtentsFor(store, found)
                                         : ExtentManifest{};
  const std::uint64_t expire_at_ms = exists ? location.expire_at_ms_ : 0;

  HashResult result;
  result.key_exists_ = exists;
  result.length_ = exists ? location.logical_size_ : 0;

  // HLEN/SCARD are carried in the record header. Loading and decoding the
  // complete monolithic value here turns an O(1) metadata lookup into an
  // O(value-size) disk read and allocation.
  if (operation.kind_ == HashOperationKind::kLength) co_return result;

  if (read_only) unlock.Unlock();
#ifndef NDEBUG
  if (read_only) {
    if (const char* configured = std::getenv("KEYLANE_HASH_READ_PAUSE_MS");
        configured != nullptr) {
      std::uint64_t milliseconds = 0;
      const char* end = configured + std::strlen(configured);
      const auto parsed = std::from_chars(configured, end, milliseconds);
      if (parsed.ec == std::errc{} && parsed.ptr == end && milliseconds != 0) {
        absl::Status paused = co_await celer::SleepFor(
            *store.worker_, std::chrono::milliseconds(milliseconds));
        if (!paused.ok()) co_return paused;
      }
    }
  }
#endif

  HashValue compact;
  if (exists) {
    auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                     location, extents);
    if (!loaded.ok()) {
      if (read_epoch_changed()) co_return HashResult{};
      co_return loaded.status();
    }
    const auto bytes = loaded->value();
    auto decoded = DecodeHashValue(std::string_view(
        reinterpret_cast<const char*>(bytes.data()), bytes.size()));
    if (!decoded.ok()) co_return decoded.status();
    compact = std::move(*decoded);
    if (compact.entries_.size() != location.logical_size_) {
      co_return absl::InternalError(
          "Hash element count does not match record metadata");
    }
  }

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
      const auto parsed =
          std::from_chars(delta.data(), delta.data() + delta.size(), increment);
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
    if (tx != nullptr && operation.kind_ == HashOperationKind::kRandomFields) {
      const std::uint64_t slots =
          operation.with_values_ &&
                  requested <= std::numeric_limits<std::uint64_t>::max() / 2
              ? requested * 2
              : requested;
      constexpr std::size_t kMinimumResultSlotBytes =
          sizeof(std::optional<std::string>);
      const std::size_t growth =
          slots > std::numeric_limits<std::size_t>::max() /
                      kMinimumResultSlotBytes
              ? std::numeric_limits<std::size_t>::max()
              : static_cast<std::size_t>(slots) * kMinimumResultSlotBytes;
      if (WouldExceedMemoryLimit(growth)) {
        RecordMemoryRejection();
        return absl::ResourceExhaustedError(
            "transactional random reply exceeds maxmemory");
      }
    }
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
    case HashOperationKind::kSet:
    case HashOperationKind::kSetIfAbsent: {
      if (operation.fields_.size() != operation.values_.size())
        co_return absl::InvalidArgumentError("Hash field/value mismatch");
      for (std::size_t i = 0; i < operation.fields_.size(); ++i) {
        if (operation.fields_[i].size() > kMaxStringBytes ||
            operation.values_[i].size() > kMaxStringBytes) {
          co_return absl::OutOfRangeError(
              "Hash field or value exceeds Redis-compatible 512 MiB limit");
        }
        HashEntry* current = lookup(operation.fields_[i]);
        if (current != nullptr) {
          if (operation.kind_ == HashOperationKind::kSetIfAbsent) continue;
          if (current->value_ != operation.values_[i]) {
            current->value_ = operation.values_[i];
            result.changed_ = true;
          }
        } else {
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
          compact.entries_.erase(entry);
          ++result.integer_;
          result.changed_ = true;
        }
      }
      break;
    case HashOperationKind::kIncrementInteger:
    case HashOperationKind::kIncrementFloat: {
      if (operation.fields_.size() != 1 || operation.values_.size() != 1)
        co_return absl::InvalidArgumentError("invalid Hash increment operands");
      const std::string_view field = operation.fields_.front();
      HashEntry* current = lookup(field);
      auto updated = incremented_value(
          current == nullptr
              ? std::optional<std::string_view>{}
              : std::optional<std::string_view>{current->value_});
      if (!updated.ok()) co_return updated.status();
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
    case HashOperationKind::kValues:
      for (const HashEntry& entry : compact.entries_) {
        if (operation.kind_ != HashOperationKind::kValues)
          result.values_.push_back(entry.field_);
        if (operation.kind_ != HashOperationKind::kKeys)
          result.values_.push_back(entry.value_);
      }
      co_return result;
    case HashOperationKind::kRandomFields: {
      absl::Status sampled = append_random();
      if (!sampled.ok()) co_return sampled;
      co_return result;
    }
    case HashOperationKind::kPopRandom: {
      auto requested = requested_samples();
      if (!requested.ok()) co_return requested.status();
      auto ranks = SampleUniqueRandomRanks(compact.entries_.size(), *requested,
                                           false, RandomSampleGenerator());
      std::sort(ranks.begin(), ranks.end());
      for (std::uint64_t rank : ranks)
        result.values_.push_back(compact.entries_[rank].field_);
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
      const std::size_t examined = static_cast<std::size_t>(
          std::min<std::uint64_t>(operation.scan_count_,
                                  compact.entries_.size() - begin));
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

  result.length_ = compact.entries_.size();
  result.key_exists_ = !compact.entries_.empty();
  if (!result.changed_) {
    if (value_type == ValueType::kHash &&
        operation.kind_ == HashOperationKind::kSet &&
        !operation.fields_.empty()) {
      tx::CurrentTxShard().MarkWatched(db_id, tx::FingerprintOf(digest));
    }
    co_return result;
  }

  RecordKind kind = RecordKind::kValue;
  ValueType published_type = value_type;
  std::string payload;
  if (compact.entries_.empty()) {
    kind = RecordKind::kTombstone;
    published_type = ValueType::kNone;
  } else {
    auto encoded = EncodeHashValue(compact);
    if (!encoded.ok()) co_return encoded.status();
    payload = std::move(*encoded);
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
  absl::Status written = co_await AppendLocked(
      store, partition, db_id, key, payload, kind, published_type,
      kind == RecordKind::kValue ? expire_at_ms : 0, tx,
      kind == RecordKind::kValue ? compact.entries_.size() : 0, nullptr,
      nullptr, replication);
  if (!written.ok()) co_return written;
  co_return result;
}

}  // namespace keylane::storage
