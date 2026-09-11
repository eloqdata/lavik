#include "impl.h"
#include "keylane/storage/detail/grouped_scratch.h"

namespace keylane::storage {

namespace {

constexpr std::string_view kListMagic = "KLL1";
constexpr std::size_t kListHeaderBytes = 8;

void AppendU32(std::string* output, std::uint32_t value) {
  output->push_back(static_cast<char>(value));
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value >> 16));
  output->push_back(static_cast<char>(value >> 24));
}

bool ReadU32(std::string_view input, std::size_t* offset,
             std::uint32_t* value) {
  if (*offset > input.size() || input.size() - *offset < sizeof(*value)) {
    return false;
  }
  const auto* bytes =
      reinterpret_cast<const unsigned char*>(input.data()) + *offset;
  *value = static_cast<std::uint32_t>(bytes[0]) |
           (static_cast<std::uint32_t>(bytes[1]) << 8) |
           (static_cast<std::uint32_t>(bytes[2]) << 16) |
           (static_cast<std::uint32_t>(bytes[3]) << 24);
  *offset += sizeof(*value);
  return true;
}

absl::StatusOr<std::vector<std::string>> DecodeList(
    std::string_view encoded, std::uint64_t expected_count) {
  if (expected_count > std::numeric_limits<std::uint32_t>::max() ||
      encoded.size() < kListHeaderBytes ||
      encoded.substr(0, kListMagic.size()) != kListMagic) {
    return absl::InternalError("invalid persisted List");
  }
  std::size_t offset = kListMagic.size();
  std::uint32_t count = 0;
  if (!ReadU32(encoded, &offset, &count) || count != expected_count) {
    return absl::InternalError("List count does not match record metadata");
  }
  if (count > (encoded.size() - offset) / sizeof(std::uint32_t)) {
    return absl::InternalError("List count exceeds its payload");
  }
  std::vector<std::string> elements;
  elements.reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    std::uint32_t bytes = 0;
    if (!ReadU32(encoded, &offset, &bytes) || offset > encoded.size() ||
        bytes > encoded.size() - offset) {
      return absl::InternalError("persisted List is truncated");
    }
    elements.emplace_back(encoded.substr(offset, bytes));
    offset += bytes;
  }
  if (offset != encoded.size()) {
    return absl::InternalError("persisted List has trailing bytes");
  }
  return elements;
}

absl::StatusOr<std::string> EncodeList(std::span<const std::string> elements) {
  if (elements.empty() ||
      elements.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::OutOfRangeError("invalid List element count");
  }
  std::uint64_t bytes = kListHeaderBytes;
  for (const std::string& element : elements) {
    if (element.size() > kMaxStringBytes ||
        bytes > kMaxStringBytes - sizeof(std::uint32_t) ||
        element.size() > kMaxStringBytes - bytes - sizeof(std::uint32_t)) {
      return absl::OutOfRangeError("List exceeds storage limits");
    }
    bytes += sizeof(std::uint32_t) + element.size();
  }
  std::string output;
  output.reserve(static_cast<std::size_t>(bytes));
  output.append(kListMagic);
  AppendU32(&output, static_cast<std::uint32_t>(elements.size()));
  for (const std::string& element : elements) {
    AppendU32(&output, static_cast<std::uint32_t>(element.size()));
    output.append(element);
  }
  return output;
}

std::optional<std::uint64_t> NormalizeIndex(std::int64_t index,
                                            std::uint64_t size) {
  if (index < 0) {
    const std::uint64_t magnitude =
        index == std::numeric_limits<std::int64_t>::min()
            ? std::uint64_t{1} << 63
            : static_cast<std::uint64_t>(-index);
    if (magnitude > size) return std::nullopt;
    return size - magnitude;
  }
  const std::uint64_t converted = static_cast<std::uint64_t>(index);
  return converted < size ? std::optional(converted) : std::nullopt;
}

std::pair<std::uint64_t, std::uint64_t> NormalizeRange(std::int64_t start,
                                                       std::int64_t stop,
                                                       std::uint64_t size) {
  if (size == 0) return {0, 0};
  auto position = [size](std::int64_t value) -> std::int64_t {
    if (value >= 0) return value;
    if (value == std::numeric_limits<std::int64_t>::min()) return value;
    if (size >
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
      return value;
    }
    return static_cast<std::int64_t>(size) + value;
  };
  std::int64_t first = position(start);
  std::int64_t last = position(stop);
  if (first < 0) first = 0;
  if (last < 0 || static_cast<std::uint64_t>(first) >= size || first > last) {
    return {size, size};
  }
  const std::uint64_t begin = static_cast<std::uint64_t>(first);
  const std::uint64_t end = static_cast<std::uint64_t>(last) >= size - 1
                                ? size
                                : static_cast<std::uint64_t>(last) + 1;
  return begin < end ? std::pair(begin, end) : std::pair(size, size);
}

std::uint64_t UnsignedMagnitude(std::int64_t value) {
  return value < 0 ? static_cast<std::uint64_t>(-(value + 1)) + 1
                   : static_cast<std::uint64_t>(value);
}

struct ListRemovePlan {
  bool forward_ = true;
  std::uint64_t limit_ = 0;
};

ListRemovePlan NormalizeListRemoveLimit(std::int64_t count) {
  return ListRemovePlan{
      .forward_ = count >= 0,
      .limit_ = count == 0 ? std::numeric_limits<std::uint64_t>::max()
                           : UnsignedMagnitude(count),
  };
}

struct ListPositionPlan {
  bool reverse_ = false;
  std::uint64_t wanted_rank_ = 0;
  std::uint64_t return_limit_ = 0;
  std::uint64_t comparison_limit_ = 0;
};

ListPositionPlan NormalizeListPosition(const ListOperation& operation) {
  return ListPositionPlan{
      .reverse_ = operation.rank_ < 0,
      .wanted_rank_ = UnsignedMagnitude(operation.rank_),
      .return_limit_ = !operation.count_provided_
                           ? 1
                           : (operation.count_ == 0
                                  ? std::numeric_limits<std::uint64_t>::max()
                                  : operation.count_),
      .comparison_limit_ =
          operation.max_length_provided_ && operation.max_length_ != 0
              ? operation.max_length_
              : std::numeric_limits<std::uint64_t>::max(),
  };
}

bool IsReadOnly(const ListOperation& operation) {
  return operation.kind_ == ListOperationKind::kLength ||
         operation.kind_ == ListOperationKind::kIndex ||
         operation.kind_ == ListOperationKind::kRange ||
         operation.kind_ == ListOperationKind::kPosition;
}

bool NeedsGroupedList(std::span<const std::string> elements) {
  std::size_t bytes = kListHeaderBytes;
  for (const auto& element : elements) {
    bytes += sizeof(std::uint32_t) + element.size();
    if (bytes >= kGroupedHashPromotionBytes) return true;
  }
  return false;
}

absl::StatusOr<OrderedCollectionMutationPlan> PrepareListGroups(
    std::vector<std::string> elements) {
  // These are private placeholder identities. Publication assigns the real
  // incarnation after admitting a unique durable command batch.
  const auto count = elements.size();
  OrderedGroupSnapshot initial{.kind_ = OrderedCollectionKind::kList,
                               .incarnation_ = 1,
                               .id_ = 1,
                               .entries_ = {}};
  initial.entries_.reserve(count);
  for (auto& element : elements)
    initial.entries_.push_back({.value_ = std::move(element)});
  auto split = SplitOrderedGroup(std::move(initial), 2);
  if (!split.ok()) return split.status();
  return OrderedCollectionMutationPlan{
      .root_ = {.kind_ = OrderedCollectionKind::kList,
                .incarnation_ = 1,
                .item_count_ = count,
                .first_group_ = split->groups_.front().id_,
                .last_group_ = split->groups_.back().id_,
                .next_group_id_ = split->next_group_id_,
                .group_count_ =
                    static_cast<std::uint32_t>(split->groups_.size())},
      .changed_ = true,
      .writes_ = std::move(split->groups_)};
}

}  // namespace

Task<absl::StatusOr<ListResult>> StorageEngine::Impl::ExecuteListLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const ListOperation& operation, TxShardWrites* tx,
    ReplicationCommandAppend* replication,
    const MutationPrecondition* mutation_precondition) {
  // Handle private allocation failures inside this coroutine, before they
  // reach Celer's fail-fast unhandled_exception boundary.
  try {
    assert(db_id < kLogicalDatabaseCount);
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
    // Non-expiring existing Lists keep the metadata-only clock-free path.
    // Creation/expiry needs one fixed observation time for later validation.
    const auto now_ms =
        stored_value && !found->value_.has_expiry() ? 0 : UnixTimeMillis();
    const bool exists = stored_value && !IsExpired(*found, now_ms);
    if (!exists && stored_value && found->value_.grouped()) {
      auto metadata = co_await ReadKeyMetadataLocked(db_id, key, digest);
      if (!metadata.ok()) co_return metadata.status();
    }
    if (exists && found->value_.value_type() != ValueType::kList) {
      co_return absl::InvalidArgumentError(
          "WRONGTYPE Operation against a key holding the wrong kind of value");
    }
    const bool read_only = IsReadOnly(operation);
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
    const RecordLocation location =
        exists ? MaterializeIndexLocation(*found) : RecordLocation{};
    const ExtentManifest extents =
        exists ? ExtentsFor(store, found) : ExtentManifest{};
    const std::uint64_t expire_at_ms = exists ? location.expire_at_ms_ : 0;

    ListResult result;
    result.key_exists_ = exists;
    result.length_ = exists ? location.logical_size_ : 0;

    if (exists && location.grouped()) {
      auto object = partition.grouped_objects_[db_id].Lookup(
          key, GroupedObjectVersion{
                   .root_ = location,
                   .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
                   .replication_epoch_ = observed_replication_epoch,
                   .index_generation_ = partition.grouped_generations_[db_id]});
      if (!object.ok()) co_return object.status();
      if (operation.kind_ == ListOperationKind::kLength) co_return result;
      if (!read_only && CanPrepareGroupedWriteUnlocked(partition)) {
        try {
          const auto snapshot =
              CaptureCompactWriteSnapshot(store, partition, db_id);
          PreparedOrderedMutation mutation;
          auto& plan = mutation.plan_;
          found = nullptr;
          unlock.Unlock();
          KEYLANE_FAULT_INJECT({
            const auto paused = co_await PauseGroupedWriteForTest(
                *store.worker_, key, "prepare");
            if (!paused.ok()) co_return paused;
          });
          auto prepared = co_await ExecuteGroupedListLocked(
              store, partition, db_id, key, digest, operation, *object, tx,
              replication, mutation_precondition, &mutation);
          if (!prepared.ok()) co_return prepared.status();
          co_await store.store_state_mutex_.Lock();
          unlock.Adopt();
          const auto valid = ValidateGroupedWriteSnapshot(
              store, partition, db_id, key, *object, snapshot,
              mutation_precondition != nullptr
                  ? mutation_precondition
                  : (tx != nullptr ? &tx->mutation_precondition_ : nullptr));
          if (!valid.ok()) co_return valid;
          if (plan.changed_) {
            const auto committed = co_await CommitGroupedOrderedMutationLocked(
                store, partition, db_id, key, digest, *object, std::move(plan),
                expire_at_ms, tx, replication, mutation_precondition);
            if (!committed.ok()) co_return committed;
          }
          co_return prepared;
        } catch (const std::bad_alloc&) {
          co_return absl::ResourceExhaustedError(
              "OOM allocating grouped List operation");
        }
      }
      co_return co_await ExecuteGroupedListLocked(
          store, partition, db_id, key, digest, operation, std::move(*object),
          tx, replication, mutation_precondition);
    }

    if (operation.kind_ == ListOperationKind::kLength) co_return result;
    // The explicit single-key opt-in excludes multi-key pop/move callers that
    // legitimately pass no durable transaction. Keep the caller's exclusive key
    // intent while preparing private bytes; only worker-wide store state
    // yields.
    const bool unlocked_compact_write =
        !read_only && operation.prepare_unlocked_ && exists &&
        CanPrepareCompactWriteUnlocked(store, partition, found, location, tx);
    const bool unlocked_create =
        !exists && operation.prepare_unlocked_ &&
        (operation.kind_ == ListOperationKind::kPushLeft ||
         operation.kind_ == ListOperationKind::kPushRight) &&
        CanPrepareCollectionCreateUnlocked(partition, tx);
    const CompactWriteSnapshot write_snapshot =
        (unlocked_compact_write || unlocked_create)
            ? CaptureCompactWriteSnapshot(store, partition, db_id)
            : CompactWriteSnapshot{};
    if (read_only || unlocked_compact_write || unlocked_create) {
      found = nullptr;
      unlock.Unlock();
    }
    KEYLANE_FAULT_INJECT(if (unlocked_create) {
      KEYLANE_FAULT_BAD_ALLOC("KEYLANE_FAIL_COLLECTION_CREATE_PREPARE_KEY",
                              key);
    });
    KEYLANE_FAULT_INJECT(if (unlocked_compact_write) {
      auto paused = co_await PauseCompactWriteForTest(*store.worker_, key);
      if (!paused.ok()) co_return paused;
    });
    KEYLANE_FAULT_INJECT(if (read_only) {
      if (const char* configured = std::getenv("KEYLANE_LIST_READ_PAUSE_MS");
          configured != nullptr) {
        std::uint64_t milliseconds = 0;
        const char* end = configured + std::strlen(configured);
        const auto parsed = std::from_chars(configured, end, milliseconds);
        if (parsed.ec == std::errc{} && parsed.ptr == end &&
            milliseconds != 0) {
          absl::Status paused = co_await celer::SleepFor(
              *store.worker_, std::chrono::milliseconds(milliseconds));
          if (!paused.ok()) co_return paused;
        }
      }
    });

    std::optional<MemoryReservation> create_admission;
    if (unlocked_create) {
      GroupedScratchBudget budget;
      for (const auto value : operation.values_) {
        const auto added = budget.AddBytes(value.size() + 256);
        if (!added.ok()) co_return added;
      }
      auto admitted = budget.Reserve(2);
      if (!admitted.ok()) co_return admitted.status();
      create_admission.emplace(std::move(*admitted));
    }
    std::vector<std::string> elements;
    if (exists) {
      auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                       location, extents);
      if (!loaded.ok()) {
        if (read_epoch_changed()) co_return ListResult{};
        co_return loaded.status();
      }
      const auto bytes = loaded->value();
      auto decoded = DecodeList(
          std::string_view(reinterpret_cast<const char*>(bytes.data()),
                           bytes.size()),
          location.logical_size_);
      if (!decoded.ok()) co_return decoded.status();
      elements = std::move(*decoded);
    }

    switch (operation.kind_) {
      case ListOperationKind::kPushLeft:
      case ListOperationKind::kPushRight:
      case ListOperationKind::kPushLeftIfExists:
      case ListOperationKind::kPushRightIfExists: {
        const bool only_if_exists =
            operation.kind_ == ListOperationKind::kPushLeftIfExists ||
            operation.kind_ == ListOperationKind::kPushRightIfExists;
        if (!exists && only_if_exists) co_return result;
        const bool left =
            operation.kind_ == ListOperationKind::kPushLeft ||
            operation.kind_ == ListOperationKind::kPushLeftIfExists;
        for (std::string_view value : operation.values_) {
          if (unlocked_create && !elements.empty() &&
              elements.size() % 256 == 0)
            co_await celer::Yield(*store.worker_);
          if (value.size() > kMaxStringBytes) {
            co_return absl::OutOfRangeError(
                "List element exceeds Redis-compatible 512 MiB limit");
          }
          if (left)
            elements.insert(elements.begin(), std::string(value));
          else
            elements.emplace_back(value);
        }
        result.changed_ = !operation.values_.empty();
        result.key_exists_ = true;
        break;
      }
      case ListOperationKind::kPopLeft:
      case ListOperationKind::kPopRight: {
        const std::size_t count = static_cast<std::size_t>(
            std::min<std::uint64_t>(operation.count_, elements.size()));
        const bool left = operation.kind_ == ListOperationKind::kPopLeft;
        for (std::size_t i = 0; i < count; ++i) {
          if (left) {
            result.values_.push_back(std::move(elements.front()));
            elements.erase(elements.begin());
          } else {
            result.values_.push_back(std::move(elements.back()));
            elements.pop_back();
          }
        }
        result.changed_ = count != 0;
        break;
      }
      case ListOperationKind::kIndex:
        if (auto position = NormalizeIndex(operation.first_, elements.size()))
          result.values_.push_back(elements[*position]);
        co_return result;
      case ListOperationKind::kRange: {
        const auto [begin, end] = NormalizeRange(
            operation.first_, operation.second_, elements.size());
        for (std::uint64_t i = begin; i < end; ++i)
          result.values_.push_back(elements[i]);
        co_return result;
      }
      case ListOperationKind::kSet: {
        if (!exists) co_return absl::NotFoundError("no such key");
        if (operation.value_.size() > kMaxStringBytes)
          co_return absl::OutOfRangeError(
              "List element exceeds storage limits");
        auto position = NormalizeIndex(operation.first_, elements.size());
        if (!position) co_return absl::OutOfRangeError("index out of range");
        elements[*position] = operation.value_;
        result.changed_ = true;
        break;
      }
      case ListOperationKind::kInsertBefore:
      case ListOperationKind::kInsertAfter: {
        if (!exists) co_return result;
        if (operation.value_.size() > kMaxStringBytes)
          co_return absl::OutOfRangeError(
              "List element exceeds storage limits");
        auto it = std::find(elements.begin(), elements.end(), operation.pivot_);
        if (it == elements.end()) {
          result.integer_ = -1;
          // A successful no-op still has to validate the captured population
          // after an unlocked read, just like a replacement or final-element
          // pop.
          break;
        }
        if (operation.kind_ == ListOperationKind::kInsertAfter) ++it;
        elements.insert(it, std::string(operation.value_));
        result.changed_ = true;
        result.integer_ = elements.size();
        break;
      }
      case ListOperationKind::kRemove: {
        const ListRemovePlan plan = NormalizeListRemoveLimit(operation.first_);
        std::uint64_t removed = 0;
        if (plan.forward_) {
          for (auto it = elements.begin();
               it != elements.end() && removed < plan.limit_;) {
            if (*it == operation.value_) {
              it = elements.erase(it);
              ++removed;
            } else {
              ++it;
            }
          }
        } else {
          for (std::size_t i = elements.size();
               i > 0 && removed < plan.limit_;) {
            --i;
            if (elements[i] == operation.value_) {
              elements.erase(elements.begin() + i);
              ++removed;
            }
          }
        }
        result.integer_ = removed;
        result.changed_ = removed != 0;
        break;
      }
      case ListOperationKind::kTrim: {
        const auto [begin, end] = NormalizeRange(
            operation.first_, operation.second_, elements.size());
        std::vector<std::string> kept;
        kept.reserve(end - begin);
        for (std::uint64_t i = begin; i < end; ++i)
          kept.push_back(std::move(elements[i]));
        result.changed_ = kept.size() != elements.size();
        elements = std::move(kept);
        break;
      }
      case ListOperationKind::kPosition: {
        const ListPositionPlan plan = NormalizeListPosition(operation);
        std::uint64_t matches = 0;
        const std::uint64_t inspect =
            std::min<std::uint64_t>(plan.comparison_limit_, elements.size());
        for (std::uint64_t step = 0;
             step < inspect && result.positions_.size() < plan.return_limit_;
             ++step) {
          const std::size_t position =
              plan.reverse_ ? elements.size() - 1 - step : step;
          if (elements[position] == operation.value_ &&
              ++matches >= plan.wanted_rank_) {
            result.positions_.push_back(position);
          }
        }
        co_return result;
      }
      case ListOperationKind::kMoveWithin: {
        if (elements.empty()) co_return result;
        const bool source_left = operation.first_ != 0;
        const bool destination_left = operation.second_ != 0;
        std::string moved = source_left ? std::move(elements.front())
                                        : std::move(elements.back());
        if (source_left)
          elements.erase(elements.begin());
        else
          elements.pop_back();
        if (destination_left)
          elements.insert(elements.begin(), moved);
        else
          elements.push_back(moved);
        result.values_.push_back(std::move(moved));
        result.changed_ =
            source_left != destination_left && elements.size() > 1;
        break;
      }
      case ListOperationKind::kLength:
        co_return result;
    }

    result.length_ = elements.size();
    std::optional<std::string> prepared_compact_payload;
    std::optional<OrderedCollectionMutationPlan> prepared_groups;
    bool compact_write_promotes = false;
    if (unlocked_compact_write || unlocked_create) {
      if (result.changed_ && !elements.empty()) {
        compact_write_promotes = NeedsGroupedList(elements);
        if (unlocked_create && compact_write_promotes) {
          auto plan = PrepareListGroups(std::move(elements));
          if (!plan.ok()) co_return plan.status();
          prepared_groups.emplace(std::move(*plan));
        } else if (!compact_write_promotes) {
          auto encoded = EncodeList(elements);
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
      absl::Status validated;
      if (unlocked_create) {
        auto* current = index.Find(digest, key);
        if (current != nullptr && !current->key_complete()) {
          auto verified = co_await FindVerifiedEntry(store, index, digest, key);
          if (!verified.ok()) co_return verified.status();
          current = *verified;
        }
        validated = ValidateCollectionCreateSnapshot(
            store, partition, db_id, current, now_ms, write_snapshot,
            mutation_precondition);
      } else {
        validated = ValidateCompactWriteSnapshot(
            store, partition, db_id, key, digest, location, write_snapshot);
      }
      if (!validated.ok()) co_return validated;
      // Same-version GC movement is permitted. AppendLocked resolves its
      // current physical predecessor; the replacement keeps the command-time
      // deadline. Existing-value promotion and empty-list deletion keep their
      // locked funnels.
    }
    if (!result.changed_) co_return result;

    if (prepared_groups ||
        (!elements.empty() &&
         (unlocked_compact_write ? compact_write_promotes
                                 : NeedsGroupedList(elements)))) {
      auto plan = prepared_groups
                      ? absl::StatusOr<OrderedCollectionMutationPlan>(
                            std::move(*prepared_groups))
                      : PrepareListGroups(std::move(elements));
      if (!plan.ok()) co_return plan.status();
      auto written = co_await CommitGroupedOrderedMutationLocked(
          store, partition, db_id, key, digest, nullptr, std::move(*plan),
          expire_at_ms, tx, replication, mutation_precondition);
      if (!written.ok()) co_return written;
      co_return result;
    }

    RecordKind kind = RecordKind::kValue;
    ValueType type = ValueType::kList;
    std::string payload;
    if (elements.empty()) {
      kind = RecordKind::kTombstone;
      type = ValueType::kNone;
    } else if (prepared_compact_payload.has_value()) {
      payload = std::move(*prepared_compact_payload);
    } else {
      auto encoded = EncodeList(elements);
      if (!encoded.ok()) co_return encoded.status();
      payload = std::move(*encoded);
    }
    absl::Status written = co_await AppendLocked(
        store, partition, db_id, key, digest, payload, kind, type,
        kind == RecordKind::kValue ? expire_at_ms : 0, tx,
        kind == RecordKind::kValue ? elements.size() : 0, nullptr, nullptr,
        replication, nullptr, true, nullptr, mutation_precondition);
    if (!written.ok()) co_return written;
    co_return result;
  } catch (const std::bad_alloc&) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError("OOM preparing List operation");
  }
}

}  // namespace keylane::storage
