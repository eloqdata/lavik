/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "impl.h"

namespace lavik::storage {

Task<absl::StatusOr<IndirectKeyHandle>> StorageEngine::Impl::FindIndirectKey(
    IndirectKeyId id) {
  const auto owner = static_cast<unsigned>((id[0] & 0x3fff) % worker_count_);
  auto find = [this, owner, id]() -> absl::StatusOr<IndirectKeyHandle> {
    const auto bytes = IndirectKeyIdBytes(id);
    // Recovery may be suspended mid-registry scan on this worker. Read-only
    // resolution must preserve its cursor; periodic maintenance drives rehash.
    const auto* entry = std::as_const(stores_[owner]->indirect_keys_)
                            .Find(ComputeDigest(bytes), bytes);
    if (entry == nullptr)
      return absl::DataLossError("record references a missing indirect key");
    return entry->value_;
  };
  if (owner == bycorf::ThisWorker().id_) co_return find();
  co_return co_await bycorf::SubmitTo(owner, std::move(find));
}

absl::Status StorageEngine::Impl::InsertIndirectKey(
    WorkerStore& store, const IndirectKeyHandle& handle) {
  const auto key = IndirectKeyIdBytes(handle->id_);
  const auto digest = ComputeDigest(key);
  auto& index = store.indirect_keys_;
  if (!index.CanAllocateEntry(key, true, false))
    return absl::ResourceExhaustedError(
        "indirect key index capacity exhausted");
  // The shared arena accounts actual bytes but relies on its caller to admit
  // allocation. No suspension may separate this check from insertion.
  auto admission = TryReserveMemory(
      index.RequiredAllocationBytes(digest, key, true, false, true));
  if (!admission) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM indirect key index admission");
  }
  if (index.InsertNew(digest, key, handle) == nullptr)
    return absl::ResourceExhaustedError("indirect key index allocation failed");
  return absl::OkStatus();
}

Task<absl::StatusOr<std::string>> StorageEngine::Impl::LoadIndirectKey(
    IndirectKeyHandle handle) {
  const auto owner =
      static_cast<unsigned>((handle->id_[0] & 0x3fff) % worker_count_);
  if (owner != bycorf::ThisWorker().id_) {
    co_return co_await bycorf::SubmitTaskTo(
        owner,
        [this,
         handle = std::move(handle)]() -> Task<absl::StatusOr<std::string>> {
          co_return co_await LoadIndirectKey(handle);
        });
  }
  WorkerStore& store = *stores_[owner];
  for (;;) {
    const auto location = handle->location_;
    const auto extents = handle->extents_;
    absl::StatusOr<LoadedValue> loaded =
        absl::InternalError("unread key record");
    if (location.external()) {
      loaded =
          co_await LoadExternalValueLocal(store, location, extents, nullptr);
    } else if (location.block_owner() == owner) {
      loaded = co_await LoadValueLocal(
          store, 0, IndirectKeyIdBytes(handle->id_), location, 1, nullptr, 1);
    } else {
      loaded = co_await bycorf::SubmitTaskTo(
          location.block_owner(),
          [this, handle, location]() -> Task<absl::StatusOr<LoadedValue>> {
            co_return co_await LoadValueLocal(
                *stores_[location.block_owner()], 0,
                IndirectKeyIdBytes(handle->id_), location, 1, nullptr, 1);
          });
    }
    if (!loaded.ok()) {
      if (loaded.status().code() == absl::StatusCode::kAborted &&
          !location.SamePhysicalRecord(handle->location_))
        continue;
      co_return loaded.status();
    }
    co_return std::string(reinterpret_cast<const char*>(loaded->value().data()),
                          loaded->value().size());
  }
}

Task<absl::Status> StorageEngine::Impl::WriteIndirectKey(
    WorkerStore& store, IndirectKeyHandle handle, std::string_view key,
    bool for_defrag, bool unlock_writer_while_waiting) {
  ExtentManifest extents = handle->extents_;
  const auto key_bytes = handle->physical_copies_ == 0
                             ? key.size()
                             : handle->location_.logical_size_;
  const bool external =
      AlignRecord(RecordHeaderBytes(sizeof(IndirectKeyId)) + key_bytes) >
      kStorageBlockBytes - kBlockHeaderBytes;
  if (external && extents == nullptr) {
    auto written = co_await WriteExtentValueLocked(store, key);
    if (!written.ok()) co_return written.status();
    extents = std::move(*written);
  }
  std::string manifest = external ? EncodeManifest(*extents) : std::string{};
  RecordLocation location;
  auto written = co_await WriteRecordLocked(
      store, 0, IndirectKeyIdBytes(handle->id_),
      external ? std::string_view(manifest) : key, RecordKind::kValue,
      ValueType::kString, 0, {}, 0, 1, for_defrag, unlock_writer_while_waiting,
      external, false, key_bytes, extents, &location, nullptr, nullptr, nullptr,
      nullptr, nullptr, nullptr, nullptr, nullptr, false, nullptr, true);
  if (!written.ok()) co_return written;
  ++handle->physical_copies_;
  // A data record must never become durable before its UUID's original key.
  // Publication happens after this fence, including for relocation. Readers
  // holding a handle continue to see the old physical copy until then.
  store.store_state_mutex_.Unlock(*store.worker_);
  const RelocationDurabilityFence fence{
      .block_id_ = location.block_id(),
      .allocation_epoch_ = location.allocation_epoch(),
      .block_owner_ = location.block_owner(),
      .committed_bytes_ = static_cast<std::uint32_t>(
          location.record_offset() + location.total_disk_bytes())};
  auto durable = co_await AwaitRelocationDurableLocal(store, fence);
  co_await store.store_state_mutex_.Lock();
  if (!durable.ok()) co_return durable;
  handle->location_ = location;
  handle->extents_ = std::move(extents);
  co_return absl::OkStatus();
}

Task<absl::StatusOr<IndirectKeyHandle>> StorageEngine::Impl::EnsureIndirectKey(
    WorkerStore& store, std::string_view key, const Digest& digest,
    TxShardWrites* tx, bool for_defrag, bool unlock_writer_while_waiting) {
  if (tx != nullptr) {
    for (const auto& cached : tx->indirect_keys_) {
      if (cached.key_ != key) continue;
      const auto bytes = IndirectKeyIdBytes(cached.id_);
      const auto* found =
          store.indirect_keys_.Find(ComputeDigest(bytes), bytes);
      if (found != nullptr && !found->value_->retired_) co_return found->value_;
    }
  }
  auto candidates = store.indirect_key_candidates_.find(digest);
  // Copy identities before I/O: another write may mutate the candidate map.
  std::vector<IndirectKeyId> ids =
      candidates == store.indirect_key_candidates_.end()
          ? std::vector<IndirectKeyId>{}
          : candidates->second;
  for (auto id : ids) {
    const auto bytes = IndirectKeyIdBytes(id);
    const auto* found = store.indirect_keys_.Find(ComputeDigest(bytes), bytes);
    if (found == nullptr || found->value_->retired_ ||
        found->value_->location_.logical_size_ != key.size())
      continue;
    auto handle = found->value_;
    auto actual = co_await LoadIndirectKey(handle);
    if (!actual.ok()) co_return actual.status();
    if (*actual != key) continue;
    if (tx != nullptr) tx->indirect_keys_.push_back({std::string(key), id});
    co_return handle;
  }
  // Atomic maintenance rewrites already have a durable identity. Creating a
  // key here would release their caller's publication lock for the first time.
  if (!unlock_writer_while_waiting)
    co_return absl::InternalError(
        "atomic rewrite lost its indirect key identity");
  auto handle = std::make_shared<IndirectKey>();
  do {
    auto hi = RandomStorageSetId();
    auto lo = RandomStorageSetId();
    if (!hi.ok()) co_return hi.status();
    if (!lo.ok()) co_return lo.status();
    handle->id_ = {(*hi & ~std::uint64_t{0x3fff}) | RedisSlot(key), *lo};
    // RFC 4122 variant/version bits; the slot occupies two different bytes.
    auto* bytes = reinterpret_cast<unsigned char*>(handle->id_.data());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
  } while (
      store.indirect_keys_.Find(ComputeDigest(IndirectKeyIdBytes(handle->id_)),
                                IndirectKeyIdBytes(handle->id_)) != nullptr);
  handle->digest_ = digest;
  // Reserve the identity before any KeyRecord reaches staging. The caller's
  // handle pins this unpublished entry through I/O; original-key candidates
  // and the cleaner see it only after the durability fence succeeds.
  auto inserted = InsertIndirectKey(store, handle);
  if (!inserted.ok()) co_return inserted;
  auto written =
      co_await WriteIndirectKey(store, handle, key, for_defrag, true);
  if (!written.ok()) {
    if (handle->physical_copies_ == 0) {
      const auto bytes = IndirectKeyIdBytes(handle->id_);
      store.indirect_keys_.Erase(ComputeDigest(bytes), bytes);
    } else {
      // A failed durability fence can leave a physical KeyRecord. Retain its
      // identity and stop writes rather than dropping its retirement metadata.
      LatchRuntimeFailure(store);
    }
    co_return written;
  }
  if (!store.indirect_key_gc_queue_)
    store.indirect_key_gc_queue_ =
        std::make_unique<std::deque<IndirectKeyId>>();
  store.indirect_key_gc_queue_->push_back(handle->id_);
  store.indirect_key_candidates_[digest].push_back(handle->id_);
  if (tx != nullptr)
    tx->indirect_keys_.push_back({std::string(key), handle->id_});
  co_return handle;
}

Task<absl::Status> StorageEngine::Impl::ReleaseIndirectKeyReferences(
    WorkerStore& store, std::uint64_t block_id,
    std::uint64_t allocation_epoch) {
  const auto identity = std::pair{block_id, allocation_epoch};
  store.indirect_key_references_.erase(identity);
  auto found = store.indirect_key_records_.find(identity);
  if (found == store.indirect_key_records_.end()) co_return absl::OkStatus();
  auto records = std::move(found->second);
  store.indirect_key_records_.erase(found);
  for (const auto& [id, offset] : records) {
    (void)offset;
    const unsigned owner = (id[0] & 0x3fff) % worker_count_;
    auto release = [this, owner, id]() -> absl::Status {
      auto& registry = *stores_[owner];
      const auto bytes = IndirectKeyIdBytes(id);
      auto* found = registry.indirect_keys_.Find(ComputeDigest(bytes), bytes);
      if (found == nullptr || found->value_->physical_copies_ == 0)
        return absl::DataLossError("indirect key copy accounting underflow");
      auto& handle = found->value_;
      if (--handle->physical_copies_ == 0) {
        if (!handle->retired_)
          return absl::DataLossError("live UUID lost its last physical copy");
        if (handle->extents_ != nullptr)
          SpawnExtentReclaim(registry, handle->extents_);
        registry.indirect_keys_.Erase(found);
      }
      return absl::OkStatus();
    };
    absl::Status released;
    if (owner == store.worker_->id())
      released = release();
    else
      released = co_await bycorf::SubmitTo(owner, std::move(release));
    if (!released.ok()) co_return released;
  }
  co_return absl::OkStatus();
}

void StorageEngine::Impl::RequestIndirectKeyCleaning(WorkerStore& store) {
  // The GC queue can empty before a shrink finishes. Periodic flush keeps
  // advancing a bounded number of buckets even without long-key traffic.
  for (unsigned step = 0; step < 64 && store.indirect_keys_.Maintain();
       ++step) {
  }
  if ((!store.indirect_key_gc_queue_ ||
       store.indirect_key_gc_queue_->empty()) ||
      store.indirect_key_cleaner_running_ ||
      shutdown_flush_requested_.load(std::memory_order_acquire))
    return;
  store.indirect_key_cleaner_running_ = true;
  store.worker_->SpawnBackground(CleanIndirectKeys(&store));
}

Task<absl::Status> StorageEngine::Impl::CleanIndirectKeys(WorkerStore* store) {
  struct Guard {
    WorkerStore* store_;
    ~Guard() { store_->indirect_key_cleaner_running_ = false; }
  } guard{store};
  const auto count =
      std::min<std::size_t>(64, store->indirect_key_gc_queue_->size());
  for (std::size_t i = 0; i < count; ++i) {
    co_await store->store_state_mutex_.Lock();
    std::optional<RetiredRecord> retired;
    {
      UnlockGuard unlock(&store->store_state_mutex_, store->worker_);
      auto id = store->indirect_key_gc_queue_->front();
      store->indirect_key_gc_queue_->pop_front();
      const auto bytes = IndirectKeyIdBytes(id);
      auto* found = store->indirect_keys_.Find(ComputeDigest(bytes), bytes);
      if (found == nullptr) continue;
      if (found->value_.use_count() != 1) {
        store->indirect_key_gc_queue_->push_back(id);
        continue;
      }
      // No allocated user record or in-flight operation can name this UUID.
      // Its own extents remain dependent on every physical KeyRecord copy;
      // recovery can still parse those stale copies until their bits clear.
      retired = RetiredRecordOf(found->value_->location_);
      found->value_->retired_ = true;
      auto candidates =
          store->indirect_key_candidates_.find(found->value_->digest_);
      if (candidates != store->indirect_key_candidates_.end()) {
        std::erase(candidates->second, id);
        if (candidates->second.empty())
          store->indirect_key_candidates_.erase(candidates);
      }
      // Keep the registry entry until all old KeyRecord copies have also
      // left the bitmap. A copied manifest can name the same extent payload.
    }
    auto dead = co_await MarkRecordDead(*retired);
    if (!dead.ok()) {
      LatchRuntimeFailure(*store);
      co_return dead;
    }
  }
  co_await store->store_state_mutex_.Lock();
  UnlockGuard unlock(&store->store_state_mutex_, store->worker_);
  if (store->active_indirect_key_block_) {
    auto* state =
        FindBlockState(*store, store->active_indirect_key_block_->block_id_);
    if (state == nullptr) {
      // An active stream must retain its allocated, owner-local block state.
      LatchRuntimeFailure(*store);
      co_return absl::InternalError(
          "active indirect key block state is missing");
    }
    const auto used = state->committed_bytes_ - kBlockHeaderBytes;
    if (used != 0 &&
        std::uint64_t{state->live_bytes_} * 1000 < std::uint64_t{used} * 501) {
      RequestFlush(*store, store->active_indirect_key_block_->block_id_);
      store->active_indirect_key_block_.reset();
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::RelocateIndirectKey(
    IndirectKeyId id, std::uint64_t block_id, std::uint32_t offset) {
  auto& store = CurrentStore();
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
  const auto bytes = IndirectKeyIdBytes(id);
  const auto* found = store.indirect_keys_.Find(ComputeDigest(bytes), bytes);
  if (found == nullptr || found->value_->retired_) co_return absl::OkStatus();
  auto handle = found->value_;
  const auto source = handle->location_;
  if (source.block_id() != block_id || source.record_offset() != offset)
    co_return absl::OkStatus();
  std::string key;
  if (handle->extents_ == nullptr) {
    auto loaded = co_await LoadIndirectKey(handle);
    if (!loaded.ok()) co_return loaded.status();
    key = std::move(*loaded);
  }
  // Extent payloads are immutable and shared by every physical KeyRecord
  // copy. Relocation rewrites only their manifest, not a multi-megabyte key.
  auto written = co_await WriteIndirectKey(store, handle, key, true, true);
  if (!written.ok()) co_return written;
  // The UUID never changes and all source-block references already share the
  // same handle. Updating this one location also updates snapshots and stale
  // records; the replacement is durable before the old copy loses liveness.
  co_return co_await MarkRecordDead(RetiredRecordOf(source));
}

Task<absl::Status> StorageEngine::Impl::SalvageIndirectKeys(
    WorkerStore& store, std::uint64_t block_id) {
  auto* state = FindBlockState(store, block_id);
  if (state == nullptr) co_return absl::OkStatus();
  auto found =
      store.indirect_key_records_.find({block_id, state->allocation_epoch_});
  if (found == store.indirect_key_records_.end())
    co_return absl::DataLossError("indirect key block has no record directory");
  // The common scheduler supplies a device reclamation permit. Only this
  // class-specific cleaner visits UUID records; ordinary defrag never decodes
  // them as user keys or probes the user index.
  auto records = found->second;
  for (const auto& [id, offset] : records) {
    const unsigned owner = (id[0] & 0x3fff) % worker_count_;
    absl::Status relocated;
    if (owner == store.worker_->id())
      relocated = co_await RelocateIndirectKey(id, block_id, offset);
    else
      relocated = co_await bycorf::SubmitTaskTo(
          owner, [this, id, block_id, offset]() -> Task<absl::Status> {
            co_return co_await RelocateIndirectKey(id, block_id, offset);
          });
    if (!relocated.ok()) {
      if (auto* state = FindBlockState(store, block_id))
        state->defragging_ = false;
      co_return relocated;
    }
    co_await bycorf::Yield(*store.worker_);
  }
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
