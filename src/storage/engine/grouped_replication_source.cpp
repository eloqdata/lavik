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

#include <type_traits>

#include "impl.h"
#include "lavik/storage/detail/collection_compact_stream.h"
#include "lavik/storage/detail/stream_records.h"

namespace lavik::storage {

struct StorageEngine::Impl::FullSyncCollection {
  // Charges precede their owners so buffers die before admission is returned.
  RetainedMemoryCharge charge_;
  WorkerStore* store_ = nullptr;
  WorkerStore::PartitionStore* partition_ = nullptr;
  WorkerStore::PartitionStore::RdbSnapshotValue saved_;
  std::string key_;
  Digest digest_{};
  std::uint64_t session_id_ = 0;
  std::uint8_t db_id_ = 0;
  HashGroupMap<std::uint64_t>::const_iterator hash_cursor_;
  std::uint64_t cursor_ = 0;
  std::uint64_t emitted_count_ = 0;
  std::uint64_t encoded_bytes_ = 0;
  std::uint64_t offset_ = 0;
  std::optional<CollectionCompactEncoder> encoder_;
  std::optional<CollectionPage> page_;
  std::string_view piece_;
  bool pages_done_ = false;
  bool reading_ = false;
  bool cancelled_ = false;
  bool releasing_ = false;
  bool released_ = false;

  bool Valid(const Impl& engine) const {
    const auto session = store_->fullsync_sessions_.find(session_id_);
    const auto& version = saved_.grouped_->version();
    return !cancelled_ && !engine.shutdown_flush_requested_ &&
           session != store_->fullsync_sessions_.end() &&
           !session->second.db_epoch_invalidated_ &&
           partition_->fullsync_subscribers_.contains(session_id_) &&
           engine.EffectiveRecordDbEpoch(*partition_, db_id_) ==
               version.db_epoch_ &&
           partition_->replication_epoch_ == version.replication_epoch_ &&
           partition_->grouped_generations_[db_id_] ==
               version.index_generation_;
  }

  void ResetCursor() {
    cursor_ = 0;
    emitted_count_ = 0;
    pages_done_ = false;
    if (!saved_.grouped_->is_ordered())
      hash_cursor_ = saved_.grouped_->directory().groups().begin();
  }
};

namespace {

struct SourceReadGuard {
  explicit SourceReadGuard(bool* reading) : reading_(reading) {
    *reading_ = true;
  }
  ~SourceReadGuard() { *reading_ = false; }
  bool* reading_;
};

}  // namespace

absl::Status StorageEngine::Impl::PrepareFullSyncPinnedValueInsert(
    WorkerStore::FullSyncCapture& capture) {
  auto& map = capture.pinned_values_;
  constexpr std::size_t kMapSlotBytes =
      sizeof(typename std::remove_reference_t<decltype(map)>::value_type) + 16;
  if (map.capacity() >
      (std::numeric_limits<std::size_t>::max() / kMapSlotBytes - 1) / 2)
    return absl::ResourceExhaustedError(
        "full-sync source map capacity overflow");
  const auto budget = (2 * map.capacity() + 1) * kMapSlotBytes;
  auto reservation = TryReserveMemory(budget);
  if (!reservation) {
    RecordMemoryRejection();
    return absl::ResourceExhaustedError("OOM full-sync source map admission");
  }
  try {
    map.reserve(map.size() + 1);
    const auto bytes = map.capacity() * kMapSlotBytes;
    if (bytes > budget)
      return absl::ResourceExhaustedError(
          "full-sync source map exceeded reserved capacity");
    capture.pinned_values_charge_.Adopt(&*reservation, bytes);
  } catch (const std::bad_alloc&) {
    return absl::ResourceExhaustedError(
        "full-sync source map allocation failed");
  }
  return absl::OkStatus();
}

Task<absl::StatusOr<CollectionPage>>
StorageEngine::Impl::NextFullSyncCollectionPage(
    std::shared_ptr<FullSyncCollection> stream) {
  auto& store = *stream->store_;
  auto& partition = *stream->partition_;
  const auto object = stream->saved_.grouped_;
  if (!stream->Valid(*this) || stream->pages_done_)
    co_return absl::CancelledError(
        "full-sync collection cursor is no longer active");
  HashGroupId id;
  if (object->is_ordered()) {
    const auto& groups = object->ordered_directory().groups();
    if (stream->cursor_ >= groups.size())
      co_return absl::DataLossError("full-sync ordered page cursor is invalid");
    id = {groups[stream->cursor_].id_, 0};
  } else {
    if (stream->hash_cursor_ == object->directory().groups().end())
      co_return absl::DataLossError("full-sync Hash page cursor is invalid");
    id = stream->hash_cursor_->second.id_;
  }
  const auto* entry = object->FindGroup(id);
  if (entry == nullptr)
    co_return absl::DataLossError("full-sync collection page is missing");
  // Pins were captured before the first suspension, so compact locations may
  // now be materialized from the still-identical block allocation epochs.
  const auto location = MaterializeIndexLocation(*entry);
  const auto extents = object->ExtentsFor(id);
  std::uint64_t payload_bytes = location.total_disk_bytes();
  if (location.external()) {
    if (extents == nullptr)
      co_return absl::DataLossError("full-sync page has no extent manifest");
    payload_bytes = 0;
    for (const auto& ref : *extents) {
      if (ref.payload_bytes_ > kMaxRecordPayloadBytes - payload_bytes)
        co_return absl::DataLossError("full-sync page extent size overflow");
      payload_bytes += ref.payload_bytes_;
    }
    const auto prefix = location.key_external() ? stream->key_.size() : 0;
    if (prefix > payload_bytes)
      co_return absl::DataLossError("full-sync page key exceeds payload");
    payload_bytes -= prefix;
  }
  // Decode retains one copy of the strings, the temporary codec entry vector,
  // a DTO vector, and a page-local duplicate-check table. Bound these from the
  // pinned physical envelope BEFORE allocating any decoded value. The checked
  // metadata count below must match this admitted cardinality before a codec
  // can reserve its vector; an oversized corrupted count cannot bypass OOM.
  constexpr std::size_t kPerEntryBudget =
      sizeof(HashEntry) + sizeof(CollectionField) + 128;
  constexpr std::size_t kPageOverhead = 4096;
  const auto count = location.logical_size_;
  if (payload_bytes > std::numeric_limits<std::size_t>::max() - kPageOverhead ||
      count > (std::numeric_limits<std::size_t>::max() - payload_bytes -
               kPageOverhead) /
                  kPerEntryBudget)
    co_return absl::ResourceExhaustedError(
        "full-sync collection page budget overflow");
  const auto budget = payload_bytes + count * kPerEntryBudget + kPageOverhead;
  auto reservation = TryReserveMemory(budget);
  if (!reservation) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError(
        "OOM full-sync collection page admission");
  }
  try {
    CollectionPage page{.value_type_ = stream->saved_.location_.value_type()};
    if (object->is_ordered()) {
      auto decoded = co_await LoadOrderedGroupSnapshot(
          store, partition, stream->db_id_, stream->key_, stream->digest_,
          object, id.prefix_, true);
      if (!decoded.ok()) co_return decoded.status();
      if (page.value_type_ == ValueType::kStream ||
          page.value_type_ == ValueType::kList) {
        page.elements_.reserve(count);
        for (auto& item : decoded->snapshot_.entries_)
          page.elements_.push_back(std::move(item.value_));
      } else {
        page.scored_members_.reserve(count);
        for (auto& item : decoded->snapshot_.entries_)
          page.scored_members_.push_back({std::move(item.value_), item.score_});
      }
      page.done_ =
          stream->cursor_ + 1 == object->ordered_directory().groups().size();
    } else {
      auto decoded = co_await LoadHashGroupSnapshot(
          store, partition, stream->db_id_, stream->key_, stream->digest_,
          object, id, true);
      if (!decoded.ok()) co_return decoded.status();
      if (page.value_type_ == ValueType::kHash)
        page.fields_.reserve(count);
      else
        page.elements_.reserve(count);
      for (auto& item : decoded->snapshot_.value_.entries_) {
        if (page.value_type_ == ValueType::kHash) {
          page.fields_.push_back(
              {std::move(item.field_), std::move(item.value_)});
        } else {
          if (!item.value_.empty())
            co_return absl::DataLossError(
                "full-sync Set page contains a Hash value");
          page.elements_.push_back(std::move(item.field_));
        }
      }
      ++stream->hash_cursor_;
      page.done_ = stream->hash_cursor_ == object->directory().groups().end();
    }
    if (!stream->Valid(*this))
      co_return absl::CancelledError(
          "full-sync population changed during page read");
    const auto total = object->is_ordered()
                           ? object->ordered_directory().root().item_count_
                           : stream->saved_.location_.logical_size_;
    if (stream->emitted_count_ > total ||
        page.size() > total - stream->emitted_count_ ||
        (page.done_ && page.size() != total - stream->emitted_count_))
      co_return absl::DataLossError(
          "full-sync collection aggregate count mismatch");
    stream->emitted_count_ += page.size();
    stream->pages_done_ = page.done_;
    page.next_cursor_ = ++stream->cursor_;
    const auto retained = page.RetainedBytes();
    if (retained > budget)
      co_return absl::ResourceExhaustedError(
          "full-sync page exceeds admitted capacity");
    page.retained_charge_.Adopt(&*reservation, retained);
    co_return page;
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "full-sync collection page allocation failed");
  }
}

Task<absl::StatusOr<std::uint64_t>> StorageEngine::Impl::PinFullSyncCollection(
    WorkerStore& store, std::uint64_t session_id,
    WorkerStore::PartitionStore& partition, std::uint8_t db_id,
    std::string_view key, const Digest& digest, RecordLocation location,
    ExtentManifest root_extents, std::uint64_t* encoded_bytes) {
  if (!location.grouped() || encoded_bytes == nullptr)
    co_return absl::InvalidArgumentError("invalid full-sync collection source");
  auto object = partition.grouped_objects_[db_id].Lookup(
      key, GroupedObjectVersion{
               .root_ = location,
               .db_epoch_ = EffectiveRecordDbEpoch(partition, db_id),
               .replication_epoch_ = partition.replication_epoch_,
               .index_generation_ = partition.grouped_generations_[db_id]});
  if (!object.ok()) co_return object.status();
  if (*object == nullptr)
    co_return absl::DataLossError("missing full-sync collection view");
  auto capture = partition.fullsync_subscribers_.find(session_id);
  if (capture == partition.fullsync_subscribers_.end())
    co_return absl::CancelledError("full-sync collection capture ended");
  if (key.size() > std::numeric_limits<std::size_t>::max() -
                       sizeof(FullSyncCollection) - 128)
    co_return absl::ResourceExhaustedError(
        "full-sync source metadata budget overflow");
  const auto budget = sizeof(FullSyncCollection) + key.size() + 128;
  auto reservation = TryReserveMemory(budget);
  if (!reservation) {
    RecordMemoryRejection();
    co_return absl::ResourceExhaustedError(
        "OOM full-sync source metadata admission");
  }
  std::shared_ptr<FullSyncCollection> stream;
  try {
    stream = std::make_shared<FullSyncCollection>();
    stream->store_ = &store;
    stream->partition_ = &partition;
    stream->session_id_ = session_id;
    stream->db_id_ = db_id;
    stream->key_ = key;
    stream->digest_ = digest;
    stream->saved_.location_ = location;
    stream->saved_.extents_ = std::move(root_extents);
    stream->saved_.grouped_ = std::move(*object);
    stream->charge_.Adopt(&*reservation, budget);
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "full-sync source state allocation failed");
  }
  // A pre-scan is not in pinned_values_ yet. Keep it visible to shutdown from
  // BEFORE the first owner hop until publication or a registered release task
  // takes ownership. Declaration order keeps that handoff continuously counted.
  active_settlements_.fetch_add(1, std::memory_order_acq_rel);
  struct PinSettlement {
    std::atomic<std::uint32_t>* active;
    ~PinSettlement() { active->fetch_sub(1, std::memory_order_acq_rel); }
  } pin_settlement{&active_settlements_};
  // This cleanup also covers a cancelled caller during an owner-hop pin/read.
  // Spawn registration precedes scheduling, so shutdown cannot miss the task.
  bool published = false;
  struct Cleanup {
    Impl* engine;
    WorkerStore* store;
    std::shared_ptr<FullSyncCollection>* stream;
    bool* published;
    ~Cleanup() {
      if (*published) return;
      engine->active_settlements_.fetch_add(1, std::memory_order_acq_rel);
      store->worker_->Spawn(
          engine->ReleaseFullSyncCollection(std::move(*stream)));
    }
  } cleanup{this, &store, &stream, &published};
  SourceReadGuard reading(&stream->reading_);
  auto prepared = PrepareGroupedSnapshotPins(&stream->saved_);
  if (!prepared.ok()) co_return prepared;
  auto pinned = co_await PinRdbSnapshotValue(&stream->saved_);
  if (!pinned.ok()) co_return pinned;
  LAVIK_FAULT_INJECT(if (LAVIK_FAULT_MATCHES(
                             "LAVIK_PAUSE_FULLSYNC_COLLECTION_SCAN_KEY", key)) {
    spdlog::info("paused full-sync collection pre-scan key={}", key);
    auto waited = co_await bycorf::SleepFor(*store.worker_,
                                            std::chrono::milliseconds(5000));
    if (!waited.ok()) co_return waited;
  });
  stream->ResetCursor();
  std::uint64_t bytes = location.value_type() == ValueType::kHash ||
                                location.value_type() == ValueType::kSet
                            ? 32
                            : 8;
  while (!stream->pages_done_) {
    auto page = co_await NextFullSyncCollectionPage(stream);
    if (!page.ok()) co_return page.status();
    auto measured = CollectionCompactEncoder::MeasurePage(*page);
    if (!measured.ok()) co_return measured.status();
    if (*measured > std::numeric_limits<std::uint64_t>::max() - bytes)
      co_return absl::OutOfRangeError(
          "full-sync collection wire length overflow");
    bytes += *measured;
  }
  auto encoder = CollectionCompactEncoder::Create(
      location.value_type(),
      location.value_type() == ValueType::kStream
          ? stream->saved_.grouped_->ordered_directory().root().item_count_
          : location.logical_size_,
      bytes);
  if (!encoder.ok()) co_return encoder.status();
  stream->encoder_.emplace(std::move(*encoder));
  stream->encoded_bytes_ = bytes;
  stream->ResetCursor();
  capture = partition.fullsync_subscribers_.find(session_id);
  if (!stream->Valid(*this) ||
      capture == partition.fullsync_subscribers_.end() ||
      capture->second.next_pinned_value_id_ == 0)
    co_return absl::CancelledError(
        "full-sync capture ended during collection scan");
  const auto id = capture->second.next_pinned_value_id_;
  // Revalidate/admit the table only after the last suspension: other snapshot
  // readers can publish sources while this stream measures its pages. Capacity
  // belongs to the capture, not an individual value released by ACK.
  auto map_prepared = PrepareFullSyncPinnedValueInsert(capture->second);
  if (!map_prepared.ok()) co_return map_prepared;
  try {
    auto [_, inserted] = capture->second.pinned_values_.emplace(
        id, WorkerStore::FullSyncCapture::PinnedValue{
                .extents_ = {}, .collection_ = stream, .value_bytes_ = bytes});
    if (!inserted)
      co_return absl::InternalError("duplicate full-sync collection source id");
  } catch (const std::bad_alloc&) {
    co_return absl::ResourceExhaustedError(
        "full-sync collection source map allocation failed");
  }
  ++capture->second.next_pinned_value_id_;
  *encoded_bytes = bytes;
  published = true;
  co_return id;
}

Task<absl::StatusOr<std::string>>
StorageEngine::Impl::ReadFullSyncCollectionChunk(
    std::shared_ptr<FullSyncCollection> stream, std::uint64_t offset,
    std::size_t max_bytes) {
  if (!stream->Valid(*this) || stream->reading_ || offset != stream->offset_ ||
      offset >= stream->encoded_bytes_ || max_bytes == 0)
    co_return absl::FailedPreconditionError(
        "invalid full-sync collection stream offset/state");
  SourceReadGuard reading(&stream->reading_);
  LAVIK_FAULT_INJECT(
      if (offset == kReplicationTransferBytes && stream->page_.has_value() &&
          LAVIK_FAULT_MATCHES("LAVIK_PAUSE_FULLSYNC_COLLECTION_CHUNK_KEY",
                              stream->key_)) {
        spdlog::info("paused full-sync collection chunk key={} page_bytes={}",
                     stream->key_, stream->page_->RetainedBytes());
        auto waited = co_await bycorf::SleepFor(
            *stream->store_->worker_, std::chrono::milliseconds(5000));
        if (!waited.ok()) co_return waited;
      });
  const auto count = static_cast<std::size_t>(
      std::min<std::uint64_t>(max_bytes, stream->encoded_bytes_ - offset));
  try {
    std::string output;
    output.reserve(count);
    while (output.size() < count) {
      if (!stream->Valid(*this)) {
        stream->cancelled_ = true;
        co_return absl::CancelledError("full-sync collection stream cancelled");
      }
      if (!stream->piece_.empty()) {
        const auto take =
            std::min(count - output.size(), stream->piece_.size());
        output.append(stream->piece_.substr(0, take));
        stream->piece_.remove_prefix(take);
        continue;
      }
      if (auto piece = stream->encoder_->Next()) {
        stream->piece_ = *piece;
        continue;
      }
      if (stream->pages_done_) {
        stream->cancelled_ = true;
        co_return absl::DataLossError(
            "full-sync collection encoder ended early");
      }
      // The last span has been consumed. Drop its page and charge before
      // admitting the next one, including when both pages contain huge items.
      stream->page_.reset();
      auto page = co_await NextFullSyncCollectionPage(stream);
      if (!page.ok()) {
        stream->cancelled_ = true;
        co_return page.status();
      }
      stream->page_.emplace(std::move(*page));
      auto started = stream->encoder_->StartPage(*stream->page_);
      if (!started.ok()) {
        stream->cancelled_ = true;
        co_return started;
      }
    }
    stream->offset_ += output.size();
    if (stream->offset_ == stream->encoded_bytes_) {
      // Empty trailing fields/values still advance cursor state, but no
      // nonempty byte may remain beyond the measured wire boundary.
      if (!stream->piece_.empty()) {
        stream->cancelled_ = true;
        co_return absl::DataLossError("full-sync encoder has bytes beyond EOF");
      }
      for (;;) {
        if (auto piece = stream->encoder_->Next()) {
          if (piece->empty()) continue;
          stream->cancelled_ = true;
          co_return absl::DataLossError(
              "full-sync collection exceeded measured length");
        }
        if (stream->pages_done_) break;
        // Empty Hash/Set routing leaves still have to advance the directory
        // cursor after its last nonempty leaf emitted the last wire byte.
        stream->page_.reset();
        auto page = co_await NextFullSyncCollectionPage(stream);
        if (!page.ok()) {
          stream->cancelled_ = true;
          co_return page.status();
        }
        stream->page_.emplace(std::move(*page));
        auto started = stream->encoder_->StartPage(*stream->page_);
        if (!started.ok()) {
          stream->cancelled_ = true;
          co_return started;
        }
      }
      auto finished = stream->encoder_->Finish();
      if (!finished.ok() || !stream->piece_.empty() || !stream->pages_done_) {
        stream->cancelled_ = true;
        co_return finished.ok()
            ? absl::DataLossError("full-sync collection EOF mismatch")
            : finished;
      }
      stream->page_.reset();
    }
    co_return output;
  } catch (const std::bad_alloc&) {
    stream->cancelled_ = true;
    co_return absl::ResourceExhaustedError(
        "full-sync collection transfer buffer allocation failed");
  }
}

Task<absl::Status> StorageEngine::Impl::ReleaseFullSyncCollection(
    std::shared_ptr<FullSyncCollection> stream) {
  // Every caller registers before Spawn, including failed admission cleanup.
  struct Settlement {
    std::atomic<std::uint32_t>* active;
    ~Settlement() { active->fetch_sub(1, std::memory_order_acq_rel); }
  } settlement{&active_settlements_};
  if (stream == nullptr) co_return absl::OkStatus();
  stream->cancelled_ = true;
  while (stream->reading_ || (stream->releasing_ && !stream->released_)) {
    const auto waited = co_await bycorf::SleepFor(*stream->store_->worker_,
                                                  std::chrono::milliseconds(1));
    if (!waited.ok()) co_await bycorf::Yield(*stream->store_->worker_);
  }
  if (stream->released_) co_return absl::OkStatus();
  stream->releasing_ = true;
#if LAVIK_FAULTS_ENABLED
  const auto pin_count = stream->saved_.block_pins_ == nullptr
                             ? 0
                             : stream->saved_.block_pins_->blocks_.size();
  const auto page_bytes = stream->page_ ? stream->page_->RetainedBytes() : 0;
  const auto page_charge =
      stream->page_ ? stream->page_->retained_charge_.bytes() : 0;
  const auto retained_before =
      WorkerMemoryAccountingBytes(stream->store_->worker_->id());
#endif
  stream->piece_ = {};
  stream->encoder_.reset();
  stream->page_.reset();
#if LAVIK_FAULTS_ENABLED
  // Measure the actual charge handoff without suspension on the source owner.
  // A later process-wide INFO sample also includes unrelated connection and
  // IO-buffer cache growth, so its net delta cannot prove this page was freed.
  const auto released_page_charge =
      retained_before -
      WorkerMemoryAccountingBytes(stream->store_->worker_->id());
#endif
  auto released = co_await ReleaseRdbSnapshotValue(&stream->saved_);
  stream->released_ = true;
  LAVIK_FAULT_INJECT(
      if ((LAVIK_FAULT_MATCHES("LAVIK_PAUSE_FULLSYNC_COLLECTION_CHUNK_KEY",
                               stream->key_) ||
           LAVIK_FAULT_MATCHES("LAVIK_PAUSE_FULLSYNC_COLLECTION_SCAN_KEY",
                               stream->key_)) &&
          released.ok() && !stream->saved_.pins_held_) {
        spdlog::info(
            "released full-sync collection key={} pins={} page_bytes={} "
            "page_charge={} released_page_charge={} held=0",
            stream->key_, pin_count, page_bytes, page_charge,
            released_page_charge);
      });
  co_return released;
}

}  // namespace lavik::storage
