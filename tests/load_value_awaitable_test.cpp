/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <sys/eventfd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <tuple>

#include "../src/storage/engine/impl.h"
#include "absl/cleanup/cleanup.h"
#include "gtest/gtest.h"
#include "lavik/storage/detail/stream_records.h"
#include "support/read_buffer_fault.h"

namespace lavik::storage {

// Deliver deterministic CQEs for identity races and real I/O for buffer
// handoff.
class LoadValueAwaitableTestPeer : public testing::Test {
 protected:
  using Awaitable = StorageEngine::Impl::LoadValueAwaitable;
  using Result = Awaitable::Result;
  using ExtentRead = StorageEngine::Impl::ReadExtentAwaitable;

  virtual std::size_t ReadBufferCount() const { return 1; }
  // One minimum-size record must consume the entire single-read fixture.
  // Wider fixtures keep byte-based assertions because range lengths vary.
  virtual std::size_t ReadSlotBytes() const { return 12 * kKiB; }
  virtual unsigned WorkerCount() const { return 1; }

  void SetUp() override {
    // Exercise the service's initialized admission model even when CTest runs
    // this fixture alone. Unlimited pre-init reservations carry zero bytes.
    ASSERT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, WorkerCount()).ok());
    saved_context_ = std::move(bycorf::MutableThisWorker());
    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    ASSERT_GE(wake_fd_, 0);
    cross_core_.mailbox(0).wake_fd_ = wake_fd_;
    worker_.BindCrossCore(0, &cross_core_);
    bycorf::SetThisWorker(0, &cross_core_, &worker_);
    ASSERT_TRUE(worker_.Init({.recv_buffer_count_ = 0}).ok());
    auto& engine = *storage_.impl_;
    engine.worker_count_ = WorkerCount();
    engine.stores_.push_back(
        std::make_unique<StorageEngine::Impl::WorkerStore>());
    auto& store = *engine.stores_[0];
    store.engine_ = &engine;
    store.worker_ = &worker_;
    store.partitions_.resize(1);
    store.partition_indexes_.resize(kLogicalDatabaseCount);
    store.partitions_[0].indexes_ = store.partition_indexes_;
    store.partitions_[0].replication_epoch_ = 1;
    store.files_.push_back(bycorf::FixedFile{});
    engine.db_epochs_[0].store(1);
    engine.devices_.push_back(
        StorageDevice{.capacity_blocks_ = WorkerCount() + 1});
    engine.block_state_lookup_.push_back(
        {blocks_.data(), 1, WorkerCount() + 1});
    block_.Reset(0, 7);
    block_.allocated_ = true;

    RegisteredBufferPoolOptions options;
    options.storage_write_buffer_count_ = 2;
    options.read_slot_bytes_ = ReadSlotBytes();
    options.registered_bytes_ = 2 * options.write_buffer_bytes_ +
                                ReadBufferCount() * options.read_slot_bytes_;
    ASSERT_TRUE(Pool().Init(worker_, options).ok());
    ASSERT_EQ(Pool().available_read_buffers(), ReadBufferCount());
    if (WorkerCount() == 2) {
      remote_wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
      ASSERT_GE(remote_wake_fd_, 0);
      cross_core_.mailbox(1).wake_fd_ = remote_wake_fd_;
      remote_worker_.BindCrossCore(1, &cross_core_);
      bycorf::SetThisWorker(1, &cross_core_, &remote_worker_);
      ASSERT_TRUE(remote_worker_.Init({.recv_buffer_count_ = 0}).ok());
      engine.stores_.push_back(
          std::make_unique<StorageEngine::Impl::WorkerStore>());
      auto& remote = *engine.stores_[1];
      remote.engine_ = &engine;
      remote.worker_ = &remote_worker_;
      remote.files_.push_back(bycorf::FixedFile{});
      ASSERT_TRUE(remote.buffers_.Init(remote_worker_, options).ok());
      blocks_[1].Reset(1, 7);
      blocks_[1].allocated_ = true;
      bycorf::SetThisWorker(0, &cross_core_, &worker_);
    }

    header_.header_bytes_ = RecordHeaderBytes(key_.size());
    header_.key_bytes_ = key_.size();
    header_.value_type_ = ValueType::kString;
    header_.logical_size_ = value_.size();
    header_.payload_bytes_ = value_.size();
    header_.total_disk_bytes_ =
        AlignRecord(header_.header_bytes_ + value_.size());
    header_.allocation_epoch_ = 7;
    header_.mutation_sequence_ = 9;
    header_.payload_checksum_ = Crc32c(std::as_bytes(std::span(value_)));
    const auto metadata = RecordLocation::PackedMetadata::Encode(
        kBlockHeaderBytes + 8, header_.total_disk_bytes_, 0, false, false,
        false, false, false, false, RecordKind::kValue, ValueType::kString);
    location_ = RecordLocation(1, 9, 7, 0, value_.size(), metadata);
  }

  void TearDown() override {
    bycorf::SetThisWorker(0, &cross_core_, &worker_);
    worker_.Shutdown();
    if (WorkerCount() == 2) {
      bycorf::SetThisWorker(1, &cross_core_, &remote_worker_);
      remote_worker_.Shutdown();
      bycorf::SetThisWorker(0, &cross_core_, &worker_);
    }
    storage_.impl_.reset();
    if (wake_fd_ >= 0) close(wake_fd_);
    if (remote_wake_fd_ >= 0) close(remote_wake_fd_);
    bycorf::MutableThisWorker() = std::move(saved_context_);
    EXPECT_TRUE(InitMemoryLimit(1024ULL * 1024 * 1024, 1).ok());
  }

  auto& Store() { return *storage_.impl_->stores_[0]; }
  RegisteredBufferPool& Pool(unsigned owner = 0) {
    return storage_.impl_->stores_[owner]->buffers_;
  }
  auto Read() {
    return storage_.impl_->LoadValue(Store(), Store().partitions_[0], 0, key_,
                                     digest_, location_, {});
  }
  auto ReadLocal() {
    return storage_.impl_->LoadValueLocal(Store(), 0, key_, location_, 1);
  }
  Task<Result> ReadExternalGrouped() {
    const auto metadata = RecordLocation::PackedMetadata::Encode(
        location_.record_offset(), location_.total_disk_bytes(), 0, false, true,
        false, false, false, false, RecordKind::kValue, ValueType::kString);
    const RecordLocation location(1, 9, 7, 0, value_.size(), metadata);
    auto manifest = std::make_shared<const std::vector<ExtentRef>>(
        std::vector<ExtentRef>{ExtentReference()});
    co_return co_await storage_.impl_->LoadExternalValueLocal(
        Store(), location, std::move(manifest), nullptr, true);
  }
  auto ReadGrouped(bool pinned = false) {
    return storage_.impl_->LoadGroupedValue(Store(), Store().partitions_[0], 0,
                                            key_, digest_, location_,
                                            pinned ? grouped_ : nullptr);
  }
  auto ReadGroupedHash(bool packed = false) {
    return storage_.impl_->ReadGroupedHashResult(
        Store(), Store().partitions_[0], 0, key_, digest_, grouped_,
        location_.value_type() == ValueType::kHash ? HashOperationKind::kGetAll
                                                   : HashOperationKind::kKeys,
        false, packed);
  }
  auto ReadMutableGroupedHash() {
    return storage_.impl_->LoadGroupedHashValue(Store(), Store().partitions_[0],
                                                0, key_, digest_, grouped_);
  }
  Task<absl::StatusOr<ListResult>> ReadGroupedList(std::int64_t first = 0,
                                                   std::int64_t last = -1,
                                                   bool packed = false) {
    const ListOperation operation{.kind_ = ListOperationKind::kRange,
                                  .first_ = first,
                                  .second_ = last,
                                  .packed_output_ = packed};
    co_return co_await storage_.impl_->ExecuteGroupedListLocked(
        Store(), Store().partitions_[0], 0, key_, digest_, operation, grouped_,
        nullptr, nullptr);
  }
  Task<absl::StatusOr<SortedSetResult>> ReadGroupedZset(
      SortedSetOperation operation = {
          .kind_ = SortedSetOperationKind::kRange}) {
    co_return co_await storage_.impl_->ExecuteGroupedSortedSetLocked(
        Store(), Store().partitions_[0], 0, key_, digest_, operation, grouped_,
        nullptr, nullptr);
  }
  auto ReadGroupedStream(std::uint64_t count = UINT64_MAX,
                         std::uint64_t last = UINT64_MAX) {
    return storage_.impl_->ReadValueForTransferLocked(
        0, key_, digest_,
        StreamRangeAccess{
            .first_ = {1, 0}, .last_ = {last, UINT64_MAX}, .count_ = count});
  }
  void CloseTransfer(TransferValue& source) {
    source.reader_ = {};
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (storage_.impl_->active_settlements_.load() != 0 &&
           std::chrono::steady_clock::now() < until)
      RunReadRound();
    ASSERT_EQ(storage_.impl_->active_settlements_.load(), 0);
    ExpectNoPageLeases();
  }
  bool HasHelper(const Awaitable& read) { return read.slow_.valid(); }

  void Encode(std::byte* data) {
    ASSERT_TRUE(EncodeRecordHeader(header_, key_,
                                   std::span(data, header_.header_bytes_)));
    std::memcpy(data + header_.header_bytes_, value_.data(), value_.size());
  }
  void Complete(Awaitable& read, int bytes = -1) {
    ASSERT_NE(read.pinned_, nullptr);
    Encode(read.lease_.io_buffer().data_ + read.record_headroom_);
    read.Complete(worker_, bytes == -1 ? read.read_bytes_ : bytes, 0);
  }
  void Stage() {
    location_.set_in_memory(true);
    block_.in_memory_ = true;
    if (block_.staging_slot_ == 0) {
      block_.staging_slot_ = Store().staging_slots_.size();
      Store().staging_slots_.emplace_back();
      ASSERT_TRUE(Pool().TryAcquireWriteBuffer(
          &Store().staging_slots_.back().write_buffer_id_));
    }
    auto buffer = storage_.impl_->StagingBufferFor(Store(), block_);
    Encode(buffer.data_ + location_.record_offset());
  }
  void StageGroupedPage(std::size_t page, bool valid_checksum = true) {
    auto& header = grouped_headers_[page];
    const auto& payload = grouped_payloads_[page];
    header.payload_checksum_ = Crc32c(std::as_bytes(std::span(payload)));
    if (!valid_checksum) header.payload_checksum_ ^= 1;
    const auto staging = storage_.impl_->StagingBufferFor(Store(), block_);
    auto* data = staging.data_ + grouped_locations_[page].record_offset();
    ASSERT_TRUE(EncodeRecordHeader(header, key_,
                                   std::span(data, header.header_bytes_)));
    std::memcpy(data + header.header_bytes_, payload.data(), payload.size());
  }
  void InstallGroupedString(std::size_t bytes) {
    digest_ = ComputeDigest(key_);
    value_.resize(bytes);
    for (std::size_t i = 0; i < bytes; ++i)
      value_[i] = static_cast<char>(i % 251);
    auto& store = Store();
    store.partition_grouped_objects_.resize(kLogicalDatabaseCount);
    store.partitions_[0].grouped_objects_ = store.partition_grouped_objects_;
    block_.in_memory_ = true;
    if (block_.staging_slot_ == 0) {
      block_.staging_slot_ = store.staging_slots_.size();
      store.staging_slots_.emplace_back();
      ASSERT_TRUE(Pool().TryAcquireWriteBuffer(
          &store.staging_slots_.back().write_buffer_id_));
    }
    const auto count = (bytes + kStringGroupBytes - 1) / kStringGroupBytes;
    const OrderedCollectionRoot root{
        .kind_ = OrderedCollectionKind::kString,
        .incarnation_ = 17,
        .item_count_ = bytes,
        .first_group_ = 1,
        .last_group_ = count,
        .next_group_id_ = count + 1,
        .group_count_ = static_cast<std::uint32_t>(count),
        .revision_ = 9};
    location_ = RecordLocation(
        1, 9, 7, 0, bytes,
        RecordLocation::PackedMetadata::Encode(
            kBlockHeaderBytes, 256, 0, true, false, false, false, false, false,
            RecordKind::kValue, ValueType::kString, false, true));
    std::vector<RecoveredOrderedGroup> records;
    std::vector<GroupedRecordLocation> locations;
    grouped_payloads_.clear();
    grouped_headers_.clear();
    grouped_locations_.clear();
    std::uint32_t offset = kBlockHeaderBytes + 256;
    for (std::size_t i = 0; i < count; ++i) {
      const auto segment = std::string_view(value_).substr(
          i * kStringGroupBytes, kStringGroupBytes);
      OrderedGroupSnapshot page{.kind_ = OrderedCollectionKind::kString,
                                .incarnation_ = 17,
                                .id_ = i + 1,
                                .previous_ = i,
                                .next_ = i + 1 == count ? 0 : i + 2,
                                .entries_ = {{.value_ = std::string(segment)}}};
      auto encoded = EncodeOrderedGroup(page);
      ASSERT_TRUE(encoded.ok()) << encoded.status();
      RecordHeader header = header_;
      header.auxiliary_group_ = true;
      header.group_incarnation_ = page.incarnation_;
      header.group_prefix_ = page.id_;
      header.header_bytes_ =
          RecordHeaderBytes(key_.size(), false, false, false, true);
      header.logical_size_ = segment.size();
      header.payload_bytes_ = encoded->size();
      header.total_disk_bytes_ =
          AlignRecord(header.header_bytes_ + encoded->size());
      const auto owner = static_cast<unsigned>(i % WorkerCount());
      const RecordLocation physical(
          owner + 1, 9, 7, 0, segment.size(),
          RecordLocation::PackedMetadata::Encode(
              offset, header.total_disk_bytes_, owner, true, false, false,
              false, false, false, RecordKind::kValue, ValueType::kString));
      records.emplace_back(OrderedGroupEntry{.incarnation_ = 17,
                                             .id_ = page.id_,
                                             .previous_ = page.previous_,
                                             .next_ = page.next_,
                                             .sequence_ = 9,
                                             .lsn_ = 9,
                                             .item_count_ = segment.size(),
                                             .encoded_bytes_ = encoded->size(),
                                             .record_token_ = i + 1});
      locations.push_back({.id_ = {i + 1, 0}, .location_ = physical});
      grouped_payloads_.push_back(std::move(*encoded));
      grouped_headers_.push_back(header);
      grouped_locations_.push_back(physical);
      ASSERT_NO_FATAL_FAILURE(StageGroupedPage(i));
      offset += header.total_disk_bytes_;
    }
    auto directory = OrderedGroupDirectory::Recover(root, 9, records, {});
    ASSERT_TRUE(directory.ok()) << directory.status();
    auto object = GroupedObject::CreateOrdered(
        {.root_ = location_, .db_epoch_ = 1, .replication_epoch_ = 1},
        std::move(*directory), locations);
    ASSERT_TRUE(object.ok()) << object.status();
    ASSERT_TRUE(store.partitions_[0]
                    .grouped_objects_[0]
                    .Publish(digest_, key_, grouped_, *object)
                    .ok());
    grouped_ = std::move(*object);
    IndexLocation();
  }
  void PrepareGroupedCollection(ValueType type, std::size_t count) {
    digest_ = ComputeDigest(key_);
    auto& store = Store();
    store.partition_grouped_objects_.resize(kLogicalDatabaseCount);
    store.partitions_[0].grouped_objects_ = store.partition_grouped_objects_;
    block_.in_memory_ = true;
    if (block_.staging_slot_ == 0) {
      block_.staging_slot_ = store.staging_slots_.size();
      store.staging_slots_.emplace_back();
      ASSERT_TRUE(Pool().TryAcquireWriteBuffer(
          &store.staging_slots_.back().write_buffer_id_));
    }
    location_ = RecordLocation(
        1, 9, 7, 0, count,
        RecordLocation::PackedMetadata::Encode(
            kBlockHeaderBytes, 256, 0, true, false, false, false, false, false,
            RecordKind::kValue, type, false, true));
    grouped_payloads_.clear();
    grouped_headers_.clear();
    grouped_locations_.clear();
  }
  void AppendGroupedPage(GroupedRecordId id, std::string payload,
                         std::uint32_t count) {
    const auto i = grouped_locations_.size();
    const auto offset = i == 0
                            ? kBlockHeaderBytes + 256
                            : grouped_locations_.back().record_offset() +
                                  grouped_locations_.back().total_disk_bytes();
    const auto owner = static_cast<unsigned>(i % WorkerCount());
    RecordHeader header = header_;
    header.value_type_ = location_.value_type();
    header.auxiliary_group_ = true;
    header.group_incarnation_ = 17;
    header.group_prefix_ = id.prefix_;
    header.group_prefix_bits_ = id.bits_;
    header.header_bytes_ =
        RecordHeaderBytes(key_.size(), false, false, false, true);
    header.logical_size_ = count;
    header.payload_bytes_ = payload.size();
    header.total_disk_bytes_ =
        AlignRecord(header.header_bytes_ + payload.size());
    grouped_locations_.emplace_back(
        owner + 1, 9, 7, 0, count,
        RecordLocation::PackedMetadata::Encode(
            offset, header.total_disk_bytes_, owner, true, false, false, false,
            false, false, RecordKind::kValue, location_.value_type()));
    grouped_payloads_.push_back(std::move(payload));
    grouped_headers_.push_back(header);
    ASSERT_NO_FATAL_FAILURE(StageGroupedPage(i));
  }
  void PublishGroupedCollection(GroupedObject::Handle object) {
    ASSERT_TRUE(Store()
                    .partitions_[0]
                    .grouped_objects_[0]
                    .Publish(digest_, key_, grouped_, object)
                    .ok());
    grouped_ = std::move(object);
    IndexLocation();
  }
  void InstallGroupedHash(ValueType type, std::size_t fields_per_page = 1) {
    ASSERT_TRUE(type == ValueType::kHash || type == ValueType::kSet);
    constexpr std::size_t count = 64;
    const DigestSeed seed{1, 2, 3, 4};
    std::array<std::vector<std::string>, count> fields;
    // Equal field counts in each six-bit prefix keep both owners' page counts
    // deterministic, independently of the production split-size heuristic.
    std::size_t filled = 0;
    for (std::size_t candidate = 0;
         candidate < 100000 && filled < count * fields_per_page; ++candidate) {
      const auto number = std::to_string(candidate);
      auto field = "field-" + std::string(6 - number.size(), '0') + number;
      field.append("\0\xff", 2);
      const auto prefix = ComputeDigest(field, seed).value_ >> 58;
      if (fields[prefix].size() < fields_per_page) {
        fields[prefix].push_back(std::move(field));
        ++filled;
      }
    }
    ASSERT_EQ(filled, count * fields_per_page);
    ASSERT_NO_FATAL_FAILURE(PrepareGroupedCollection(type, filled));
    hash_entries_.clear();
    std::vector<RecoveredGroupedRecord> records;
    std::vector<GroupedRecordLocation> locations;
    for (std::size_t i = 0; i < count; ++i) {
      const GroupedRecordId id{static_cast<std::uint64_t>(i) << 58, 6};
      HashGroupSnapshot page{.incarnation_ = 17, .id_ = id};
      for (const auto& field : fields[i]) {
        std::string value;
        if (type == ValueType::kHash) {
          value = "value-" + std::to_string(i);
          value.append("\0\xfe", 2);
          value.append(17 + i % 31, static_cast<char>(i));
        }
        hash_entries_.emplace_back(field, value);
        page.value_.entries_.push_back({.digest_ = ComputeDigest(field),
                                        .field_ = field,
                                        .value_ = std::move(value)});
      }
      auto encoded = EncodeHashGroup(page);
      ASSERT_TRUE(encoded.ok()) << encoded.status();
      records.push_back({.incarnation_ = 17,
                         .id_ = id,
                         .sequence_ = 9,
                         .lsn_ = 9,
                         .field_count_ = fields_per_page,
                         .encoded_bytes_ = encoded->size(),
                         .record_token_ = i + 1});
      ASSERT_NO_FATAL_FAILURE(
          AppendGroupedPage(id, std::move(*encoded), fields_per_page));
      locations.push_back({.id_ = id, .location_ = grouped_locations_.back()});
    }
    auto directory = HashGroupDirectory::Recover({.incarnation_ = 17,
                                                  .seed_ = seed,
                                                  .field_count_ = filled,
                                                  .group_count_ = count,
                                                  .revision_ = 9},
                                                 9, records, {});
    ASSERT_TRUE(directory.ok()) << directory.status();
    auto object = GroupedObject::Create(
        {.root_ = location_, .db_epoch_ = 1, .replication_epoch_ = 1},
        std::move(*directory), locations);
    ASSERT_TRUE(object.ok()) << object.status();
    ASSERT_NO_FATAL_FAILURE(PublishGroupedCollection(std::move(*object)));
  }
  void InstallGroupedList(std::size_t pages = 70) {
    InstallGroupedOrdered(OrderedCollectionKind::kList, pages);
  }
  void InstallGroupedZset(std::size_t pages = 70,
                          bool equal_member_lengths = false) {
    InstallGroupedOrdered(OrderedCollectionKind::kSortedSet, pages,
                          equal_member_lengths);
  }
  void InstallGroupedOrdered(OrderedCollectionKind kind, std::size_t pages,
                             bool equal_member_lengths = false) {
    constexpr std::size_t per_page = 3;
    const bool sorted = kind == OrderedCollectionKind::kSortedSet;
    ASSERT_NO_FATAL_FAILURE(PrepareGroupedCollection(
        sorted ? ValueType::kSortedSet : ValueType::kList, pages * per_page));
    list_values_.clear();
    zset_values_.clear();
    std::vector<RecoveredOrderedGroup> records;
    std::vector<GroupedRecordLocation> locations;
    for (std::size_t i = 0; i < pages; ++i) {
      OrderedGroupSnapshot page{.kind_ = kind,
                                .incarnation_ = 17,
                                .id_ = i + 1,
                                .previous_ = i,
                                .next_ = i + 1 == pages ? 0 : i + 2};
      for (std::size_t j = 0; j < per_page; ++j) {
        const auto rank = i * per_page + j;
        const auto number = std::to_string(rank);
        auto value = "item-" +
                     (sorted ? std::string(6 - number.size(), '0') : "") +
                     number;
        value.append("\0\xff", 2);
        value.append(equal_member_lengths ? 17 : 17 + (i + j) % 31,
                     static_cast<char>(i + j));
        list_values_.push_back(value);
        // Ties cross page boundaries; binary member order must remain stable
        // when owner-bucket submission order differs from score/rank order.
        const auto score = sorted ? static_cast<double>(rank / 4) : 0;
        if (sorted) zset_values_.emplace_back(value, score);
        page.entries_.push_back({.value_ = std::move(value), .score_ = score});
      }
      auto encoded = EncodeOrderedGroup(page);
      ASSERT_TRUE(encoded.ok()) << encoded.status();
      records.emplace_back(
          OrderedGroupEntry{.incarnation_ = 17,
                            .id_ = page.id_,
                            .previous_ = page.previous_,
                            .next_ = page.next_,
                            .sequence_ = 9,
                            .lsn_ = 9,
                            .item_count_ = per_page,
                            .encoded_bytes_ = encoded->size(),
                            .record_token_ = i + 1,
                            .min_score_ = page.entries_.front().score_,
                            .max_score_ = page.entries_.back().score_});
      const GroupedRecordId id{i + 1, 0};
      ASSERT_NO_FATAL_FAILURE(
          AppendGroupedPage(id, std::move(*encoded), per_page));
      locations.push_back({.id_ = id, .location_ = grouped_locations_.back()});
    }
    OrderedCollectionRoot root{
        .kind_ = kind,
        .incarnation_ = 17,
        .item_count_ = pages * per_page,
        .first_group_ = 1,
        .last_group_ = pages,
        .next_group_id_ = pages + 1,
        .group_count_ = static_cast<std::uint32_t>(pages),
        .revision_ = 9};
    std::optional<HashGroupDirectory> members;
    if (sorted) {
      root.member_index_ = GroupedHashRoot{.incarnation_ = 17,
                                           .field_count_ = pages * per_page,
                                           .group_count_ = 1,
                                           .revision_ = 9};
      HashGroupSnapshot page{.incarnation_ = 17, .id_ = {0, 0}};
      for (const auto& [member, score] : zset_values_)
        page.value_.entries_.push_back(
            {.digest_ = ComputeDigest(member),
             .field_ = member,
             .value_ = EncodeSortedSetMemberScore(score)});
      auto encoded = EncodeHashGroup(page);
      ASSERT_TRUE(encoded.ok()) << encoded.status();
      const RecoveredGroupedRecord member_record{
          .incarnation_ = 17,
          .id_ = {0, 0},
          .sequence_ = 9,
          .lsn_ = 9,
          .field_count_ = pages * per_page,
          .encoded_bytes_ = encoded->size(),
          .record_token_ = pages + 1};
      auto recovered = HashGroupDirectory::Recover(
          *root.member_index_, 9, std::span(&member_record, 1), {});
      ASSERT_TRUE(recovered.ok()) << recovered.status();
      members.emplace(std::move(*recovered));
      ASSERT_NO_FATAL_FAILURE(
          AppendGroupedPage(page.id_, std::move(*encoded), pages * per_page));
      locations.push_back(
          {.id_ = page.id_, .location_ = grouped_locations_.back()});
    }
    auto directory = OrderedGroupDirectory::Recover(root, 9, records, {}, 0,
                                                    std::move(members));
    ASSERT_TRUE(directory.ok()) << directory.status();
    auto object = GroupedObject::CreateOrdered(
        {.root_ = location_, .db_epoch_ = 1, .replication_epoch_ = 1},
        std::move(*directory), locations);
    ASSERT_TRUE(object.ok()) << object.status();
    ASSERT_NO_FATAL_FAILURE(PublishGroupedCollection(std::move(*object)));
  }
  void ExpectHash(const absl::StatusOr<HashValue>& result) {
    ASSERT_TRUE(result.ok()) << result.status();
    std::vector<std::pair<std::string, std::string>> actual;
    for (const auto& entry : result->entries_)
      actual.emplace_back(entry.field_, entry.value_);
    auto expected = hash_entries_;
    std::sort(actual.begin(), actual.end());
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(actual, expected);
  }
  void ExpectHash(const absl::StatusOr<HashResult>& result) {
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_TRUE(result->key_exists_);
    ASSERT_EQ(result->length_, hash_entries_.size());
    const std::size_t width =
        location_.value_type() == ValueType::kHash ? 2 : 1;
    ASSERT_EQ(result->value_count(), hash_entries_.size() * width);
    std::size_t retained = sizeof(HashResult);
    if (result->snapshot_) {
      EXPECT_TRUE(result->values_.empty());
      const auto& snapshot = *result->snapshot_;
      // Parent assembly keeps one child block per page; offsets must remain
      // correct when owner waves finish in a different order than directory
      // traversal, without retaining any child's page lease or slot array.
      EXPECT_EQ(snapshot.blocks_.size(), grouped_payloads_.size());
      retained += sizeof(HashReadSnapshot) +
                  snapshot.blocks_.capacity() * sizeof(std::string) +
                  snapshot.values_.capacity() * sizeof(HashReadSnapshot::Slice);
      for (const auto& block : snapshot.blocks_)
        retained += block.capacity() + 1;
    } else {
      retained += result->values_.capacity() * sizeof(result->values_[0]);
      for (const auto& value : result->values_) {
        ASSERT_TRUE(value);
        retained += value->capacity() + 1;
      }
    }
    EXPECT_EQ(result->retained_charge_.bytes(), retained);
    std::vector<std::pair<std::string, std::string>> actual;
    for (std::size_t i = 0; i < result->value_count(); i += width) {
      ASSERT_TRUE(result->value_view(i).has_value());
      if (width == 2) ASSERT_TRUE(result->value_view(i + 1).has_value());
      actual.emplace_back(
          *result->value_view(i),
          width == 2 ? *result->value_view(i + 1) : std::string_view{});
    }
    auto expected = hash_entries_;
    std::sort(actual.begin(), actual.end());
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(actual, expected);
  }
  void InstallGroupedStream() {
    // Transfer entry points route by Redis slot, while this fixture owns only
    // partition zero. Use its real routing rule rather than widening stores.
    for (unsigned candidate = 0;; ++candidate) {
      key_ = "stream-" + std::to_string(candidate);
      if (RedisSlot(key_) == 0) break;
      ASSERT_LT(candidate, 1000000);
    }
    header_.key_bytes_ = key_.size();
    constexpr std::size_t count = 210, per_page = 3;
    std::string wire("LXS1");
    auto put = [&](std::uint64_t value, unsigned width) {
      for (unsigned i = 0; i < width; ++i)
        wire.push_back(static_cast<char>(value >> (8 * i)));
    };
    auto string = [&](std::string_view value) {
      put(value.size(), 4);
      wire.append(value);
    };
    put(count, 8);
    put(0, 8);
    wire.append(16, '\0');
    put(count, 8);
    put(count, 4);
    for (std::size_t i = 1; i <= count; ++i) {
      put(i, 8);
      put(0, 8);
      put(2, 4);
      string("field");
      string(std::string("value\0", 6) + std::to_string(i));
    }
    put(1, 4);
    put(count, 4);
    put(0, 4);
    auto decoded = DecodeStreamRecords(wire, count);
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    stream_messages_.clear();
    for (const auto& entry : *decoded) {
      auto key = StreamRecordKey(entry.value_);
      ASSERT_TRUE(key.ok()) << key.status();
      if (key->front() == '\1') stream_messages_.push_back(entry.value_);
    }
    ASSERT_EQ(stream_messages_.size(), count);
    ASSERT_NO_FATAL_FAILURE(
        PrepareGroupedCollection(ValueType::kStream, count));
    const auto pages = (decoded->size() + per_page - 1) / per_page;
    std::vector<RecoveredOrderedGroup> records;
    std::vector<GroupedRecordLocation> locations;
    for (std::size_t i = 0; i < pages; ++i) {
      const auto first = i * per_page;
      const auto end = std::min(first + per_page, decoded->size());
      OrderedGroupSnapshot page{
          .kind_ = OrderedCollectionKind::kStream,
          .incarnation_ = 17,
          .id_ = i + 1,
          .previous_ = i,
          .next_ = i + 1 == pages ? 0 : i + 2,
          .entries_ = {decoded->begin() + first, decoded->begin() + end}};
      auto encoded = EncodeOrderedGroup(page);
      ASSERT_TRUE(encoded.ok()) << encoded.status();
      records.emplace_back(OrderedGroupEntry{.incarnation_ = 17,
                                             .id_ = page.id_,
                                             .previous_ = page.previous_,
                                             .next_ = page.next_,
                                             .sequence_ = 9,
                                             .lsn_ = 9,
                                             .item_count_ = end - first,
                                             .encoded_bytes_ = encoded->size(),
                                             .record_token_ = i + 1});
      const auto max_key = StreamRecordKey(page.entries_.back().value_);
      ASSERT_TRUE(max_key.ok()) << max_key.status();
      records.back().stream_max_key_.Set(*max_key);
      const GroupedRecordId id{i + 1, 0};
      ASSERT_NO_FATAL_FAILURE(
          AppendGroupedPage(id, std::move(*encoded), end - first));
      locations.push_back({.id_ = id, .location_ = grouped_locations_.back()});
    }
    auto directory = OrderedGroupDirectory::Recover(
        {.kind_ = OrderedCollectionKind::kStream,
         .incarnation_ = 17,
         .item_count_ = decoded->size(),
         .first_group_ = 1,
         .last_group_ = pages,
         .next_group_id_ = pages + 1,
         .group_count_ = static_cast<std::uint32_t>(pages),
         .revision_ = 9,
         .stream_length_ = count},
        9, records, {});
    ASSERT_TRUE(directory.ok()) << directory.status();
    auto object = GroupedObject::CreateOrdered(
        {.root_ = location_, .db_epoch_ = 1, .replication_epoch_ = 1},
        std::move(*directory), locations);
    ASSERT_TRUE(object.ok()) << object.status();
    ASSERT_NO_FATAL_FAILURE(PublishGroupedCollection(std::move(*object)));
  }
  void ExpectZset(const absl::StatusOr<SortedSetResult>& result,
                  std::size_t first = 0, std::size_t end = SIZE_MAX,
                  bool reverse = false) {
    ASSERT_TRUE(result.ok()) << result.status();
    if (end == SIZE_MAX) end = zset_values_.size();
    ASSERT_EQ(result->member_count(), end - first);
    EXPECT_EQ(result->length_, zset_values_.size());
    if (result->packed_members_) {
      EXPECT_TRUE(result->members_.empty());
      EXPECT_EQ(result->packed_members_->members_.values_.size(),
                result->member_count());
    }
    for (std::size_t i = 0; i < result->member_count(); ++i) {
      const auto& expected = zset_values_[reverse ? end - 1 - i : first + i];
      EXPECT_EQ(result->member_view(i).member_, expected.first) << i;
      EXPECT_EQ(result->member_view(i).score_, expected.second) << i;
    }
  }
  void ReplaceHashField(std::size_t page, std::string_view old_field,
                        std::string_view new_field) {
    ASSERT_EQ(old_field.size(), new_field.size());
    auto& payload = grouped_payloads_[page];
    const auto offset = payload.find(old_field);
    ASSERT_NE(offset, std::string::npos);
    payload.replace(offset, old_field.size(), new_field);
    // Repair CRC to distinguish trusted terminal reads from strict logical
    // validation. Fixed field widths preserve the encoded framing.
    ASSERT_NO_FATAL_FAILURE(StageGroupedPage(page));
  }
  void ExpectStoredHashFields(std::string_view strict_error, bool packed) {
    ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
    ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
    for (const bool scarce : {false, true}) {
      SCOPED_TRACE(scarce ? "serial admission fallback" : "parallel batch");
      const auto previous_shard = CurrentMemoryAccountingShard();
      absl::Cleanup restore = [&] {
        (void)InitMemoryLimit(1024ULL * 1024 * 1024, WorkerCount());
        BindMemoryAccountingShard(previous_shard == 0 ? kMaxMemoryWorkers
                                                      : previous_shard - 1);
      };
      if (scarce) {
        BindMemoryAccountingShard(kMaxMemoryWorkers);
        RefreshMemoryStats();
        const auto target = GetMemoryStats().used_bytes_ + 96 * 1024;
        ASSERT_TRUE(InitMemoryLimit((target * 10 + 8) / 9, WorkerCount()).ok());
      }
      auto task = ReadGroupedHash(packed);
      Start(task);
      RunReadRound();
      EXPECT_EQ(blocks_[0].pin_count() + blocks_[1].pin_count(),
                scarce ? 1 : 32);
      ASSERT_NO_FATAL_FAILURE(Drain(task));
      const auto result = std::move(task).TakeResult();
      ExpectHash(result);
      ExpectNoPageLeases();
    }
    // Materialization feeds mutations and still validates the durable logical
    // invariants before returning entries with process-local lookup digests.
    auto strict = ReadMutableGroupedHash();
    Start(strict);
    ASSERT_NO_FATAL_FAILURE(Drain(strict));
    const auto result = std::move(strict).TakeResult();
    EXPECT_TRUE(absl::IsDataLoss(result.status())) << result.status();
    EXPECT_EQ(result.status().message(), strict_error);
    ExpectNoPageLeases();
  }
  void RelocateGroupedPagesToRemoteOwner() {
    ASSERT_EQ(WorkerCount(), 2);
    for (std::size_t i = 0; i < grouped_locations_.size(); i += 2) {
      auto& old = grouped_locations_[i];
      const RecordLocation replacement(
          2, old.mutation_sequence_, 7, 0, old.logical_size_,
          RecordLocation::PackedMetadata::Encode(
              old.record_offset(), old.total_disk_bytes(), 1, false, false,
              false, false, false, false, RecordKind::kValue,
              old.value_type()));
      const auto& header = grouped_headers_[i];
      auto moved = GroupedObject::RelocateGroup(
          grouped_, {header.group_prefix_, header.group_prefix_bits_}, old,
          replacement);
      ASSERT_TRUE(moved.ok()) << moved.status();
      ASSERT_TRUE(Store()
                      .partitions_[0]
                      .grouped_objects_[0]
                      .Publish(digest_, key_, grouped_, *moved)
                      .ok());
      grouped_ = std::move(*moved);
      old = replacement;
    }
  }
  void IndexLocation() {
    auto& index = Store().partitions_[0].indexes_[0];
    if (auto* existing = index.Find(digest_, key_); existing != nullptr) {
      // The fixture changes logical-size widths as it builds larger grouped
      // values. Use the same representation replacement as the append funnel.
      RecordIndex::Entry* replaced = nullptr;
      ASSERT_NE(index.ReplaceValue(existing, location_, digest_, &replaced),
                nullptr);
      index.DestroyDetached(replaced);
    } else {
      ASSERT_NE(index.InsertNew(digest_, key_, location_), nullptr);
    }
  }
  void Invalidate(bool database) {
    if (database)
      storage_.impl_->db_epochs_[0].fetch_add(1);
    else
      ++Store().partitions_[0].replication_epoch_;
  }
  void CandidatePopulation() {
    storage_.impl_->replica_loading_.store(true);
    auto& partition = Store().partitions_[0];
    partition.replica_sync_ = std::make_unique<
        StorageEngine::Impl::WorkerStore::PartitionStore::ReplicaSyncState>();
    partition.replica_sync_->local_db_epochs_[0] = 4;
    header_.db_epoch_ = 4;
  }
  void ExpectValue(const Result& result) {
    ASSERT_TRUE(result.ok()) << result.status();
    auto bytes = result->value();
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(bytes.data()),
                               bytes.size()),
              value_);
  }

  // Simulate only the submission, retaining the real completion -> retry ->
  // parent continuation chain, with an owned Task to catch early/double resume.
  Task<Result> AwaitCompletion(Awaitable& read) {
    struct Arm {
      Awaitable& read_;
      bool await_ready() const { return false; }
      void await_suspend(std::coroutine_handle<> parent) {
        read_.awaiting_ = parent;
      }
      void await_resume() const {}
    };
    co_await Arm{read};
    co_return read.await_resume();
  }
  Task<Result> AwaitExisting(Awaitable& read) { co_return co_await read; }
  Task<absl::Status> AwaitExtent(ExtentRead& read) { co_return co_await read; }
  bool BufferWaitIsBackground(Awaitable& read) {
    return read.task_class_ == bycorf::TaskClass::kBackground;
  }
  auto ExtentSlice(ExtentRef ref, std::size_t offset,
                   std::span<std::byte> destination) {
    return storage_.impl_->ReadExtentSlice(Store(), ref, 0, offset,
                                           destination);
  }
  auto RoutedExtentSlice(ExtentRef ref, std::size_t offset,
                         std::span<std::byte> destination) {
    return ExtentRead(*storage_.impl_, Store(), ref, 0, offset, destination,
                      true);
  }
  bool ExtentChunksAreBackground(ExtentRead& read) {
    return read.chunked_.valid() &&
           worker_.IsBackground(
               std::move(read.chunked_).operator co_await().handle_);
  }

  // Use a real fixed-file read for pool -> I/O handoff, rather than completing
  // the request before the continuation actually submits it.
  void InstallDiskImage(std::span<const std::byte> bytes) {
    char path[] = "/tmp/lavik-awaitable-XXXXXX";
    const int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    // The two-owner fixture uses the same encoded image at both physical
    // blocks; its directory selects alternating pages from each owner.
    for (unsigned owner = 0; owner < WorkerCount(); ++owner) {
      const auto offset = storage_.impl_->FileOffset(owner + 1).second;
      const auto written = pwrite(fd, bytes.data(), bytes.size(), offset);
      EXPECT_EQ(written, static_cast<ssize_t>(bytes.size()));
    }
    close(fd);
    struct Opened : bycorf::IoCompletion {
      bool done_ = false;
      int result_ = -1;
      void Complete(bycorf::Worker&, int result, unsigned) override {
        result_ = result;
        done_ = true;
      }
    };
    absl::Cleanup cleanup = [&] {
      unlink(path);
      bycorf::SetThisWorker(0, &cross_core_, &worker_);
    };
    for (unsigned owner = 0; owner < WorkerCount(); ++owner) {
      auto& worker = owner == 0 ? worker_ : remote_worker_;
      bycorf::SetThisWorker(owner, &cross_core_, &worker);
      Opened opened;
      ASSERT_TRUE(worker.RegisterFixedFiles(1).ok());
      auto submitted =
          worker.SubmitOpenDirect(path, O_RDONLY | O_DIRECT, 0, {0}, &opened);
      if (submitted.ok()) {
        const auto until =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!opened.done_ && std::chrono::steady_clock::now() < until)
          worker.RunOnce(false);
      }
      ASSERT_TRUE(submitted.ok()) << submitted;
      ASSERT_TRUE(opened.done_);
      ASSERT_EQ(opened.result_, 0);
    }
  }
  void ResizeRecord(std::size_t bytes) {
    value_.resize(bytes);
    for (std::size_t i = 0; i < bytes; ++i) value_[i] = 'a' + i % 23;
    header_.logical_size_ = bytes;
    header_.payload_bytes_ = bytes;
    header_.total_disk_bytes_ = AlignRecord(header_.header_bytes_ + bytes);
    header_.payload_checksum_ = Crc32c(std::as_bytes(std::span(value_)));
    location_ =
        RecordLocation(1, 9, 7, 0, bytes,
                       RecordLocation::PackedMetadata::Encode(
                           location_.record_offset(), header_.total_disk_bytes_,
                           0, false, false, false, false, false, false,
                           RecordKind::kValue, ValueType::kString));
  }
  void InstallRecord(bool corrupt = false, std::size_t truncated_bytes = 0) {
    std::vector<std::byte> bytes(
        AlignDirect(location_.record_offset() + location_.total_disk_bytes()));
    Encode(bytes.data() + location_.record_offset());
    if (corrupt)
      bytes[location_.record_offset() + header_.header_bytes_ + value_.size() -
            1] ^= std::byte{1};
    ASSERT_LE(truncated_bytes, bytes.size());
    bytes.resize(bytes.size() - truncated_bytes);
    InstallDiskImage(bytes);
  }
  void InstallGroupedDiskImage() {
    ASSERT_FALSE(grouped_locations_.empty());
    const auto& last = grouped_locations_.back();
    const auto bytes =
        AlignDirect(last.record_offset() + last.total_disk_bytes());
    const auto staging = storage_.impl_->StagingBufferFor(Store(), block_);
    ASSERT_LE(bytes, staging.size_);
    ASSERT_NO_FATAL_FAILURE(InstallDiskImage(std::span(staging.data_, bytes)));
    // Keep the graph and its locations unchanged while evicting its staging
    // image. Every subsequent page must acquire a slot and complete real I/O.
    block_.in_memory_ = false;
  }
  ExtentRef ExtentReference(bool no_crc = false, unsigned owner = 0) {
    return {owner + 1, 7, static_cast<std::uint32_t>(value_.size()),
            no_crc ? 0 : Crc32c(std::as_bytes(std::span(value_)))};
  }
  void InstallExtent(bool corrupt = false, bool no_crc = false,
                     unsigned owner = 0, std::size_t truncated_bytes = 0) {
    blocks_[owner].kind_ = BlockKind::kPayloadExtent;
    auto ref = ExtentReference(no_crc, owner);
    std::vector<std::byte> bytes(
        AlignDirect(kBlockHeaderBytes + value_.size()));
    BlockHeader header;
    header.block_id_ = owner + 1;
    header.allocation_epoch_ = 7;
    header.kind_ = BlockKind::kPayloadExtent;
    header.extent_no_crc_ = no_crc;
    header.layout_worker_count_ = 1;
    header.committed_bytes_ = kBlockHeaderBytes + value_.size();
    header.extent_payload_bytes_ = value_.size();
    header.extent_payload_checksum_ = ref.payload_checksum_;
    EncodeBlockHeader(header, std::span<std::byte, kBlockHeaderSlotBytes>(
                                  bytes.data(), kBlockHeaderSlotBytes));
    std::memcpy(bytes.data() + kBlockHeaderBytes, value_.data(), value_.size());
    // Corrupt beyond the slice returned by the test, inside the actual payload.
    if (corrupt) bytes[kBlockHeaderBytes + value_.size() - 1] ^= std::byte{1};
    ASSERT_LE(truncated_bytes, bytes.size());
    bytes.resize(bytes.size() - truncated_bytes);
    InstallDiskImage(bytes);
  }

  template <typename T>
  std::coroutine_handle<> Start(Task<T>& task, bool background = false) {
    auto handle = std::move(task).operator co_await().await_suspend(
        std::noop_coroutine());
    if (background) worker_.RegisterBackground(handle);
    handle.resume();
    return handle;
  }
  template <typename T>
  void Drain(Task<T>& task) {
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!task.done() && std::chrono::steady_clock::now() < until)
      RunReadRound();
    ASSERT_TRUE(task.done());
  }
  void RunReadRound() {
    worker_.RunOnce(false);
    if (WorkerCount() == 2) {
      bycorf::SetThisWorker(1, &cross_core_, &remote_worker_);
      remote_worker_.RunOnce(false);
      bycorf::SetThisWorker(0, &cross_core_, &worker_);
    }
  }
  void ExpectNoPageLeases() {
    for (unsigned owner = 0; owner < WorkerCount(); ++owner) {
      EXPECT_EQ(blocks_[owner].pin_count(), 0);
      EXPECT_EQ(Pool(owner).available_read_buffers(), ReadBufferCount());
    }
  }
  std::uint64_t RetainedBytes() const {
    std::uint64_t bytes = 0;
    for (unsigned owner = 0; owner < WorkerCount(); ++owner)
      bytes += GetWorkerMemoryStats(owner).retained_bytes_;
    return bytes;
  }
  bool HelperIsBackground(Awaitable& read) {
    return worker_.IsBackground(
        std::move(read.slow_).operator co_await().handle_);
  }

  bycorf::CurrentWorker saved_context_;
  bycorf::CrossCore cross_core_{2};
  int wake_fd_ = -1;
  int remote_wake_fd_ = -1;
  bycorf::Worker worker_;
  bycorf::Worker remote_worker_;
  std::array<BlockState, 2> blocks_;
  BlockState& block_ = blocks_[0];
  StorageEngine storage_{StorageEngineOptions{}};
  std::string key_ = "direct-key";
  std::string value_ = "direct value";
  Digest digest_{17};
  RecordHeader header_;
  RecordLocation location_;
  GroupedObject::Handle grouped_;
  std::vector<std::string> grouped_payloads_;
  std::vector<RecordHeader> grouped_headers_;
  std::vector<RecordLocation> grouped_locations_;
  std::vector<std::pair<std::string, std::string>> hash_entries_;
  std::vector<std::string> list_values_;
  std::vector<std::pair<std::string, double>> zset_values_;
  std::vector<std::string> stream_messages_;
};

TEST_F(LoadValueAwaitableTestPeer, ExhaustedPoolWaitsWithoutGrowingDmaMemory) {
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  const auto retained = RetainedBytes();
  {
    auto read = Read();
    auto task = AwaitExisting(read);
    Start(task);
    EXPECT_FALSE(task.done());
    EXPECT_EQ(Pool().output_buffer_count(), 0);
    EXPECT_EQ(RetainedBytes(), retained);
    held->Reset();
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ExpectValue(std::move(task).TakeResult());
    EXPECT_FALSE(HasHelper(read));
  }
  ExpectNoPageLeases();
}

TEST_F(LoadValueAwaitableTestPeer, OwnedWriteSourceReleasesTheReadArena) {
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  auto read = ReadLocal();
  auto task = AwaitExisting(read);
  Start(task);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  auto result = std::move(task).TakeResult();
  ASSERT_TRUE(result.ok());
  ASSERT_NE(result->lease_.buffer_id(), 0);
  ASSERT_EQ(Pool().available_read_bytes(), 0);
  const auto txid = result->txid_;
  ASSERT_TRUE(result->MakeOwned(Pool()).ok());
  EXPECT_EQ(result->txid_, txid);
  EXPECT_EQ(result->lease_.buffer_id(), 0);
  EXPECT_EQ(Pool().available_read_bytes(), Pool().read_capacity_bytes());
  // A prepared writer can keep its value while an earlier lock owner reads.
  auto next = Pool().TryAcquireReadBuffer(4 * kKiB);
  ASSERT_TRUE(next.ok());
  ExpectValue(result);
}

TEST_F(LoadValueAwaitableTestPeer, BufferWaitersPreserveFifoHandoff) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  lavik::test::ScopedReadBufferAllocationFailure failure;
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  auto first = Read();
  auto second = Read();
  auto first_task = AwaitExisting(first);
  auto second_task = AwaitExisting(second);
  Start(first_task, true);
  Start(second_task);
  EXPECT_TRUE(BufferWaitIsBackground(first));
  EXPECT_EQ(block_.pin_count(), 0);
  held->Reset();
  // Handoff schedules I/O; it must not resume inside the release stack.
  EXPECT_FALSE(first_task.done());
  EXPECT_EQ(Pool().available_read_buffers(), 0);
  ASSERT_NO_FATAL_FAILURE(Drain(first_task));
  EXPECT_FALSE(second_task.done());
  // Taking/destroying the first result hands its sole slot to the second.
  ExpectValue(std::move(first_task).TakeResult());
  ASSERT_NO_FATAL_FAILURE(Drain(second_task));
  ExpectValue(std::move(second_task).TakeResult());
  EXPECT_FALSE(HasHelper(first));
  EXPECT_FALSE(HasHelper(second));
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, PopulationChangeDuringBufferWaitSkipsIo) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  lavik::test::ScopedReadBufferAllocationFailure failure;
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  auto read = Read();
  auto task = AwaitExisting(read);
  Start(task);
  Invalidate(true);
  held->Reset();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsNotFound(std::move(task).TakeResult().status()));
  EXPECT_FALSE(HasHelper(read));
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, RelocationDuringBufferWaitUsesNewLocation) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  lavik::test::ScopedReadBufferAllocationFailure failure;
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  auto read = Read();
  auto task = AwaitExisting(read);
  Start(task, true);
  EXPECT_EQ(block_.pin_count(), 0);
  // No physical pointer was retained while waiting: recycle before handoff.
  block_.Reset(0, 8);
  block_.allocated_ = true;
  header_.allocation_epoch_ = 8;
  location_ = RecordLocation(1, 9, 8, 0, value_.size(), location_.metadata_);
  Stage();
  IndexLocation();
  held->Reset();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectValue(std::move(task).TakeResult());
  EXPECT_TRUE(HasHelper(read));
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, ExtentWaitRetainsPinAndReturnsSlice) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  lavik::test::ScopedReadBufferAllocationFailure failure;
  ASSERT_NO_FATAL_FAILURE(InstallExtent());
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  std::array<std::byte, 3> output{};
  {
    auto read = ExtentSlice(ExtentReference(), 2, output);
    EXPECT_FALSE(read.await_ready());
    EXPECT_EQ(block_.pin_count(), 1);
    auto task = AwaitExtent(read);
    Start(task, true);
    held->Reset();
    EXPECT_FALSE(task.done());
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto status = std::move(task).TakeResult();
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()),
                               output.size()),
              std::string_view(value_).substr(2, output.size()));
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, ExtentSliceChecksWholePayloadBeforeCopying) {
  ASSERT_NO_FATAL_FAILURE(InstallExtent(true));
  std::array<std::byte, 2> output{std::byte{0x55}, std::byte{0x55}};
  {
    auto read = ExtentSlice(ExtentReference(), 0, output);
    auto task = AwaitExtent(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto status = std::move(task).TakeResult();
    EXPECT_TRUE(absl::IsInternal(status)) << status;
    EXPECT_EQ(status.message(), "extent payload checksum mismatch");
    EXPECT_EQ(output[0], std::byte{0x55});
    EXPECT_EQ(output[1], std::byte{0x55});
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer,
       ExtentAllocationFailureReadsChunksAfterSlotWait) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  value_.resize(3 * 24 * kKiB + 17);
  for (std::size_t i = 0; i < value_.size(); ++i)
    value_[i] = static_cast<char>(i % 251);
  ASSERT_NO_FATAL_FAILURE(InstallExtent());
  const auto baseline = WorkerMemoryAccountingBytes(0);
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  lavik::test::ScopedReadBufferAllocationFailure failure;
  std::vector<std::byte> output(value_.size());
  {
    auto read = ExtentSlice(ExtentReference(), 0, output);
    auto task = AwaitExtent(read);
    Start(task, true);
    EXPECT_FALSE(task.done());
    EXPECT_TRUE(ExtentChunksAreBackground(read));
    EXPECT_EQ(block_.pin_count(), 1);
    held->Reset();
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ASSERT_TRUE(std::move(task).TakeResult().ok());
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()),
                               output.size()),
              value_);
  }
  ExpectNoPageLeases();
  EXPECT_EQ(Pool().output_buffer_count(), 0);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

class RecordChunkFallbackTest : public LoadValueAwaitableTestPeer {
 protected:
  void SetUp() override {
    LoadValueAwaitableTestPeer::SetUp();
#if !LAVIK_FAULTS_ENABLED
    GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
    failure_.emplace();
    ResizeRecord(3 * 24 * kKiB + 17);
  }
  std::optional<lavik::test::ScopedReadBufferAllocationFailure> failure_;
};

TEST_F(RecordChunkFallbackTest, ReadsUnalignedRecordIntoAccountedHeapOutput) {
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  const auto baseline = RetainedBytes();
  {
    auto read = ReadLocal();
    ASSERT_TRUE(HasHelper(read));
    EXPECT_EQ(block_.pin_count(), 1);
    auto task = AwaitExisting(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto result = std::move(task).TakeResult();
    ExpectValue(result);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result->lease_.buffer_id(), 0);
    EXPECT_EQ(Pool().available_read_buffers(), 1);
    EXPECT_GE(RetainedBytes() - baseline, value_.size());
    // The reply may outlive another read using the sole DMA slot.
    auto next = ReadLocal();
    auto next_task = AwaitExisting(next);
    Start(next_task);
    ASSERT_NO_FATAL_FAILURE(Drain(next_task));
    ExpectValue(std::move(next_task).TakeResult());
    ExpectValue(result);
  }
  ExpectNoPageLeases();
  EXPECT_EQ(RetainedBytes(), baseline);
}

TEST_F(RecordChunkFallbackTest, WaitRetainsPinAndBackgroundScheduling) {
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  {
    auto read = Read();
    auto task = AwaitExisting(read);
    Start(task, true);
    EXPECT_FALSE(task.done());
    EXPECT_EQ(block_.pin_count(), 1);
    EXPECT_TRUE(HelperIsBackground(read));
    held->Reset();
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ExpectValue(std::move(task).TakeResult());
  }
  ExpectNoPageLeases();
}

TEST_F(RecordChunkFallbackTest, StagedRecordNeedsNoReadSlot) {
  Stage();
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  {
    auto read = Read();
    ASSERT_TRUE(read.await_ready());
    EXPECT_FALSE(HasHelper(read));
    ExpectValue(read.await_resume());
    EXPECT_EQ(block_.pin_count(), 0);
  }
  held->Reset();
  ExpectNoPageLeases();
}

class RecordChunkValidationTest : public RecordChunkFallbackTest,
                                  public testing::WithParamInterface<bool> {};

TEST_P(RecordChunkValidationTest, ValidatesFinalChunkAndHonorsNoCrc) {
  const bool no_crc = GetParam();
  header_.no_crc_ = no_crc;
  ASSERT_NO_FATAL_FAILURE(InstallRecord(true));
  const auto baseline = RetainedBytes();
  {
    auto read = ReadLocal();
    auto task = AwaitExisting(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto result = std::move(task).TakeResult();
    if (no_crc) {
      value_.back() ^= 1;
      ExpectValue(result);
    } else {
      EXPECT_TRUE(absl::IsInternal(result.status()));
      EXPECT_EQ(result.status().message(), "record value checksum mismatch");
    }
  }
  ExpectNoPageLeases();
  EXPECT_EQ(RetainedBytes(), baseline);
}

TEST_P(RecordChunkValidationTest, RejectsShortFinalChunkAndStaleIdentity) {
  const bool short_read = GetParam();
  if (!short_read) ++header_.allocation_epoch_;
  ASSERT_NO_FATAL_FAILURE(
      InstallRecord(false, short_read ? kDirectIoAlignment : 0));
  const auto baseline = RetainedBytes();
  {
    auto read = ReadLocal();
    auto task = AwaitExisting(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto result = std::move(task).TakeResult();
    EXPECT_EQ(result.status().code(), short_read ? absl::StatusCode::kInternal
                                                 : absl::StatusCode::kAborted);
  }
  ExpectNoPageLeases();
  EXPECT_EQ(RetainedBytes(), baseline);
}

INSTANTIATE_TEST_SUITE_P(Modes, RecordChunkValidationTest, testing::Bool());

TEST_F(RecordChunkFallbackTest, PopulationChangeDuringWaitDiscardsOutput) {
  for (bool database : {false, true}) {
    SCOPED_TRACE(database);
    auto held = Pool().AcquireReadBuffer().await_resume();
    ASSERT_TRUE(held.ok());
    {
      auto read = Read();
      auto task = AwaitExisting(read);
      Start(task);
      ASSERT_FALSE(task.done());
      Invalidate(database);
      held->Reset();
      // No file was opened: the invalidated reader must skip even its first
      // I/O.
      ASSERT_NO_FATAL_FAILURE(Drain(task));
      EXPECT_TRUE(absl::IsNotFound(std::move(task).TakeResult().status()));
    }
    ExpectNoPageLeases();
  }
}

TEST_F(RecordChunkFallbackTest, RelocationRetryReleasesSoleSlot) {
  ++header_.allocation_epoch_;
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  --header_.allocation_epoch_;
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  {
    auto read = Read();
    auto task = AwaitExisting(read);
    Start(task, true);
    ASSERT_FALSE(task.done());
    location_.metadata_ = RecordLocation::PackedMetadata::Encode(
        location_.record_offset() + 512, location_.total_disk_bytes(), 0, false,
        false, false, false, false, false, RecordKind::kValue,
        ValueType::kString);
    Stage();
    IndexLocation();
    held->Reset();
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ExpectValue(std::move(task).TakeResult());
  }
  ExpectNoPageLeases();
}

TEST_F(RecordChunkFallbackTest, UnchangedBadLocationStillReportsCorruption) {
  IndexLocation();
  ++header_.allocation_epoch_;
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  {
    auto read = Read();
    auto task = AwaitExisting(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    EXPECT_TRUE(absl::IsInternal(std::move(task).TakeResult().status()));
  }
  ExpectNoPageLeases();
}

class RecordSmallSlotFallbackTest : public RecordChunkFallbackTest {
 protected:
  std::size_t ReadSlotBytes() const override { return 12 * kKiB; }
};

TEST_F(RecordSmallSlotFallbackTest, ReadsWithMinimumConfiguredSlot) {
  ASSERT_NO_FATAL_FAILURE(InstallRecord());
  {
    auto read = ReadLocal();
    auto task = AwaitExisting(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ExpectValue(std::move(task).TakeResult());
  }
  ExpectNoPageLeases();
}

class ExtentFaultFallbackTest : public LoadValueAwaitableTestPeer,
                                public testing::WithParamInterface<bool> {};

TEST_P(ExtentFaultFallbackTest, ValidatesWholePayloadAndNoCrc) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  value_.assign(3 * 24 * kKiB + 17, 'q');
  const bool no_crc = GetParam();
  ASSERT_NO_FATAL_FAILURE(InstallExtent(true, no_crc));
  const auto baseline = WorkerMemoryAccountingBytes(0);
  lavik::test::ScopedReadBufferAllocationFailure failure;
  // Cross a chunk boundary and corrupt only the final, unrequested chunk.
  std::vector<std::byte> output(19, std::byte{0x55});
  {
    auto read = ExtentSlice(ExtentReference(no_crc), 24 * kKiB - 5, output);
    auto task = AwaitExtent(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    const auto status = std::move(task).TakeResult();
    if (no_crc) {
      ASSERT_TRUE(status.ok()) << status;
      EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()),
                                 output.size()),
                std::string(19, 'q'));
    } else {
      EXPECT_TRUE(absl::IsInternal(status));
      EXPECT_EQ(status.message(), "extent payload checksum mismatch");
      EXPECT_EQ(output, std::vector<std::byte>(19, std::byte{0x55}));
    }
  }
  ExpectNoPageLeases();
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

TEST_P(ExtentFaultFallbackTest, RejectsShortReadsAndBadHeaders) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  value_.assign(3 * 24 * kKiB + 17, 'r');
  const bool short_read = GetParam();
  ASSERT_NO_FATAL_FAILURE(
      InstallExtent(false, false, 0, short_read ? kDirectIoAlignment : 0));
  auto ref = ExtentReference();
  if (!short_read) ++ref.payload_checksum_;
  lavik::test::ScopedReadBufferAllocationFailure failure;
  std::array<std::byte, 2> output{std::byte{0x55}, std::byte{0x55}};
  const auto baseline = WorkerMemoryAccountingBytes(0);
  {
    auto read = ExtentSlice(ref, 0, output);
    auto task = AwaitExtent(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    const auto status = std::move(task).TakeResult();
    EXPECT_TRUE(absl::IsInternal(status));
    EXPECT_EQ(status.message(), short_read
                                    ? "short extent block read"
                                    : "extent header does not match manifest");
    EXPECT_EQ(output[0], std::byte{0x55});
    EXPECT_EQ(output[1], std::byte{0x55});
  }
  ExpectNoPageLeases();
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
}

INSTANTIATE_TEST_SUITE_P(Modes, ExtentFaultFallbackTest, testing::Bool());

class ExtentSmallSlotFallbackTest : public LoadValueAwaitableTestPeer {
 protected:
  std::size_t ReadSlotBytes() const override { return 12 * kKiB; }
};

TEST_F(ExtentSmallSlotFallbackTest, HeaderCanSpanMultipleFixedSlotReads) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  value_.assign(3 * 4 * kKiB + 17, 's');
  ASSERT_NO_FATAL_FAILURE(InstallExtent());
  lavik::test::ScopedReadBufferAllocationFailure failure;
  std::vector<std::byte> output(value_.size());
  {
    auto read = ExtentSlice(ExtentReference(), 0, output);
    auto task = AwaitExtent(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ASSERT_TRUE(std::move(task).TakeResult().ok());
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()),
                               output.size()),
              value_);
  }
  ExpectNoPageLeases();
}

class ExtentRemoteFallbackTest : public LoadValueAwaitableTestPeer {
 protected:
  unsigned WorkerCount() const override { return 2; }
};

TEST_F(ExtentRemoteFallbackTest, ChunkFallbackRunsOnPhysicalOwner) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  value_.assign(3 * 24 * kKiB + 17, 't');
  ASSERT_NO_FATAL_FAILURE(InstallExtent(false, false, 1));
  lavik::test::ScopedReadBufferAllocationFailure failure;
  const auto baseline = RetainedBytes();
  std::vector<std::byte> output(19);
  {
    auto read =
        RoutedExtentSlice(ExtentReference(false, 1), 24 * kKiB - 5, output);
    auto task = AwaitExtent(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ASSERT_TRUE(std::move(task).TakeResult().ok());
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()),
                               output.size()),
              std::string(19, 't'));
  }
  ExpectNoPageLeases();
  EXPECT_EQ(RetainedBytes(), baseline);
}

TEST_F(LoadValueAwaitableTestPeer,
       ExternalAssemblyDoesNotRequireDynamicStorageMemory) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  value_.assign(4097, 'x');
  ASSERT_NO_FATAL_FAILURE(InstallExtent());
  lavik::test::ScopedReadBufferAllocationFailure failure;
  const auto baseline = WorkerMemoryAccountingBytes(0);
  auto task = ReadExternalGrouped();
  Start(task);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  auto result = std::move(task).TakeResult();
  ASSERT_TRUE(result.ok()) << result.status();
  ExpectValue(result);
  EXPECT_EQ(result->lease_.buffer_id(), 0);
  EXPECT_EQ(WorkerMemoryAccountingBytes(0),
            baseline + result->lease_.bytes().size());
  result->lease_.Reset();
  EXPECT_EQ(WorkerMemoryAccountingBytes(0), baseline);
  ExpectNoPageLeases();
}

TEST_F(LoadValueAwaitableTestPeer,
       ConcurrentExternalGroupedReadsProgressWithOnePageSlot) {
  value_.resize(4097);
  for (std::size_t i = 0; i < value_.size(); ++i)
    value_[i] = static_cast<char>(i % 251);
  ASSERT_LT(value_.size(), RegisteredBufferPoolOptions{}.read_payload_bytes());
  ASSERT_NO_FATAL_FAILURE(InstallExtent());
  auto first = ReadExternalGrouped();
  auto second = ReadExternalGrouped();
  Start(first);
  Start(second);
  EXPECT_FALSE(first.done());
  EXPECT_FALSE(second.done());
  ASSERT_NO_FATAL_FAILURE(Drain(first));
  auto first_result = std::move(first).TakeResult();
  ASSERT_TRUE(first_result.ok()) << first_result.status();
  EXPECT_EQ(first_result->lease_.buffer_id(), 0);
  // Even these small outputs must leave the sole fixed slot for extent I/O.
  // Retain the first output until the second read has also completed.
  ASSERT_NO_FATAL_FAILURE(Drain(second));
  auto second_result = std::move(second).TakeResult();
  ASSERT_TRUE(second_result.ok()) << second_result.status();
  EXPECT_EQ(second_result->lease_.buffer_id(), 0);
  ExpectValue(first_result);
  ExpectValue(second_result);
  ExpectNoPageLeases();
}

TEST_F(LoadValueAwaitableTestPeer, ExtentWithoutCrcSkipsPayloadCheck) {
  ASSERT_NO_FATAL_FAILURE(InstallExtent(true, true));
  std::array<std::byte, 2> output{};
  {
    auto read = ExtentSlice(ExtentReference(true), 0, output);
    auto task = AwaitExtent(read);
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    auto status = std::move(task).TakeResult();
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()),
                               output.size()),
              value_.substr(0, output.size()));
  }
  EXPECT_EQ(block_.pin_count(), 0);
}

TEST_F(LoadValueAwaitableTestPeer, RecordWithoutCrcWorksOnDiskAndInStaging) {
  // The encoder must clear this stale CRC when the persisted policy is off.
  header_.no_crc_ = true;
  header_.payload_checksum_ ^= 1;
  {
    auto read = Read();
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    ExpectValue(read.await_resume());
  }
  ASSERT_NO_FATAL_FAILURE(Stage());
  {
    auto read = Read();
    ASSERT_TRUE(read.await_ready());
    ExpectValue(read.await_resume());
  }
  EXPECT_EQ(block_.pin_count(), 0);
}

TEST_F(LoadValueAwaitableTestPeer, CompletionTransfersLeaseAndReleasesPin) {
  std::optional<Result> result;
  {
    auto read = Read();
    ASSERT_FALSE(read.await_ready());
    ASSERT_NO_FATAL_FAILURE(Complete(read));
    result = read.await_resume();
    ExpectValue(*result);
  }
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 0);
  result.reset();
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer, PopulationChangesDiscardCompletedRead) {
  for (bool database : {false, true}) {
    // Exercise valid/mismatched metadata at completion and the scheduling gap.
    for (int timing : {0, 1, 2}) {
      SCOPED_TRACE(testing::Message()
                   << "database=" << database << " timing=" << timing);
      {
        auto read = Read();
        if (timing != 2) Invalidate(database);
        if (timing == 1) ++header_.allocation_epoch_;
        ASSERT_NO_FATAL_FAILURE(Complete(read));
        if (timing == 1) --header_.allocation_epoch_;
        if (timing == 2) Invalidate(database);
        EXPECT_FALSE(HasHelper(read));
        EXPECT_TRUE(absl::IsNotFound(read.await_resume().status()));
      }
      EXPECT_EQ(block_.pin_count(), 0);
      EXPECT_EQ(Pool().available_read_buffers(), 1);
      if (database)
        ++header_.db_epoch_;
      else
        ++header_.replication_epoch_;
    }
  }
}

TEST_F(LoadValueAwaitableTestPeer, CandidatePopulationUsesMappedEpoch) {
  CandidatePopulation();
  auto read = Read();
  EXPECT_FALSE(HasHelper(read));
  ASSERT_NO_FATAL_FAILURE(Complete(read));
  ExpectValue(read.await_resume());
}

TEST_F(LoadValueAwaitableTestPeer, ErrorsReleaseResources) {
  for (int completion : {0, -EIO, 4096}) {
    {
      auto read = Read();
      ASSERT_FALSE(read.await_ready());
      header_.payload_checksum_ ^= 1;
      ASSERT_NO_FATAL_FAILURE(Complete(read, completion));
      header_.payload_checksum_ ^= 1;
      // A full CQE parses metadata only; CRC failure is discovered when the
      // caller resumes, not while the I/O poller is processing completions.
      if (completion == 4096) EXPECT_FALSE(read.await_ready());
      EXPECT_FALSE(read.await_resume().ok());
    }
    EXPECT_EQ(block_.pin_count(), 0);
    EXPECT_EQ(Pool().available_read_buffers(), 1);
  }
}

TEST_F(LoadValueAwaitableTestPeer, LocalReadReturnsStaleLocationToItsCaller) {
  block_.freeing_ = true;
  auto read = ReadLocal();
  ASSERT_TRUE(read.await_ready());
  EXPECT_FALSE(HasHelper(read));
  EXPECT_TRUE(absl::IsAborted(read.await_resume().status()));
}

TEST_F(LoadValueAwaitableTestPeer,
       RelocationRetriesBeforeResumingBackgroundCaller) {
  auto read = Read();
  auto task = AwaitCompletion(read);
  Start(task, true);
  ++header_.allocation_epoch_;
  ASSERT_NO_FATAL_FAILURE(Complete(read));
  EXPECT_FALSE(task.done());
  EXPECT_TRUE(HelperIsBackground(read));
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
  --header_.allocation_epoch_;
  // The new physical record has the same logical mutation and lives in staging.
  location_.metadata_ = RecordLocation::PackedMetadata::Encode(
      location_.record_offset() + 512, location_.total_disk_bytes(), 0, false,
      false, false, false, false, false, RecordKind::kValue,
      ValueType::kString);
  Stage();
  IndexLocation();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectValue(std::move(task).TakeResult());
}

TEST_F(LoadValueAwaitableTestPeer, UnchangedBadLocationIsCorruption) {
  IndexLocation();
  auto read = Read();
  auto task = AwaitCompletion(read);
  Start(task);
  ++header_.allocation_epoch_;
  ASSERT_NO_FATAL_FAILURE(Complete(read));
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsInternal(std::move(task).TakeResult().status()));
}

TEST_F(LoadValueAwaitableTestPeer, ConcurrentLogicalMutationIsNotRelocation) {
  auto read = Read();
  auto task = AwaitCompletion(read);
  Start(task);
  ++header_.mutation_sequence_;
  ASSERT_NO_FATAL_FAILURE(Complete(read));
  ++location_.mutation_sequence_;
  IndexLocation();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsNotFound(std::move(task).TakeResult().status()));
}

TEST_F(LoadValueAwaitableTestPeer,
       GroupedStringAssemblyLeavesTheSinglePageSlotAvailable) {
  // Include grouped lengths that fit in a fixed read slot: reserving that
  // slot for the output before loading a page would deadlock this fixture.
  for (std::size_t bytes : {2 * kStringGroupBytes, 2 * kStringGroupBytes + 17,
                            3 * kStringGroupBytes, 1024UL * 1024 + 3}) {
    SCOPED_TRACE(bytes);
    ASSERT_NO_FATAL_FAILURE(InstallGroupedString(bytes));
    for (bool pinned : {false, true}) {
      auto task = ReadGrouped(pinned);
      Start(task);
      ASSERT_NO_FATAL_FAILURE(Drain(task));
      auto result = std::move(task).TakeResult();
      ASSERT_TRUE(result.ok()) << result.status();
      ExpectValue(result);
      EXPECT_EQ(result->lease_.buffer_id(), 0);
      EXPECT_EQ(Pool().available_read_buffers(), 1);
      // A later read may reuse the page slot without invalidating the reply.
      auto next = ReadGrouped(pinned);
      Start(next);
      ASSERT_NO_FATAL_FAILURE(Drain(next));
      ExpectValue(std::move(next).TakeResult());
      ExpectValue(result);
    }
  }
}

TEST_F(LoadValueAwaitableTestPeer,
       GroupedStringAssemblyRejectsCorruptSegmentsAndReleasesSlots) {
  for (bool pinned : {false, true}) {
    for (int corruption = 0; corruption != 5; ++corruption) {
      SCOPED_TRACE(testing::Message()
                   << "pinned=" << pinned << " corruption=" << corruption);
      ASSERT_NO_FATAL_FAILURE(InstallGroupedString(2 * kStringGroupBytes + 17));
      // Corrupt the second page, after the first page has filled part of the
      // private output. Recompute CRC except for its dedicated failure case.
      auto& payload = grouped_payloads_[1];
      if (corruption == 0) payload[48] ^= 1;  // Segment byte count.
      if (corruption == 1) payload[40] ^= 1;  // Next link.
      if (corruption == 2) payload[16] ^= 1;  // Incarnation.
      if (corruption == 3) payload[24] ^= 1;  // Page ID.
      ASSERT_NO_FATAL_FAILURE(StageGroupedPage(1, corruption != 4));
      auto task = ReadGrouped(pinned);
      Start(task);
      ASSERT_NO_FATAL_FAILURE(Drain(task));
      const auto result = std::move(task).TakeResult();
      EXPECT_EQ(result.status().code(), corruption == 4
                                            ? absl::StatusCode::kInternal
                                            : absl::StatusCode::kDataLoss);
      EXPECT_EQ(Pool().available_read_buffers(), 1);
    }
  }
}

TEST_F(LoadValueAwaitableTestPeer,
       GroupedStringAssemblyRechecksPopulationAfterPageBufferWait) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires read buffer allocation failure injection";
#endif
  // Heap output assembly succeeds even when all dynamic storage allocations
  // fail. The population change races with a real fixed-slot page wait.
  lavik::test::ScopedReadBufferAllocationFailure failure;
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(2 * kStringGroupBytes + 17));
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  auto task = ReadGrouped(true);
  Start(task);
  EXPECT_FALSE(task.done());
  Invalidate(true);
  held->Reset();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsNotFound(std::move(task).TakeResult().status()));
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer,
       GroupedStringAssemblyRejectsOomBeforeTakingPageBuffer) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(2 * kStringGroupBytes));
  struct RestoreLimit {
    ~RestoreLimit() { (void)InitMemoryLimit(1024ULL * 1024 * 1024, 1); }
  } restore;
  ASSERT_TRUE(InitMemoryLimit(1, 1).ok());
  auto held = Pool().AcquireReadBuffer().await_resume();
  ASSERT_TRUE(held.ok());
  auto task = ReadGrouped(true);
  Start(task);
  ASSERT_TRUE(task.done());  // Admission fails before waiting on the held slot.
  EXPECT_TRUE(absl::IsResourceExhausted(std::move(task).TakeResult().status()));
  held->Reset();
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

TEST_F(LoadValueAwaitableTestPeer,
       ConcurrentGroupedStringReadsProgressWithOnePageSlot) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(35 * kStringGroupBytes + 17));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  auto first = ReadGrouped();
  auto second = ReadGrouped(true);
  Start(first);
  Start(second);
  EXPECT_FALSE(first.done());
  EXPECT_FALSE(second.done());
  ASSERT_NO_FATAL_FAILURE(Drain(first));
  auto first_result = std::move(first).TakeResult();
  ASSERT_TRUE(first_result.ok()) << first_result.status();
  EXPECT_EQ(first_result->lease_.buffer_id(), 0);
  // Retain the first reply while the other batch finishes. Completed page
  // reads must release their sole shared slot before their batch joins.
  ASSERT_NO_FATAL_FAILURE(Drain(second));
  auto second_result = std::move(second).TakeResult();
  ASSERT_TRUE(second_result.ok()) << second_result.status();
  EXPECT_EQ(second_result->lease_.buffer_id(), 0);
  ExpectValue(first_result);
  ExpectValue(second_result);
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), 1);
}

class GroupedStringParallelReadTest : public LoadValueAwaitableTestPeer {
 protected:
  std::size_t ReadBufferCount() const override { return 32; }
};

TEST_F(GroupedStringParallelReadTest, DiskPagesOverlapWithinTheOwnerWindow) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(35 * kStringGroupBytes + 17));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  // A wide scheduler slice admits the complete first window before returning
  // to the poller. RunOnce(false) flushes these reads at its end, so no CQE can
  // have released their physical pins when this round returns.
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGrouped();
  Start(task);
  worker_.RunOnce(false);
  EXPECT_FALSE(task.done());
  EXPECT_EQ(block_.pin_count(), 16);
  EXPECT_LT(Pool().available_read_bytes(), Pool().read_capacity_bytes());
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  // Distinct binary contents and a partial tail detect copying in completion
  // order or reusing an output offset across the following windows.
  ExpectValue(std::move(task).TakeResult());
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), ReadBufferCount());
}

TEST_F(GroupedStringParallelReadTest,
       ScarceScratchHeadroomFallsBackToSerialReads) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(35 * kStringGroupBytes + 17));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  const auto previous_shard = CurrentMemoryAccountingShard();
  absl::Cleanup restore = [&] {
    (void)InitMemoryLimit(1024ULL * 1024 * 1024, 1);
    BindMemoryAccountingShard(previous_shard == 0 ? kMaxMemoryWorkers
                                                  : previous_shard - 1);
  };
  BindMemoryAccountingShard(kMaxMemoryWorkers);
  RefreshMemoryStats();
  // Leave room for the roughly 280 KiB reply and one page of scratch, but
  // not sixteen page reads. An optional concurrency reservation must not turn
  // this otherwise admissible read into OOM.
  const auto target = GetMemoryStats().used_bytes_ + 384 * 1024;
  ASSERT_TRUE(InitMemoryLimit((target * 10 + 8) / 9, 1).ok());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGrouped();
  Start(task);
  worker_.RunOnce(false);
  EXPECT_FALSE(task.done());
  EXPECT_EQ(block_.pin_count(), 1);
  EXPECT_EQ(Pool().available_read_bytes(),
            Pool().read_capacity_bytes() - 20 * kKiB);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectValue(std::move(task).TakeResult());
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), ReadBufferCount());
}

TEST_F(GroupedStringParallelReadTest, ChildrenKeepTheirBackgroundTaskClass) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(35 * kStringGroupBytes + 17));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetBackgroundBudgetUs(1000000).ok());
  auto task = ReadGrouped(true);
  auto handle =
      std::move(task).operator co_await().await_suspend(std::noop_coroutine());
  worker_.RegisterBackground(handle);
  worker_.Enqueue(handle);
  (void)worker_.TakeSchedulerStats();
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectValue(std::move(task).TakeResult());
  const auto stats = worker_.TakeSchedulerStats();
  EXPECT_EQ(stats.foreground_resumes_, 0);
  EXPECT_GT(stats.background_resumes_, 16);
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), ReadBufferCount());
}

TEST_F(GroupedStringParallelReadTest,
       CorruptPageJoinsOtherStartedReadsBeforeReturning) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(35 * kStringGroupBytes + 17));
  ASSERT_NO_FATAL_FAILURE(StageGroupedPage(1, false));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGrouped(true);
  Start(task);
  worker_.RunOnce(false);
  EXPECT_EQ(block_.pin_count(), 16);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsInternal(std::move(task).TakeResult().status()));
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), ReadBufferCount());

  ASSERT_NO_FATAL_FAILURE(StageGroupedPage(1));
  block_.in_memory_ = true;
  auto next = ReadGrouped();
  Start(next);
  ASSERT_NO_FATAL_FAILURE(Drain(next));
  ExpectValue(std::move(next).TakeResult());
  EXPECT_EQ(Pool().available_read_buffers(), ReadBufferCount());
}

TEST_F(GroupedStringParallelReadTest, PartialStartFailureJoinsTheStartedRead) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires grouped String batch-start failure injection";
#endif
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(3 * kStringGroupBytes + 17));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  constexpr auto variable = "LAVIK_FAIL_STRING_READ_BATCH_START_KEY";
  const char* previous = std::getenv(variable);
  const auto saved =
      previous ? std::optional<std::string>(previous) : std::nullopt;
  const auto restore = [&] {
    if (saved)
      setenv(variable, saved->c_str(), 1);
    else
      unsetenv(variable);
  };
  absl::Cleanup restore_environment = restore;
  ASSERT_EQ(setenv(variable, key_.c_str(), 1), 0);
  auto task = ReadGrouped(true);
  Start(task);
  worker_.RunOnce(false);
  EXPECT_FALSE(task.done());
  EXPECT_EQ(block_.pin_count(), 1);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsResourceExhausted(std::move(task).TakeResult().status()));
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), ReadBufferCount());

  restore();
  auto next = ReadGrouped();
  Start(next);
  ASSERT_NO_FATAL_FAILURE(Drain(next));
  ExpectValue(std::move(next).TakeResult());
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(Pool().available_read_buffers(), ReadBufferCount());
}

class GroupedStringTwoOwnerReadTest : public GroupedStringParallelReadTest {
 protected:
  unsigned WorkerCount() const override { return 2; }
};

TEST_F(GroupedStringTwoOwnerReadTest, EachPhysicalOwnerHasItsOwnReadWindow) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(69 * kStringGroupBytes + 17));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGrouped();
  Start(task);
  worker_.RunOnce(false);
  bycorf::SetThisWorker(1, &cross_core_, &remote_worker_);
  remote_worker_.RunOnce(false);
  bycorf::SetThisWorker(0, &cross_core_, &worker_);
  // Both rings have flushed their first window, and neither has polled the
  // resulting read CQEs. A request-wide cap of 16 would strand the other
  // owner's capacity; removing owner limits would consume all 32 slots.
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count(), 16);
  EXPECT_EQ(blocks_[1].pin_count(), 16);
  EXPECT_LT(Pool(0).available_read_bytes(), Pool(0).read_capacity_bytes());
  EXPECT_LT(Pool(1).available_read_bytes(), Pool(1).read_capacity_bytes());
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectValue(std::move(task).TakeResult());
  EXPECT_EQ(blocks_[0].pin_count(), 0);
  EXPECT_EQ(blocks_[1].pin_count(), 0);
  EXPECT_EQ(Pool(0).available_read_buffers(), ReadBufferCount());
  EXPECT_EQ(Pool(1).available_read_buffers(), ReadBufferCount());
}

TEST_F(GroupedStringTwoOwnerReadTest,
       RelocationKeepsTheNewOwnerWithinItsReadWindow) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedString(69 * kStringGroupBytes + 17));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGrouped();
  Start(task);
  // The parent has already planned 16 children for each owner. Publish an
  // unchanged logical graph whose pages now all live on owner 1 before the
  // queued children refresh their routes, exercising its permit wait queue.
  ASSERT_NO_FATAL_FAILURE(RelocateGroupedPagesToRemoteOwner());
  worker_.RunOnce(false);
  bycorf::SetThisWorker(1, &cross_core_, &remote_worker_);
  remote_worker_.RunOnce(false);
  bycorf::SetThisWorker(0, &cross_core_, &worker_);
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count(), 0);
  EXPECT_EQ(blocks_[1].pin_count(), 16);
  EXPECT_EQ(Pool(0).available_read_buffers(), ReadBufferCount());
  EXPECT_LT(Pool(1).available_read_bytes(), Pool(1).read_capacity_bytes());
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectValue(std::move(task).TakeResult());
  EXPECT_EQ(blocks_[1].pin_count(), 0);
  EXPECT_EQ(Pool(1).available_read_buffers(), ReadBufferCount());
}

class GroupedCollectionTwoOwnerReadTest : public LoadValueAwaitableTestPeer {
 protected:
  std::size_t ReadBufferCount() const override { return 32; }
  unsigned WorkerCount() const override { return 2; }
};

class GroupedHashParallelReadTest
    : public GroupedCollectionTwoOwnerReadTest,
      public testing::WithParamInterface<std::tuple<ValueType, bool>> {};

TEST_P(GroupedHashParallelReadTest, EachOwnerReadsSixteenLeaves) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedHash(std::get<0>(GetParam())));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  const auto before = RetainedBytes();
  auto task = ReadGroupedHash(std::get<1>(GetParam()));
  Start(task);
  RunReadRound();
  EXPECT_FALSE(task.done());
  for (unsigned owner = 0; owner < WorkerCount(); ++owner) {
    EXPECT_EQ(blocks_[owner].pin_count(), 16);
    EXPECT_LT(Pool(owner).available_read_bytes(),
              Pool(owner).read_capacity_bytes());
  }
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  {
    auto result = std::move(task).TakeResult();
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(result->snapshot_ != nullptr, std::get<1>(GetParam()));
    ExpectHash(result);
    EXPECT_EQ(RetainedBytes(), before + result->retained_charge_.bytes());
  }
  EXPECT_EQ(RetainedBytes(), before);
  ExpectNoPageLeases();
}

TEST_P(GroupedHashParallelReadTest, CorruptLeafJoinsBothOwnersBeforeReturning) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedHash(std::get<0>(GetParam())));
  ASSERT_NO_FATAL_FAILURE(StageGroupedPage(1, false));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  const auto before = RetainedBytes();
  auto task = ReadGroupedHash(std::get<1>(GetParam()));
  Start(task);
  RunReadRound();
  EXPECT_EQ(blocks_[0].pin_count(), 16);
  EXPECT_EQ(blocks_[1].pin_count(), 16);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsInternal(std::move(task).TakeResult().status()));
  EXPECT_EQ(RetainedBytes(), before);
  ExpectNoPageLeases();
}

TEST_P(GroupedHashParallelReadTest, RelocatedLeavesShareTheNewOwnersWindow) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedHash(std::get<0>(GetParam())));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGroupedHash(std::get<1>(GetParam()));
  Start(task);
  ASSERT_NO_FATAL_FAILURE(RelocateGroupedPagesToRemoteOwner());
  RunReadRound();
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count(), 0);
  EXPECT_EQ(blocks_[1].pin_count(), 16);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectHash(std::move(task).TakeResult());
  ExpectNoPageLeases();
}

TEST_P(GroupedHashParallelReadTest,
       TerminalReadsReturnStoredFieldsWithoutRouteRehash) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedHash(std::get<0>(GetParam())));
  ASSERT_NO_FATAL_FAILURE(
      ReplaceHashField(1, hash_entries_[1].first, hash_entries_[0].first));
  hash_entries_[1].first = hash_entries_[0].first;
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ExpectStoredHashFields("Hash field outside its group route",
                         std::get<1>(GetParam()));
}

TEST_P(GroupedHashParallelReadTest,
       TerminalReadsReturnStoredDuplicatesWithoutDeduplication) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedHash(std::get<0>(GetParam()), 2));
  ASSERT_NO_FATAL_FAILURE(
      ReplaceHashField(1, hash_entries_[3].first, hash_entries_[2].first));
  hash_entries_[3].first = hash_entries_[2].first;
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ExpectStoredHashFields("duplicate field in Hash group",
                         std::get<1>(GetParam()));
}

TEST_P(GroupedHashParallelReadTest,
       MutableMaterializationStillBuildsProcessDigests) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedHash(std::get<0>(GetParam())));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  auto task = ReadMutableGroupedHash();
  Start(task);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  const auto result = std::move(task).TakeResult();
  ASSERT_TRUE(result.ok()) << result.status();
  ExpectHash(result);
  for (const auto& entry : result->entries_)
    EXPECT_EQ(entry.digest_, ComputeDigest(entry.field_));
  ExpectNoPageLeases();
}

INSTANTIATE_TEST_SUITE_P(HashAndSet, GroupedHashParallelReadTest,
                         testing::Combine(testing::Values(ValueType::kHash,
                                                          ValueType::kSet),
                                          testing::Bool()));

class GroupedHashSingleSlotReadTest
    : public LoadValueAwaitableTestPeer,
      public testing::WithParamInterface<std::tuple<ValueType, bool>> {};

TEST_P(GroupedHashSingleSlotReadTest,
       ConcurrentReadsReleaseEachLeafBeforeJoin) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedHash(std::get<0>(GetParam())));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  const auto before = RetainedBytes();
  auto first = ReadGroupedHash(std::get<1>(GetParam()));
  auto second = ReadGroupedHash(std::get<1>(GetParam()));
  Start(first);
  Start(second);
  ASSERT_NO_FATAL_FAILURE(Drain(first));
  {
    const auto first_result = std::move(first).TakeResult();
    ASSERT_TRUE(first_result.ok()) << first_result.status();
    EXPECT_EQ(first_result->snapshot_ != nullptr, std::get<1>(GetParam()));
    ASSERT_NO_FATAL_FAILURE(Drain(second));
    const auto second_result = std::move(second).TakeResult();
    ASSERT_TRUE(second_result.ok()) << second_result.status();
    ExpectHash(first_result);
    ExpectHash(second_result);
    EXPECT_EQ(RetainedBytes(), before + first_result->retained_charge_.bytes() +
                                   second_result->retained_charge_.bytes());
  }
  EXPECT_EQ(RetainedBytes(), before);
  ExpectNoPageLeases();
}

INSTANTIATE_TEST_SUITE_P(HashAndSet, GroupedHashSingleSlotReadTest,
                         testing::Combine(testing::Values(ValueType::kHash,
                                                          ValueType::kSet),
                                          testing::Bool()));

TEST_P(GroupedHashParallelReadTest, PartialStartFailureJoinsItsLeaf) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires grouped Hash batch-start failure injection";
#endif
  ASSERT_NO_FATAL_FAILURE(InstallGroupedHash(std::get<0>(GetParam())));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  constexpr auto variable = "LAVIK_FAIL_HASH_READ_BATCH_START_KEY";
  const char* previous = std::getenv(variable);
  const auto saved =
      previous ? std::optional<std::string>(previous) : std::nullopt;
  absl::Cleanup restore = [&] {
    if (saved)
      setenv(variable, saved->c_str(), 1);
    else
      unsetenv(variable);
  };
  ASSERT_EQ(setenv(variable, key_.c_str(), 1), 0);
  const auto before = RetainedBytes();
  auto task = ReadGroupedHash(std::get<1>(GetParam()));
  Start(task);
  RunReadRound();
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count(), 1);
  EXPECT_EQ(blocks_[1].pin_count(), 0);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsResourceExhausted(std::move(task).TakeResult().status()));
  EXPECT_EQ(RetainedBytes(), before);
  ExpectNoPageLeases();
  ASSERT_EQ(unsetenv(variable), 0);
  auto next = ReadGroupedHash(std::get<1>(GetParam()));
  Start(next);
  ASSERT_NO_FATAL_FAILURE(Drain(next));
  ExpectHash(std::move(next).TakeResult());
  ExpectNoPageLeases();
}

class GroupedListParallelReadTest : public GroupedCollectionTwoOwnerReadTest,
                                    public testing::WithParamInterface<bool> {};
class GroupedListSingleSlotReadTest : public LoadValueAwaitableTestPeer,
                                      public testing::WithParamInterface<bool> {
};

void ExpectListValues(const ListResult& result,
                      const std::vector<std::string>& expected, bool packed) {
  EXPECT_EQ(result.packed_values_ != nullptr, packed);
  ASSERT_EQ(result.value_count(), expected.size());
  for (std::size_t i = 0; i < expected.size(); ++i)
    EXPECT_EQ(result.value_view(i), expected[i]) << i;
  if (packed) EXPECT_TRUE(result.values_.empty());
}

TEST_P(GroupedListParallelReadTest,
       ListRangeUsesBothOwnersAndPreservesPartialPageRanks) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedList());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  const auto before = RetainedBytes();
  auto task = ReadGroupedList(1, -2, GetParam());
  Start(task);
  RunReadRound();
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count(), 16);
  EXPECT_EQ(blocks_[1].pin_count(), 16);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  {
    const auto result = std::move(task).TakeResult();
    ASSERT_TRUE(result.ok()) << result.status();
    const std::vector<std::string> expected(list_values_.begin() + 1,
                                            list_values_.end() - 1);
    ExpectListValues(*result, expected, GetParam());
    EXPECT_EQ(result->length_, list_values_.size());
    EXPECT_EQ(RetainedBytes(), before + result->retained_charge_.bytes());
  }
  EXPECT_EQ(RetainedBytes(), before);
  ExpectNoPageLeases();
}

TEST_P(GroupedListParallelReadTest,
       ListRangeFallsBackToSerialWhenWindowScratchDoesNotFit) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedList());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  const auto previous_shard = CurrentMemoryAccountingShard();
  absl::Cleanup restore = [&] {
    (void)InitMemoryLimit(1024ULL * 1024 * 1024, 1);
    BindMemoryAccountingShard(previous_shard == 0 ? kMaxMemoryWorkers
                                                  : previous_shard - 1);
  };
  BindMemoryAccountingShard(kMaxMemoryWorkers);
  RefreshMemoryStats();
  // The selected values and one page loader fit, while 32 concurrent page
  // tasks need more scratch. Preserve the existing read's admission floor.
  const auto target = GetMemoryStats().used_bytes_ + 128 * 1024;
  ASSERT_TRUE(InitMemoryLimit((target * 10 + 8) / 9, WorkerCount()).ok());
  auto task = ReadGroupedList(1, -2, GetParam());
  Start(task);
  RunReadRound();
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count() + blocks_[1].pin_count(), 1);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  const auto result = std::move(task).TakeResult();
  ASSERT_TRUE(result.ok()) << result.status();
  const std::vector<std::string> expected(list_values_.begin() + 1,
                                          list_values_.end() - 1);
  ExpectListValues(*result, expected, GetParam());
  ExpectNoPageLeases();
}

TEST_P(GroupedListParallelReadTest,
       CorruptListPageJoinsBothOwnersBeforeReturning) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedList());
  ASSERT_NO_FATAL_FAILURE(StageGroupedPage(1, false));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGroupedList(0, -1, GetParam());
  Start(task);
  RunReadRound();
  EXPECT_EQ(blocks_[0].pin_count(), 16);
  EXPECT_EQ(blocks_[1].pin_count(), 16);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsInternal(std::move(task).TakeResult().status()));
  ExpectNoPageLeases();
}

TEST_P(GroupedListSingleSlotReadTest,
       ConcurrentListRangesReleaseEachPageBeforeJoin) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedList());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  auto first = ReadGroupedList(0, -1, GetParam());
  auto second = ReadGroupedList(0, -1, GetParam());
  Start(first);
  Start(second);
  ASSERT_NO_FATAL_FAILURE(Drain(first));
  const auto first_result = std::move(first).TakeResult();
  ASSERT_TRUE(first_result.ok()) << first_result.status();
  ASSERT_NO_FATAL_FAILURE(Drain(second));
  const auto second_result = std::move(second).TakeResult();
  ASSERT_TRUE(second_result.ok()) << second_result.status();
  ExpectListValues(*first_result, list_values_, GetParam());
  ExpectListValues(*second_result, list_values_, GetParam());
  ExpectNoPageLeases();
}

TEST_P(GroupedListParallelReadTest, RelocationKeepsTheNewOwnersWindow) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedList());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGroupedList(0, -1, GetParam());
  Start(task);
  ASSERT_NO_FATAL_FAILURE(RelocateGroupedPagesToRemoteOwner());
  RunReadRound();
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count(), 0);
  EXPECT_EQ(blocks_[1].pin_count(), 16);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  const auto result = std::move(task).TakeResult();
  ASSERT_TRUE(result.ok()) << result.status();
  ExpectListValues(*result, list_values_, GetParam());
  ExpectNoPageLeases();
}

TEST_P(GroupedListParallelReadTest, PartialStartFailureJoinsItsPage) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires grouped List batch-start failure injection";
#endif
  ASSERT_NO_FATAL_FAILURE(InstallGroupedList());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  constexpr auto variable = "LAVIK_FAIL_LIST_READ_BATCH_START_KEY";
  const char* previous = std::getenv(variable);
  const auto saved =
      previous ? std::optional<std::string>(previous) : std::nullopt;
  absl::Cleanup restore = [&] {
    if (saved)
      setenv(variable, saved->c_str(), 1);
    else
      unsetenv(variable);
  };
  ASSERT_EQ(setenv(variable, key_.c_str(), 1), 0);
  const auto before = RetainedBytes();
  auto task = ReadGroupedList(0, -1, GetParam());
  Start(task);
  RunReadRound();
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count(), 1);
  EXPECT_EQ(blocks_[1].pin_count(), 0);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsResourceExhausted(std::move(task).TakeResult().status()));
  EXPECT_EQ(RetainedBytes(), before);
  ExpectNoPageLeases();
  ASSERT_EQ(unsetenv(variable), 0);
  auto next = ReadGroupedList(0, -1, GetParam());
  Start(next);
  ASSERT_NO_FATAL_FAILURE(Drain(next));
  const auto result = std::move(next).TakeResult();
  ASSERT_TRUE(result.ok()) << result.status();
  ExpectListValues(*result, list_values_, GetParam());
  ExpectNoPageLeases();
}

INSTANTIATE_TEST_SUITE_P(OwnedAndPacked, GroupedListParallelReadTest,
                         testing::Bool());
INSTANTIATE_TEST_SUITE_P(OwnedAndPacked, GroupedListSingleSlotReadTest,
                         testing::Bool());

class GroupedZsetParallelReadTest
    : public GroupedCollectionTwoOwnerReadTest,
      public testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(GroupedZsetParallelReadTest,
       RankPagesOverlapOnBothOwnersAndKeepPartialRanks) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGroupedZset({.kind_ = SortedSetOperationKind::kRange,
                               .reverse_ = std::get<0>(GetParam()),
                               .first_ = 1,
                               .last_ = -2,
                               .packed_output_ = std::get<1>(GetParam())});
  Start(task);
  // Children already enter through the scheduler's ready queue. Their first
  // slice submits the complete wave without an extra per-page Yield.
  RunReadRound();
  EXPECT_FALSE(task.done());
  for (unsigned owner = 0; owner < WorkerCount(); ++owner) {
    EXPECT_EQ(blocks_[owner].pin_count(), 16);
    EXPECT_LT(Pool(owner).available_read_bytes(),
              Pool(owner).read_capacity_bytes());
  }
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectZset(std::move(task).TakeResult(), 1, zset_values_.size() - 1,
             std::get<0>(GetParam()));
  ExpectNoPageLeases();
}

TEST_P(GroupedZsetParallelReadTest,
       ScoreBoundsAndLimitKeepCrossPageTiesInRequestedOrder) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  auto task = ReadGroupedZset({.kind_ = SortedSetOperationKind::kRange,
                               .range_mode_ = SortedSetRangeMode::kScore,
                               .reverse_ = std::get<0>(GetParam()),
                               .minimum_score_ = {2, true},
                               .maximum_score_ = {48, false},
                               .limit_ = true,
                               .offset_ = 5,
                               .count_ = 150,
                               .packed_output_ = std::get<1>(GetParam())});
  Start(task);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  // Matching ranks are [12, 196). OFFSET applies in traversal order, before
  // LIMIT, including ties split across the first and last selected pages.
  ExpectZset(std::move(task).TakeResult(), std::get<0>(GetParam()) ? 41 : 17,
             std::get<0>(GetParam()) ? 191 : 167, std::get<0>(GetParam()));
  ExpectNoPageLeases();
}

INSTANTIATE_TEST_SUITE_P(OwnedPackedForwardReverse, GroupedZsetParallelReadTest,
                         testing::Combine(testing::Bool(), testing::Bool()));

class GroupedZsetPackedReadTest : public GroupedCollectionTwoOwnerReadTest,
                                  public testing::WithParamInterface<bool> {
 protected:
  auto ReadGroupedZset(SortedSetOperation operation = {
                           .kind_ = SortedSetOperationKind::kRange}) {
    operation.packed_output_ = GetParam();
    return GroupedCollectionTwoOwnerReadTest::ReadGroupedZset(operation);
  }
};

class GroupedZsetSingleSlotReadTest : public LoadValueAwaitableTestPeer,
                                      public testing::WithParamInterface<bool> {
 protected:
  auto ReadGroupedZset() {
    return LoadValueAwaitableTestPeer::ReadGroupedZset(
        {.kind_ = SortedSetOperationKind::kRange,
         .packed_output_ = GetParam()});
  }
};

TEST_P(GroupedZsetPackedReadTest,
       ZsetRangeReturnsStoredDuplicatesWithoutDeduplication) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset(70, true));
  auto& payload = grouped_payloads_[1];
  ASSERT_EQ(zset_values_[3].first.size(), zset_values_[5].first.size());
  const auto offset = payload.find(zset_values_[3].first);
  ASSERT_NE(offset, std::string::npos);
  payload.replace(offset, zset_values_[3].first.size(), zset_values_[5].first);
  zset_values_[3].first = zset_values_[5].first;
  // The repeated member has different scores, so score/member order remains
  // valid. A repaired CRC isolates the read's uniqueness-validation policy.
  ASSERT_TRUE(absl::IsDataLoss(DecodeOrderedGroup(payload).status()));
  ASSERT_NO_FATAL_FAILURE(StageGroupedPage(1));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  for (const bool scarce : {false, true}) {
    SCOPED_TRACE(scarce ? "serial admission fallback" : "parallel batch");
    const auto previous_shard = CurrentMemoryAccountingShard();
    absl::Cleanup restore = [&] {
      (void)InitMemoryLimit(1024ULL * 1024 * 1024, WorkerCount());
      BindMemoryAccountingShard(previous_shard == 0 ? kMaxMemoryWorkers
                                                    : previous_shard - 1);
    };
    if (scarce) {
      BindMemoryAccountingShard(kMaxMemoryWorkers);
      RefreshMemoryStats();
      const auto target = GetMemoryStats().used_bytes_ + 128 * 1024;
      ASSERT_TRUE(InitMemoryLimit((target * 10 + 8) / 9, WorkerCount()).ok());
    }
    auto task = ReadGroupedZset();
    Start(task);
    RunReadRound();
    EXPECT_EQ(blocks_[0].pin_count() + blocks_[1].pin_count(), scarce ? 1 : 32);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ExpectZset(std::move(task).TakeResult());
    ExpectNoPageLeases();
  }
}

TEST_P(GroupedZsetPackedReadTest,
       ZsetRangeFallsBackToSerialWhenWindowScratchDoesNotFit) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  const auto previous_shard = CurrentMemoryAccountingShard();
  absl::Cleanup restore = [&] {
    (void)InitMemoryLimit(1024ULL * 1024 * 1024, 1);
    BindMemoryAccountingShard(previous_shard == 0 ? kMaxMemoryWorkers
                                                  : previous_shard - 1);
  };
  BindMemoryAccountingShard(kMaxMemoryWorkers);
  RefreshMemoryStats();
  const auto target = GetMemoryStats().used_bytes_ + 128 * 1024;
  ASSERT_TRUE(InitMemoryLimit((target * 10 + 8) / 9, WorkerCount()).ok());
  auto task = ReadGroupedZset();
  Start(task);
  RunReadRound();
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count() + blocks_[1].pin_count(), 1);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  ExpectZset(std::move(task).TakeResult());
  ExpectNoPageLeases();
}

TEST_P(GroupedZsetPackedReadTest,
       CorruptZsetPageJoinsBothOwnersBeforeReturning) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset());
  ASSERT_NO_FATAL_FAILURE(StageGroupedPage(1, false));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  auto task = ReadGroupedZset();
  Start(task);
  RunReadRound();
  EXPECT_EQ(blocks_[0].pin_count(), 16);
  EXPECT_EQ(blocks_[1].pin_count(), 16);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsInternal(std::move(task).TakeResult().status()));
  ExpectNoPageLeases();
}

TEST_P(GroupedZsetPackedReadTest, ZsetPartialStartFailureJoinsItsPage) {
#if !LAVIK_FAULTS_ENABLED
  GTEST_SKIP() << "requires grouped Zset batch-start failure injection";
#endif
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  constexpr auto variable = "LAVIK_FAIL_ZSET_READ_BATCH_START_KEY";
  const char* previous = std::getenv(variable);
  const auto saved =
      previous ? std::optional<std::string>(previous) : std::nullopt;
  absl::Cleanup restore = [&] {
    if (saved)
      setenv(variable, saved->c_str(), 1);
    else
      unsetenv(variable);
  };
  ASSERT_EQ(setenv(variable, key_.c_str(), 1), 0);
  auto task = ReadGroupedZset();
  Start(task);
  RunReadRound();
  EXPECT_FALSE(task.done());
  EXPECT_EQ(blocks_[0].pin_count(), 1);
  EXPECT_EQ(blocks_[1].pin_count(), 0);
  ASSERT_NO_FATAL_FAILURE(Drain(task));
  EXPECT_TRUE(absl::IsResourceExhausted(std::move(task).TakeResult().status()));
  ExpectNoPageLeases();
  ASSERT_EQ(unsetenv(variable), 0);
  auto next = ReadGroupedZset();
  Start(next);
  ASSERT_NO_FATAL_FAILURE(Drain(next));
  ExpectZset(std::move(next).TakeResult());
  ExpectNoPageLeases();
}

TEST_P(GroupedZsetSingleSlotReadTest,
       ConcurrentZsetRangesReleaseEachPageBeforeJoin) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  const auto before = RetainedBytes();
  auto first = ReadGroupedZset();
  auto second = ReadGroupedZset();
  Start(first);
  Start(second);
  ASSERT_NO_FATAL_FAILURE(Drain(first));
  {
    const auto first_result = std::move(first).TakeResult();
    ASSERT_TRUE(first_result.ok()) << first_result.status();
    EXPECT_EQ(first_result->packed_members_.has_value(), GetParam());
    ASSERT_NO_FATAL_FAILURE(Drain(second));
    const auto second_result = std::move(second).TakeResult();
    ASSERT_TRUE(second_result.ok()) << second_result.status();
    ExpectZset(first_result);
    ExpectZset(second_result);
    const auto retained = [](const SortedSetResult& result) {
      auto bytes = result.retained_charge_.bytes();
      for (const auto& member : result.members_)
        bytes += member.retained_charge_.bytes();
      return bytes;
    };
    EXPECT_EQ(RetainedBytes(),
              before + retained(*first_result) + retained(*second_result));
  }
  EXPECT_EQ(RetainedBytes(), before);
  ExpectNoPageLeases();
}

TEST_F(LoadValueAwaitableTestPeer,
       StagedZsetSerialScanCompletesWithoutExtraYield) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset());
  // ZCOUNT keeps the serial page path. Staging supplies every page without
  // disk I/O, so the scan can finish before returning to the worker loop.
  auto task = ReadGroupedZset(
      {.kind_ = SortedSetOperationKind::kCount,
       .range_mode_ = SortedSetRangeMode::kScore,
       .minimum_score_ = {-std::numeric_limits<double>::infinity(), false},
       .maximum_score_ = {std::numeric_limits<double>::infinity(), false}});
  Start(task);
  ASSERT_TRUE(task.done());
  const auto result = std::move(task).TakeResult();
  ASSERT_TRUE(result.ok()) << result.status();
  EXPECT_EQ(result->count_, zset_values_.size());
  ExpectNoPageLeases();
}

TEST_P(GroupedZsetPackedReadTest, ZsetLimitDoesNotReadAnUnneededFailingPage) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedZset());
  ASSERT_NO_FATAL_FAILURE(StageGroupedPage(2, false));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  for (const bool partial : {false, true}) {
    // Six members consume two complete pages. The exclusive lower bound
    // instead starts inside page two and LIMIT 1 ends there. In either case,
    // the corrupt third page matches the bounds but must remain unread.
    auto task = ReadGroupedZset({.kind_ = SortedSetOperationKind::kRange,
                                 .range_mode_ = SortedSetRangeMode::kScore,
                                 .minimum_score_ = {0, partial},
                                 .maximum_score_ = {partial ? 1. : 2., false},
                                 .limit_ = true,
                                 .count_ = partial ? 1 : 6});
    Start(task);
    ASSERT_NO_FATAL_FAILURE(Drain(task));
    ExpectZset(std::move(task).TakeResult(), partial ? 4 : 0, partial ? 5 : 6);
    ExpectNoPageLeases();
  }
}

INSTANTIATE_TEST_SUITE_P(OwnedAndPacked, GroupedZsetPackedReadTest,
                         testing::Bool());
INSTANTIATE_TEST_SUITE_P(OwnedAndPacked, GroupedZsetSingleSlotReadTest,
                         testing::Bool());

TEST_F(GroupedCollectionTwoOwnerReadTest,
       StreamRangeRefreshesPhysicalGraphAfterBoundaryReads) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedStream());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  auto open = ReadGroupedStream(UINT64_MAX, 4);
  Start(open);
  ASSERT_FALSE(open.done());
  ASSERT_EQ(block_.pin_count(), 1);
  // Finish the local lower-bound probe, but leave the remote upper-bound
  // probe queued. Its suspension permits GC to move the captured graph.
  const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (block_.pin_count() != 0 && std::chrono::steady_clock::now() < until)
    worker_.RunOnce(false);
  ASSERT_EQ(block_.pin_count(), 0);
  ASSERT_FALSE(open.done());
  ASSERT_NO_FATAL_FAILURE(RelocateGroupedPagesToRemoteOwner());
  auto version = grouped_->version();
  location_ = RecordLocation(
      2, location_.mutation_sequence_, 7, 0, location_.logical_size_,
      RecordLocation::PackedMetadata::Encode(
          location_.record_offset(), location_.total_disk_bytes(), 1, false,
          false, false, false, false, false, RecordKind::kValue,
          ValueType::kStream, false, true));
  version.root_ = location_;
  auto moved = GroupedObject::RelocateRoot(grouped_, version);
  ASSERT_TRUE(moved.ok()) << moved.status();
  ASSERT_NO_FATAL_FAILURE(PublishGroupedCollection(std::move(*moved)));
  // No reader pins remain on the source allocation. Retiring it makes an
  // attempt to materialize the old compact coordinates fail deterministically.
  block_.Reset(kUnownedBlock);
  ASSERT_NO_FATAL_FAILURE(Drain(open));
  auto result = std::move(open).TakeResult();
  ASSERT_TRUE(result.ok()) << result.status();
  auto source = std::move(*result);
  EXPECT_EQ(block_.pin_count(), 0);
  EXPECT_EQ(blocks_[1].pin_count(), 1);
  std::vector<std::string> actual;
  for (bool done = false; !done;) {
    auto read = source.reader_();
    Start(read);
    ASSERT_NO_FATAL_FAILURE(Drain(read));
    auto page = std::move(read).TakeResult();
    ASSERT_TRUE(page.ok()) << page.status();
    actual.insert(actual.end(), page->elements_.begin(), page->elements_.end());
    done = page->done_;
  }
  EXPECT_EQ(actual, (std::vector<std::string>{stream_messages_.begin(),
                                              stream_messages_.begin() + 4}));
  ASSERT_NO_FATAL_FAILURE(CloseTransfer(source));
}

TEST_F(GroupedCollectionTwoOwnerReadTest,
       StreamInteriorWindowReadsSixteenPagesOnEachOwner) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedStream());
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  ASSERT_TRUE(worker_.SetForegroundBudgetUs(1000000).ok());
  ASSERT_TRUE(remote_worker_.SetForegroundBudgetUs(1000000).ok());
  TransferValue source;
  {
    auto open = ReadGroupedStream();
    Start(open);
    ASSERT_NO_FATAL_FAILURE(Drain(open));
    auto result = std::move(open).TakeResult();
    ASSERT_TRUE(result.ok()) << result.status();
    source = std::move(*result);
  }
  ASSERT_TRUE(source.reader_);
  std::vector<std::string> actual;
  {
    auto boundary = source.reader_();
    Start(boundary);
    ASSERT_NO_FATAL_FAILURE(Drain(boundary));
    auto page = std::move(boundary).TakeResult();
    ASSERT_TRUE(page.ok()) << page.status();
    ASSERT_FALSE(page->done_);
    actual = std::move(page->elements_);
  }
  const std::array pins{blocks_[0].pin_count(), blocks_[1].pin_count()};
  // Snapshot graph pins survive the reader; count physical I/O pins above
  // that baseline so the test distinguishes 32 active reads from 16 total.
  {
    auto window = source.reader_();
    Start(window);
    RunReadRound();
    EXPECT_FALSE(window.done());
    for (unsigned owner = 0; owner < WorkerCount(); ++owner) {
      EXPECT_EQ(blocks_[owner].pin_count(), pins[owner] + 16);
      EXPECT_LT(Pool(owner).available_read_bytes(),
                Pool(owner).read_capacity_bytes());
    }
    ASSERT_NO_FATAL_FAILURE(Drain(window));
    auto page = std::move(window).TakeResult();
    ASSERT_TRUE(page.ok()) << page.status();
    EXPECT_EQ(page->elements_.size(), 32 * 3);
    actual.insert(actual.end(), page->elements_.begin(), page->elements_.end());
  }
  for (bool done = false; !done;) {
    auto next = source.reader_();
    Start(next);
    ASSERT_NO_FATAL_FAILURE(Drain(next));
    auto page = std::move(next).TakeResult();
    ASSERT_TRUE(page.ok()) << page.status();
    actual.insert(actual.end(), page->elements_.begin(), page->elements_.end());
    done = page->done_;
  }
  EXPECT_EQ(actual, stream_messages_);
  ASSERT_NO_FATAL_FAILURE(CloseTransfer(source));
}

TEST_F(GroupedCollectionTwoOwnerReadTest,
       StreamCountStopsBeforeAnUnneededCorruptInteriorPage) {
  ASSERT_NO_FATAL_FAILURE(InstallGroupedStream());
  ASSERT_NO_FATAL_FAILURE(StageGroupedPage(1, false));
  ASSERT_NO_FATAL_FAILURE(InstallGroupedDiskImage());
  TransferValue source;
  {
    auto open = ReadGroupedStream(1);
    Start(open);
    ASSERT_NO_FATAL_FAILURE(Drain(open));
    auto result = std::move(open).TakeResult();
    ASSERT_TRUE(result.ok()) << result.status();
    source = std::move(*result);
  }
  {
    auto read = source.reader_();
    Start(read);
    ASSERT_NO_FATAL_FAILURE(Drain(read));
    const auto page = std::move(read).TakeResult();
    ASSERT_TRUE(page.ok()) << page.status();
    EXPECT_TRUE(page->done_);
    ASSERT_EQ(page->elements_.size(), 1);
    EXPECT_EQ(page->elements_[0], stream_messages_[0]);
  }
  ASSERT_NO_FATAL_FAILURE(CloseTransfer(source));
}

}  // namespace lavik::storage
