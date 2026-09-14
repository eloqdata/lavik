#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "celer/net/server.h"
#include "keylane/memory.h"
#include "keylane/metrics.h"
#include "keylane/storage/detail/collection_compact_stream.h"
#include "keylane/storage/detail/grouped_commit.h"
#include "keylane/storage/engine.h"
#include "keylane/storage/format.h"
#include "keylane/tx/tx_shard.h"
#include "support/test_data_path.h"

namespace {

using keylane::storage::PartitionSnapshotBatch;
using keylane::storage::ReplicaPartitionEpoch;
using keylane::storage::ReplicaPartitionReset;
using keylane::storage::SnapshotRecord;
using keylane::storage::StorageEngine;
using keylane::storage::StorageEngineOptions;

constexpr std::size_t kMiB = 1024 * 1024;
constexpr std::size_t kExternalValueBytes = 10 * kMiB;
constexpr std::uint64_t kRetainedTolerance = 512 * 1024;
constexpr unsigned kStreamEntries = 1024;
constexpr std::size_t kTransferBytes = 2 * kMiB;

void Check(bool condition, std::string_view message) {
  if (!condition) throw std::runtime_error(std::string(message));
}

class ScopedDataFile {
 public:
  explicit ScopedDataFile(std::string path) : path_(std::move(path)) {}
  ~ScopedDataFile() {
    if (owned_) (void)::unlink(path_.c_str());
  }

  const std::string& path() const noexcept { return path_; }

  void Create(bool large = false) {
    const int fd =
        ::open(path_.c_str(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    Check(fd >= 0, "failed to create replica-reclaim test data file");
    // Large stress images are private sparse files, never real data devices.
    const int allocated = large
                              ? ::ftruncate(fd, std::uint64_t{8} * 1024 * kMiB)
                              : ::posix_fallocate(fd, 0, 256 * kMiB);
    const int closed = ::close(fd);
    Check(allocated == 0 && closed == 0,
          "failed to allocate replica-reclaim test data file");
    owned_ = true;
  }

 private:
  std::string path_;
  bool owned_ = false;
};

class ReplicaAbortReclaimService final : public celer::Service {
 public:
  explicit ReplicaAbortReclaimService(StorageEngine* storage,
                                      keylane::storage::ValueType large_type =
                                          keylane::storage::ValueType::kNone,
                                      bool verify_ingest = false)
      : storage_(storage),
        large_type_(large_type),
        verify_ingest_(verify_ingest) {}

  void Prepare(unsigned thread_count) override {
    Check(thread_count == 1, "replica-reclaim test requires one worker");
  }

  celer::Task<absl::Status> Run(celer::Worker& worker,
                                celer::ServiceContext) override {
    worker_ = &worker;
    keylane::BindMemoryAccountingShard(worker.id());
    keylane::tx::TxRuntime::Get()->shard(worker.id()).Bind(worker);
    result_ = co_await storage_->InitializeWorker(worker);
    if (verify_ingest_) {
      if (result_.ok()) result_ = co_await VerifyOrdinaryCollectionIngest();
    } else if (large_type_ != keylane::storage::ValueType::kNone) {
      if (result_.ok()) result_ = co_await ExerciseLargeCollection();
    } else {
      if (result_.ok()) result_ = co_await ExerciseRepeatedAbort();
      if (result_.ok()) result_ = co_await ExerciseRepeatedPromotion();
      if (result_.ok()) result_ = co_await ExerciseCollectionStreams();
      if (result_.ok()) result_ = co_await ExerciseOrdinaryCollectionIngest();
    }
    worker.RequestStop();
    co_return result_;
  }

  void Stop() noexcept override {}

  // Mirror the production RedisService lifecycle: release each WorkerStore on
  // its owning worker. Without this, final cleanup destroys owner-thread
  // state (e.g. LocalSharedPtr<GroupIndexNode>) on the main thread and trips
  // the owner-thread assertion.
  void FinalizeWorker(celer::Worker& worker) noexcept override {
    storage_->FinalizeWorker(worker);
  }

  const absl::Status& result() const noexcept { return result_; }

 private:
  celer::Task<absl::Status> VerifyOrdinaryCollectionIngest() {
    using namespace keylane::storage;
    for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                      ValueType::kSortedSet}) {
      const auto key = "ordinary-ingest-" + std::to_string(unsigned(type));
      auto value = co_await storage_->ReadRawValue(0, key);
      if (!value.ok()) co_return value.status();
      Check(
          value->value_type_ == type && value->logical_size_ == 258 &&
              value->encoded_.find(std::string(1024, 'b')) !=
                  std::string::npos &&
              value->encoded_.find(std::string(1024, 'x')) == std::string::npos,
          "cold recovery resurrected the failed ingest or lost its prefix");
      auto guard = co_await storage_->Get(0, key + "-guard");
      if (!guard.ok()) co_return guard.status();
      const auto bytes = guard->value_bytes();
      Check(std::string_view(reinterpret_cast<const char*>(bytes.data()),
                             bytes.size()) == "tail",
            "cold recovery lost the enclosing transaction's later command");
      std::cout << "ordinary ingest COLD prefix/abort PASS type="
                << unsigned(type) << std::endl;
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> ExerciseOrdinaryCollectionIngest() {
    using namespace keylane::storage;
    for (auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                      ValueType::kSortedSet}) {
      const auto key = "ordinary-ingest-" + std::to_string(unsigned(type));
      const auto digest = ComputeDigest(key);
      // Reverse scores deliberately exercise unordered RDB-style ZSet input.
      auto reader = [type](char fill, bool fail) -> CollectionPageReader {
        return [type, fill, fail, index = 0U]() mutable
                   -> celer::Task<absl::StatusOr<CollectionPage>> {
          if (index == 1 && fail)
            co_return absl::DataLossError("injected late collection page");
          CollectionPage page{.value_type_ = type, .done_ = index == 1};
          for (unsigned i = 0; i < 129; ++i) {
            const auto ordinal = index * 129 + i;
            auto name = std::to_string(ordinal);
            if (type == ValueType::kHash)
              page.fields_.push_back({name, std::string(8192, fill)});
            else if (type == ValueType::kSortedSet)
              page.scored_members_.push_back(
                  {name + std::string(8192, fill), double(258 - ordinal)});
            else
              page.elements_.push_back(name + std::string(8192, fill));
          }
          ++index;
          co_return std::move(page);
        };
      };
      auto initial = co_await storage_->RestoreCollectionValue(
          0, key, type, 0, true, 258, reader('a', false));
      if (!initial.ok()) co_return initial.status();
      TxShardWrites writes;
      const auto txid = StorageEngine::AllocateWriteTxid();
      storage_->InitializeTxWrites(txid, std::span(&writes, 1));
      writes.collect_undo_ = true;
      auto hold = co_await keylane::tx::CurrentTxShard().AcquireKey(
          0, keylane::tx::FingerprintOf(digest),
          keylane::tx::LockMode::kExclusive);
      // A successful earlier write to this exact key must survive a later
      // streamed command failure inside the same EXEC/Lua accumulator.
      auto prefix = co_await storage_->RestoreCollectionValueLocked(
          0, key, digest, type, 0, true, 258, reader('b', false), &writes);
      if (!prefix.ok()) co_return prefix.status();
      const auto guard_key = key + "-guard";
      const auto guard_digest = ComputeDigest(guard_key);
      auto guard_hold = co_await keylane::tx::CurrentTxShard().AcquireKey(
          0, keylane::tx::FingerprintOf(guard_digest),
          keylane::tx::LockMode::kExclusive);
      auto guard = co_await storage_->SetLocked(0, guard_key, guard_digest,
                                                "prefix", {}, &writes);
      if (!guard.ok()) co_return guard.status();
      auto failed = co_await storage_->RestoreCollectionValueLocked(
          0, key, digest, type, 0, true, 258, reader('x', true), &writes);
      Check(!failed.ok(), "late ordinary ingest page unexpectedly succeeded");
      Check(StorageEngine::ValidateTxCommit(std::span(&writes, 1)).ok(),
            "recoverable ingest failure poisoned its outer transaction");
      auto tail = co_await storage_->SetLocked(0, guard_key, guard_digest,
                                               "tail", {}, &writes);
      if (!tail.ok()) co_return tail.status();
      auto committed = co_await storage_->CommitTxWrites(txid, {&writes});
      if (!committed.ok()) co_return committed;
      committed = co_await storage_->DiscardTxUndoLocal(txid);
      if (!committed.ok()) co_return committed;
      guard_hold.Reset();
      hold.Reset();
      auto value = co_await storage_->ReadRawValue(0, key);
      if (!value.ok()) co_return value.status();
      Check(
          value->value_type_ == type && value->logical_size_ == 258 &&
              value->encoded_.find(std::string(1024, 'b')) !=
                  std::string::npos &&
              value->encoded_.find(std::string(1024, 'x')) == std::string::npos,
          "ordinary ingest abort lost its earlier grouped prefix");
      auto guard_value = co_await storage_->Get(0, guard_key);
      if (!guard_value.ok()) co_return guard_value.status();
      const auto guard_bytes = guard_value->value_bytes();
      Check(std::string_view(reinterpret_cast<const char*>(guard_bytes.data()),
                             guard_bytes.size()) == "tail",
            "outer suffix command was lost");
      std::cout << "ordinary ingest prefix/abort PASS type=" << unsigned(type)
                << std::endl;

      const auto failed_key = key + "-failed-decision";
      const auto failed_digest = ComputeDigest(failed_key);
      TxShardWrites rejected;
      const auto rejected_txid = StorageEngine::AllocateWriteTxid();
      storage_->InitializeTxWrites(rejected_txid, std::span(&rejected, 1));
      rejected.collect_undo_ = true;
      auto failed_hold = co_await keylane::tx::CurrentTxShard().AcquireKey(
          0, keylane::tx::FingerprintOf(failed_digest),
          keylane::tx::LockMode::kExclusive);
      const std::uint64_t tentative_expiry =
          type == ValueType::kSortedSet
              ? std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                        .count() +
                    60'000
              : 0;
      auto published = co_await storage_->RestoreCollectionValueLocked(
          0, failed_key, failed_digest, type, tentative_expiry, true, 258,
          reader('f', false), &rejected);
      if (!published.ok()) co_return published.status();
      Check(rejected.grouped_decision_ != nullptr,
            "published grouped root has no outer decision");
      rejected.grouped_decision_->FailPending();
      failed_hold.Reset();
      auto metadata = co_await storage_->ReadKeyMetadata(0, failed_key);
      Check(absl::IsFailedPrecondition(metadata.status()),
            "failed grouped decision exposed TYPE/TTL/EXISTS metadata");
      // Count shortcuts are data reads too: none may expose the partial
      // cardinality after a post-publication outer decision failure.
      absl::Status count_status;
      if (type == ValueType::kHash) {
        const auto count = co_await storage_->ExecuteHash(0, failed_key, {});
        count_status = count.status();
      } else if (type == ValueType::kSet) {
        const auto count = co_await storage_->ExecuteSet(0, failed_key, {});
        count_status = count.status();
      } else if (type == ValueType::kList) {
        const auto count = co_await storage_->ExecuteList(0, failed_key, {});
        count_status = count.status();
      } else {
        const auto count =
            co_await storage_->ExecuteSortedSet(0, failed_key, {});
        count_status = count.status();
      }
      Check(absl::IsFailedPrecondition(count_status),
            "failed grouped decision exposed a cardinality");
      auto leaked = co_await storage_->ReadRawValue(0, failed_key);
      Check(absl::IsFailedPrecondition(leaked.status()),
            "failed grouped decision exposed its payload");
      if (type == ValueType::kSortedSet) {
        // Legacy read callbacks must not see either the partial collection or
        // a fabricated absent value when that failed root's tentative TTL
        // expires. Passing the observation clock exercises both branches
        // without sleeping or committing a TTL-only mutation to the bad root.
        for (const auto now : {std::uint64_t{0}, tentative_expiry + 1}) {
          bool callback_ran = false;
          const auto legacy = co_await storage_->ExecuteCompact(
              0, failed_key, type, true,
              [&](std::optional<CompactValueView>)
                  -> absl::StatusOr<CompactValueUpdate> {
                callback_ran = true;
                return CompactValueUpdate{};
              },
              now);
          Check(absl::IsFailedPrecondition(legacy) && !callback_ran,
                "legacy callback observed failed or tentatively expired root");
        }
      }
      failed_hold = co_await keylane::tx::CurrentTxShard().AcquireKey(
          0, keylane::tx::FingerprintOf(failed_digest),
          keylane::tx::LockMode::kExclusive);
      auto cleaned = co_await storage_->RollbackTxLocal(rejected_txid);
      if (!cleaned.ok()) co_return cleaned;
      failed_hold.Reset();
      Check(!co_await storage_->Exists(0, failed_key),
            "read rejection prevented failed graph cleanup");
      std::cout << "failed grouped decision READ GUARD PASS type="
                << unsigned(type) << std::endl;
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> ExerciseLargeCollection() {
    using namespace keylane::storage;
    constexpr std::uint64_t count = 140000;
    constexpr std::size_t item_bytes = 8192;
    constexpr std::uint64_t session = 9001;
    const std::string key = "native-many-small-pages";
    const auto partition = RedisSlot(key);
    auto reset = co_await ResetFullRoot(session);
    if (!reset.ok()) co_return reset.status();
    const auto epoch = (*reset)[partition].replication_epoch_;
    std::uint64_t encoded_bytes = large_type_ == ValueType::kHash ? 32 : 8;
    for (std::uint64_t i = 0; i < count; ++i)
      encoded_bytes += large_type_ == ValueType::kHash
                           ? 8 + std::to_string(i).size() + item_bytes
                           : 4 + item_bytes;
    Check(encoded_bytes > std::uint64_t{1024} * kMiB,
          "large fixture must exceed the old aggregate limit");
    auto encoder =
        CollectionCompactEncoder::Create(large_type_, count, encoded_bytes);
    if (!encoder.ok()) co_return encoder.status();
    SnapshotRecord frame{.kind_ = SnapshotRecord::Kind::kValueBegin,
                         .db_id_ = 0,
                         .db_epoch_ = storage_->DbEpoch(0),
                         .mutation_sequence_ = 1,
                         .value_type_ = large_type_,
                         .logical_size_ = encoded_bytes,
                         .chunk_count_ = static_cast<std::uint32_t>(
                             (encoded_bytes - 1) / kTransferBytes + 1),
                         .key_ = key,
                         .value_ = std::string(8, '\0')};
    for (unsigned byte = 0; byte < 8; ++byte)
      frame.value_[byte] = static_cast<char>(count >> (byte * 8));
    auto apply = [&]() {
      return storage_->ApplyReplicaRecords(session, partition, epoch,
                                           std::span(&frame, 1));
    };
    const auto started = std::chrono::steady_clock::now();
    auto status = co_await apply();
    if (!status.ok()) co_return status;
    frame.kind_ = SnapshotRecord::Kind::kValueChunk;
    frame.value_.clear();
    frame.value_.reserve(kTransferBytes);
    auto flush = [&]() -> celer::Task<absl::Status> {
      if (frame.value_.empty()) co_return absl::OkStatus();
      const auto written = co_await apply();
      if (!written.ok()) {
        std::cerr << "large target failed at chunk " << frame.chunk_index_
                  << ": " << written << '\n';
        co_return written;
      }
      ++frame.chunk_index_;
      frame.value_.clear();
      keylane::RefreshMemoryStats();
      if (frame.chunk_index_ % 64 == 0) {
        const auto memory = keylane::GetMemoryStats();
        std::cout << "large target chunks=" << frame.chunk_index_
                  << " retained=" << memory.used_bytes_
                  << " retained_peak=" << memory.peak_used_bytes_
                  << " rss=" << memory.rss_bytes_ << std::endl;
      }
      co_return absl::OkStatus();
    };
    for (std::uint64_t first = 0; first < count; first += 128) {
      CollectionPage page{.value_type_ = large_type_};
      for (auto i = first; i < std::min(count, first + 128); ++i) {
        if (large_type_ == ValueType::kHash)
          page.fields_.push_back(
              {std::to_string(i), std::string(item_bytes, 'v')});
        else
          page.elements_.push_back(std::string(item_bytes, 'v'));
      }
      status = encoder->StartPage(page);
      if (!status.ok()) co_return status;
      while (auto encoded = encoder->Next()) {
        auto remaining = *encoded;
        while (!remaining.empty()) {
          const auto bytes =
              std::min(kTransferBytes - frame.value_.size(), remaining.size());
          frame.value_.append(remaining.substr(0, bytes));
          remaining.remove_prefix(bytes);
          if (frame.value_.size() == kTransferBytes) {
            status = co_await flush();
            if (!status.ok()) co_return status;
          }
        }
      }
    }
    status = encoder->Finish();
    if (!status.ok()) co_return status;
    status = co_await flush();
    if (!status.ok()) co_return status;
    frame.kind_ = SnapshotRecord::Kind::kValueCommit;
    status = co_await apply();
    if (!status.ok()) co_return status;
    status = co_await HandoffAll(session, *reset);
    if (!status.ok()) co_return status;
    status = co_await storage_->PromoteReplicaRoot(session);
    if (!status.ok()) co_return status;
    storage_->SetReplicaLoading(false);
    // Verify point access without assembling a >1 GiB RawValue.
    if (large_type_ == ValueType::kHash) {
      auto length = co_await storage_->ExecuteHash(0, key, HashOperation{});
      if (!length.ok()) co_return length.status();
      Check(length->length_ == count, "large Hash cardinality mismatch");
      const auto last = std::to_string(count - 1);
      auto value = co_await storage_->ExecuteHash(
          0, key,
          HashOperation{.kind_ = HashOperationKind::kGet, .fields_ = {last}});
      if (!value.ok()) co_return value.status();
      Check(value->values_.size() == 1 && value->values_[0] &&
                *value->values_[0] == std::string(item_bytes, 'v'),
            "large Hash last field mismatch");
    } else {
      auto length = co_await storage_->ExecuteList(0, key, ListOperation{});
      if (!length.ok()) co_return length.status();
      Check(length->length_ == count, "large List cardinality mismatch");
      auto value = co_await storage_->ExecuteList(
          0, key,
          ListOperation{.kind_ = ListOperationKind::kIndex, .first_ = -1});
      if (!value.ok()) co_return value.status();
      Check(value->values_ ==
                std::vector<std::string>{std::string(item_bytes, 'v')},
            "large List last item mismatch");
    }
    const auto copy_key = key + "-copy";
    {
      const auto digest = ComputeDigest(key);
      const auto copy_digest = ComputeDigest(copy_key);
      auto source_hold = co_await keylane::tx::CurrentTxShard().AcquireKey(
          0, keylane::tx::FingerprintOf(digest),
          keylane::tx::LockMode::kExclusive);
      auto target_hold = co_await keylane::tx::CurrentTxShard().AcquireKey(
          0, keylane::tx::FingerprintOf(copy_digest),
          keylane::tx::LockMode::kExclusive);
      auto source =
          co_await storage_->ReadValueForTransferLocked(0, key, digest);
      if (!source.ok()) co_return source.status();
      Check(bool(source->reader_), "large transfer expanded its entire source");
      status = co_await storage_->WriteValueForTransferLocked(
          0, copy_key, copy_digest, *source);
      if (!status.ok()) co_return status;
    }
    if (large_type_ == ValueType::kHash) {
      auto length =
          co_await storage_->ExecuteHash(0, copy_key, HashOperation{});
      if (!length.ok()) co_return length.status();
      Check(length->length_ == count, "large copied Hash cardinality mismatch");
      const auto last = std::to_string(count - 1);
      auto value = co_await storage_->ExecuteHash(
          0, copy_key,
          HashOperation{.kind_ = HashOperationKind::kGet, .fields_ = {last}});
      if (!value.ok()) co_return value.status();
      Check(value->values_.size() == 1 && value->values_[0] &&
                *value->values_[0] == std::string(item_bytes, 'v'),
            "large copied Hash last field mismatch");
    } else {
      auto length =
          co_await storage_->ExecuteList(0, copy_key, ListOperation{});
      if (!length.ok()) co_return length.status();
      Check(length->length_ == count, "large copied List cardinality mismatch");
      auto value = co_await storage_->ExecuteList(
          0, copy_key,
          ListOperation{.kind_ = ListOperationKind::kIndex, .first_ = -1});
      if (!value.ok()) co_return value.status();
      Check(value->values_ ==
                std::vector<std::string>{std::string(item_bytes, 'v')},
            "large copied List last item mismatch");
    }
    keylane::RefreshMemoryStats();
    const auto memory = keylane::GetMemoryStats();
    const auto seconds = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - started)
                             .count();
    std::cout << "large target+ordinary transfer PASS type="
              << unsigned(large_type_) << " entries=" << count
              << " encoded_bytes=" << encoded_bytes << " seconds=" << seconds
              << " retained=" << memory.used_bytes_
              << " retained_peak=" << memory.peak_used_bytes_
              << " rss=" << memory.rss_bytes_ << std::endl;
    co_return absl::OkStatus();
  }
  static std::string CollectionBytes(keylane::storage::ValueType type,
                                     char fill) {
    using namespace keylane::storage;
    CollectionPage page{.value_type_ = type};
    for (unsigned i = 0; i < kStreamEntries; ++i) {
      const auto name = std::to_string(i);
      if (type == ValueType::kHash)
        page.fields_.push_back({name, std::string(2048, fill)});
      else if (type == ValueType::kSortedSet)
        page.scored_members_.push_back(
            {name + std::string(2048, fill), double(i)});
      else
        page.elements_.push_back(name + std::string(2048, fill));
    }
    auto measured = CollectionCompactEncoder::MeasurePage(page);
    Check(measured.ok(), "collection fixture measurement failed");
    const auto header =
        type == ValueType::kHash || type == ValueType::kSet ? 32 : 8;
    auto encoder =
        CollectionCompactEncoder::Create(type, page.size(), *measured + header);
    Check(encoder.ok() && encoder->StartPage(page).ok(),
          "collection fixture encoding failed");
    std::string result;
    while (auto part = encoder->Next()) result.append(*part);
    Check(encoder->Finish().ok(), "collection fixture encoding did not finish");
    return result;
  }

  celer::Task<absl::Status> SendCollection(std::uint64_t session,
                                           std::uint64_t epoch,
                                           std::string_view key,
                                           keylane::storage::ValueType type,
                                           std::uint64_t sequence,
                                           unsigned mode, char fill = 'v') {
    const auto encoded = CollectionBytes(type, fill);
    SnapshotRecord frame{.kind_ = SnapshotRecord::Kind::kValueBegin,
                         .db_id_ = 0,
                         .db_epoch_ = storage_->DbEpoch(0),
                         .mutation_sequence_ = sequence,
                         .value_type_ = type,
                         .logical_size_ = encoded.size(),
                         .chunk_count_ = static_cast<std::uint32_t>(
                             (encoded.size() - 1) / kTransferBytes + 1),
                         .key_ = std::string(key),
                         .value_ = std::string(8, '\0')};
    for (unsigned byte = 0; byte < 8; ++byte)
      frame.value_[byte] =
          static_cast<char>(std::uint64_t{kStreamEntries} >> (8 * byte));
    const auto partition = keylane::storage::RedisSlot(key);
    auto apply = [&]() {
      return storage_->ApplyReplicaRecords(session, partition, epoch,
                                           std::span(&frame, 1));
    };
    auto status = co_await apply();
    if (!status.ok()) co_return status;
    frame.kind_ = SnapshotRecord::Kind::kValueChunk;
    // Cross both transport and bounded transaction-batch boundaries so the
    // abort cases exercise squashed intermediate graphs, not only one root.
    for (std::size_t offset = 0; offset < encoded.size();
         offset += kTransferBytes, ++frame.chunk_index_) {
      auto bytes = std::min(kTransferBytes, encoded.size() - offset);
      if (mode == 2 && offset + bytes == encoded.size()) --bytes;
      frame.value_ = encoded.substr(offset, bytes);
      status = co_await apply();
      if (!status.ok()) co_return status;
    }
    if (mode == 1) co_return status;
    frame.kind_ = SnapshotRecord::Kind::kValueCommit;
    frame.value_.clear();
    co_return co_await apply();
  }

  celer::Task<absl::Status> ExerciseCollectionStreams() {
    using namespace keylane::storage;
    std::uint64_t session = 1200;
    for (const auto type : {ValueType::kHash, ValueType::kSet, ValueType::kList,
                            ValueType::kSortedSet}) {
      const std::string key =
          "native-collection-" + std::to_string(unsigned(type));
      const auto partition = RedisSlot(key);
      for (unsigned mode = 0; mode < 3; ++mode) {
        ++session;
        std::vector<ReplicaPartitionEpoch> epochs;
        if (mode == 0) {
          // Import one partition before resetting the others. A worker-global
          // index generation change must not invalidate this grouped graph.
          ReplicaPartitionReset first{.partition_id_ = partition};
          for (std::uint8_t db = 0; db < kLogicalDatabaseCount; ++db)
            first.db_epochs_[db] = storage_->DbEpoch(db);
          auto reset = co_await storage_->ResetReplicaPartitions(
              session, std::span(&first, 1));
          if (!reset.ok()) co_return reset.status();
          epochs = std::move(*reset);
        } else {
          auto reset = co_await ResetFullRoot(session);
          if (!reset.ok()) co_return reset.status();
          epochs = std::move(*reset);
        }
        const auto epoch = mode == 0 ? epochs.front().replication_epoch_
                                     : epochs[partition].replication_epoch_;
        if (mode == 2) {
          const auto seeded =
              co_await SendCollection(session, epoch, key, type, 1, 0);
          if (!seeded.ok()) co_return seeded;
        }
        const auto sent = co_await SendCollection(session, epoch, key, type,
                                                  mode == 2 ? 2 : 1, mode,
                                                  mode == 2 ? 'x' : 'v');
        if (mode == 2) {
          Check(!sent.ok(),
                "truncated collection stream unexpectedly committed");
          auto restored = co_await storage_->ReadRawValue(0, key);
          if (!restored.ok()) co_return restored.status();
          Check(restored->value_type_ == type &&
                    restored->logical_size_ == kStreamEntries,
                "failed collection stream did not restore its predecessor");
          Check(restored->encoded_.find(std::string(1024, 'v')) !=
                        std::string::npos &&
                    restored->encoded_.find(std::string(1024, 'x')) ==
                        std::string::npos,
                "failed collection stream exposed replacement bytes");
        } else if (!sent.ok()) {
          co_return sent;
        }
        if (mode != 0) {
          const auto handoff = co_await storage_->HandoffReplicaPartition(
              session, partition, epoch);
          Check(!handoff.ok(),
                "incomplete/failed collection was eligible for handoff");
          const auto aborted = co_await storage_->AbortReplicaRoot(session);
          if (!aborted.ok()) co_return aborted;
          Check(!co_await storage_->Exists(0, key),
                "aborted collection remained visible");
        } else {
          std::vector<ReplicaPartitionReset> rest;
          for (std::uint16_t id = 0; id < kLogicalStorageShards; ++id) {
            if (id == partition) continue;
            ReplicaPartitionReset reset{.partition_id_ = id};
            for (std::uint8_t db = 0; db < kLogicalDatabaseCount; ++db)
              reset.db_epochs_[db] = storage_->DbEpoch(db);
            rest.push_back(reset);
          }
          auto reset = co_await storage_->ResetReplicaPartitions(session, rest);
          if (!reset.ok()) co_return reset.status();
          epochs.insert(epochs.end(), reset->begin(), reset->end());
          auto ready = co_await HandoffAll(session, epochs);
          if (!ready.ok()) co_return ready;
          ready = co_await storage_->PromoteReplicaRoot(session);
          if (!ready.ok()) co_return ready;
          auto value = co_await storage_->ReadRawValue(0, key);
          if (!value.ok()) co_return value.status();
          Check(value->value_type_ == type &&
                    value->logical_size_ == kStreamEntries,
                "grouped target was invalid after split reset/promotion");
        }
        storage_->SetReplicaLoading(false);
      }
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::StatusOr<PartitionSnapshotBatch>> PinActiveExternalValue(
      std::uint64_t session_id, std::uint8_t db_id, std::string_view key,
      char fill) {
    auto written = co_await storage_->Set(
        db_id, key, std::string(kExternalValueBytes, fill), {});
    if (!written.ok()) co_return written.status();

    const std::uint16_t partition = keylane::storage::RedisSlot(key);
    auto session = storage_->BeginFullSyncSession(session_id);
    if (!session.ok()) co_return session.status();
    auto start = storage_->BeginPartitionReplication(session_id, partition);
    if (!start.ok()) co_return start.status();
    absl::Status db =
        storage_->BeginPartitionDbReplication(session_id, partition, db_id);
    if (!db.ok()) co_return db;
    auto batch = co_await storage_->SnapshotPartition(
        session_id, partition, db_id, 0, 1, 1,
        keylane::storage::kReplicationTransferBytes);
    if (!batch.ok()) co_return batch.status();
    if (batch->records_.size() != 1 || batch->records_.front().key_ != key ||
        batch->records_.front().source_id_ == 0) {
      co_return absl::FailedPreconditionError(
          "reclaim test did not pin the active external value");
    }
    co_return std::move(*batch);
  }

  void ReleasePinnedValue(std::uint64_t session_id, std::string_view key,
                          const PartitionSnapshotBatch& batch) {
    const std::uint16_t partition = keylane::storage::RedisSlot(key);
    storage_->AcknowledgePartitionSnapshotRecords(session_id, partition,
                                                  batch.records_);
    storage_->EndPartitionReplication(session_id, partition);
    storage_->EndFullSyncSession(session_id);
  }

  celer::Task<absl::StatusOr<std::vector<ReplicaPartitionEpoch>>> ResetFullRoot(
      std::uint64_t session_id) {
    std::array<std::uint64_t, keylane::storage::kLogicalDatabaseCount>
        source_db_epochs{};
    for (std::uint8_t db_id = 0;
         db_id < keylane::storage::kLogicalDatabaseCount; ++db_id) {
      source_db_epochs[db_id] = storage_->DbEpoch(db_id);
    }
    std::vector<ReplicaPartitionReset> resets;
    resets.reserve(keylane::storage::kLogicalStorageShards);
    for (std::uint16_t partition = 0;
         partition < keylane::storage::kLogicalStorageShards; ++partition) {
      resets.push_back(ReplicaPartitionReset{
          .partition_id_ = partition,
          .db_epochs_ = source_db_epochs,
      });
    }
    co_return co_await storage_->ResetReplicaPartitions(session_id, resets);
  }

  celer::Task<absl::Status> ApplyCandidate(
      std::uint64_t session_id,
      std::span<const ReplicaPartitionEpoch> partition_epochs,
      std::uint8_t db_id, std::string_view key, char fill) {
    const std::uint16_t partition = keylane::storage::RedisSlot(key);
    Check(partition_epochs.size() == keylane::storage::kLogicalStorageShards,
          "replica reset did not cover the full root");
    Check(partition_epochs[partition].partition_id_ == partition,
          "replica reset epochs are not partition ordered");
    SnapshotRecord candidate{
        .kind_ = SnapshotRecord::Kind::kValue,
        .db_id_ = db_id,
        .db_epoch_ = storage_->DbEpoch(db_id),
        .mutation_sequence_ = 1,
        .value_type_ = keylane::storage::ValueType::kString,
        .logical_size_ = kExternalValueBytes,
        .key_ = std::string(key),
        .value_ = std::string(kExternalValueBytes, fill),
    };
    co_return co_await storage_->ApplyReplicaRecords(
        session_id, partition, partition_epochs[partition].replication_epoch_,
        std::span(&candidate, 1));
  }

  celer::Task<absl::Status> HandoffAll(
      std::uint64_t session_id,
      std::span<const ReplicaPartitionEpoch> partition_epochs) {
    for (const auto& epoch : partition_epochs) {
      absl::Status handed_off = co_await storage_->HandoffReplicaPartition(
          session_id, epoch.partition_id_, epoch.replication_epoch_);
      if (!handed_off.ok()) co_return handed_off;
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> WaitForCapacityIncrease(std::uint64_t previous,
                                                    std::string_view failure) {
    for (unsigned attempt = 0; attempt < 500; ++attempt) {
      absl::Status slept =
          co_await celer::SleepFor(*worker_, std::chrono::milliseconds(1));
      if (!slept.ok()) co_return slept;
      const auto metrics = co_await storage_->CollectMetrics();
      if (metrics.devices_.front().available_bytes_ > previous) {
        co_return absl::OkStatus();
      }
    }
    co_return absl::FailedPreconditionError(std::string(failure));
  }

  void CheckRetainedMemory(std::optional<std::uint64_t>& first_retained,
                           std::string_view failure) {
    keylane::RefreshMemoryStats();
    const std::uint64_t retained = keylane::GetMemoryStats().used_bytes_;
    if (first_retained.has_value()) {
      Check(retained <= *first_retained + kRetainedTolerance, failure);
    } else {
      first_retained = retained;
    }
  }

  celer::Task<absl::Status> ExerciseRepeatedAbort() {
    constexpr std::uint8_t kDb = 7;
    const std::string key = "replica-abort-candidate";
    std::optional<std::uint64_t> first_retained;

    for (unsigned round = 0; round < 2; ++round) {
      const std::uint64_t pin_session = 900 + round;
      const std::uint64_t replica_session = 910 + round;
      const auto capacity_baseline = co_await storage_->CollectMetrics();
      Check(capacity_baseline.devices_.size() == 1,
            "replica-abort test expected one storage device");
      auto pinned = co_await PinActiveExternalValue(
          pin_session, kDb, key, static_cast<char>('a' + round));
      if (!pinned.ok()) co_return pinned.status();

      auto reset = co_await ResetFullRoot(replica_session);
      if (!reset.ok()) co_return reset.status();
      absl::Status applied = co_await ApplyCandidate(
          replica_session, *reset, kDb, key, static_cast<char>('k' + round));
      if (!applied.ok()) co_return applied;
      absl::Status aborted =
          co_await storage_->AbortReplicaRoot(replica_session);
      if (!aborted.ok()) co_return aborted;
      Check(!co_await storage_->Exists(kDb, key),
            "replica abort left the candidate key visible");

      const auto while_pinned = co_await storage_->CollectMetrics();
      Check(while_pinned.devices_.front().available_bytes_ <=
                capacity_baseline.devices_.front().available_bytes_,
            "replica abort reported pinned retired capacity as available");
      ReleasePinnedValue(pin_session, key, *pinned);
      absl::Status reclaimed = co_await WaitForCapacityIncrease(
          while_pinned.devices_.front().available_bytes_,
          "replica-abort retired capacity did not become reusable");
      if (!reclaimed.ok()) co_return reclaimed;
      storage_->SetReplicaLoading(false);
      CheckRetainedMemory(first_retained,
                          "successive aborts accumulated detached index RAM");
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> ExerciseRepeatedPromotion() {
    constexpr std::uint8_t kDb = 9;
    const std::string key = "replica-promote-candidate";
    std::optional<std::uint64_t> first_retained;

    for (unsigned round = 0; round < 2; ++round) {
      const std::uint64_t pin_session = 950 + round;
      const std::uint64_t replica_session = 960 + round;
      const auto capacity_baseline = co_await storage_->CollectMetrics();
      auto pinned = co_await PinActiveExternalValue(
          pin_session, kDb, key, static_cast<char>('c' + round));
      if (!pinned.ok()) co_return pinned.status();

      auto reset = co_await ResetFullRoot(replica_session);
      if (!reset.ok()) co_return reset.status();
      absl::Status applied = co_await ApplyCandidate(
          replica_session, *reset, kDb, key, static_cast<char>('x' + round));
      if (!applied.ok()) co_return applied;
      absl::Status handed_off = co_await HandoffAll(replica_session, *reset);
      if (!handed_off.ok()) co_return handed_off;
      absl::Status promoted =
          co_await storage_->PromoteReplicaRoot(replica_session);
      if (!promoted.ok()) co_return promoted;
      Check(co_await storage_->Exists(kDb, key),
            "replica promotion did not publish the candidate key");

      const auto while_pinned = co_await storage_->CollectMetrics();
      Check(while_pinned.devices_.front().available_bytes_ <=
                capacity_baseline.devices_.front().available_bytes_,
            "replica promotion reported pinned retired capacity as available");
      ReleasePinnedValue(pin_session, key, *pinned);
      absl::Status reclaimed = co_await WaitForCapacityIncrease(
          while_pinned.devices_.front().available_bytes_,
          "replica-promotion retired capacity did not become reusable");
      if (!reclaimed.ok()) co_return reclaimed;
      // Production keeps replica loading enabled after root promotion so tail
      // commands continue to require their per-partition apply context. This
      // storage-only test seeds the next candidate with a direct client-style
      // write, so explicitly leave replica mode between independent rounds.
      storage_->SetReplicaLoading(false);
      CheckRetainedMemory(
          first_retained,
          "successive promotions accumulated detached index RAM");
    }
    co_return absl::OkStatus();
  }

  StorageEngine* storage_ = nullptr;
  keylane::storage::ValueType large_type_;
  bool verify_ingest_ = false;
  celer::Worker* worker_ = nullptr;
  absl::Status result_ = absl::UnknownError("test service did not run");
};

int Run(const std::string& path, keylane::storage::ValueType large_type,
        bool verify_ingest = false) {
  StorageEngineOptions options;
  options.data_files_ = {path};
  options.buffers_.registered_bytes_ = 64 * kMiB;
  options.replication_publish_queue_bytes_ = 16 * kMiB;
  StorageEngine storage(std::move(options));
  keylane::InitWorkerMetrics(1);
  absl::Status memory = keylane::InitMemoryLimit(
      (large_type == keylane::storage::ValueType::kNone ? 512 : 2048) * kMiB,
      1);
  if (!memory.ok()) {
    std::cerr << memory << '\n';
    return 1;
  }
  keylane::InitStorage(&storage, nullptr);
  absl::Status prepared = storage.Prepare(1);
  if (!prepared.ok()) {
    std::cerr << prepared << '\n';
    return 1;
  }
  keylane::tx::TxRuntime::Create(1);
  ReplicaAbortReclaimService service(&storage, large_type, verify_ingest);
  celer::Server server;
  server.AddService(&service);
  celer::ServerOptions runtime;
  runtime.thread_count_ = 1;
  runtime.pin_workers_ = false;
  runtime.recv_buffer_count_ = 0;
  absl::Status started = server.Start(runtime);
  if (!started.ok()) {
    std::cerr << started << '\n';
    return 1;
  }
  server.WaitUntilStopped();
  if (!service.result().ok()) {
    std::cerr << service.result() << '\n';
    return 1;
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    using keylane::storage::ValueType;
    if (argc == 3 && std::string_view(argv[1]) == "--verify-ingest")
      return Run(argv[2], ValueType::kNone, true);
    ValueType large_type = ValueType::kNone;
    if (argc == 2 && std::string_view(argv[1]) == "--large-list")
      large_type = ValueType::kList;
    else if (argc == 2 && std::string_view(argv[1]) == "--large-hash")
      large_type = ValueType::kHash;
    else
      Check(argc == 1, "usage: replica_abort [--large-list|--large-hash]");
    const bool large = large_type != ValueType::kNone;
    const std::string prefix =
        large ? "/mnt/dev/keylane-native-large-"
              : keylane::test::TestDataPath("keylane-replica-abort-reclaim-");
    ScopedDataFile data_file(prefix + std::to_string(::getpid()) + ".data");
    data_file.Create(large);
    const auto result = Run(data_file.path(), large_type);
    if (result != 0 || large) return result;
    // Exec starts a fresh storage/transaction runtime over the same private
    // image. The parent alone retains ownership of its exact cleanup path.
    const auto child = ::fork();
    Check(child >= 0, "cannot fork cold ingest verification");
    if (child == 0) {
      ::execl("/proc/self/exe", argv[0], "--verify-ingest",
              data_file.path().c_str(), static_cast<char*>(nullptr));
      ::_exit(127);
    }
    int status = 0;
    Check(::waitpid(child, &status, 0) == child && WIFEXITED(status) &&
              WEXITSTATUS(status) == 0,
          "fresh-process cold ingest verification failed");
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
