#include "celer/runtime/cross_core.h"
#include "keylane/rdb.h"

namespace keylane::rdb {

celer::Task<absl::StatusOr<storage::RestoreRawResult>> RestoreFileEntry(
    storage::StorageEngine* storage, FileReader* reader, const FileEntry& entry,
    bool replace) {
  const unsigned owner = storage->OwnerForKey(entry.key_);
  auto apply =
      [storage, reader, &entry,
       replace]() -> celer::Task<absl::StatusOr<storage::RestoreRawResult>> {
    if (!entry.collection_stream_)
      co_return co_await storage->RestoreRawValue(
          entry.db_id_, entry.key_, entry.value_, replace, nullptr);
    storage::CollectionPageReader next =
        [reader]() -> celer::Task<absl::StatusOr<storage::CollectionPage>> {
      co_return reader->ReadCollectionPage();
    };
    auto result = co_await storage->RestoreCollectionValue(
        entry.db_id_, entry.key_, entry.value_.value_type_,
        entry.value_.expire_at_ms_, replace, entry.expected_items_,
        std::move(next));
    // Expired values can be skipped by storage. Drain on the same owner so
    // decoder accounting stays local and the next file header is reachable.
    if (result.ok() && !result->busy_) {
      auto drained = reader->DrainCollection();
      if (!drained.ok()) co_return drained;
    }
    co_return result;
  };
  co_return owner == celer::ThisWorker().id_
      ? co_await apply()
      : co_await celer::SubmitTaskTo(owner, apply);
}

}  // namespace keylane::rdb
