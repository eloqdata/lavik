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

#include "bycorf/runtime/cross_core.h"
#include "lavik/rdb.h"
#include "lavik/status_macros.h"

namespace lavik::rdb {

bycorf::Task<absl::StatusOr<storage::RestoreRawResult>> RestoreFileEntry(
    storage::StorageEngine* storage, FileReader* reader, const FileEntry& entry,
    bool replace) {
  const unsigned owner = storage->OwnerForKey(entry.key_);
  auto apply =
      [storage, reader, &entry,
       replace]() -> bycorf::Task<absl::StatusOr<storage::RestoreRawResult>> {
    if (!entry.collection_stream_)
      co_return co_await storage->RestoreRawValue(
          entry.db_id_, entry.key_, entry.value_, replace, nullptr);
    storage::CollectionPageReader next =
        [reader]() -> bycorf::Task<absl::StatusOr<storage::CollectionPage>> {
      co_return reader->ReadCollectionPage();
    };
    auto result = co_await storage->RestoreCollectionValue(
        entry.db_id_, entry.key_, entry.value_.value_type_,
        entry.value_.expire_at_ms_, replace, entry.expected_items_,
        std::move(next));
    // Expired values can be skipped by storage. Drain on the same owner so
    // decoder accounting stays local and the next file header is reachable.
    if (result.ok() && !result->busy_) {
      LAVIK_CO_RETURN_IF_ERROR(reader->DrainCollection());
    }
    co_return result;
  };
  // if/else, not ?:, to keep the two co_awaits in separate full expressions.
  // GCC 13 can reuse the wrong coroutine-frame slot when both arms of ?:
  // contain co_await, which can run the restore on the wrong worker.
  if (owner == bycorf::ThisWorker().id_) co_return co_await apply();
  co_return co_await bycorf::SubmitTaskTo(owner, apply);
}

}  // namespace lavik::rdb
