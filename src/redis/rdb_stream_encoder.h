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

#pragma once

#include <array>
#include <memory>
#include <optional>
#include <string>

#include "lavik/storage/collection_page.h"
#include "lavik/storage/detail/stream_records.h"
#include "rdb_record_spool.h"

namespace lavik::rdb {

// Streams messages directly. Only consumer headers and PEL owner associations
// require a bounded external reorder, because RDB writes those after the global
// PEL. Output spans live until Next; failures surface through Finish/StartPage.
class StreamFileEncoder {
 public:
  explicit StreamFileEncoder(std::uint64_t length)
      : validator_(length), expected_(length) {}
  absl::Status StartPage(const storage::CollectionPage& page);
  std::optional<std::string_view> Next() noexcept;
  absl::Status Finish() const;

 private:
  absl::StatusOr<std::optional<std::string_view>> Advance();
  absl::Status OutputBudget(std::size_t bytes);
  RetainedMemoryCharge state_charge_;
  std::optional<MemoryReservation> output_admission_, consumer_admission_;
  storage::StreamRecordValidator validator_;
  std::unique_ptr<RecordSpool> consumers_;
  const storage::CollectionPage* page_ = nullptr;
  std::size_t entry_ = 0;
  std::uint64_t expected_, messages_ = 0, pending_ = 0, consumer_count_ = 0;
  std::string output_, header_, consumer_prefix_;
  std::array<std::uint64_t, 2> first_id_{};
  enum class Phase { kRecords, kConsumerCount, kConsumers };
  Phase phase_ = Phase::kRecords;
  absl::Status status_;
  bool done_ = false;
};

}  // namespace lavik::rdb
