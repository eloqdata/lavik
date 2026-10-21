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

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/status.h"
#include "bycorf/runtime/task.h"
#include "lavik/command_args.h"

namespace lavik {

inline constexpr std::int64_t kDefaultSlowLogThresholdMicros = 10'000;
inline constexpr std::size_t kDefaultSlowLogMaxLen = 128;

struct SlowLogEntry {
  std::uint64_t id_ = 0;
  std::int64_t unix_time_seconds_ = 0;
  std::uint64_t duration_micros_ = 0;
  std::vector<std::string> args_;
  std::string client_address_;
  std::string client_name_;
};

// Allocates one single-owner shard per runtime worker. Each shard can retain
// max_len entries so merging the shards can always recover the global newest
// max_len entries, even when every command is handled by one worker.
void InitSlowLog(unsigned worker_count, std::int64_t threshold_micros,
                 std::size_t max_len);

// Fast command-completion hook. It returns before reading command arguments
// unless the local threshold is enabled and exceeded.
void MaybeRecordSlowCommand(CommandArgsView args,
                            std::string_view client_address,
                            std::string_view client_name,
                            std::uint64_t elapsed_ticks, bool may_block);

bycorf::Task<std::vector<SlowLogEntry>> CollectSlowLog(std::size_t count);
bycorf::Task<std::size_t> SlowLogLength();
bycorf::Task<absl::Status> ResetSlowLog();
bycorf::Task<absl::Status> ConfigureSlowLogThreshold(
    std::int64_t threshold_micros);
bycorf::Task<absl::Status> ConfigureSlowLogMaxLen(std::size_t max_len);

std::int64_t SlowLogThresholdMicros() noexcept;
std::size_t SlowLogMaxLen() noexcept;

}  // namespace lavik
