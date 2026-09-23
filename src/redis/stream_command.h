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

#include <cstdint>
#include <span>

#include "absl/status/status.h"
#include "lavik/command.h"

namespace lavik {

void InitStreamCommandStorage(storage::StorageEngine* engine);
std::uint32_t StreamNodeMaxEntries() noexcept;
absl::Status SetStreamNodeMaxEntries(std::uint64_t value);

struct StreamExecKey {
  storage::Digest digest_;
  std::uint16_t owner_ = 0;
  std::uint16_t arg_ = 0;
};

Task<CommandReply> ExecuteStreamCommand(const CommandRequest& request,
                                        ReplyBuilder& reply_builder,
                                        std::uint64_t client_id = 0);
Task<CommandReply> ExecuteStreamCommandLocked(const CommandRequest& request,
                                              const storage::Digest& digest,
                                              storage::TxShardWrites* tx,
                                              ReplyBuilder& reply_builder);
Task<std::string> ExecuteStreamReadLocked(
    const CommandRequest& request, std::span<const StreamExecKey> keys,
    std::vector<storage::TxShardWrites>& tx_writes,
    ReplyChunkSource* chunks = nullptr);

}  // namespace lavik
