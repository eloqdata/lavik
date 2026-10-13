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

#include <span>

#include "lavik/command.h"

namespace lavik {

void InitZSetCommandStorage(storage::StorageEngine* engine);

struct ZSetExecKey {
  storage::Digest digest_;
  std::uint16_t owner_ = 0;
  std::uint16_t arg_ = 0;
};

// Captures the command-time population used by bounded negative-count reply
// streaming. The caller owns the shared key lock during this call.
Task<absl::StatusOr<storage::HashResult>> ZSetRandomSnapshotLocked(
    std::uint8_t db_id, std::string_view key, const storage::Digest& digest,
    bool with_scores, storage::TxShardWrites* tx, std::uint64_t now_ms);

// Returns members in the sorted set's native score/member order. SORT uses
// this for BY nosort while already holding the key's transaction lock.
Task<absl::StatusOr<std::vector<std::string>>> ZSetMembersSnapshotLocked(
    std::uint8_t db_id, std::string_view key, const storage::Digest& digest);

// A supplied compact snapshot bypasses storage only for standalone read-only
// commands. Its payload lease must outlive the returned task; replies own any
// data retained after completion. A null snapshot uses ordinary owner storage.
Task<CommandReply> ExecuteZSetCommand(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    const storage::CompactReadResult* compact = nullptr);
Task<CommandReply> ExecuteZSetCommandLocked(const CommandRequest& request,
                                            const storage::Digest& digest,
                                            storage::TxShardWrites* tx,
                                            ReplyBuilder& reply_builder);
Task<CommandReply> ExecuteZSetMultiKey(const CommandRequest& request,
                                       ReplyBuilder& reply_builder);
Task<CommandReply> ExecuteBlockingZSetCommand(const CommandRequest& request,
                                              ReplyBuilder& reply_builder,
                                              std::uint64_t client_id = 0);
Task<std::string> ExecuteZSetMultiKeyLocked(
    const CommandRequest& request, std::span<const ZSetExecKey> keys,
    std::vector<storage::TxShardWrites>& tx_writes);
Task<std::string> ExecuteZSetMultiPopLocked(
    const CommandRequest& request, std::span<const ZSetExecKey> keys,
    std::vector<storage::TxShardWrites>& tx_writes);

}  // namespace lavik
