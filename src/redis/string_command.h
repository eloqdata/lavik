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
#include <string>
#include <vector>

#include "lavik/command.h"
#include "lavik/storage/engine.h"

namespace lavik {

struct EncodedCommandReply;

struct StringExecKey {
  storage::Digest digest_;
  std::uint16_t owner_ = 0;
  std::uint16_t arg_ = 0;
};

void InitStringCommandStorage(storage::StorageEngine* engine);

bycorf::Task<CommandReply> ExecuteStringCommand(const CommandRequest& request,
                                                ReplyBuilder& reply_builder);

bycorf::Task<CommandReply> ExecuteStringCommandLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, ReplyBuilder& reply_builder,
    const storage::MutationPrecondition* mutation_precondition = nullptr);

bycorf::Task<CommandReply> ExecuteBitmapCommand(const CommandRequest& request,
                                                ReplyBuilder& reply_builder);

bycorf::Task<CommandReply> ExecuteBitmapCommandLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, ReplyBuilder& reply_builder,
    const storage::MutationPrecondition* mutation_precondition = nullptr);

bycorf::Task<CommandReply> ExecuteBitOpCommand(const CommandRequest& request,
                                               ReplyBuilder& reply_builder);

bycorf::Task<EncodedCommandReply> ExecuteBitOpLocked(
    const CommandRequest& request, std::span<const StringExecKey> locked_keys,
    std::vector<storage::TxShardWrites>& tx_writes);

bycorf::Task<CommandReply> ExecuteLcsCommand(const CommandRequest& request,
                                             ReplyBuilder& reply_builder);

bycorf::Task<EncodedCommandReply> ExecuteLcsLocked(
    const CommandRequest& request, std::span<const StringExecKey> locked_keys);

}  // namespace lavik
