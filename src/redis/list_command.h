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

#include "blocking_wait.h"
#include "lavik/command.h"

namespace lavik {

class ReplyBuilder;

void InitListCommandStorage(storage::StorageEngine* engine);

// A supplied compact snapshot bypasses storage only for standalone read-only
// commands. Its payload lease must outlive the returned task; replies own any
// data retained after completion. A null snapshot uses ordinary owner storage.
Task<CommandReply> ExecuteSingleListCommand(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    const storage::CompactReadResult* compact = nullptr);

Task<CommandReply> ExecuteSingleListCommandLocked(const CommandRequest& request,
                                                  const storage::Digest& digest,
                                                  storage::TxShardWrites* tx,
                                                  ReplyBuilder& reply_builder);

Task<CommandReply> ExecuteListMultiKey(const CommandRequest& request,
                                       ReplyBuilder& reply_builder,
                                       bool* unavailable = nullptr);

Task<CommandReply> ExecuteBlockingListCommand(const CommandRequest& request,
                                              ReplyBuilder& reply_builder,
                                              std::uint64_t client_id = 0);

}  // namespace lavik
