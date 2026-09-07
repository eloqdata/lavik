#pragma once

#include "blocking_wait.h"
#include "keylane/command.h"

namespace keylane {

class ReplyBuilder;

void InitListCommandStorage(storage::StorageEngine* engine);

Task<CommandReply> ExecuteSingleListCommand(const CommandRequest& request,
                                            ReplyBuilder& reply_builder);

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

}  // namespace keylane
