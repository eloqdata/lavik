#pragma once

#include "keylane/command.h"
#include "blocking_wait.h"

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
                                              ReplyBuilder& reply_builder);

}  // namespace keylane
