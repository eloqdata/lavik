#pragma once

#include "keylane/command.h"

namespace keylane {

class ReplyBuilder;

void InitSetCommandStorage(storage::StorageEngine* engine);

Task<CommandReply> ExecuteSetCommand(const CommandRequest& request,
                                     ReplyBuilder& reply_builder);

Task<CommandReply> ExecuteSetCommandLocked(const CommandRequest& request,
                                           const storage::Digest& digest,
                                           storage::TxShardWrites* tx,
                                           ReplyBuilder& reply_builder);

Task<CommandReply> ExecuteSetMultiKey(const CommandRequest& request,
                                      ReplyBuilder& reply_builder);

}  // namespace keylane
