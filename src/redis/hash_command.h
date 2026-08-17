#pragma once

#include "keylane/command.h"

namespace keylane {

class ReplyBuilder;

void InitHashCommandStorage(storage::StorageEngine* engine);

Task<CommandReply> ExecuteHashCommand(const CommandRequest& request,
                                      ReplyBuilder& reply_builder);

Task<CommandReply> ExecuteHashCommandLocked(const CommandRequest& request,
                                            const storage::Digest& digest,
                                            storage::TxShardWrites* tx,
                                            ReplyBuilder& reply_builder);

}  // namespace keylane
