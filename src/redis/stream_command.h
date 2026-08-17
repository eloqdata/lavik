#pragma once

#include <span>

#include "keylane/command.h"

namespace keylane {

void InitStreamCommandStorage(storage::StorageEngine* engine);

struct StreamExecKey {
  storage::Digest digest_;
  std::uint16_t owner_ = 0;
  std::uint16_t arg_ = 0;
};

Task<CommandReply> ExecuteStreamCommand(const CommandRequest& request,
                                        ReplyBuilder& reply_builder);
Task<CommandReply> ExecuteStreamCommandLocked(const CommandRequest& request,
                                              const storage::Digest& digest,
                                              storage::TxShardWrites* tx,
                                              ReplyBuilder& reply_builder);
Task<std::string> ExecuteStreamReadLocked(
    const CommandRequest& request, std::span<const StreamExecKey> keys,
    std::vector<storage::TxShardWrites>& tx_writes);

}  // namespace keylane
