#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "keylane/command.h"
#include "keylane/storage/engine.h"

namespace keylane {

struct StringExecKey {
  storage::Digest digest_;
  std::uint16_t owner_ = 0;
  std::uint16_t arg_ = 0;
};

void InitStringCommandStorage(storage::StorageEngine* engine);

celer::Task<CommandReply> ExecuteStringCommand(const CommandRequest& request,
                                               ReplyBuilder& reply_builder);

celer::Task<CommandReply> ExecuteStringCommandLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, ReplyBuilder& reply_builder,
    const storage::MutationPrecondition* mutation_precondition = nullptr);

celer::Task<CommandReply> ExecuteBitmapCommand(const CommandRequest& request,
                                               ReplyBuilder& reply_builder);

celer::Task<CommandReply> ExecuteBitmapCommandLocked(
    const CommandRequest& request, const storage::Digest& digest,
    storage::TxShardWrites* tx, ReplyBuilder& reply_builder,
    const storage::MutationPrecondition* mutation_precondition = nullptr);

celer::Task<CommandReply> ExecuteBitOpCommand(const CommandRequest& request,
                                              ReplyBuilder& reply_builder);

celer::Task<std::string> ExecuteBitOpLocked(
    const CommandRequest& request, std::span<const StringExecKey> locked_keys,
    std::vector<storage::TxShardWrites>& tx_writes);

celer::Task<CommandReply> ExecuteLcsCommand(const CommandRequest& request,
                                            ReplyBuilder& reply_builder);

celer::Task<std::string> ExecuteLcsLocked(
    const CommandRequest& request, std::span<const StringExecKey> locked_keys);

}  // namespace keylane
