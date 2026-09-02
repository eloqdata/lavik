#pragma once

#include <span>

#include "keylane/command.h"

namespace keylane {

class ReplyBuilder;

struct SortExecKey {
  storage::Digest digest_;
  std::uint16_t owner_ = 0;
  std::uint16_t arg_ = 0;
};

void InitSortCommandStorage(storage::StorageEngine* engine);

Task<CommandReply> ExecuteSortCommand(const CommandRequest& request,
                                      ReplyBuilder& reply_builder);

// EXEC already owns all statically discoverable SORT keys. Pattern-derived
// BY/GET keys cannot be added after EXEC has acquired its global lock set and
// are rejected unless they alias one of those keys. Script callers request
// deterministic ordering for Set input with a constant BY pattern, matching
// Valkey's replication-safe Lua behavior.
Task<std::string> ExecuteSortCommandLocked(
    const CommandRequest& request, std::span<const SortExecKey> keys,
    std::vector<storage::TxShardWrites>& tx_writes,
    bool deterministic_set_order);

}  // namespace keylane
