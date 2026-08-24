#pragma once

#include <span>

#include "keylane/command.h"

namespace keylane {

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

Task<CommandReply> ExecuteZSetCommand(const CommandRequest& request,
                                      ReplyBuilder& reply_builder);
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

}  // namespace keylane
