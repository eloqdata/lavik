#pragma once

// CLUSTER command handlers for cluster mode. Called from
// ExecuteCluster (src/redis/command.cpp) when cluster mode is enabled;
// standalone mode keeps the legacy replication-derived NODES/SLOTS shim.
//
// Supported subcommands: SLOTS, NODES, MYID, INFO, KEYSLOT. Anything else
// gets Redis's exact unknown-subcommand error. ASK/ASKING and the gossip
// management subcommands are deliberately out of scope for v1.

#include "keylane/command.h"

namespace keylane {

Task<CommandReply> ExecuteClusterModeCommand(const CommandRequest& request,
                                             ReplyBuilder& reply_builder);

}  // namespace keylane
