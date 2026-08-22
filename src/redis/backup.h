#pragma once

#include <string>

#include "keylane/command.h"

namespace keylane {

void InitRdbBackup(storage::StorageEngine* storage, std::string target_path);
Task<CommandReply> ExecuteRdbBackupCommand(const CommandRequest& request,
                                           ReplyBuilder& reply_builder);
void WaitForRdbBackupDrained() noexcept;

}  // namespace keylane
