/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <string>
#include <vector>

#include "bycorf/runtime/worker.h"
#include "lavik/command.h"
#include "lavik/server.h"

namespace lavik {

void InitRdbBackup(storage::StorageEngine* storage, std::string target_path,
                   std::vector<RdbSaveRule> save_rules);
// True after initialization only when at least one automatic save policy was
// configured. Callers use this to avoid creating an idle timer coroutine.
bool AutomaticRdbBackupsConfigured() noexcept;
// Runs the worker-zero-owned automatic/scheduled save coordinator. It checks
// configured save policies once per second; writes never touch shared state.
Task<absl::Status> RunRdbBackupScheduler(bycorf::Worker& worker);
Task<CommandReply> ExecuteRdbBackupCommand(const CommandRequest& request,
                                           ReplyBuilder& reply_builder);
// Stops the policy timer coroutine while allowing an explicitly acknowledged
// BGSAVE SCHEDULE request to drain during orderly shutdown. A sleeping timer
// observes the stop request at its next one-second wakeup.
void StopAutomaticRdbBackups() noexcept;
void WaitForRdbBackupDrained() noexcept;

}  // namespace lavik
