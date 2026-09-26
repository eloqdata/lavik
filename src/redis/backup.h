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

#include <atomic>
#include <memory>
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
// Runs the worker-zero-owned automatic save coordinator. It checks
// configured save policies once per second using worker-local change counters.
Task<absl::Status> RunRdbBackupScheduler(bycorf::Worker& worker);
// EXEC supplies a completion token: its BGSAVE is scheduled only after the
// entire transaction releases its database/key holds, including later writes.
Task<CommandReply> ExecuteRdbBackupCommand(
    const CommandRequest& request, ReplyBuilder& reply_builder,
    std::shared_ptr<std::atomic<bool>> exec_finished = {});
// Cooperative client admission barrier for synchronous SAVE. Control-plane
// and replication coroutines keep running while ordinary clients wait.
bool SynchronousRdbSaveActive() noexcept;
Task<absl::Status> WaitForSynchronousRdbSave();
// Collect a consistent Redis persistence view on the backup coordinator.
Task<std::string> RdbPersistenceInfo();
// Stops the policy timer; an active save or an EXEC-scheduled save still
// drains during orderly shutdown. A sleeping timer observes the stop request
// at its next one-second wakeup.
void StopAutomaticRdbBackups() noexcept;
void WaitForRdbBackupDrained() noexcept;

}  // namespace lavik
