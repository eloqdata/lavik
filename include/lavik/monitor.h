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

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "bycorf/runtime/task.h"
#include "lavik/command_args.h"

namespace bycorf {
class TcpStream;
}

namespace lavik {

struct CommandRequest;
struct ConnectionContext;
class MonitorSession;

void PrepareMonitor(unsigned worker_count);

// The ordinary command path performs this single check before doing any
// monitor-specific validation, formatting, allocation, or cross-worker work.
bool HasMonitorSessions() noexcept;

// Returns null for commands that Valkey excludes from MONITOR (ADMIN,
// SKIP_MONITOR, unknown commands, and invalid arity).
std::shared_ptr<const std::string> PrepareMonitorMessage(
    std::uint8_t db_id, std::string_view endpoint, CommandArgsView args,
    const CommandRequest* request = nullptr);

// One-way fanout. The caller never waits for monitor consumers.
void PublishMonitorMessage(std::shared_ptr<const std::string> message);

// Publishes validated queued commands when EXEC actually reaches execution.
void PublishExecMonitorCommands(const ConnectionContext& context,
                                std::span<const CommandRequest> commands);

std::shared_ptr<MonitorSession> RegisterMonitorSession(int fd);
void UnregisterMonitorSession(const std::shared_ptr<MonitorSession>& session);

bycorf::Task<absl::Status> StreamMonitorMessages(
    bycorf::TcpStream& stream, const std::shared_ptr<MonitorSession>& session);

}  // namespace lavik
