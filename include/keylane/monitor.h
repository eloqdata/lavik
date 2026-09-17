#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "bycorf/runtime/task.h"

namespace bycorf {
class TcpStream;
}

namespace keylane {

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
    std::uint8_t db_id, std::string_view endpoint,
    std::span<const std::string> args, const CommandRequest* request = nullptr);

// One-way fanout. The caller never waits for monitor consumers.
void PublishMonitorMessage(std::shared_ptr<const std::string> message);

// Publishes validated queued commands when EXEC actually reaches execution.
void PublishExecMonitorCommands(const ConnectionContext& context,
                                std::span<const CommandRequest> commands);

std::shared_ptr<MonitorSession> RegisterMonitorSession(int fd);
void UnregisterMonitorSession(const std::shared_ptr<MonitorSession>& session);

bycorf::Task<absl::Status> StreamMonitorMessages(
    bycorf::TcpStream& stream, const std::shared_ptr<MonitorSession>& session);

}  // namespace keylane
