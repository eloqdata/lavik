#pragma once

#include <cstdint>
#include <string_view>

#include "celer/base/status.h"
#include "celer/net/connection.h"
#include "celer/runtime/task.h"
#include "celer/runtime/worker.h"

namespace celer::redis {

int RunServer(std::string_view bind_ip, std::uint16_t port, unsigned thread_count,
              int idle_timeout_ms);
Task<Status> RedisSession(Worker& worker, Connection* connection);

}  // namespace celer::redis
