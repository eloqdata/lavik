#pragma once

#include <cstddef>
#include <string>

#include "absl/status/status.h"

namespace keylane {

struct LoggingOptions {
  std::string log_dir_{"./logs"};
  std::size_t max_log_size_mb_ = 100;
  std::size_t max_log_files_ = 10;
  bool log_to_stderr_ = false;
  bool also_log_to_stderr_ = false;
};

// Validates values shared by config-file, command-line, and direct callers.
absl::Status ValidateLoggingOptions(const LoggingOptions& options);

// Installs Keylane's synchronous spdlog logger as the process-wide default.
absl::Status InitializeLogging(const LoggingOptions& options);

// Flushes and releases all spdlog resources owned by the process registry.
void ShutdownLogging();

}  // namespace keylane
