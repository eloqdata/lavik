#include "keylane/logging.h"

#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace keylane {
namespace {

constexpr std::size_t kMiB = 1024 * 1024;
constexpr std::size_t kMaxRotatedFiles = 200'000;

}  // namespace

absl::Status ValidateLoggingOptions(const LoggingOptions& options) {
  if (options.log_dir_.empty()) {
    return absl::InvalidArgumentError("log-dir must not be empty");
  }
  if (options.max_log_size_mb_ == 0) {
    return absl::InvalidArgumentError("max-log-size-mb must be nonzero");
  }
  if (options.max_log_size_mb_ >
      std::numeric_limits<std::size_t>::max() / kMiB) {
    return absl::OutOfRangeError("max-log-size-mb is too large");
  }
  if (options.max_log_files_ == 0) {
    return absl::InvalidArgumentError("max-log-files must be nonzero");
  }
  // spdlog counts archived files, while Keylane's option includes the active
  // file.
  if (options.max_log_files_ - 1 > kMaxRotatedFiles) {
    return absl::OutOfRangeError("max-log-files is too large");
  }
  return absl::OkStatus();
}

absl::Status InitializeLogging(const LoggingOptions& options) {
  const absl::Status validated = ValidateLoggingOptions(options);
  if (!validated.ok()) return validated;

  try {
    std::vector<spdlog::sink_ptr> sinks;
    if (!options.log_to_stderr_) {
      const std::filesystem::path filename =
          std::filesystem::path(options.log_dir_) / "keylane.log";
      const std::size_t max_file_size = options.max_log_size_mb_ * kMiB;
      const std::size_t archived_files = options.max_log_files_ - 1;
      sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
          filename.string(), max_file_size, archived_files,
          /*rotate_on_open=*/false));
    }
    if (options.log_to_stderr_ || options.also_log_to_stderr_) {
      sinks.push_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
    }

    auto logger =
        std::make_shared<spdlog::logger>("keylane", sinks.begin(), sinks.end());
    logger->set_level(spdlog::level::info);
    logger->flush_on(spdlog::level::warn);
    spdlog::set_default_logger(std::move(logger));
  } catch (const std::exception& error) {
    return absl::InternalError(
        absl::StrCat("failed to initialize logging: ", error.what()));
  }
  return absl::OkStatus();
}

void ShutdownLogging() {
  if (auto logger = spdlog::default_logger(); logger != nullptr) {
    logger->flush();
  }
  spdlog::shutdown();
}

}  // namespace keylane
