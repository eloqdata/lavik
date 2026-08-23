#include "keylane/logging.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>

#include "spdlog/spdlog.h"

namespace {

class LoggingTest : public ::testing::Test {
 protected:
  LoggingTest() {
    const auto suffix =
        std::chrono::steady_clock::now().time_since_epoch().count();
    temp_dir_ = std::filesystem::temp_directory_path() /
                ("keylane-logging-test-" + std::to_string(suffix));
    std::filesystem::create_directories(temp_dir_);
  }

  void SetUp() override { original_logger_ = spdlog::default_logger(); }

  void TearDown() override {
    if (auto logger = spdlog::default_logger(); logger != nullptr) {
      logger->flush();
    }
    spdlog::set_default_logger(original_logger_);
    spdlog::drop("keylane");
  }

  ~LoggingTest() override {
    std::error_code ignored;
    std::filesystem::remove_all(temp_dir_, ignored);
  }

  keylane::LoggingOptions Options() const {
    keylane::LoggingOptions options;
    options.log_dir_ = (temp_dir_ / "logs").string();
    return options;
  }

  std::filesystem::path LogPath() const {
    return temp_dir_ / "logs" / "keylane.log";
  }

  static std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
  }

  std::filesystem::path temp_dir_;
  std::shared_ptr<spdlog::logger> original_logger_;
};

TEST_F(LoggingTest, DefaultsToRotatingFileOnly) {
  const keylane::LoggingOptions options = Options();
  testing::internal::CaptureStderr();
  ASSERT_TRUE(keylane::InitializeLogging(options).ok());
  spdlog::info("default-file-marker");
  spdlog::default_logger()->flush();
  const std::string stderr_output = testing::internal::GetCapturedStderr();

  EXPECT_TRUE(stderr_output.empty());
  EXPECT_NE(ReadFile(LogPath()).find("default-file-marker"), std::string::npos);
}

TEST_F(LoggingTest, LogToStderrDisablesFileSink) {
  keylane::LoggingOptions options = Options();
  options.log_to_stderr_ = true;
  testing::internal::CaptureStderr();
  ASSERT_TRUE(keylane::InitializeLogging(options).ok());
  spdlog::info("stderr-only-marker");
  spdlog::default_logger()->flush();
  const std::string stderr_output = testing::internal::GetCapturedStderr();

  EXPECT_NE(stderr_output.find("stderr-only-marker"), std::string::npos);
  EXPECT_FALSE(std::filesystem::exists(options.log_dir_));
}

TEST_F(LoggingTest, AlsoLogToStderrWritesBothDestinations) {
  keylane::LoggingOptions options = Options();
  options.also_log_to_stderr_ = true;
  testing::internal::CaptureStderr();
  ASSERT_TRUE(keylane::InitializeLogging(options).ok());
  spdlog::info("dual-sink-marker");
  spdlog::default_logger()->flush();
  const std::string stderr_output = testing::internal::GetCapturedStderr();

  EXPECT_NE(stderr_output.find("dual-sink-marker"), std::string::npos);
  EXPECT_NE(ReadFile(LogPath()).find("dual-sink-marker"), std::string::npos);
}

TEST_F(LoggingTest, LogToStderrWinsWhenBothFlagsAreEnabled) {
  keylane::LoggingOptions options = Options();
  options.log_to_stderr_ = true;
  options.also_log_to_stderr_ = true;
  testing::internal::CaptureStderr();
  ASSERT_TRUE(keylane::InitializeLogging(options).ok());
  spdlog::info("stderr-precedence-marker");
  spdlog::default_logger()->flush();
  const std::string stderr_output = testing::internal::GetCapturedStderr();

  EXPECT_NE(stderr_output.find("stderr-precedence-marker"), std::string::npos);
  EXPECT_FALSE(std::filesystem::exists(options.log_dir_));
}

TEST_F(LoggingTest, WarningFlushesEarlierInfoMessages) {
  const keylane::LoggingOptions options = Options();
  ASSERT_TRUE(keylane::InitializeLogging(options).ok());
  spdlog::info("buffered-info-marker");
  spdlog::warn("flushing-warning-marker");

  const std::string contents = ReadFile(LogPath());
  EXPECT_NE(contents.find("buffered-info-marker"), std::string::npos);
  EXPECT_NE(contents.find("flushing-warning-marker"), std::string::npos);
}

TEST_F(LoggingTest, NormalShutdownFlushesInfoMessages) {
  const keylane::LoggingOptions options = Options();
  ASSERT_TRUE(keylane::InitializeLogging(options).ok());
  spdlog::info("shutdown-flush-marker");

  keylane::ShutdownLogging();

  EXPECT_NE(ReadFile(LogPath()).find("shutdown-flush-marker"),
            std::string::npos);
}

TEST_F(LoggingTest, RetainsConfiguredTotalFileCountDuringRotation) {
  keylane::LoggingOptions options = Options();
  options.max_log_size_mb_ = 1;
  options.max_log_files_ = 2;
  ASSERT_TRUE(keylane::InitializeLogging(options).ok());

  const std::string payload(700 * 1024, 'x');
  spdlog::info("first-{}", payload);
  spdlog::info("second-{}", payload);
  spdlog::info("third-{}", payload);
  spdlog::default_logger()->flush();

  EXPECT_TRUE(std::filesystem::exists(LogPath()));
  EXPECT_TRUE(std::filesystem::exists(temp_dir_ / "logs" / "keylane.1.log"));
  EXPECT_FALSE(std::filesystem::exists(temp_dir_ / "logs" / "keylane.2.log"));
}

}  // namespace
