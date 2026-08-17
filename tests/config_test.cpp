#include "keylane/config.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include "keylane/command.h"
#include "keylane/server.h"

namespace {

using keylane::ApplyRedisConfigDirective;
using keylane::LoadRedisConfigFile;
using keylane::ParseRedisConfigLine;
using keylane::ParseReplicaOfRequest;
using keylane::ServerOptions;

class TempConfigFile {
 public:
  explicit TempConfigFile(std::string_view contents) {
    const auto suffix =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = std::filesystem::temp_directory_path() /
            ("keylane-config-test-" + std::to_string(suffix) + ".conf");
    std::ofstream output(path_);
    output << contents;
  }

  ~TempConfigFile() {
    std::error_code ignored;
    std::filesystem::remove(path_, ignored);
  }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

TEST(RedisConfigTest, TokenizesQuotesEscapesAndComments) {
  auto parsed = ParseRedisConfigLine(
      R"(replicaof "redis upstream" 6380 # trailing comment)");
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  EXPECT_EQ(*parsed,
            (std::vector<std::string>{"replicaof", "redis upstream", "6380"}));

  parsed = ParseRedisConfigLine(R"(bind '127.0.0.1')");
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  EXPECT_EQ(*parsed, (std::vector<std::string>{"bind", "127.0.0.1"}));

  parsed = ParseRedisConfigLine("  # comment only");
  ASSERT_TRUE(parsed.ok()) << parsed.status();
  EXPECT_TRUE(parsed->empty());
  EXPECT_FALSE(ParseRedisConfigLine("bind \"unterminated").ok());
}

TEST(RedisConfigTest, AppliesSupportedDirectives) {
  ServerOptions options;
  ASSERT_TRUE(ApplyRedisConfigDirective({"bind", "0.0.0.0"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"port", "6380"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"io-threads", "4"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"replicaof", "redis.internal", "6379"},
                                        &options)
                  .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"replica-read-only", "no"}, &options).ok());

  EXPECT_EQ(options.bind_ip_, "0.0.0.0");
  EXPECT_EQ(options.port_, 6380);
  EXPECT_EQ(options.thread_count_, 4u);
  ASSERT_TRUE(options.replicaof_.has_value());
  EXPECT_EQ(options.replicaof_->host_, "redis.internal");
  EXPECT_EQ(options.replicaof_->port_, 6379);
  EXPECT_FALSE(options.replication_options_.replica_read_only_);
}

TEST(RedisConfigTest, RejectsInvalidAndUnsupportedDirectives) {
  ServerOptions options;
  EXPECT_FALSE(ApplyRedisConfigDirective({"port", "70000"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"replicaof", "host", "zero"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"replica-read-only", "maybe"}, &options).ok());
  EXPECT_FALSE(ApplyRedisConfigDirective({"appendonly", "yes"}, &options).ok());
}

TEST(RedisConfigTest, LoadsFileAndReportsLineNumber) {
  TempConfigFile valid(
      "# keylane test\nport 6381\nio-threads 2\nreplicaof redis.local 6379\n");
  ServerOptions options;
  absl::Status loaded = LoadRedisConfigFile(valid.path().string(), &options);
  ASSERT_TRUE(loaded.ok()) << loaded;
  EXPECT_EQ(options.config_file_, valid.path().string());
  EXPECT_EQ(options.port_, 6381);
  EXPECT_EQ(options.thread_count_, 2u);
  ASSERT_TRUE(options.replicaof_.has_value());

  TempConfigFile invalid("port 6381\nappendonly yes\n");
  loaded = LoadRedisConfigFile(invalid.path().string(), &options);
  ASSERT_FALSE(loaded.ok());
  EXPECT_NE(loaded.message().find(":2:"), std::string_view::npos);
}

TEST(ReplicaOfCommandTest, ParsesFollowAndNoOneForms) {
  auto follow = ParseReplicaOfRequest(
      std::vector<std::string>{"REPLICAOF", "redis.internal", "6380"});
  ASSERT_TRUE(follow.ok()) << follow.status();
  ASSERT_TRUE(follow->host_.has_value());
  EXPECT_EQ(*follow->host_, "redis.internal");
  EXPECT_EQ(follow->port_, 6380);

  auto no_one =
      ParseReplicaOfRequest(std::vector<std::string>{"replicaof", "NO", "ONE"});
  ASSERT_TRUE(no_one.ok()) << no_one.status();
  EXPECT_FALSE(no_one->host_.has_value());
  EXPECT_EQ(no_one->port_, 0);

  EXPECT_FALSE(
      ParseReplicaOfRequest(std::vector<std::string>{"replicaof", "host", "0"})
          .ok());
  EXPECT_FALSE(ParseReplicaOfRequest(
                   std::vector<std::string>{"replicaof", "host", "65536"})
                   .ok());
}

}  // namespace
