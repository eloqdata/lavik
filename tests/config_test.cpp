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

#include "lavik/config.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "lavik/command.h"
#include "lavik/server.h"
#include "support/test_data_path.h"

namespace {

using lavik::ApplyRedisConfigDirective;
using lavik::LoadRedisConfigFile;
using lavik::ParseClientBufferLimit;
using lavik::ParseClientQueryBufferLimit;
using lavik::ParseMemorySize;
using lavik::ParseRedisConfigLine;
using lavik::ParseReplicaOfRequest;
using lavik::RewriteRedisConfigFile;
using lavik::ServerOptions;
using lavik::ValidateServerOptions;

class TempConfigFile {
 public:
  explicit TempConfigFile(std::string_view contents) {
    const auto suffix =
        std::chrono::steady_clock::now().time_since_epoch().count();
    path_ = lavik::test::TestDataDirectory() /
            ("lavik-config-test-" + std::to_string(suffix) + ".conf");
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

TEST(RedisConfigTest, ParsesRedisMemorySizes) {
  ASSERT_TRUE(ParseMemorySize("64mb").ok());
  EXPECT_EQ(*ParseMemorySize("64mb"), 64ULL * 1024 * 1024);
  EXPECT_EQ(*ParseMemorySize("1GB"), 1024ULL * 1024 * 1024);
  EXPECT_EQ(*ParseMemorySize("8388608"), 8ULL * 1024 * 1024);
  EXPECT_FALSE(ParseMemorySize("1.5gb").ok());
  EXPECT_FALSE(ParseMemorySize("8xb").ok());
}

TEST(RedisConfigTest, ParsesClientBufferPercentAndAbsoluteLimits) {
  auto percentage = ParseClientBufferLimit("7%");
  ASSERT_TRUE(percentage.ok()) << percentage.status();
  EXPECT_TRUE(percentage->percentage_);
  EXPECT_EQ(percentage->value_, 7);

  auto bytes = ParseClientBufferLimit("256mb");
  ASSERT_TRUE(bytes.ok()) << bytes.status();
  EXPECT_FALSE(bytes->percentage_);
  EXPECT_EQ(bytes->value_, 256ULL * 1024 * 1024);

  auto disabled = ParseClientBufferLimit("0");
  ASSERT_TRUE(disabled.ok()) << disabled.status();
  EXPECT_FALSE(disabled->percentage_);
  EXPECT_EQ(disabled->value_, 0);
  EXPECT_FALSE(ParseClientBufferLimit("101%").ok());
  EXPECT_FALSE(ParseClientBufferLimit("%").ok());
}

TEST(RedisConfigTest, ParsesRedisClientQueryBufferLimitRange) {
  EXPECT_EQ(*ParseClientQueryBufferLimit("1gb"), 1ULL * 1024 * 1024 * 1024);
  EXPECT_EQ(*ParseClientQueryBufferLimit("2gb"), 2ULL * 1024 * 1024 * 1024);
  EXPECT_EQ(*ParseClientQueryBufferLimit("1048576"), 1ULL * 1024 * 1024);
  EXPECT_FALSE(ParseClientQueryBufferLimit("1048575").ok());
  EXPECT_FALSE(ParseClientQueryBufferLimit("0").ok());
  EXPECT_FALSE(ParseClientQueryBufferLimit("5%").ok());
}

TEST(RedisConfigTest, ReplicaStaleReadsDefaultYesAndAcceptRedisBoolean) {
  ServerOptions options;
  EXPECT_TRUE(options.replication_options_.replica_serve_stale_data_);
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"replica-serve-stale-data", "no"}, &options)
          .ok());
  EXPECT_FALSE(options.replication_options_.replica_serve_stale_data_);
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"replica-serve-stale-data", "maybe"}, &options)
          .ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"replica-serve-stale-data"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"replica-serve-stale-data", "yes"}, &options)
          .ok());
  EXPECT_TRUE(options.replication_options_.replica_serve_stale_data_);
}

TEST(RedisConfigTest, AppliesSupportedDirectives) {
  ServerOptions options;
  EXPECT_TRUE(options.replication_options_.backlog_backpressure_);
  ASSERT_TRUE(ApplyRedisConfigDirective(
                  {"bind", "0.0.0.0", "::1", "redis.internal"}, &options)
                  .ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"port", "6380"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"io-threads", "4"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"maxclients", "12000"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"maxmemory-clients", "7%"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"client-query-buffer-limit", "64mb"}, &options)
          .ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"replicaof", "redis.internal", "6379"},
                                        &options)
                  .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"replica-read-only", "no"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective(
                  {"replication-backlog-backpressure", "no"}, &options)
                  .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"replica-priority", "42"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective(
                  {"replication-publish-queue-mb-per-worker", "64"}, &options)
                  .ok());
  ASSERT_TRUE(ApplyRedisConfigDirective(
                  {"registered-buffer-mb-per-worker", "192"}, &options)
                  .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"recv-buffers-per-worker", "2048"}, &options)
          .ok());
  ASSERT_TRUE(ApplyRedisConfigDirective(
                  {"storage-write-buffers-per-worker", "6"}, &options)
                  .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"storage-read-buffer-kb", "2048"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"repl-backlog-size", "2gb"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"load-rdb", "/backup/dump.rdb"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"load-rdb-replace", "yes"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"shutdown-checkpoint", "yes"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"dir", "/backup"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"dbfilename", "snapshot.rdb"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"foreground-budget-us", "250"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"background-budget-us", "20"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"background-warrant-percent", "7"}, &options)
          .ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"spdk-max-completions-per-poll", "4"},
                                        &options)
                  .ok());
  ASSERT_TRUE(ApplyRedisConfigDirective(
                  {"replication-snapshot-batch-size", "32"}, &options)
                  .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"slowlog-log-slower-than", "2500"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"slowlog-max-len", "64"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"lua-time-limit", "1234"}, &options).ok());

  EXPECT_EQ(options.bind_addresses_,
            (std::vector<std::string>{"0.0.0.0", "::1", "redis.internal"}));
  EXPECT_EQ(options.port_, 6380);
  EXPECT_EQ(options.thread_count_, 4u);
  EXPECT_EQ(options.max_clients_, 12000u);
  EXPECT_TRUE(options.maxmemory_clients_.percentage_);
  EXPECT_EQ(options.maxmemory_clients_.value_, 7u);
  EXPECT_EQ(options.client_query_buffer_limit_bytes_, 64ULL * 1024 * 1024);
  EXPECT_TRUE(options.shutdown_checkpoint_);
  ASSERT_TRUE(options.replicaof_.has_value());
  EXPECT_EQ(options.replicaof_->host_, "redis.internal");
  EXPECT_EQ(options.replicaof_->port_, 6379);
  EXPECT_FALSE(options.replication_options_.replica_read_only_);
  EXPECT_FALSE(options.replication_options_.backlog_backpressure_);
  EXPECT_EQ(options.replication_options_.replica_priority_, 42u);
  EXPECT_EQ(options.replication_publish_queue_bytes_, 64ULL * 1024 * 1024);
  EXPECT_EQ(options.registered_buffer_bytes_, 192ULL * 1024 * 1024);
  EXPECT_EQ(options.recv_buffer_count_, 2048u);
  EXPECT_EQ(options.storage_write_buffer_count_, 6u);
  EXPECT_EQ(options.storage_read_buffer_bytes_, 2ULL * 1024 * 1024);
  EXPECT_EQ(options.replication_options_.backlog_size_bytes_,
            2ULL * 1024 * 1024 * 1024);
  EXPECT_EQ(options.load_rdb_file_, "/backup/dump.rdb");
  EXPECT_TRUE(options.load_rdb_replace_);
  EXPECT_EQ(options.rdb_dir_, "/backup");
  EXPECT_EQ(options.dbfilename_, "snapshot.rdb");
  EXPECT_EQ(options.foreground_budget_us_, 250u);
  EXPECT_EQ(options.background_budget_us_, 20u);
  EXPECT_EQ(options.background_warrant_percent_, 7u);
  EXPECT_EQ(options.spdk_max_completions_per_poll_, 4u);
  EXPECT_EQ(options.replication_options_.snapshot_batch_size_, 32u);
  EXPECT_EQ(options.slowlog_log_slower_than_us_, 2500);
  EXPECT_EQ(options.slowlog_max_len_, 64u);
  EXPECT_EQ(options.lua_time_limit_ms_, 1234u);
}

TEST(RedisConfigTest, AppliesLoggingDirectivesAndAliases) {
  ServerOptions options;
  ASSERT_TRUE(ApplyRedisConfigDirective({"logtostderr", "yes"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"alsologtostderr", "yes"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"log_dir", "/var/log/lavik"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"max_log_size_mb", "256"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"max-log-files", "12"}, &options).ok());

  EXPECT_TRUE(options.logging_.log_to_stderr_);
  EXPECT_TRUE(options.logging_.also_log_to_stderr_);
  EXPECT_EQ(options.logging_.log_dir_, "/var/log/lavik");
  EXPECT_EQ(options.logging_.max_log_size_mb_, 256u);
  EXPECT_EQ(options.logging_.max_log_files_, 12u);

  ASSERT_TRUE(ApplyRedisConfigDirective({"logtostderr", "no"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"log-dir", "./logs"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"max-log-size-mb", "100"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"max_log_files", "10"}, &options).ok());
  EXPECT_FALSE(options.logging_.log_to_stderr_);
  EXPECT_EQ(options.logging_.log_dir_, "./logs");
  EXPECT_EQ(options.logging_.max_log_size_mb_, 100u);
  EXPECT_EQ(options.logging_.max_log_files_, 10u);
}

TEST(RedisConfigTest, RejectsInvalidLoggingConfiguration) {
  ServerOptions options;
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"logtostderr", "maybe"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"alsologtostderr", "1"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"max-log-size-mb", "0"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"max-log-files", "0"}, &options).ok());

  options.logging_.log_dir_.clear();
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  options.logging_.log_dir_ = "./logs";
  options.logging_.max_log_size_mb_ = std::numeric_limits<std::size_t>::max();
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  options.logging_.max_log_size_mb_ = 100;
  options.logging_.max_log_files_ = 200'002;
  EXPECT_FALSE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RejectsInvalidAndUnsupportedDirectives) {
  ServerOptions options;
  options.max_clients_ = 0;
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  options.max_clients_ = lavik::kDefaultMaxClients;
  EXPECT_FALSE(ApplyRedisConfigDirective({"port", "70000"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"replicaof", "host", "zero"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"replica-read-only", "maybe"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"shutdown-checkpoint", "maybe"}, &options)
          .ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"replica-priority", "-1"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"redis-export-backpressure", "yes"}, &options)
          .ok());
  EXPECT_FALSE(ApplyRedisConfigDirective(
                   {"replication-backlog-backpressure", "maybe"}, &options)
                   .ok());
  EXPECT_FALSE(ApplyRedisConfigDirective(
                   {"replication-publish-queue-mb-per-worker", "0"}, &options)
                   .ok());
  EXPECT_FALSE(ApplyRedisConfigDirective(
                   {"registered-buffer-mb-per-worker", "0"}, &options)
                   .ok());
  EXPECT_FALSE(ApplyRedisConfigDirective(
                   {"storage-write-buffers-per-worker", "0"}, &options)
                   .ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"storage-read-buffer-kb", "0"}, &options)
          .ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"repl-backlog-size", "0"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"repl-backlog-size", "large"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"foreground-budget-us", "0"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"background-budget-us", "0"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"background-warrant-percent", "101"}, &options)
          .ok());
  EXPECT_FALSE(ApplyRedisConfigDirective(
                   {"replication-snapshot-batch-size", "0"}, &options)
                   .ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"slowlog-log-slower-than", "-2"}, &options)
          .ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"slowlog-log-slower-than", "fast"}, &options)
          .ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"slowlog-max-len", "-1"}, &options).ok());
  EXPECT_FALSE(ApplyRedisConfigDirective({"maxclients", "0"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"maxmemory-clients", "101%"}, &options).ok());
  EXPECT_FALSE(ApplyRedisConfigDirective({"client-query-buffer-limit", "512kb"},
                                         &options)
                   .ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"maxclients", "many"}, &options).ok());
  EXPECT_FALSE(ApplyRedisConfigDirective({"appendonly", "yes"}, &options).ok());
}

TEST(RedisConfigTest, MetaSeedsSelectManagementWithoutLocalMode) {
  ServerOptions options;
  EXPECT_TRUE(ValidateServerOptions(options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"meta-seed", "127.0.0.1:17001"}, &options)
          .ok());
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"node-id", std::string(40, 'a')}, &options)
          .ok());
  EXPECT_TRUE(ValidateServerOptions(options).ok());
  options.meta_seeds_.clear();
  EXPECT_TRUE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RemovedModeDirectivesAreRejected) {
  ServerOptions options;
  for (const auto* name :
       {"cluster-enabled", "cluster-node-id", "cluster-meta-seed",
        "cluster-announce-ip", "cluster-announce-port",
        "cluster-announce-tls-port"}) {
    EXPECT_FALSE(ApplyRedisConfigDirective({name, "yes"}, &options).ok())
        << name;
  }
}

TEST(RedisConfigTest, RemovedLocalModesGiveMigrationInstructions) {
  ServerOptions options;
  for (const auto* name : {"client-mode", "meta-managed"}) {
    for (const auto* value : {"single", "cluster", "yes", "no"}) {
      const auto status = ApplyRedisConfigDirective({name, value}, &options);
      EXPECT_FALSE(status.ok());
      EXPECT_NE(status.message().find("removed"), std::string_view::npos);
      EXPECT_NE(status.message().find("Meta creation manifest"),
                std::string_view::npos);
    }
  }
}

TEST(RedisConfigTest, ClusterModeRejectsStandalonePopulationSources) {
  ServerOptions options;
  ASSERT_TRUE(
      ApplyRedisConfigDirective(
          {"node-id", "0123456789abcdef0123456789abcdef01234567"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"meta-seed", "127.0.0.1:17001"}, &options)
          .ok());
  EXPECT_TRUE(ValidateServerOptions(options).ok());

  options.replicaof_ = lavik::ReplicaOfConfig{"lavik.local", 6379};
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  options.replicaof_.reset();

  options.redis_replicaof_ = lavik::ReplicaOfConfig{"redis.local", 6380};
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  options.redis_replicaof_.reset();

  options.load_rdb_file_ = "/backup/dump.rdb";
  EXPECT_FALSE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, AppliesAndValidatesTlsAndPasswordDirectives) {
  ServerOptions options;
  ASSERT_TRUE(ApplyRedisConfigDirective({"port", "0"}, &options).ok());
  ASSERT_TRUE(ApplyRedisConfigDirective({"tls-port", "6380"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"tls-cert-file", "server.crt"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"tls-key-file", "server.key"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"tls-ca-cert-file", "ca.crt"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"tls-auth-clients", "optional"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"tls-replication", "yes"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"requirepass", "client-secret"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"masteruser", "default"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"masterauth", "source-secret"}, &options)
          .ok());

  EXPECT_TRUE(ValidateServerOptions(options).ok());
  EXPECT_EQ(options.tls_port_, 6380);
  EXPECT_EQ(options.tls_auth_clients_, "optional");
  EXPECT_TRUE(options.tls_replication_);
  EXPECT_EQ(options.requirepass_, "client-secret");
  EXPECT_EQ(options.masterauth_, "source-secret");

  options.tls_auth_clients_ = "invalid";
  EXPECT_FALSE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RejectsLoadRdbWithReplicaOf) {
  ServerOptions options;
  options.load_rdb_file_ = "/backup/dump.rdb";
  options.replicaof_ = lavik::ReplicaOfConfig{"redis.local", 6379};
  EXPECT_FALSE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, ParsesRedisPsyncFollower) {
  ServerOptions options;
  ASSERT_TRUE(ApplyRedisConfigDirective(
                  {"redis-replicaof", "redis.local", "6380"}, &options)
                  .ok());
  ASSERT_TRUE(options.redis_replicaof_.has_value());
  EXPECT_EQ(options.redis_replicaof_->host_, "redis.local");
  EXPECT_EQ(options.redis_replicaof_->port_, 6380);
  EXPECT_TRUE(ValidateServerOptions(options).ok());

  options.replication_options_.replica_read_only_ = false;
  EXPECT_TRUE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RejectsConflictingRedisPsyncSources) {
  ServerOptions options;
  options.replicaof_ = lavik::ReplicaOfConfig{"lavik.local", 6379};
  options.redis_replicaof_ = lavik::ReplicaOfConfig{"redis.local", 6380};
  EXPECT_FALSE(ValidateServerOptions(options).ok());

  options.replicaof_.reset();
  options.load_rdb_file_ = "/backup/dump.rdb";
  EXPECT_FALSE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RejectsLoadRdbReplaceWithoutSource) {
  ServerOptions options;
  options.load_rdb_replace_ = true;
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"load-rdb-replace", "maybe"}, &options).ok());
}

TEST(RedisConfigTest, RejectsInvalidRdbOutputNames) {
  ServerOptions options;
  options.rdb_dir_.clear();
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  options.rdb_dir_ = "/backup";
  options.dbfilename_ = "nested/dump.rdb";
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  options.dbfilename_ = "..";
  EXPECT_FALSE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, LoadsFileAndReportsLineNumber) {
  TempConfigFile valid(
      "# lavik test\nport 6381\nio-threads 2\n"
      "registered-buffer-mb-per-worker 128\n"
      "storage-write-buffers-per-worker 3\n"
      "replication-publish-queue-mb-per-worker 12\n"
      "recv-buffers-per-worker 0\nreplicaof redis.local 6379\n");
  ServerOptions options;
  absl::Status loaded = LoadRedisConfigFile(valid.path().string(), &options);
  ASSERT_TRUE(loaded.ok()) << loaded;
  EXPECT_EQ(options.config_file_, valid.path().string());
  EXPECT_EQ(options.port_, 6381);
  EXPECT_EQ(options.thread_count_, 2u);
  EXPECT_EQ(options.registered_buffer_bytes_, 128ULL * 1024 * 1024);
  EXPECT_EQ(options.storage_write_buffer_count_, 3u);
  EXPECT_EQ(options.replication_publish_queue_bytes_, 12ULL * 1024 * 1024);
  EXPECT_EQ(options.recv_buffer_count_, 0u);
  ASSERT_TRUE(options.replicaof_.has_value());

  TempConfigFile invalid("port 6381\nappendonly yes\n");
  loaded = LoadRedisConfigFile(invalid.path().string(), &options);
  ASSERT_FALSE(loaded.ok());
  EXPECT_NE(loaded.message().find(":2:"), std::string_view::npos);
}

TEST(RedisConfigTest, AtomicallyRewritesFailoverManagedDirectives) {
  TempConfigFile config(
      "# keep this comment\n"
      "bind 127.0.0.1\n"
      "replicaof old.example 6379\n"
      "replica-priority 80\n");

  absl::Status rewritten = RewriteRedisConfigFile(
      config.path().string(), lavik::ReplicaOfConfig{"new upstream#1", 6380},
      false, 20);
  ASSERT_TRUE(rewritten.ok()) << rewritten;

  ServerOptions following;
  ASSERT_TRUE(LoadRedisConfigFile(config.path().string(), &following).ok());
  EXPECT_EQ(following.bind_addresses_, (std::vector<std::string>{"127.0.0.1"}));
  ASSERT_TRUE(following.replicaof_.has_value());
  EXPECT_EQ(following.replicaof_->host_, "new upstream#1");
  EXPECT_EQ(following.replicaof_->port_, 6380);
  EXPECT_EQ(following.replication_options_.replica_priority_, 20u);

  rewritten =
      RewriteRedisConfigFile(config.path().string(), std::nullopt, false, 0);
  ASSERT_TRUE(rewritten.ok()) << rewritten;
  ServerOptions promoted;
  ASSERT_TRUE(LoadRedisConfigFile(config.path().string(), &promoted).ok());
  EXPECT_FALSE(promoted.replicaof_.has_value());
  EXPECT_FALSE(promoted.redis_replicaof_.has_value());
  EXPECT_EQ(promoted.replication_options_.replica_priority_, 0u);

  std::ifstream contents(config.path());
  const std::string text((std::istreambuf_iterator<char>(contents)),
                         std::istreambuf_iterator<char>());
  EXPECT_NE(text.find("# keep this comment"), std::string::npos);
  EXPECT_NE(text.find("bind 127.0.0.1"), std::string::npos);
  EXPECT_EQ(text.find("old.example"), std::string::npos);
  EXPECT_EQ(text.find("new upstream#1"), std::string::npos);
  EXPECT_NE(text.find("# Generated by CONFIG REWRITE"), std::string::npos);
}

TEST(RedisConfigTest, ConfigRewritePreservesMetaSeeds) {
  TempConfigFile config(
      "node-id 0123456789abcdef0123456789abcdef01234567\n"
      "meta-seed 127.0.0.1:17001\n"
      "replica-priority 80\n");

  absl::Status rewritten =
      RewriteRedisConfigFile(config.path().string(), std::nullopt, false, 0);
  ASSERT_TRUE(rewritten.ok()) << rewritten;

  ServerOptions options;
  ASSERT_TRUE(LoadRedisConfigFile(config.path().string(), &options).ok());
  EXPECT_EQ(options.meta_seeds_, (std::vector<std::string>{"127.0.0.1:17001"}));
  EXPECT_EQ(options.replication_options_.replica_priority_, 0u);
  EXPECT_TRUE(ValidateServerOptions(options).ok());

  std::ifstream contents(config.path());
  const std::string text((std::istreambuf_iterator<char>(contents)),
                         std::istreambuf_iterator<char>());
  EXPECT_NE(text.find("meta-seed 127.0.0.1:17001"), std::string::npos);
}

TEST(RedisConfigTest, RewriteRequiresConfigurationFile) {
  const absl::Status rewritten =
      RewriteRedisConfigFile({}, std::nullopt, false, 100);
  EXPECT_FALSE(rewritten.ok());
  EXPECT_NE(rewritten.message().find("without a config file"),
            std::string_view::npos);
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

TEST(RedisConfigTest, AppliesClusterDirectives) {
  ServerOptions options;
  ASSERT_TRUE(
      ApplyRedisConfigDirective(
          {"node-id", "0123456789abcdef0123456789abcdef01234567"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"meta-seed", "127.0.0.1:17001"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"announce-ip", "10.0.0.8"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"announce-port", "7390"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"announce-tls-port", "7391"}, &options).ok());

  EXPECT_EQ(options.meta_seeds_, (std::vector<std::string>{"127.0.0.1:17001"}));
  EXPECT_EQ(options.announce_ip_, "10.0.0.8");
  EXPECT_EQ(options.announce_port_, 7390);
  EXPECT_EQ(options.announce_tls_port_, 7391);
  EXPECT_TRUE(ValidateServerOptions(options).ok());

  // A zero announce port follows the corresponding listen port.
  ASSERT_TRUE(ApplyRedisConfigDirective({"announce-port", "0"}, &options).ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"announce-tls-port", "0"}, &options).ok());
  EXPECT_EQ(options.announce_port_, 0);
  EXPECT_EQ(options.announce_tls_port_, 0);
  EXPECT_TRUE(ValidateServerOptions(options).ok());

  EXPECT_EQ(options.client_mode_, lavik::ClientMode::kSingle);
}

TEST(RedisConfigTest, AppliesMetaControlledClusterDirectives) {
  ServerOptions options;
  ASSERT_TRUE(
      ApplyRedisConfigDirective(
          {"node-id", "0123456789abcdef0123456789abcdef01234567"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"meta-seed", "127.0.0.1:17001"}, &options)
          .ok());
  ASSERT_TRUE(
      ApplyRedisConfigDirective({"meta-seed", "[::1]:17002"}, &options).ok());

  EXPECT_EQ(options.node_id_, "0123456789abcdef0123456789abcdef01234567");
  EXPECT_EQ(options.meta_seeds_,
            (std::vector<std::string>{"127.0.0.1:17001", "[::1]:17002"}));
  EXPECT_TRUE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RequiresCompleteMetaControlConfiguration) {
  ServerOptions options;
  EXPECT_TRUE(ValidateServerOptions(options).ok());

  options.meta_seeds_.push_back("127.0.0.1:17001");
  EXPECT_FALSE(ValidateServerOptions(options).ok());  // node id required
  options.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  EXPECT_TRUE(ValidateServerOptions(options).ok());

  options.meta_seeds_.clear();
  EXPECT_TRUE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RejectsMalformedMetaNodeIdentityAndSeeds) {
  ServerOptions options;
  options.node_id_ = "0123456789abcdef0123456789abcdef01234567";

  for (const std::string seed : {"localhost:17001", "127.0.0.1", "::1",
                                 "::1:17001", "127.0.0.1:0", "[::1]:70000"}) {
    options.meta_seeds_ = {seed};
    EXPECT_FALSE(ValidateServerOptions(options).ok()) << seed;
  }
  options.meta_seeds_ = {"127.0.0.1:17001"};
  for (const std::string node_id :
       {"short", "0123456789ABCDEF0123456789abcdef01234567",
        "g123456789abcdef0123456789abcdef01234567"}) {
    options.node_id_ = node_id;
    EXPECT_FALSE(ValidateServerOptions(options).ok()) << node_id;
  }
}

TEST(RedisConfigTest, MetaControlMtlsRequiresCompleteClientIdentity) {
  ServerOptions options;
  options.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  options.meta_seeds_ = {"127.0.0.1:17001"};
  options.tls_replication_ = true;
  options.tls_ca_cert_file_ = "ca.crt";
  EXPECT_FALSE(ValidateServerOptions(options).ok());
  options.tls_cert_file_ = "node.crt";
  options.tls_key_file_ = "node.key";
  EXPECT_TRUE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RejectsInvalidClusterDirectives) {
  ServerOptions options;
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"meta-managed", "maybe"}, &options).ok());
  EXPECT_FALSE(ApplyRedisConfigDirective({"meta-managed"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"meta-managed", "yes", "extra"}, &options)
          .ok());
  EXPECT_FALSE(ApplyRedisConfigDirective({"announce-ip"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"announce-port", "65536"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"announce-port", "-1"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"announce-port", "http"}, &options).ok());
  EXPECT_FALSE(
      ApplyRedisConfigDirective({"announce-tls-port", "65536"}, &options).ok());
}

TEST(RedisConfigTest, RequiresNodeIdAndMetaSeedWhenManaged) {
  ServerOptions options;
  EXPECT_TRUE(ValidateServerOptions(options).ok());
  options.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  EXPECT_TRUE(ValidateServerOptions(options).ok());
  options.meta_seeds_ = {"127.0.0.1:17001"};
  EXPECT_TRUE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, RejectsClusterWithReplicationUpstream) {
  ServerOptions options;
  options.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  options.meta_seeds_ = {"127.0.0.1:17001"};
  options.replicaof_ = lavik::ReplicaOfConfig{"lavik.local", 6379};
  EXPECT_FALSE(ValidateServerOptions(options).ok());

  options.replicaof_.reset();
  options.redis_replicaof_ = lavik::ReplicaOfConfig{"redis.local", 6380};
  EXPECT_FALSE(ValidateServerOptions(options).ok());

  options.redis_replicaof_.reset();
  EXPECT_TRUE(ValidateServerOptions(options).ok());
}

TEST(RedisConfigTest, ValidatesClusterAnnouncePortResolution) {
  ServerOptions options;
  options.node_id_ = "0123456789abcdef0123456789abcdef01234567";
  options.meta_seeds_ = {"127.0.0.1:17001"};

  // TLS-only deployment: the zero announce ports follow the listen ports, so
  // the resolved TLS announce port is nonzero and the node is valid.
  options.port_ = 0;
  options.tls_port_ = 6380;
  options.tls_cert_file_ = "server.crt";
  options.tls_key_file_ = "server.key";
  EXPECT_TRUE(ValidateServerOptions(options).ok());

  // An explicit announce port wins over the listen port it follows.
  options.announce_tls_port_ = 7391;
  EXPECT_TRUE(ValidateServerOptions(options).ok());
  options.announce_tls_port_ = 0;

  // With neither a plaintext nor a TLS listener there is no client endpoint
  // left to announce, and cluster mode must refuse to start.
  options.tls_port_ = 0;
  EXPECT_FALSE(ValidateServerOptions(options).ok());
}

}  // namespace
