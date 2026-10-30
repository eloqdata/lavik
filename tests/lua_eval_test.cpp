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

#include "lua_eval.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "lavik/command.h"
#include "lavik/resp.h"

namespace {

class LuaCatalogReset {
 public:
  ~LuaCatalogReset() {
    lavik::AbortStagedLuaFunctionCatalogLocally();
    const std::vector<std::string> empty;
    if (lavik::StageCompleteLuaFunctionCatalogLocally(empty).ok()) {
      lavik::CommitStagedLuaFunctionCatalogLocally();
    }
  }
};

TEST(LuaEvalTest, CatalogSwapKeepsSuspendedExecutionRuntimeAlive) {
  LuaCatalogReset reset;
  const std::vector<std::string> first_catalog{
      "#!lua name=runtime_owner\n"
      "redis.register_function{function_name='runtime_owner_value', "
      "callback=function(keys, args) return redis.call('PING') end, "
      "flags={'no-writes'}}"};
  ASSERT_TRUE(
      lavik::StageCompleteLuaFunctionCatalogLocally(first_catalog).ok());
  lavik::CommitStagedLuaFunctionCatalogLocally();

  const std::vector<std::string> no_arguments;
  auto old_execution = lavik::LuaExecution::CreateFunction(
      "runtime_owner_value", no_arguments, no_arguments);
  ASSERT_TRUE(old_execution.ok());
  lavik::LuaExecutionStep old_step = (*old_execution)->Start(false);
  ASSERT_TRUE(old_step.call_.has_value());
  ASSERT_EQ(old_step.call_->args_.size(), 1);
  EXPECT_EQ(old_step.call_->args_.front(), "PING");

  const std::vector<std::string> replacement_catalog{
      "#!lua name=runtime_owner\n"
      "redis.register_function{function_name='runtime_owner_value', "
      "callback=function(keys, args) return 'new' end, "
      "flags={'no-writes'}}"};
  ASSERT_TRUE(
      lavik::StageCompleteLuaFunctionCatalogLocally(replacement_catalog).ok());
  lavik::CommitStagedLuaFunctionCatalogLocally();

  old_step = (*old_execution)->Resume("+PONG\r\n");
  EXPECT_FALSE(old_step.call_.has_value());
  EXPECT_EQ(old_step.reply_, "+PONG\r\n");

  auto new_execution = lavik::LuaExecution::CreateFunction(
      "runtime_owner_value", no_arguments, no_arguments);
  ASSERT_TRUE(new_execution.ok());
  const lavik::LuaExecutionStep new_step = (*new_execution)->Start(false);
  EXPECT_FALSE(new_step.call_.has_value());
  EXPECT_EQ(new_step.reply_, "$3\r\nnew\r\n");
}

TEST(LuaEvalTest, RedisCallOwnsPackedArgumentsAcrossVmTeardownAndHandoff) {
  const std::vector<std::string> keys{std::string("key\0suffix", 10)};
  for (std::size_t size : {64, 4096}) {
    std::string value(size, 'v');
    value[size / 2] = '\0';
    const std::vector<std::string> argv{value};
    auto execution = lavik::LuaExecution::Create(
        "return redis.pcall('SET', KEYS[1], ARGV[1], 'PX', 9007199254740991)",
        keys, argv);
    ASSERT_TRUE(execution.ok());
    auto step = (*execution)->Start(false);
    ASSERT_TRUE(step.call_.has_value());
    EXPECT_TRUE(step.call_->protected_call_);
    auto* owner = &step.call_->args_.Get();
    auto* index = owner->args.data();
    auto* bytes = owner->bytes.data();

    // Subcommand bytes belong to the yielded handle, not the suspended VM.
    execution->reset();
    auto call = std::move(*step.call_);
    auto request = lavik::BuildCommandRequest(
        lavik::RespCommand{.args_ = std::move(call.args_)}, 0);
    ASSERT_TRUE(request.ok());
    EXPECT_FALSE(call.args_.HasStorage());
    EXPECT_EQ(&request->args_.Get(), owner);
    EXPECT_EQ(request->args_.Get().args.data(), index);
    EXPECT_EQ(request->args_.Get().bytes.data(), bytes);
    ASSERT_EQ(request->args_.size(), 5);
    EXPECT_EQ(request->args_[0], "SET");
    EXPECT_EQ(request->args_[1], keys.front());
    EXPECT_EQ(request->args_[2], value);
    EXPECT_EQ(request->args_[3], "PX");
    EXPECT_EQ(request->args_[4], "9007199254740991");
  }
}

TEST(LuaEvalTest, ErrorObjectsPreserveCountedProvenance) {
  const std::vector<std::string> no_arguments;
  for (const auto script :
       {"return redis.pcall('GET','key')", "return redis.call('GET','key')",
        "local e=redis.pcall('GET','key'); return {e,e}"}) {
    auto execution =
        lavik::LuaExecution::Create(script, no_arguments, no_arguments);
    ASSERT_TRUE(execution.ok());
    ASSERT_TRUE((*execution)->Start(false).call_.has_value());
    auto step = (*execution)->Resume("-WRONGTYPE wrong kind\r\n", true);
    EXPECT_EQ(step.outcome_.result_, lavik::CommandResult::kFailed);
    EXPECT_TRUE(step.errors_.empty()) << script;
    EXPECT_NE(step.reply_.find("-WRONGTYPE wrong kind\r\n"), std::string::npos);
  }
  auto execution = lavik::LuaExecution::Create(
      "local e=redis.pcall('GET','key'); return {err=e.err}", no_arguments,
      no_arguments);
  ASSERT_TRUE(execution.ok());
  ASSERT_TRUE((*execution)->Start(false).call_.has_value());
  auto step = (*execution)->Resume("-WRONGTYPE wrong kind\r\n", true);
  ASSERT_EQ(step.errors_.size(), 1);
  EXPECT_EQ(step.errors_[0].prefix_, lavik::CommandErrorPrefix::kWrongType);
}

TEST(LuaEvalTest, CaughtBoundaryErrorsAndNestedNewErrorsAreCounted) {
  const std::vector<std::string> no_arguments;
  auto execution = lavik::LuaExecution::Create("redis.pcall(); return 1",
                                               no_arguments, no_arguments);
  ASSERT_TRUE(execution.ok());
  auto step = (*execution)->Start(false);
  EXPECT_EQ(step.reply_, ":1\r\n");
  ASSERT_EQ(step.errors_.size(), 1);
  EXPECT_EQ(step.errors_[0].prefix_, lavik::CommandErrorPrefix::kErr);

  execution = lavik::LuaExecution::Create(
      "return {redis.error_reply('ERR first'), {redis.error_reply('OOM "
      "second')}}",
      no_arguments, no_arguments);
  ASSERT_TRUE(execution.ok());
  step = (*execution)->Start(false);
  ASSERT_EQ(step.errors_.size(), 2);
  EXPECT_EQ(step.errors_[0].prefix_, lavik::CommandErrorPrefix::kErr);
  EXPECT_EQ(step.errors_[1].prefix_, lavik::CommandErrorPrefix::kOom);
  EXPECT_EQ(step.outcome_.result_, lavik::CommandResult::kFailed);
  EXPECT_EQ(step.outcome_.prefix_, lavik::CommandErrorPrefix::kNone);
}

TEST(LuaEvalTest, ErrorProvenanceRequiresBooleanAndTransportIsBounded) {
  const std::vector<std::string> no_arguments;
  for (const auto script :
       {"return {err='ERR test',ignore_error_stats_update='false'}",
        "error({err='ERR test',ignore_error_stats_update=1})"}) {
    auto execution =
        lavik::LuaExecution::Create(script, no_arguments, no_arguments);
    ASSERT_TRUE(execution.ok());
    const auto step = (*execution)->Start(false);
    ASSERT_EQ(step.errors_.size(), 1);
    EXPECT_EQ(step.errors_[0].prefix_, lavik::CommandErrorPrefix::kErr);
    EXPECT_EQ(step.errors_[0].count_, 1);
  }
  auto execution =
      lavik::LuaExecution::Create("for i=1,1000 do redis.pcall() end; return 1",
                                  no_arguments, no_arguments);
  ASSERT_TRUE(execution.ok());
  const auto step = (*execution)->Start(false);
  EXPECT_EQ(step.reply_, ":1\r\n");
  ASSERT_EQ(step.errors_.size(), 1);
  EXPECT_EQ(step.errors_[0].count_, 1000);
}

}  // namespace
