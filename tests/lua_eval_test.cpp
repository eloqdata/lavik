#include "lua_eval.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace {

class LuaCatalogReset {
 public:
  ~LuaCatalogReset() {
    keylane::AbortStagedLuaFunctionCatalogLocally();
    const std::vector<std::string> empty;
    if (keylane::StageCompleteLuaFunctionCatalogLocally(empty).ok()) {
      keylane::CommitStagedLuaFunctionCatalogLocally();
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
      keylane::StageCompleteLuaFunctionCatalogLocally(first_catalog).ok());
  keylane::CommitStagedLuaFunctionCatalogLocally();

  const std::vector<std::string> no_arguments;
  auto old_execution = keylane::LuaExecution::CreateFunction(
      "runtime_owner_value", no_arguments, no_arguments);
  ASSERT_TRUE(old_execution.ok());
  keylane::LuaExecutionStep old_step = (*old_execution)->Start(false);
  ASSERT_TRUE(old_step.call_.has_value());
  EXPECT_EQ(old_step.call_->args_, (std::vector<std::string>{"PING"}));

  const std::vector<std::string> replacement_catalog{
      "#!lua name=runtime_owner\n"
      "redis.register_function{function_name='runtime_owner_value', "
      "callback=function(keys, args) return 'new' end, "
      "flags={'no-writes'}}"};
  ASSERT_TRUE(
      keylane::StageCompleteLuaFunctionCatalogLocally(replacement_catalog)
          .ok());
  keylane::CommitStagedLuaFunctionCatalogLocally();

  old_step = (*old_execution)->Resume("+PONG\r\n");
  EXPECT_FALSE(old_step.call_.has_value());
  EXPECT_EQ(old_step.reply_, "+PONG\r\n");

  auto new_execution = keylane::LuaExecution::CreateFunction(
      "runtime_owner_value", no_arguments, no_arguments);
  ASSERT_TRUE(new_execution.ok());
  const keylane::LuaExecutionStep new_step = (*new_execution)->Start(false);
  EXPECT_FALSE(new_step.call_.has_value());
  EXPECT_EQ(new_step.reply_, "$3\r\nnew\r\n");
}

}  // namespace
