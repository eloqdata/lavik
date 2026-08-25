#pragma once

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"

namespace keylane {

struct LuaRedisCall {
  bool protected_call_ = false;
  std::vector<std::string> args_;
};

struct LuaExecutionStep {
  std::optional<LuaRedisCall> call_;
  std::string reply_;
};

class LuaExecution {
 public:
  static absl::StatusOr<std::unique_ptr<LuaExecution>> Create(
      std::string_view script, std::span<const std::string> keys,
      std::span<const std::string> argv);
  static absl::StatusOr<std::unique_ptr<LuaExecution>> CreateFromBytecode(
      std::string_view bytecode, std::span<const std::string> keys,
      std::span<const std::string> argv);

  LuaExecution(const LuaExecution&) = delete;
  LuaExecution& operator=(const LuaExecution&) = delete;
  ~LuaExecution();

  // lua_dump output for the user chunk. It can be loaded into another
  // lua_State created by this binary without parsing the source again.
  std::string_view bytecode() const;
  LuaExecutionStep Start();
  LuaExecutionStep Resume(std::string_view command_reply);

 private:
  struct Impl;
  explicit LuaExecution(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

std::string LuaScriptSha1(std::string_view script);
// Stores one stable process-wide copy of the compiled chunk and returns a view
// that is valid until the script cache is flushed or the process exits.
std::string_view StoreLuaScript(std::string_view sha,
                                std::string_view bytecode);

// The index is worker-local. Its string_view refers to StoreLuaScript-owned
// immutable storage, so EVALSHA reads need neither a lock nor a source copy.
void CacheLuaScriptLocally(std::string_view sha, std::string_view bytecode);
std::optional<std::string_view> FindCachedLuaScript(std::string_view sha);
void ClearLocalLuaScriptCache();
void ClearStoredLuaScripts();

}  // namespace keylane
