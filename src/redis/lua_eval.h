#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "keylane/resp_version.h"

namespace keylane {

enum LuaFunctionFlag : std::uint64_t {
  kLuaFunctionNoWrites = 1ULL << 0,
  kLuaFunctionAllowOom = 1ULL << 1,
  kLuaFunctionAllowStale = 1ULL << 2,
  kLuaFunctionNoCluster = 1ULL << 3,
  kLuaFunctionAllowCrossSlotKeys = 1ULL << 4,
};

struct LuaFunctionInfo {
  std::string name_;
  std::optional<std::string> description_;
  std::uint64_t flags_ = 0;
};

struct LuaFunctionLibrary {
  std::string name_;
  std::string engine_ = "LUA";
  std::string code_;
  std::vector<LuaFunctionInfo> functions_;
};

struct LuaRedisCall {
  bool protected_call_ = false;
  std::vector<std::string> args_;
};

struct LuaExecutionStep {
  std::optional<LuaRedisCall> call_;
  std::string reply_;
  bool scheduler_yield_ = false;
};

enum class LuaScriptKillResult {
  kKilled,
  kNotBusy,
  kUnkillableWrite,
  kUnkillableReplication,
  kWrongInvocationKind,
};

struct LuaRunningInvocation {
  bool is_function_ = false;
  std::string name_;
  std::vector<std::string> command_;
  std::uint64_t duration_ms_ = 0;
};

class LuaExecution {
 public:
  static absl::StatusOr<std::unique_ptr<LuaExecution>> Create(
      std::string_view script, std::span<const std::string> keys,
      std::span<const std::string> argv,
      RespVersion client_resp_version = RespVersion::k2);
  static absl::StatusOr<std::unique_ptr<LuaExecution>> CreateCached(
      std::string_view sha, std::span<const std::string> keys,
      std::span<const std::string> argv,
      RespVersion client_resp_version = RespVersion::k2);
  static absl::StatusOr<std::unique_ptr<LuaExecution>> CreateFunction(
      std::string_view name, std::span<const std::string> keys,
      std::span<const std::string> argv,
      RespVersion client_resp_version = RespVersion::k2);

  LuaExecution(const LuaExecution&) = delete;
  LuaExecution& operator=(const LuaExecution&) = delete;
  ~LuaExecution();

  // lua_dump output for the user chunk. It can be loaded into another
  // lua_State created by this binary without parsing the source again.
  std::string_view bytecode() const;
  LuaExecutionStep Start(bool replication_origin,
                         std::string_view script_name = "user_script",
                         std::span<const std::string> invocation_command = {});
  LuaExecutionStep Resume(std::string_view command_reply);
  LuaExecutionStep ResumeAfterSchedulerYield();
  // Returns false if SCRIPT KILL won the race before this write started.
  bool MarkWriteCommand();
  RespVersion resp_version() const;
  std::uint64_t function_flags() const;

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

// Each worker keeps a persistent Lua VM and one registry function per SHA.
// The bytecode view refers to StoreLuaScript-owned immutable storage.
bool CacheLuaScriptLocally(std::string_view sha, std::string_view bytecode);
std::optional<std::string_view> FindCachedLuaScript(std::string_view sha);
void ClearLocalLuaScriptCache();
void ClearStoredLuaScripts();
// Returns the process-wide SCRIPT cache population. The count is synchronized
// with SCRIPT LOAD/EVAL insertion and SCRIPT FLUSH.
std::size_t StoredLuaScriptCount();

// FUNCTION LOAD is staged independently on every worker. Callers commit only
// after every worker compiled the source and registered an identical function
// set; abort discards the unpublished registry references.
absl::StatusOr<LuaFunctionLibrary> StageLuaFunctionLibraryLocally(
    std::string_view code, bool replace);
void CommitStagedLuaFunctionLibraryLocally();
void AbortStagedLuaFunctionLibraryLocally();
bool DeleteLuaFunctionLibraryLocally(std::string_view name);
void ClearLuaFunctionLibrariesLocally();

void StoreLuaFunctionLibrary(LuaFunctionLibrary library);
bool DeleteStoredLuaFunctionLibrary(std::string_view name);
void ClearStoredLuaFunctionLibraries();
std::vector<LuaFunctionLibrary> SnapshotLuaFunctionLibraries();

LuaScriptKillResult RequestLuaScriptKill(bool function);
std::optional<LuaRunningInvocation> SnapshotLuaRunningInvocation();
bool LuaScriptsBusy();
void SetLuaScriptBusyThresholdMs(std::uint64_t milliseconds);
std::uint64_t LuaScriptBusyThresholdMs();

}  // namespace keylane
