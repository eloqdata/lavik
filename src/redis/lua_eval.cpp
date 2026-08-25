#include "lua_eval.h"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "keylane/resp.h"
#include "spdlog/spdlog.h"

extern "C" {
#include "lauxlib.h"
#include "lua.h"
#include "lualib.h"

int luaopen_bit(lua_State* state);
int luaopen_cjson(lua_State* state);
int luaopen_cmsgpack(lua_State* state);
int luaopen_struct(lua_State* state);
}

namespace keylane {
namespace {

constexpr int kMaxReplyDepth = 128;
std::atomic<std::uint64_t> g_script_busy_threshold_ms{5000};

struct ScriptKey : std::array<char, 40> {
  ScriptKey() = default;
  explicit ScriptKey(std::string_view sha) {
    std::copy_n(sha.data(), size(), data());
  }
};

// Like Dragonfly's ScriptMgr, the flat table stores a separately allocated
// immutable body. Rehashing the table therefore cannot invalidate views held
// by worker-local indexes. SCRIPT FLUSH clears every local index before
// releasing these bodies.
std::mutex g_script_bodies_mutex;
absl::flat_hash_map<ScriptKey, std::unique_ptr<const std::string>>
    g_script_bodies;

enum class LuaRunState : std::uint8_t {
  kClean,
  kDirty,
  kKillRequested,
};

struct LuaRunControl {
  std::chrono::steady_clock::time_point deadline_;
  std::atomic<LuaRunState> state_{LuaRunState::kClean};
  std::atomic<bool> busy_{false};
  bool replication_origin_ = false;
  bool scheduler_yield_pending_ = false;
  std::string script_name_;
  RespVersion resp_version_ = RespVersion::k2;
};

char g_run_control_registry_key;
std::mutex g_active_scripts_mutex;
std::vector<LuaRunControl*> g_active_scripts;
std::atomic<std::uint32_t> g_busy_script_count{0};

// Redis replaces Lua's libc-dependent rand() implementation with the fixed
// 48-bit LCG used by redisLrand48(). Keep one stream next to each worker-local
// Lua runtime; math.randomseed() makes a particular invocation reproducible
// regardless of the host libc or architecture.
thread_local std::uint64_t g_lua_random_state = 0x1234abcd330eULL;

std::int32_t NextLuaRandom() {
  constexpr std::uint64_t kMask = (1ULL << 48) - 1;
  g_lua_random_state =
      (g_lua_random_state * 0x5deece66dULL + 0xbULL) & kMask;
  return static_cast<std::int32_t>(g_lua_random_state >> 17);
}

int RedisMathRandom(lua_State* state) {
  constexpr std::int32_t kRandomMax = 0x7fffffff;
  const lua_Number random =
      static_cast<lua_Number>(NextLuaRandom() % kRandomMax) /
      static_cast<lua_Number>(kRandomMax);
  switch (lua_gettop(state)) {
    case 0:
      lua_pushnumber(state, random);
      return 1;
    case 1: {
      const int upper = luaL_checkint(state, 1);
      luaL_argcheck(state, upper >= 1, 1, "interval is empty");
      lua_pushnumber(state, std::floor(random * upper) + 1);
      return 1;
    }
    case 2: {
      const int lower = luaL_checkint(state, 1);
      const int upper = luaL_checkint(state, 2);
      luaL_argcheck(state, lower <= upper, 2, "interval is empty");
      lua_pushnumber(state,
                     std::floor(random * (upper - lower + 1.0)) + lower);
      return 1;
    }
    default:
      return luaL_error(state, "wrong number of arguments");
  }
}

int RedisMathRandomSeed(lua_State* state) {
  const std::int32_t seed = luaL_checkint(state, 1);
  g_lua_random_state =
      (static_cast<std::uint64_t>(static_cast<std::uint32_t>(seed)) << 16) |
      0x330eULL;
  return 0;
}

int RedisReplicateCommands(lua_State* state) {
  // Since Redis 7, effects replication is always enabled and this legacy API
  // is retained solely for compatibility.
  lua_pushboolean(state, 1);
  return 1;
}

int RedisSetResp(lua_State* state) {
  if (lua_gettop(state) != 1) {
    return luaL_error(state, "redis.setresp() requires one argument.");
  }
  const int version = static_cast<int>(lua_tonumber(state, 1));
  if (version != 2 && version != 3) {
    return luaL_error(state, "RESP version must be 2 or 3.");
  }
  lua_pushlightuserdata(state, &g_run_control_registry_key);
  lua_rawget(state, LUA_REGISTRYINDEX);
  auto* control = static_cast<LuaRunControl*>(lua_touserdata(state, -1));
  lua_pop(state, 1);
  if (control == nullptr) {
    return luaL_error(state, "redis.setresp() is only available while running a script.");
  }
  control->resp_version_ =
      version == 3 ? RespVersion::k3 : RespVersion::k2;
  return 0;
}

int RedisLog(lua_State* state) {
  const int argc = lua_gettop(state);
  if (argc < 2) {
    return luaL_error(state, "redis.log() requires two arguments or more.");
  }
  if (!lua_isnumber(state, 1)) {
    return luaL_error(state, "First argument must be a number (log level).");
  }
  const int level = static_cast<int>(lua_tonumber(state, 1));
  if (level < 0 || level > 3) {
    return luaL_error(state, "Invalid debug level.");
  }

  std::string message;
  for (int index = 2; index <= argc; ++index) {
    std::size_t size = 0;
    const char* value = lua_tolstring(state, index, &size);
    if (value == nullptr) continue;
    if (index != 2) message.push_back(' ');
    message.append(value, size);
  }
  switch (level) {
    case 0:
    case 1:
      spdlog::debug("{}", message);
      break;
    case 2:
      spdlog::info("{}", message);
      break;
    case 3:
      spdlog::warn("{}", message);
      break;
  }
  return 0;
}

void RegisterActiveScript(LuaRunControl* run) {
  std::lock_guard lock(g_active_scripts_mutex);
  g_active_scripts.push_back(run);
}

void UnregisterActiveScript(LuaRunControl* run) {
  std::lock_guard lock(g_active_scripts_mutex);
  const auto found = std::find(g_active_scripts.begin(), g_active_scripts.end(),
                               run);
  if (found != g_active_scripts.end()) g_active_scripts.erase(found);
}

void ScriptInstructionHook(lua_State* state, lua_Debug*) {
  lua_pushlightuserdata(state, &g_run_control_registry_key);
  lua_rawget(state, LUA_REGISTRYINDEX);
  auto* control = static_cast<LuaRunControl*>(lua_touserdata(state, -1));
  lua_pop(state, 1);
  if (control == nullptr) return;
  if (control->state_.load(std::memory_order_acquire) ==
      LuaRunState::kKillRequested) {
    (void)luaL_error(state, "Script killed by user with SCRIPT KILL...");
    return;
  }
  if (std::chrono::steady_clock::now() < control->deadline_) return;
  if (!control->busy_.exchange(true, std::memory_order_acq_rel)) {
    g_busy_script_count.fetch_add(1, std::memory_order_acq_rel);
    spdlog::warn(
        "Slow script detected: still in execution after {} milliseconds. "
        "You can try killing the script using the SCRIPT KILL command. "
        "Script name is: {}.",
        g_script_busy_threshold_ms.load(std::memory_order_acquire),
        control->script_name_);
  }
  // After the busy threshold, yield the coroutine so this worker can process
  // pending requests including SCRIPT KILL without aborting the script.
  control->scheduler_yield_pending_ = true;
  (void)lua_yield(state, 0);
}

std::string LuaError(lua_State* state) {
  std::size_t size = 0;
  const char* text = lua_tolstring(state, -1, &size);
  return text == nullptr ? "unknown Lua error" : std::string(text, size);
}

int AppendBytecode(lua_State*, const void* bytes, std::size_t size,
                   void* output) {
  static_cast<std::string*>(output)->append(static_cast<const char*>(bytes),
                                            size);
  return 0;
}

void OpenLibrary(lua_State* state, const char* name, lua_CFunction open) {
  lua_pushcfunction(state, open);
  lua_pushstring(state, name);
  lua_call(state, 1, 0);
}

int YieldRedisCall(lua_State* state) {
  return lua_yield(state, lua_gettop(state));
}

int Sha1Hex(lua_State* state) {
  std::size_t size = 0;
  const char* text = luaL_checklstring(state, 1, &size);
  const std::string sha = LuaScriptSha1(std::string_view(text, size));
  lua_pushlstring(state, sha.data(), sha.size());
  return 1;
}

constexpr std::string_view kRedisLibrary = R"LUA(
redis = {}

local function keylane_dispatch(protected_call, ...)
  local result = {__keylane_call(protected_call, ...)}
  if not result[1] then
    if protected_call then
      return {err = result[2]}
    end
    error(result[2], 2)
  end
  return result[2]
end

function redis.call(...)
  return keylane_dispatch(false, ...)
end

function redis.pcall(...)
  return keylane_dispatch(true, ...)
end

function redis.error_reply(message)
  return {err = message}
end

function redis.status_reply(message)
  return {ok = message}
end

redis.sha1hex = __keylane_sha1hex
redis.REDIS_VERSION = "7.2.4"
redis.REDIS_VERSION_NUM = 0x00070204
redis.LOG_DEBUG = 0
redis.LOG_VERBOSE = 1
redis.LOG_NOTICE = 2
redis.LOG_WARNING = 3
)LUA";

void SetTableFunction(lua_State* state, int table, const char* name,
                      lua_CFunction function) {
  if (table < 0) table = lua_gettop(state) + table + 1;
  lua_pushcfunction(state, function);
  lua_setfield(state, table, name);
}

void SetStringArray(lua_State* state, const char* name,
                    std::span<const std::string> values) {
  lua_createtable(state, static_cast<int>(values.size()), 0);
  for (std::size_t i = 0; i < values.size(); ++i) {
    lua_pushlstring(state, values[i].data(), values[i].size());
    lua_rawseti(state, -2, static_cast<int>(i + 1));
  }
  lua_enablereadonlytable(state, LUA_GLOBALSINDEX, 0);
  lua_setglobal(state, name);
  lua_enablereadonlytable(state, LUA_GLOBALSINDEX, 1);
}

int AbsoluteStackIndex(lua_State* state, int index) {
  return index < 0 ? lua_gettop(state) + index + 1 : index;
}

void ProtectTableRecursively(lua_State* state, int index) {
  index = AbsoluteStackIndex(state, index);
  if (!lua_istable(state, index) || lua_isreadonlytable(state, index)) return;
  // Mark before descending so cycles such as _G._G terminate immediately.
  lua_enablereadonlytable(state, index, 1);
  lua_pushnil(state);
  while (lua_next(state, index) != 0) {
    if (lua_istable(state, -1)) ProtectTableRecursively(state, -1);
    lua_pop(state, 1);
  }
  if (lua_getmetatable(state, index) != 0) {
    ProtectTableRecursively(state, -1);
    lua_pop(state, 1);
  }
}

struct CachedLuaFunction {
  std::string_view bytecode_;
  int registry_ref_ = LUA_NOREF;
};

class LuaWorkerRuntime {
 public:
  static absl::StatusOr<std::unique_ptr<LuaWorkerRuntime>> Create() {
    auto runtime = std::unique_ptr<LuaWorkerRuntime>(new LuaWorkerRuntime());
    runtime->root_ = luaL_newstate();
    if (runtime->root_ == nullptr) {
      return absl::ResourceExhaustedError("unable to create Lua interpreter");
    }
    lua_State* state = runtime->root_;
    OpenLibrary(state, "", luaopen_base);
    OpenLibrary(state, LUA_TABLIBNAME, luaopen_table);
    OpenLibrary(state, LUA_STRLIBNAME, luaopen_string);
    OpenLibrary(state, LUA_MATHLIBNAME, luaopen_math);
    OpenLibrary(state, "cjson", luaopen_cjson);
    OpenLibrary(state, "struct", luaopen_struct);
    OpenLibrary(state, "cmsgpack", luaopen_cmsgpack);
    OpenLibrary(state, "bit", luaopen_bit);
    for (const char* unsafe : {"dofile", "loadfile", "print"}) {
      lua_pushnil(state);
      lua_setglobal(state, unsafe);
    }
    lua_pushcfunction(state, YieldRedisCall);
    lua_setglobal(state, "__keylane_call");
    lua_pushcfunction(state, Sha1Hex);
    lua_setglobal(state, "__keylane_sha1hex");
    if (luaL_loadbuffer(state, kRedisLibrary.data(), kRedisLibrary.size(),
                        "@keylane_redis") != 0 ||
        lua_pcall(state, 0, 0, 0) != 0) {
      return absl::InternalError(LuaError(state));
    }

    lua_getglobal(state, "redis");
    SetTableFunction(state, -1, "log", RedisLog);
    SetTableFunction(state, -1, "replicate_commands",
                     RedisReplicateCommands);
    SetTableFunction(state, -1, "setresp", RedisSetResp);
    lua_pop(state, 1);
    lua_getglobal(state, "math");
    SetTableFunction(state, -1, "random", RedisMathRandom);
    SetTableFunction(state, -1, "randomseed", RedisMathRandomSeed);
    lua_pop(state, 1);

    // Valkey protects its persistent global environment recursively. KEYS and
    // ARGV are replaced by the host between serialized executions by briefly
    // opening only the global table itself.
    lua_pushvalue(state, LUA_GLOBALSINDEX);
    ProtectTableRecursively(state, -1);
    lua_pop(state, 1);
    return runtime;
  }

  LuaWorkerRuntime(const LuaWorkerRuntime&) = delete;
  LuaWorkerRuntime& operator=(const LuaWorkerRuntime&) = delete;

  ~LuaWorkerRuntime() {
    if (root_ != nullptr) lua_close(root_);
  }

  lua_State* NewThread(int* registry_ref) {
    lua_State* thread = lua_newthread(root_);
    *registry_ref = luaL_ref(root_, LUA_REGISTRYINDEX);
    return thread;
  }

  void ReleaseThread(int registry_ref) {
    luaL_unref(root_, LUA_REGISTRYINDEX, registry_ref);
    if (++completed_executions_ % 50 == 0) {
      (void)lua_gc(root_, LUA_GCSTEP, 50);
    }
  }

  bool Cache(std::string_view sha, std::string_view bytecode) {
    if (sha.size() != ScriptKey{}.size()) return false;
    const ScriptKey key(sha);
    if (scripts_.contains(key)) return true;
    if (luaL_loadbytecode(root_, bytecode.data(), bytecode.size(),
                          "@cached_script") != 0) {
      lua_pop(root_, 1);
      return false;
    }
    const int function_ref = luaL_ref(root_, LUA_REGISTRYINDEX);
    scripts_.emplace(key, CachedLuaFunction{bytecode, function_ref});
    return true;
  }

  bool PushCached(lua_State* thread, std::string_view sha) {
    if (sha.size() != ScriptKey{}.size()) return false;
    const auto found = scripts_.find(ScriptKey(sha));
    if (found == scripts_.end()) return false;
    lua_rawgeti(root_, LUA_REGISTRYINDEX, found->second.registry_ref_);
    lua_xmove(root_, thread, 1);
    // A script may call setfenv on itself. Restore the protected worker-global
    // environment before every invocation of the cached closure.
    lua_pushvalue(thread, LUA_GLOBALSINDEX);
    (void)lua_setfenv(thread, -2);
    return true;
  }

  std::optional<std::string_view> Find(std::string_view sha) const {
    if (sha.size() != ScriptKey{}.size()) return std::nullopt;
    const auto found = scripts_.find(ScriptKey(sha));
    if (found == scripts_.end()) return std::nullopt;
    return found->second.bytecode_;
  }

  void ClearScripts() {
    for (const auto& [key, script] : scripts_) {
      (void)key;
      luaL_unref(root_, LUA_REGISTRYINDEX, script.registry_ref_);
    }
    scripts_.clear();
    (void)lua_gc(root_, LUA_GCCOLLECT, 0);
  }

 private:
  LuaWorkerRuntime() = default;

  lua_State* root_ = nullptr;
  absl::flat_hash_map<ScriptKey, CachedLuaFunction> scripts_;
  std::uint64_t completed_executions_ = 0;
};

thread_local std::unique_ptr<LuaWorkerRuntime> g_lua_runtime;

absl::StatusOr<LuaWorkerRuntime*> WorkerLuaRuntime() {
  if (g_lua_runtime == nullptr) {
    auto runtime = LuaWorkerRuntime::Create();
    if (!runtime.ok()) return runtime.status();
    g_lua_runtime = std::move(*runtime);
  }
  return g_lua_runtime.get();
}

absl::Status ReadLine(std::string_view encoded, std::size_t* position,
                      std::string_view* line) {
  const std::size_t end = encoded.find("\r\n", *position);
  if (end == std::string_view::npos) {
    return absl::InvalidArgumentError("truncated RESP reply");
  }
  *line = encoded.substr(*position, end - *position);
  *position = end + 2;
  return absl::OkStatus();
}

absl::StatusOr<long long> ParseLength(std::string_view text) {
  long long value = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto parsed = std::from_chars(begin, end, value);
  if (parsed.ec != std::errc{} || parsed.ptr != end) {
    return absl::InvalidArgumentError("invalid RESP length");
  }
  return value;
}

absl::Status PushWrappedString(lua_State* state, const char* field,
                               std::string_view value) {
  lua_createtable(state, 0, 1);
  lua_pushlstring(state, value.data(), value.size());
  lua_setfield(state, -2, field);
  return absl::OkStatus();
}

absl::Status ReadBulkPayload(std::string_view encoded, std::size_t* position,
                             std::string_view* payload) {
  std::string_view line;
  absl::Status status = ReadLine(encoded, position, &line);
  if (!status.ok()) return status;
  auto length = ParseLength(line);
  if (!length.ok()) return length.status();
  if (*length < 0 ||
      static_cast<std::uint64_t>(*length) > encoded.size() - *position) {
    return absl::InvalidArgumentError("invalid RESP bulk length");
  }
  const std::size_t size = static_cast<std::size_t>(*length);
  if (encoded.size() - *position < size + 2 ||
      encoded.substr(*position + size, 2) != "\r\n") {
    return absl::InvalidArgumentError("truncated RESP bulk reply");
  }
  *payload = encoded.substr(*position, size);
  *position += size + 2;
  return absl::OkStatus();
}

absl::Status PushRespValue(lua_State* state, std::string_view encoded,
                           std::size_t* position, int depth) {
  if (depth > kMaxReplyDepth || *position >= encoded.size()) {
    return absl::InvalidArgumentError("invalid RESP reply");
  }
  const char type = encoded[(*position)++];
  std::string_view line;
  switch (type) {
    case '+': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      return PushWrappedString(state, "ok", line);
    }
    case ':': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      auto value = ParseLength(line);
      if (!value.ok()) return value.status();
      lua_pushnumber(state, static_cast<lua_Number>(*value));
      return absl::OkStatus();
    }
    case '$': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      auto length = ParseLength(line);
      if (!length.ok()) return length.status();
      if (*length == -1) {
        lua_pushboolean(state, 0);
        return absl::OkStatus();
      }
      if (*length < 0 ||
          static_cast<std::uint64_t>(*length) > encoded.size() - *position) {
        return absl::InvalidArgumentError("invalid RESP bulk length");
      }
      const std::size_t size = static_cast<std::size_t>(*length);
      if (encoded.size() - *position < size + 2 ||
          encoded.substr(*position + size, 2) != "\r\n") {
        return absl::InvalidArgumentError("truncated RESP bulk reply");
      }
      lua_pushlstring(state, encoded.data() + *position, size);
      *position += size + 2;
      return absl::OkStatus();
    }
    case '=': {
      std::string_view payload;
      absl::Status status = ReadBulkPayload(encoded, position, &payload);
      if (!status.ok()) return status;
      if (payload.size() < 4 || payload[3] != ':') {
        return absl::InvalidArgumentError("invalid RESP verbatim string");
      }
      lua_createtable(state, 0, 1);
      lua_createtable(state, 0, 2);
      lua_pushlstring(state, payload.data() + 4, payload.size() - 4);
      lua_setfield(state, -2, "string");
      lua_pushlstring(state, payload.data(), 3);
      lua_setfield(state, -2, "format");
      lua_setfield(state, -2, "verbatim_string");
      return absl::OkStatus();
    }
    case '_': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok() || !line.empty()) {
        return absl::InvalidArgumentError("invalid RESP null");
      }
      lua_pushnil(state);
      return absl::OkStatus();
    }
    case '#': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok() || (line != "t" && line != "f")) {
        return absl::InvalidArgumentError("invalid RESP boolean");
      }
      lua_pushboolean(state, line == "t");
      return absl::OkStatus();
    }
    case ',': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      double value = 0;
      if (line == "inf") {
        value = std::numeric_limits<double>::infinity();
      } else if (line == "-inf") {
        value = -std::numeric_limits<double>::infinity();
      } else if (line == "nan") {
        value = std::numeric_limits<double>::quiet_NaN();
      } else {
        const auto parsed =
            std::from_chars(line.data(), line.data() + line.size(), value);
        if (parsed.ec != std::errc{} ||
            parsed.ptr != line.data() + line.size()) {
          return absl::InvalidArgumentError("invalid RESP double");
        }
      }
      lua_createtable(state, 0, 1);
      lua_pushnumber(state, value);
      lua_setfield(state, -2, "double");
      return absl::OkStatus();
    }
    case '(': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      return PushWrappedString(state, "big_number", line);
    }
    case '*': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      auto length = ParseLength(line);
      if (!length.ok()) return length.status();
      if (*length == -1) {
        lua_pushboolean(state, 0);
        return absl::OkStatus();
      }
      if (*length < 0 || *length > std::numeric_limits<int>::max()) {
        return absl::InvalidArgumentError("invalid RESP array length");
      }
      lua_createtable(state, static_cast<int>(*length), 0);
      for (int i = 1; i <= static_cast<int>(*length); ++i) {
        status = PushRespValue(state, encoded, position, depth + 1);
        if (!status.ok()) return status;
        lua_rawseti(state, -2, i);
      }
      return absl::OkStatus();
    }
    case '%': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      auto length = ParseLength(line);
      if (!length.ok()) return length.status();
      if (*length < 0 || *length > std::numeric_limits<int>::max()) {
        return absl::InvalidArgumentError("invalid RESP map length");
      }
      lua_createtable(state, 0, 1);
      lua_createtable(state, 0, static_cast<int>(*length));
      for (int i = 0; i < static_cast<int>(*length); ++i) {
        status = PushRespValue(state, encoded, position, depth + 1);
        if (!status.ok()) return status;
        status = PushRespValue(state, encoded, position, depth + 1);
        if (!status.ok()) return status;
        lua_rawset(state, -3);
      }
      lua_setfield(state, -2, "map");
      return absl::OkStatus();
    }
    case '~': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      auto length = ParseLength(line);
      if (!length.ok()) return length.status();
      if (*length < 0 || *length > std::numeric_limits<int>::max()) {
        return absl::InvalidArgumentError("invalid RESP set length");
      }
      lua_createtable(state, 0, 1);
      lua_createtable(state, 0, static_cast<int>(*length));
      for (int i = 0; i < static_cast<int>(*length); ++i) {
        status = PushRespValue(state, encoded, position, depth + 1);
        if (!status.ok()) return status;
        lua_pushboolean(state, 1);
        lua_rawset(state, -3);
      }
      lua_setfield(state, -2, "set");
      return absl::OkStatus();
    }
    case '>': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      auto length = ParseLength(line);
      if (!length.ok()) return length.status();
      if (*length < 0 || *length > std::numeric_limits<int>::max()) {
        return absl::InvalidArgumentError("invalid RESP push length");
      }
      lua_createtable(state, static_cast<int>(*length), 0);
      for (int i = 1; i <= static_cast<int>(*length); ++i) {
        status = PushRespValue(state, encoded, position, depth + 1);
        if (!status.ok()) return status;
        lua_rawseti(state, -2, i);
      }
      return absl::OkStatus();
    }
    case '|': {
      absl::Status status = ReadLine(encoded, position, &line);
      if (!status.ok()) return status;
      auto length = ParseLength(line);
      if (!length.ok()) return length.status();
      if (*length < 0 || *length > std::numeric_limits<int>::max()) {
        return absl::InvalidArgumentError("invalid RESP attribute length");
      }
      for (int i = 0; i < static_cast<int>(*length) * 2; ++i) {
        status = PushRespValue(state, encoded, position, depth + 1);
        if (!status.ok()) return status;
        lua_pop(state, 1);
      }
      return PushRespValue(state, encoded, position, depth + 1);
    }
    default:
      return absl::InvalidArgumentError("unsupported RESP reply type");
  }
}

std::string RespErrorMessage(std::string_view encoded) {
  if (!encoded.empty() && encoded.front() == '-') encoded.remove_prefix(1);
  const std::size_t end = encoded.find("\r\n");
  if (end != std::string_view::npos) encoded = encoded.substr(0, end);
  return std::string(encoded);
}

void RawGetField(lua_State* state, int table, const char* field) {
  table = AbsoluteStackIndex(state, table);
  lua_pushstring(state, field);
  lua_rawget(state, table);
}

std::size_t LuaTableEntryCount(lua_State* state, int table) {
  table = AbsoluteStackIndex(state, table);
  std::size_t count = 0;
  lua_pushnil(state);
  while (lua_next(state, table) != 0) {
    ++count;
    lua_pop(state, 1);
  }
  return count;
}

std::string SanitizeRespLine(std::string_view value) {
  std::string result(value);
  for (char& byte : result) {
    if (byte == '\r' || byte == '\n') byte = ' ';
  }
  return result;
}

void AppendLuaValue(lua_State* state, int index, ReplyBuilder* builder,
                    RespVersion script_resp_version, int depth) {
  if (depth > kMaxReplyDepth) {
    builder->AppendError("ERR Lua reply has too many nested levels");
    return;
  }
  if (index < 0) index = lua_gettop(state) + index + 1;
  switch (lua_type(state, index)) {
    case LUA_TNIL:
      builder->AppendNull();
      return;
    case LUA_TBOOLEAN:
      if (script_resp_version == RespVersion::k3) {
        builder->AppendBoolean(lua_toboolean(state, index) != 0);
      } else if (lua_toboolean(state, index) != 0) {
        builder->AppendInteger(1);
      } else {
        builder->AppendNull();
      }
      return;
    case LUA_TNUMBER: {
      const lua_Number number = lua_tonumber(state, index);
      if (!std::isfinite(number) ||
          number >
              static_cast<lua_Number>(std::numeric_limits<long long>::max()) ||
          number <
              static_cast<lua_Number>(std::numeric_limits<long long>::min())) {
        builder->AppendError("ERR invalid Lua number");
      } else {
        builder->AppendInteger(static_cast<long long>(number));
      }
      return;
    }
    case LUA_TSTRING: {
      std::size_t size = 0;
      const char* text = lua_tolstring(state, index, &size);
      builder->AppendBulkString(std::string_view(text, size));
      return;
    }
    case LUA_TTABLE: {
      RawGetField(state, index, "err");
      if (lua_type(state, -1) == LUA_TSTRING) {
        std::size_t size = 0;
        const char* text = lua_tolstring(state, -1, &size);
        if (text == nullptr)
          builder->AppendError("ERR invalid error reply");
        else
          builder->AppendError(SanitizeRespLine(std::string_view(text, size)));
        lua_pop(state, 1);
        return;
      }
      lua_pop(state, 1);
      RawGetField(state, index, "ok");
      if (lua_type(state, -1) == LUA_TSTRING) {
        std::size_t size = 0;
        const char* text = lua_tolstring(state, -1, &size);
        if (text == nullptr)
          builder->AppendError("ERR invalid status reply");
        else
          builder->AppendSimpleString(
              SanitizeRespLine(std::string_view(text, size)));
        lua_pop(state, 1);
        return;
      }
      lua_pop(state, 1);
      RawGetField(state, index, "double");
      if (lua_type(state, -1) == LUA_TNUMBER) {
        builder->AppendDouble(lua_tonumber(state, -1));
        lua_pop(state, 1);
        return;
      }
      lua_pop(state, 1);
      RawGetField(state, index, "big_number");
      if (lua_type(state, -1) == LUA_TSTRING) {
        std::size_t size = 0;
        const char* text = lua_tolstring(state, -1, &size);
        builder->AppendBigNumber(
            SanitizeRespLine(std::string_view(text, size)));
        lua_pop(state, 1);
        return;
      }
      lua_pop(state, 1);
      RawGetField(state, index, "verbatim_string");
      if (lua_istable(state, -1)) {
        RawGetField(state, -1, "format");
        RawGetField(state, -2, "string");
        if (lua_type(state, -2) == LUA_TSTRING &&
            lua_type(state, -1) == LUA_TSTRING) {
          std::size_t format_size = 0;
          std::size_t value_size = 0;
          const char* format = lua_tolstring(state, -2, &format_size);
          const char* value = lua_tolstring(state, -1, &value_size);
          if (format_size == 3) {
            builder->AppendVerbatimString(
                std::string_view(format, format_size),
                std::string_view(value, value_size));
            lua_pop(state, 3);
            return;
          }
        }
        lua_pop(state, 2);
      }
      lua_pop(state, 1);
      RawGetField(state, index, "map");
      if (lua_istable(state, -1)) {
        const int map = AbsoluteStackIndex(state, -1);
        builder->AppendMapHeader(LuaTableEntryCount(state, map));
        lua_pushnil(state);
        while (lua_next(state, map) != 0) {
          lua_pushvalue(state, -2);
          AppendLuaValue(state, -1, builder, script_resp_version, depth + 1);
          lua_pop(state, 1);
          AppendLuaValue(state, -1, builder, script_resp_version, depth + 1);
          lua_pop(state, 1);
        }
        lua_pop(state, 1);
        return;
      }
      lua_pop(state, 1);
      RawGetField(state, index, "set");
      if (lua_istable(state, -1)) {
        const int set = AbsoluteStackIndex(state, -1);
        builder->AppendSetHeader(LuaTableEntryCount(state, set));
        lua_pushnil(state);
        while (lua_next(state, set) != 0) {
          lua_pop(state, 1);
          lua_pushvalue(state, -1);
          AppendLuaValue(state, -1, builder, script_resp_version, depth + 1);
          lua_pop(state, 1);
        }
        lua_pop(state, 1);
        return;
      }
      lua_pop(state, 1);
      std::size_t count = 0;
      for (;;) {
        lua_rawgeti(state, index, static_cast<int>(count + 1));
        const bool end = lua_isnil(state, -1);
        lua_pop(state, 1);
        if (end) break;
        ++count;
      }
      builder->AppendArrayHeader(count);
      for (std::size_t i = 1; i <= count; ++i) {
        lua_rawgeti(state, index, static_cast<int>(i));
        AppendLuaValue(state, -1, builder, script_resp_version, depth + 1);
        lua_pop(state, 1);
      }
      return;
    }
    default:
      builder->AppendNullBulkString();
      return;
  }
}

LuaExecutionStep RuntimeErrorStep(lua_State* state) {
  return LuaExecutionStep{
      .call_ = std::nullopt,
      .reply_ = EncodeError(
          absl::StrCat("ERR Error running script: ", LuaError(state))),
  };
}

LuaExecutionStep KilledStep() {
  return LuaExecutionStep{
      .call_ = std::nullopt,
      .reply_ = EncodeError("ERR Script killed by user with SCRIPT KILL..."),
  };
}

}  // namespace

struct LuaExecution::Impl {
  LuaWorkerRuntime* runtime_ = nullptr;
  lua_State* state_ = nullptr;
  int thread_ref_ = LUA_NOREF;
  LuaRunControl run_control_;
  std::string bytecode_;
  bool active_registered_ = false;
  RespVersion client_resp_version_ = RespVersion::k2;

  ~Impl() {
    if (state_ == nullptr) return;
    lua_sethook(state_, nullptr, 0, 0);
    if (run_control_.busy_.exchange(false, std::memory_order_acq_rel)) {
      g_busy_script_count.fetch_sub(1, std::memory_order_acq_rel);
    }
    if (active_registered_) {
      UnregisterActiveScript(&run_control_);
      lua_pushlightuserdata(state_, &g_run_control_registry_key);
      lua_pushnil(state_);
      lua_rawset(state_, LUA_REGISTRYINDEX);
    }
    runtime_->ReleaseThread(thread_ref_);
  }

  LuaExecutionStep Run(int resume_args) {
    for (;;) {
      const int status = lua_resume(state_, resume_args);
      resume_args = 0;
      if (status == 0) {
        if (run_control_.state_.load(std::memory_order_acquire) ==
            LuaRunState::kKillRequested) {
          return KilledStep();
        }
        ReplyBuilder builder(client_resp_version_);
        if (lua_gettop(state_) == 0)
          builder.AppendNull();
        else
          AppendLuaValue(state_, -1, &builder, run_control_.resp_version_, 0);
        return LuaExecutionStep{.call_ = std::nullopt,
                                .reply_ = std::move(builder).Release()};
      }
      if (status != LUA_YIELD) return RuntimeErrorStep(state_);

      if (run_control_.scheduler_yield_pending_) {
        run_control_.scheduler_yield_pending_ = false;
        return LuaExecutionStep{
            .call_ = std::nullopt, .reply_ = {}, .scheduler_yield_ = true};
      }

      const int count = lua_gettop(state_);
      bool valid = count >= 2 && lua_isboolean(state_, 1);
      LuaRedisCall call;
      if (valid) {
        call.protected_call_ = lua_toboolean(state_, 1) != 0;
        call.args_.reserve(static_cast<std::size_t>(count - 1));
        for (int i = 2; i <= count; ++i) {
          std::size_t size = 0;
          const char* value = lua_tolstring(state_, i, &size);
          if (value == nullptr) {
            valid = false;
            break;
          }
          call.args_.emplace_back(value, size);
        }
      }
      if (valid && !call.args_.empty()) {
        return LuaExecutionStep{.call_ = std::move(call), .reply_ = {}};
      }

      lua_settop(state_, 0);
      lua_pushboolean(state_, 0);
      constexpr std::string_view error =
          "ERR Lua redis() command arguments must be strings or integers";
      lua_pushlstring(state_, error.data(), error.size());
      resume_args = 2;
    }
  }
};

LuaExecution::LuaExecution(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

LuaExecution::~LuaExecution() = default;

absl::StatusOr<std::unique_ptr<LuaExecution>> LuaExecution::Create(
    std::string_view script, std::span<const std::string> keys,
    std::span<const std::string> argv, RespVersion client_resp_version) {
  auto runtime = WorkerLuaRuntime();
  if (!runtime.ok()) return runtime.status();
  auto impl = std::make_unique<Impl>();
  impl->client_resp_version_ = client_resp_version;
  impl->runtime_ = *runtime;
  impl->state_ = (*runtime)->NewThread(&impl->thread_ref_);
  lua_State* state = impl->state_;
  lua_sethook(state, ScriptInstructionHook, LUA_MASKCOUNT, 100000);
  SetStringArray(state, "KEYS", keys);
  SetStringArray(state, "ARGV", argv);
  if (luaL_loadbuffer(state, script.data(), script.size(), "@user_script") !=
      0) {
    return absl::InvalidArgumentError(LuaError(state));
  }
  if (lua_dump(state, AppendBytecode, &impl->bytecode_) != 0) {
    return absl::InternalError("unable to serialize compiled Lua script");
  }
  return std::unique_ptr<LuaExecution>(new LuaExecution(std::move(impl)));
}

absl::StatusOr<std::unique_ptr<LuaExecution>> LuaExecution::CreateCached(
    std::string_view sha, std::span<const std::string> keys,
    std::span<const std::string> argv, RespVersion client_resp_version) {
  auto runtime = WorkerLuaRuntime();
  if (!runtime.ok()) return runtime.status();
  auto impl = std::make_unique<Impl>();
  impl->client_resp_version_ = client_resp_version;
  impl->runtime_ = *runtime;
  impl->state_ = (*runtime)->NewThread(&impl->thread_ref_);
  lua_State* state = impl->state_;
  lua_sethook(state, ScriptInstructionHook, LUA_MASKCOUNT, 100000);
  SetStringArray(state, "KEYS", keys);
  SetStringArray(state, "ARGV", argv);
  if (!(*runtime)->PushCached(state, sha)) {
    return absl::NotFoundError("script is not present in the worker cache");
  }
  return std::unique_ptr<LuaExecution>(new LuaExecution(std::move(impl)));
}

std::string_view LuaExecution::bytecode() const { return impl_->bytecode_; }

LuaExecutionStep LuaExecution::Start(bool replication_origin,
                                     std::string_view script_name) {
  const auto now = std::chrono::steady_clock::now();
  const std::uint64_t threshold =
      g_script_busy_threshold_ms.load(std::memory_order_acquire);
  const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::time_point::max() - now)
                             .count();
  impl_->run_control_.deadline_ =
      threshold > static_cast<std::uint64_t>(remaining)
          ? std::chrono::steady_clock::time_point::max()
          : now + std::chrono::milliseconds(threshold);
  impl_->run_control_.replication_origin_ = replication_origin;
  impl_->run_control_.busy_.store(false, std::memory_order_release);
  impl_->run_control_.script_name_.assign(script_name);
  impl_->run_control_.resp_version_ = RespVersion::k2;
  impl_->run_control_.state_.store(LuaRunState::kClean,
                                   std::memory_order_release);
  lua_pushlightuserdata(impl_->state_, &g_run_control_registry_key);
  lua_pushlightuserdata(impl_->state_, &impl_->run_control_);
  lua_rawset(impl_->state_, LUA_REGISTRYINDEX);
  impl_->active_registered_ = true;
  RegisterActiveScript(&impl_->run_control_);
  return impl_->Run(0);
}

LuaExecutionStep LuaExecution::Resume(std::string_view command_reply) {
  lua_settop(impl_->state_, 0);
  if (!command_reply.empty() && command_reply.front() == '-') {
    const std::string error = RespErrorMessage(command_reply);
    lua_pushboolean(impl_->state_, 0);
    lua_pushlstring(impl_->state_, error.data(), error.size());
    return impl_->Run(2);
  }
  lua_pushboolean(impl_->state_, 1);
  std::size_t position = 0;
  absl::Status parsed =
      PushRespValue(impl_->state_, command_reply, &position, 0);
  if (!parsed.ok() || position != command_reply.size()) {
    lua_settop(impl_->state_, 0);
    lua_pushboolean(impl_->state_, 0);
    const std::string error =
        absl::StrCat("ERR invalid internal command reply: ", parsed.message());
    lua_pushlstring(impl_->state_, error.data(), error.size());
  }
  return impl_->Run(2);
}

LuaExecutionStep LuaExecution::ResumeAfterSchedulerYield() {
  return impl_->Run(0);
}

bool LuaExecution::MarkWriteCommand() {
  LuaRunState expected = LuaRunState::kClean;
  if (impl_->run_control_.state_.compare_exchange_strong(
          expected, LuaRunState::kDirty, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return true;
  }
  return expected == LuaRunState::kDirty;
}

RespVersion LuaExecution::resp_version() const {
  return impl_->run_control_.resp_version_;
}

std::string LuaScriptSha1(std::string_view script) {
  std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
  unsigned int digest_size = 0;
  EVP_MD_CTX* context = EVP_MD_CTX_new();
  if (context == nullptr) return {};
  const bool ok =
      EVP_DigestInit_ex(context, EVP_sha1(), nullptr) == 1 &&
      EVP_DigestUpdate(context, script.data(), script.size()) == 1 &&
      EVP_DigestFinal_ex(context, digest.data(), &digest_size) == 1;
  EVP_MD_CTX_free(context);
  if (!ok) return {};
  constexpr char hex[] = "0123456789abcdef";
  std::string result(digest_size * 2, '\0');
  for (unsigned int i = 0; i < digest_size; ++i) {
    result[i * 2] = hex[digest[i] >> 4];
    result[i * 2 + 1] = hex[digest[i] & 0x0f];
  }
  return result;
}

std::string_view StoreLuaScript(std::string_view sha,
                                std::string_view bytecode) {
  if (sha.size() != ScriptKey{}.size()) return {};
  std::lock_guard<std::mutex> lock(g_script_bodies_mutex);
  auto [found, inserted] = g_script_bodies.try_emplace(ScriptKey(sha), nullptr);
  if (inserted || found->second == nullptr) {
    found->second = std::make_unique<const std::string>(bytecode);
  }
  return *found->second;
}

bool CacheLuaScriptLocally(std::string_view sha, std::string_view bytecode) {
  auto runtime = WorkerLuaRuntime();
  return runtime.ok() && (*runtime)->Cache(sha, bytecode);
}

std::optional<std::string_view> FindCachedLuaScript(std::string_view sha) {
  if (g_lua_runtime == nullptr) return std::nullopt;
  return g_lua_runtime->Find(sha);
}

void ClearLocalLuaScriptCache() {
  if (g_lua_runtime != nullptr) g_lua_runtime->ClearScripts();
}

void ClearStoredLuaScripts() {
  std::lock_guard<std::mutex> lock(g_script_bodies_mutex);
  g_script_bodies.clear();
}

LuaScriptKillResult RequestLuaScriptKill() {
  std::lock_guard lock(g_active_scripts_mutex);
  if (g_active_scripts.empty()) return LuaScriptKillResult::kNotBusy;

  bool killed = false;
  bool dirty = false;
  bool replication = false;
  for (LuaRunControl* run : g_active_scripts) {
    if (run->replication_origin_) {
      replication = true;
      continue;
    }
    LuaRunState expected = LuaRunState::kClean;
    if (run->state_.compare_exchange_strong(
            expected, LuaRunState::kKillRequested, std::memory_order_acq_rel,
            std::memory_order_acquire) ||
        expected == LuaRunState::kKillRequested) {
      killed = true;
    } else if (expected == LuaRunState::kDirty) {
      dirty = true;
    }
  }
  if (killed) return LuaScriptKillResult::kKilled;
  if (replication) return LuaScriptKillResult::kUnkillableReplication;
  if (dirty) return LuaScriptKillResult::kUnkillableWrite;
  return LuaScriptKillResult::kNotBusy;
}

bool LuaScriptsBusy() {
  return g_busy_script_count.load(std::memory_order_acquire) != 0;
}

void SetLuaScriptBusyThresholdMs(std::uint64_t milliseconds) {
  g_script_busy_threshold_ms.store(milliseconds, std::memory_order_release);
}

std::uint64_t LuaScriptBusyThresholdMs() {
  return g_script_busy_threshold_ms.load(std::memory_order_acquire);
}

}  // namespace keylane
