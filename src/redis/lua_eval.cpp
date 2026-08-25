#include "lua_eval.h"

#include <openssl/evp.h>

#include <array>
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
constexpr std::chrono::seconds kScriptTimeLimit{5};

struct ScriptKey : std::array<char, 40> {
  ScriptKey() = default;
  explicit ScriptKey(std::string_view sha) {
    std::copy_n(sha.data(), size(), data());
  }
};

// Like Dragonfly's ScriptMgr, the flat table stores a separately allocated
// immutable body. Rehashing the table therefore cannot invalidate views held
// by worker-local indexes. A future SCRIPT FLUSH must clear every local index
// before releasing these bodies.
std::mutex g_script_bodies_mutex;
absl::flat_hash_map<ScriptKey, std::unique_ptr<const std::string>>
    g_script_bodies;

// EVAL broadcasts a successfully compiled script to every worker before
// replying, so EVALSHA's normal path needs no lock or cross-core hop.
thread_local absl::flat_hash_map<ScriptKey, std::string_view> g_script_cache;

struct LuaRunLimit {
  std::chrono::steady_clock::time_point deadline_;
};

char g_run_limit_registry_key;

void ScriptInstructionHook(lua_State* state, lua_Debug*) {
  lua_pushlightuserdata(state, &g_run_limit_registry_key);
  lua_rawget(state, LUA_REGISTRYINDEX);
  auto* limit = static_cast<LuaRunLimit*>(lua_touserdata(state, -1));
  lua_pop(state, 1);
  if (limit != nullptr &&
      std::chrono::steady_clock::now() >= limit->deadline_) {
    luaL_error(state, "Script timed out");
  }
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
redis.LOG_DEBUG = 0
redis.LOG_VERBOSE = 1
redis.LOG_NOTICE = 2
redis.LOG_WARNING = 3
function redis.log(...) end
)LUA";

void SetStringArray(lua_State* state, const char* name,
                    std::span<const std::string> values) {
  lua_createtable(state, static_cast<int>(values.size()), 0);
  for (std::size_t i = 0; i < values.size(); ++i) {
    lua_pushlstring(state, values[i].data(), values[i].size());
    lua_rawseti(state, -2, static_cast<int>(i + 1));
  }
  lua_setglobal(state, name);
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
      lua_pushlstring(state, line.data(), line.size());
      return absl::OkStatus();
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

void AppendLuaValue(lua_State* state, int index, ReplyBuilder* builder,
                    int depth) {
  if (depth > kMaxReplyDepth) {
    builder->AppendError("ERR Lua reply has too many nested levels");
    return;
  }
  if (index < 0) index = lua_gettop(state) + index + 1;
  switch (lua_type(state, index)) {
    case LUA_TNIL:
      builder->AppendNullBulkString();
      return;
    case LUA_TBOOLEAN:
      if (lua_toboolean(state, index) != 0)
        builder->AppendInteger(1);
      else
        builder->AppendNullBulkString();
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
      lua_getfield(state, index, "err");
      if (!lua_isnil(state, -1)) {
        std::size_t size = 0;
        const char* text = lua_tolstring(state, -1, &size);
        if (text == nullptr)
          builder->AppendError("ERR invalid error reply");
        else
          builder->AppendError(std::string_view(text, size));
        lua_pop(state, 1);
        return;
      }
      lua_pop(state, 1);
      lua_getfield(state, index, "ok");
      if (!lua_isnil(state, -1)) {
        std::size_t size = 0;
        const char* text = lua_tolstring(state, -1, &size);
        if (text == nullptr)
          builder->AppendError("ERR invalid status reply");
        else
          builder->AppendSimpleString(std::string_view(text, size));
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
        AppendLuaValue(state, -1, builder, depth + 1);
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

}  // namespace

struct LuaExecution::Impl {
  lua_State* state_ = nullptr;
  LuaRunLimit run_limit_;
  std::string bytecode_;

  ~Impl() {
    if (state_ != nullptr) lua_close(state_);
  }

  LuaExecutionStep Run(int resume_args) {
    for (;;) {
      const int status = lua_resume(state_, resume_args);
      resume_args = 0;
      if (status == 0) {
        ReplyBuilder builder;
        if (lua_gettop(state_) == 0)
          builder.AppendNullBulkString();
        else
          AppendLuaValue(state_, -1, &builder, 0);
        return LuaExecutionStep{.call_ = std::nullopt,
                                .reply_ = std::move(builder).Release()};
      }
      if (status != LUA_YIELD) return RuntimeErrorStep(state_);

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
    std::span<const std::string> argv) {
  auto execution = CreateFromBytecode({}, keys, argv);
  if (!execution.ok()) return execution.status();
  lua_State* state = (*execution)->impl_->state_;
  if (luaL_loadbuffer(state, script.data(), script.size(), "@user_script") !=
      0) {
    return absl::InvalidArgumentError(LuaError(state));
  }
  if (lua_dump(state, AppendBytecode, &(*execution)->impl_->bytecode_) != 0) {
    return absl::InternalError("unable to serialize compiled Lua script");
  }
  return execution;
}

absl::StatusOr<std::unique_ptr<LuaExecution>> LuaExecution::CreateFromBytecode(
    std::string_view bytecode, std::span<const std::string> keys,
    std::span<const std::string> argv) {
  auto impl = std::make_unique<Impl>();
  impl->state_ = luaL_newstate();
  if (impl->state_ == nullptr) {
    return absl::ResourceExhaustedError("unable to create Lua interpreter");
  }
  lua_State* state = impl->state_;
  lua_pushlightuserdata(state, &g_run_limit_registry_key);
  lua_pushlightuserdata(state, &impl->run_limit_);
  lua_rawset(state, LUA_REGISTRYINDEX);
  lua_sethook(state, ScriptInstructionHook, LUA_MASKCOUNT, 100000);
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
  SetStringArray(state, "KEYS", keys);
  SetStringArray(state, "ARGV", argv);
  if (!bytecode.empty() &&
      luaL_loadbytecode(state, bytecode.data(), bytecode.size(),
                        "@cached_script") != 0) {
    return absl::InvalidArgumentError(LuaError(state));
  }
  return std::unique_ptr<LuaExecution>(new LuaExecution(std::move(impl)));
}

std::string_view LuaExecution::bytecode() const { return impl_->bytecode_; }

LuaExecutionStep LuaExecution::Start() {
  impl_->run_limit_.deadline_ =
      std::chrono::steady_clock::now() + kScriptTimeLimit;
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

void CacheLuaScriptLocally(std::string_view sha, std::string_view bytecode) {
  if (sha.size() != ScriptKey{}.size()) return;
  g_script_cache.insert_or_assign(ScriptKey(sha), bytecode);
}

std::optional<std::string_view> FindCachedLuaScript(std::string_view sha) {
  if (sha.size() != ScriptKey{}.size()) return std::nullopt;
  const auto found = g_script_cache.find(ScriptKey(sha));
  if (found == g_script_cache.end()) return std::nullopt;
  return found->second;
}

void ClearLocalLuaScriptCache() { g_script_cache.clear(); }

void ClearStoredLuaScripts() {
  std::lock_guard<std::mutex> lock(g_script_bodies_mutex);
  g_script_bodies.clear();
}

}  // namespace keylane
