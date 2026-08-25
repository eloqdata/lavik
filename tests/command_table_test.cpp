#include "keylane/command_table.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "keylane/command.h"
#include "keylane/glob.h"

namespace {

using keylane::CommandCanonicalName;
using keylane::CommandKind;
using keylane::CommandSpec;
using keylane::DetermineKeys;
using keylane::FindCommand;
using keylane::KeyIndexView;

// Test-only transliteration of Valkey's stringmatchlen_impl (nocase=false).
// Keeping the oracle independent of the iterative production matcher catches
// subtle cursor-consumption differences in malformed/trailing character
// classes that are difficult to establish by inspection.
bool ValkeyGlobReferenceImpl(const char* pattern, int pattern_length,
                             const char* text, int text_length,
                             bool* skip_longer_matches) {
  while (pattern_length != 0 && text_length != 0) {
    switch (pattern[0]) {
      case '*':
        while (pattern_length > 1 && pattern[1] == '*') {
          ++pattern;
          --pattern_length;
        }
        if (pattern_length == 1) return true;
        while (text_length != 0) {
          if (ValkeyGlobReferenceImpl(pattern + 1, pattern_length - 1, text,
                                      text_length, skip_longer_matches)) {
            return true;
          }
          if (*skip_longer_matches) return false;
          ++text;
          --text_length;
        }
        *skip_longer_matches = true;
        return false;
      case '?':
        ++text;
        --text_length;
        break;
      case '[': {
        ++pattern;
        --pattern_length;
        bool negate = pattern_length != 0 && pattern[0] == '^';
        if (negate) {
          ++pattern;
          --pattern_length;
        }
        bool matched = false;
        while (true) {
          if (pattern_length >= 2 && pattern[0] == '\\') {
            ++pattern;
            --pattern_length;
            matched = matched || pattern[0] == text[0];
          } else if (pattern_length != 0 && pattern[0] == ']') {
            break;
          } else if (pattern_length == 0) {
            --pattern;
            ++pattern_length;
            break;
          } else if (pattern_length >= 3 && pattern[1] == '-') {
            unsigned char first = static_cast<unsigned char>(pattern[0]);
            unsigned char last = static_cast<unsigned char>(pattern[2]);
            if (first > last) std::swap(first, last);
            const unsigned char value = static_cast<unsigned char>(text[0]);
            matched = matched || (value >= first && value <= last);
            pattern += 2;
            pattern_length -= 2;
          } else {
            matched = matched || pattern[0] == text[0];
          }
          ++pattern;
          --pattern_length;
        }
        if (negate) matched = !matched;
        if (!matched) return false;
        ++text;
        --text_length;
        break;
      }
      case '\\':
        if (pattern_length >= 2) {
          ++pattern;
          --pattern_length;
        }
        [[fallthrough]];
      default:
        if (pattern[0] != text[0]) return false;
        ++text;
        --text_length;
        break;
    }
    ++pattern;
    --pattern_length;
    if (text_length == 0) {
      while (pattern_length != 0 && pattern[0] == '*') {
        ++pattern;
        --pattern_length;
      }
      break;
    }
  }
  return pattern_length == 0 && text_length == 0;
}

bool ValkeyGlobReference(std::string_view pattern, std::string_view text) {
  bool skip_longer_matches = false;
  return ValkeyGlobReferenceImpl(
      pattern.data(), static_cast<int>(pattern.size()), text.data(),
      static_cast<int>(text.size()), &skip_longer_matches);
}

#define EXPECT_CHECK(condition, message) EXPECT_TRUE(condition) << message

void CheckKind(std::string_view name, CommandKind kind) {
  const CommandSpec* spec = FindCommand(name);
  EXPECT_CHECK(spec != nullptr, std::string(name) + " should resolve");
  if (spec != nullptr) {
    EXPECT_CHECK(spec->kind_ == kind, std::string(name) + " kind mismatch");
  }
}

void CheckArity(std::string_view name, std::size_t argc, bool ok) {
  const CommandSpec* spec = FindCommand(name);
  EXPECT_CHECK(spec != nullptr, std::string(name) + " should resolve");
  if (spec == nullptr) {
    return;
  }
  auto keys = DetermineKeys(*spec, argc);
  EXPECT_CHECK(keys.ok() == ok, std::string(name) +
                                    " argc=" + std::to_string(argc) +
                                    (ok ? " should pass" : " should fail"));
  if (!ok && !keys.ok()) {
    const std::string expected = "wrong number of arguments for '" +
                                 std::string(spec->name_) + "' command";
    EXPECT_CHECK(keys.status().message() == expected,
                 std::string(name) + " arity error message mismatch: " +
                     std::string(keys.status().message()));
  }
}

KeyIndexView Keys(std::string_view name, std::size_t argc) {
  const CommandSpec* spec = FindCommand(name);
  EXPECT_CHECK(spec != nullptr, std::string(name) + " should resolve");
  if (spec == nullptr) {
    return {};
  }
  auto keys = DetermineKeys(*spec, argc);
  EXPECT_CHECK(keys.ok(), std::string(name) + " DetermineKeys should pass");
  return keys.ok() ? *keys : KeyIndexView{};
}

}  // namespace

TEST(CommandTableTest, LookupFlagsArityAndKeyPositions) {
  // Lookup: case-insensitivity and unknowns.
  CheckKind("GET", CommandKind::kGet);
  CheckKind("get", CommandKind::kGet);
  CheckKind("GeT", CommandKind::kGet);
  CheckKind("SET", CommandKind::kSet);
  CheckKind("SCRIPT", CommandKind::kScript);
  CheckKind("EVAL_RO", CommandKind::kEvalRo);
  CheckKind("EVALSHA_RO", CommandKind::kEvalShaRo);
  CheckKind("APPEND", CommandKind::kAppend);
  CheckKind("GETBIT", CommandKind::kGetBit);
  CheckKind("SETBIT", CommandKind::kSetBit);
  CheckKind("BITCOUNT", CommandKind::kBitCount);
  CheckKind("BITPOS", CommandKind::kBitPos);
  CheckKind("BITFIELD", CommandKind::kBitField);
  CheckKind("BITFIELD_RO", CommandKind::kBitFieldRo);
  CheckKind("BITOP", CommandKind::kBitOp);
  CheckKind("DECR", CommandKind::kDecr);
  CheckKind("DECRBY", CommandKind::kDecrBy);
  CheckKind("GETDEL", CommandKind::kGetDel);
  CheckKind("GETEX", CommandKind::kGetEx);
  CheckKind("GETRANGE", CommandKind::kGetRange);
  CheckKind("GETSET", CommandKind::kGetSet);
  CheckKind("INCRBY", CommandKind::kIncrBy);
  CheckKind("INCRBYFLOAT", CommandKind::kIncrByFloat);
  CheckKind("LCS", CommandKind::kLcs);
  CheckKind("MSETNX", CommandKind::kMSetNx);
  CheckKind("PSETEX", CommandKind::kPSetEx);
  CheckKind("SETEX", CommandKind::kSetEx);
  CheckKind("SETNX", CommandKind::kSetNx);
  CheckKind("SETRANGE", CommandKind::kSetRange);
  CheckKind("SUBSTR", CommandKind::kSubstr);
  CheckKind("LPUSH", CommandKind::kLPush);
  CheckKind("DEL", CommandKind::kDel);
  CheckKind("UNLINK", CommandKind::kUnlink);
  CheckKind("RENAME", CommandKind::kRename);
  CheckKind("RENAMENX", CommandKind::kRenameNx);
  CheckKind("COPY", CommandKind::kCopy);
  CheckKind("EXISTS", CommandKind::kExists);
  CheckKind("TOUCH", CommandKind::kTouch);
  CheckKind("RANDOMKEY", CommandKind::kRandomKey);
  CheckKind("INCR", CommandKind::kIncr);
  CheckKind("STRLEN", CommandKind::kStrlen);
  CheckKind("EXPIRE", CommandKind::kExpire);
  CheckKind("PEXPIRE", CommandKind::kPExpire);
  CheckKind("EXPIREAT", CommandKind::kExpireAt);
  CheckKind("PEXPIREAT", CommandKind::kPExpireAt);
  CheckKind("PERSIST", CommandKind::kPersist);
  CheckKind("TTL", CommandKind::kTtl);
  CheckKind("PTTL", CommandKind::kPttl);
  CheckKind("EXPIRETIME", CommandKind::kExpireTime);
  CheckKind("PEXPIRETIME", CommandKind::kPExpireTime);
  CheckKind("PING", CommandKind::kPing);
  CheckKind("ECHO", CommandKind::kEcho);
  CheckKind("PUBLISH", CommandKind::kPublish);
  CheckKind("PUBSUB", CommandKind::kPubSub);
  CheckKind("PSUBSCRIBE", CommandKind::kPSubscribe);
  CheckKind("PUNSUBSCRIBE", CommandKind::kPUnsubscribe);
  CheckKind("SUBSCRIBE", CommandKind::kSubscribe);
  CheckKind("UNSUBSCRIBE", CommandKind::kUnsubscribe);
  CheckKind("QUIT", CommandKind::kQuit);
  CheckKind("RESET", CommandKind::kReset);
  CheckKind("AUTH", CommandKind::kAuth);
  CheckKind("HELLO", CommandKind::kHello);
  CheckKind("SELECT", CommandKind::kSelect);
  CheckKind("DBSIZE", CommandKind::kDbSize);
  CheckKind("SCAN", CommandKind::kScan);
  CheckKind("TYPE", CommandKind::kType);
  CheckKind("DUMP", CommandKind::kDump);
  CheckKind("RESTORE", CommandKind::kRestore);
  CheckKind("SORT", CommandKind::kSort);
  CheckKind("SORT_RO", CommandKind::kSortRo);
  CheckKind("FLUSHDB", CommandKind::kFlushDb);
  CheckKind("FLUSHALL", CommandKind::kFlushAll);
  CheckKind("CONFIG", CommandKind::kConfig);
  CheckKind("REPLICAOF", CommandKind::kReplicaOf);
  CheckKind("SLAVEOF", CommandKind::kReplicaOf);
  CheckKind("ROLE", CommandKind::kRole);
  CheckKind("MONITOR", CommandKind::kMonitor);
  CheckKind("TOMBRAIDER", CommandKind::kTombRaider);
  CheckKind("DEFRAG", CommandKind::kDefrag);
  CheckKind("ZADD", CommandKind::kZAdd);
  CheckKind("ZMPOP", CommandKind::kZMPop);
  CheckKind("BZMPOP", CommandKind::kBZMPop);
  CheckKind("BZPOPMIN", CommandKind::kBZPopMin);
  CheckKind("BZPOPMAX", CommandKind::kBZPopMax);
  CheckKind("ZRANGESTORE", CommandKind::kZRangeStore);
  CheckKind("GEOSEARCH", CommandKind::kGeoSearch);
  CheckKind("GEOSEARCHSTORE", CommandKind::kGeoSearchStore);
  CheckKind("GEORADIUS_RO", CommandKind::kGeoRadiusRo);
  CheckKind("GEORADIUSBYMEMBER_RO", CommandKind::kGeoRadiusByMemberRo);
  CheckKind("XADD", CommandKind::kXAdd);
  CheckKind("XREADGROUP", CommandKind::kXReadGroup);
  EXPECT_CHECK(FindCommand("NOPE") == nullptr,
               "unknown command should not resolve");
  EXPECT_CHECK(FindCommand("") == nullptr, "empty name should not resolve");
  EXPECT_CHECK(FindCommand("GETT") == nullptr,
               "prefix collision should not resolve");
  const CommandSpec* monitor = FindCommand("MONITOR");
  ASSERT_NE(monitor, nullptr);
  EXPECT_NE(monitor->flags_ & keylane::kCmdAdmin, 0u);
  EXPECT_NE(monitor->flags_ & keylane::kCmdSkipMonitor, 0u);
  const CommandSpec* publish = FindCommand("PUBLISH");
  ASSERT_NE(publish, nullptr);
  EXPECT_NE(publish->flags_ & keylane::kCmdMayReplicate, 0u);
  EXPECT_EQ(publish->flags_ & keylane::kCmdWrite, 0u);
  EXPECT_NE(publish->flags_ & keylane::kCmdNoKeys, 0u);
  CheckArity("publish", 2, false);
  CheckArity("publish", 3, true);
  CheckArity("publish", 4, false);
  CheckArity("pubsub", 1, false);
  CheckArity("pubsub", 2, true);
  CheckArity("psubscribe", 1, false);
  CheckArity("psubscribe", 2, true);
  CheckArity("punsubscribe", 1, true);
  CheckArity("subscribe", 1, false);
  CheckArity("subscribe", 2, true);
  CheckArity("subscribe", 8, true);
  CheckArity("unsubscribe", 1, true);
  CheckArity("unsubscribe", 8, true);
  CheckArity("quit", 1, true);
  CheckArity("quit", 2, false);
  CheckArity("reset", 1, true);
  CheckArity("script", 1, false);
  CheckArity("script", 2, true);
  CheckArity("script", 8, true);
  CheckArity("slaveof", 2, false);
  CheckArity("slaveof", 3, true);
  CheckArity("slaveof", 4, false);
  EXPECT_EQ(CommandCanonicalName(CommandKind::kReplicaOf), "replicaof");
  for (std::size_t value = 0;
       value < static_cast<std::size_t>(CommandKind::kUnknown); ++value) {
    EXPECT_NE(CommandCanonicalName(static_cast<CommandKind>(value)), "unknown");
  }
  EXPECT_EQ(CommandCanonicalName(CommandKind::kUnknown), "unknown");
  CheckArity("config", 2, true);
  CheckArity("config", 3, true);
  CheckArity("config", 4, true);
  CheckArity("config", 5, false);
  CheckArity("dump", 1, false);
  CheckArity("dump", 2, true);
  CheckArity("dump", 3, false);
  CheckArity("restore", 3, false);
  CheckArity("restore", 4, true);
  CheckArity("restore", 8, true);
  {
    const KeyIndexView dump = Keys("dump", 2);
    const KeyIndexView restore = Keys("restore", 6);
    EXPECT_EQ(dump.first_, 1);
    EXPECT_EQ(dump.last_, 1);
    EXPECT_EQ(restore.first_, 1);
    EXPECT_EQ(restore.last_, 1);
  }
  // Leave the upper arity open so the option parser can report Redis's
  // syntax error for trailing tokens instead of the global arity error.
  EXPECT_EQ(FindCommand("zrangebyscore")->max_args_, 0);
  EXPECT_EQ(FindCommand("zrevrangebyscore")->max_args_, 0);
  EXPECT_EQ(FindCommand("geopos")->min_args_, 2);
  EXPECT_EQ(FindCommand("geohash")->min_args_, 2);
  for (const char* name :
       {"blpop", "brpop", "blmove", "brpoplpush", "blmpop", "bzmpop",
        "bzpopmin", "bzpopmax", "xread", "xreadgroup"}) {
    const CommandSpec* spec = FindCommand(name);
    EXPECT_CHECK(spec != nullptr && (spec->flags_ & keylane::kCmdMayBlock) != 0,
                 std::string(name) + " should have kCmdMayBlock");
  }
  EXPECT_EQ(FindCommand("lpop")->flags_ & keylane::kCmdMayBlock, 0u);
  EXPECT_EQ(FindCommand("xrange")->flags_ & keylane::kCmdMayBlock, 0u);
  const CommandSpec* radius_member = FindCommand("georadiusbymember");
  ASSERT_NE(radius_member, nullptr);
  EXPECT_NE(radius_member->flags_ & keylane::kCmdWrite, 0u);
  EXPECT_NE(radius_member->flags_ & keylane::kCmdMultiShard, 0u);
  EXPECT_NE(radius_member->flags_ & keylane::kCmdMovableKeys, 0u);

  // Flag consistency: the write set must match the read-only replica check,
  // the gate set must match today's uses_db list, and NoKeys <=> first_key==0.
  const char* write_cmds[] = {"set",      "setbit",    "bitfield", "bitop",
                              "lpush",    "incr",      "del",      "unlink",
                              "rename",   "renamenx",  "expire",   "pexpire",
                              "expireat", "pexpireat", "copy",     "persist",
                              "restore",  "flushdb",   "flushall"};
  const char* read_cmds[] = {
      "get",    "getbit", "bitcount",   "bitpos",      "bitfield_ro", "strlen",
      "ttl",    "pttl",   "expiretime", "pexpiretime", "type",        "dump",
      "exists", "touch",  "randomkey",  "dbsize",      "scan"};
  for (const char* name : write_cmds) {
    const CommandSpec* spec = FindCommand(name);
    EXPECT_CHECK(spec != nullptr && (spec->flags_ & keylane::kCmdWrite) != 0,
                 std::string(name) + " should have kCmdWrite");
    EXPECT_CHECK(spec != nullptr && (spec->flags_ & keylane::kCmdReadOnly) == 0,
                 std::string(name) + " should not have kCmdReadOnly");
  }
  for (const char* name : read_cmds) {
    const CommandSpec* spec = FindCommand(name);
    EXPECT_CHECK(spec != nullptr && (spec->flags_ & keylane::kCmdReadOnly) != 0,
                 std::string(name) + " should have kCmdReadOnly");
    EXPECT_CHECK(spec != nullptr && (spec->flags_ & keylane::kCmdWrite) == 0,
                 std::string(name) + " should not have kCmdWrite");
  }
  {
    const char* gated[] = {
        "dbsize",    "scan",     "type",        "dump",        "restore",
        "del",       "unlink",   "rename",      "renamenx",    "exists",
        "get",       "strlen",   "set",         "lpush",       "incr",
        "expire",    "pexpire",  "expireat",    "pexpireat",   "persist",
        "ttl",       "pttl",     "expiretime",  "pexpiretime", "touch",
        "randomkey", "copy",     "getbit",      "setbit",      "bitcount",
        "bitpos",    "bitfield", "bitfield_ro", "bitop"};
    const char* ungated[] = {"ping", "select", "flushdb", "flushall",
                             "tombraider"};
    for (const char* name : gated) {
      const CommandSpec* spec = FindCommand(name);
      EXPECT_CHECK(
          spec != nullptr && (spec->flags_ & keylane::kCmdUsesDbGate) != 0,
          std::string(name) + " should have kCmdUsesDbGate");
    }
    for (const char* name : ungated) {
      const CommandSpec* spec = FindCommand(name);
      EXPECT_CHECK(
          spec != nullptr && (spec->flags_ & keylane::kCmdUsesDbGate) == 0,
          std::string(name) + " should not have kCmdUsesDbGate");
    }
  }
  {
    const char* no_keys[] = {"ping",    "echo",     "select",
                             "dbsize",  "scan",     "randomkey",
                             "flushdb", "flushall", "tombraider"};
    for (const char* name : no_keys) {
      const CommandSpec* spec = FindCommand(name);
      EXPECT_CHECK(spec != nullptr &&
                       (spec->flags_ & keylane::kCmdNoKeys) != 0 &&
                       spec->first_key_ == 0,
                   std::string(name) + " should be keyless");
    }
    const char* keyed[] = {
        "get",         "set",     "dump",     "restore",    "lpush",
        "del",         "unlink",  "rename",   "renamenx",   "exists",
        "incr",        "strlen",  "expire",   "pexpire",    "expireat",
        "pexpireat",   "persist", "ttl",      "pttl",       "expiretime",
        "pexpiretime", "copy",    "touch",    "getbit",     "setbit",
        "bitcount",    "bitpos",  "bitfield", "bitfield_ro"};
    for (const char* name : keyed) {
      const CommandSpec* spec = FindCommand(name);
      EXPECT_CHECK(spec != nullptr &&
                       (spec->flags_ & keylane::kCmdNoKeys) == 0 &&
                       spec->first_key_ == 1,
                   std::string(name) + " should have keys at arg 1");
    }
  }

  // Arity ranges.
  CheckArity("get", 2, true);
  CheckArity("get", 1, false);
  CheckArity("get", 3, false);
  CheckArity("set", 2, false);
  CheckArity("set", 3, true);
  CheckArity("set", 8, true);  // options validated by the parser, not arity
  CheckArity("lpush", 2, false);
  CheckArity("lpush", 3, true);
  CheckArity("lpush", 100, true);
  CheckArity("expire", 3, true);
  CheckArity("expire", 4, true);
  CheckArity("expire", 2, false);
  CheckArity("expire", 5, false);
  CheckArity("ping", 1, true);
  CheckArity("ping", 2, true);
  CheckArity("ping", 3, false);
  CheckArity("echo", 2, true);
  CheckArity("echo", 1, false);
  CheckArity("select", 2, true);
  CheckArity("select", 1, false);
  CheckArity("del", 1, false);
  CheckArity("del", 2, true);
  CheckArity("del", 100, true);
  CheckArity("unlink", 1, false);
  CheckArity("unlink", 2, true);
  CheckArity("unlink", 100, true);
  CheckArity("rename", 2, false);
  CheckArity("rename", 3, true);
  CheckArity("rename", 4, false);
  CheckArity("renamenx", 3, true);
  CheckArity("copy", 2, false);
  CheckArity("copy", 3, true);
  CheckArity("copy", 7, true);
  CheckArity("touch", 1, false);
  CheckArity("touch", 2, true);
  CheckArity("touch", 100, true);
  CheckArity("randomkey", 1, true);
  CheckArity("randomkey", 2, false);
  CheckArity("append", 3, true);
  CheckArity("getbit", 3, true);
  CheckArity("getbit", 2, false);
  CheckArity("getbit", 4, false);
  CheckArity("setbit", 4, true);
  CheckArity("setbit", 3, false);
  CheckArity("bitcount", 2, true);
  CheckArity("bitcount", 5, true);
  CheckArity("bitcount", 6, true);  // parser reports syntax errors
  CheckArity("bitpos", 3, true);
  CheckArity("bitpos", 6, true);
  CheckArity("bitpos", 7, true);  // parser reports syntax errors
  CheckArity("bitfield", 2, true);
  CheckArity("bitfield", 20, true);
  CheckArity("bitfield_ro", 2, true);
  CheckArity("bitop", 4, true);
  CheckArity("bitop", 3, false);
  CheckArity("bitop", 20, true);
  CheckArity("getex", 2, true);
  CheckArity("getex", 4, true);
  CheckArity("getex", 5, true);  // parser reports illegal option shapes
  CheckArity("lcs", 3, true);
  CheckArity("lcs", 8, true);
  CheckArity("msetnx", 3, true);
  CheckArity("setex", 4, true);
  CheckArity("setrange", 4, true);
  CheckArity("dbsize", 1, true);
  CheckArity("dbsize", 2, false);
  CheckArity("expireat", 3, true);
  CheckArity("expireat", 4, true);
  CheckArity("pexpiretime", 2, true);
  CheckArity("flushall", 1, true);
  CheckArity("flushall", 2, true);
  CheckArity("tombraider", 1, false);
  CheckArity("tombraider", 2, true);
  CheckArity("tombraider", 3, true);
  CheckArity("tombraider", 4, false);

  // Key position resolution.
  {
    KeyIndexView view = Keys("get", 2);
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 1 && view.step_ == 1 &&
                     view.count() == 1,
                 "GET key view mismatch");
  }
  {
    KeyIndexView view = Keys("set", 5);  // SET k v EX 10
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 1 && view.count() == 1,
                 "SET key view must cover only the key");
  }
  {
    KeyIndexView view = Keys("lcs", 7);
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 2 && view.count() == 2,
                 "LCS key view must cover both strings");
  }
  {
    KeyIndexView view = Keys("msetnx", 7);
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 6 && view.step_ == 2 &&
                     view.count() == 3,
                 "MSETNX key view mismatch");
  }
  {
    KeyIndexView view = Keys("bitop", 6);  // BITOP op dst src1 src2 src3
    EXPECT_CHECK(view.first_ == 2 && view.last_ == 5 && view.step_ == 1 &&
                     view.count() == 4,
                 "BITOP key view mismatch");
  }
  {
    KeyIndexView view = Keys("del", 5);  // DEL k1 k2 k3 k4
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 4 && view.step_ == 1 &&
                     view.count() == 4,
                 "DEL key view mismatch");
  }
  {
    KeyIndexView view = Keys("unlink", 5);  // UNLINK k1 k2 k3 k4
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 4 && view.step_ == 1 &&
                     view.count() == 4,
                 "UNLINK key view mismatch");
  }
  {
    KeyIndexView view = Keys("exists", 2);
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 1 && view.count() == 1,
                 "EXISTS single-key view mismatch");
  }
  {
    KeyIndexView view = Keys("copy", 7);
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 2 && view.count() == 2,
                 "COPY key view mismatch");
  }
  {
    KeyIndexView view = Keys("touch", 5);
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 4 && view.count() == 4,
                 "TOUCH key view mismatch");
  }
  {
    KeyIndexView view = Keys("ping", 1);
    EXPECT_CHECK(view.empty() && view.count() == 0,
                 "PING view should be empty");
  }
}

TEST(CommandTableTest, ResolvesEvalKeys) {
  const CommandSpec* eval = FindCommand("EVAL");
  const CommandSpec* evalsha = FindCommand("evalsha");
  const CommandSpec* eval_ro = FindCommand("eval_ro");
  const CommandSpec* evalsha_ro = FindCommand("EVALSHA_RO");
  ASSERT_NE(eval, nullptr);
  ASSERT_NE(evalsha, nullptr);
  ASSERT_NE(eval_ro, nullptr);
  ASSERT_NE(evalsha_ro, nullptr);
  EXPECT_NE(eval_ro->flags_ & keylane::kCmdReadOnly, 0u);
  EXPECT_EQ(eval_ro->flags_ & keylane::kCmdDynamicWrite, 0u);
  EXPECT_NE(evalsha_ro->flags_ & keylane::kCmdReadOnly, 0u);

  std::vector<std::string> args = {"EVAL",  "return ARGV[1]", "2",
                                   "first", "second",         "argument"};
  auto keys = DetermineKeys(*eval, args);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_EQ(keys->first_, 3);
  EXPECT_EQ(keys->last_, 4);
  EXPECT_EQ(keys->count(), 2);

  args = {"EVALSHA", "digest", "0", "argument"};
  keys = DetermineKeys(*evalsha, args);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_TRUE(keys->empty());

  args = {"EVAL_RO", "return redis.call('GET',KEYS[1])", "1", "first"};
  keys = DetermineKeys(*eval_ro, args);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_EQ(keys->first_, 3);
  EXPECT_EQ(keys->last_, 3);

  args = {"EVALSHA_RO", "digest", "0"};
  keys = DetermineKeys(*evalsha_ro, args);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_TRUE(keys->empty());

  args = {"EVAL", "return 1", "-1"};
  EXPECT_EQ(DetermineKeys(*eval, args).status().message(),
            "Number of keys can't be negative");
  args = {"EVAL", "return 1", "2", "only-one"};
  EXPECT_EQ(DetermineKeys(*eval, args).status().message(),
            "Number of keys can't be greater than number of args");
}

TEST(CommandTableTest, ResolvesStreamReadMovableKeys) {
  const CommandSpec* read = FindCommand("xread");
  ASSERT_NE(read, nullptr);
  const std::vector<std::string> args = {"XREAD",   "COUNT", "5", "BLOCK", "10",
                                         "STREAMS", "a",     "b", "0-0",   "$"};
  auto keys = DetermineKeys(*read, args);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_EQ(keys->first_, 6);
  EXPECT_EQ(keys->last_, 7);
  EXPECT_EQ(keys->count(), 2);

  const std::vector<std::string> unbalanced = {"XREAD", "STREAMS", "a", "b",
                                               "0-0"};
  auto unbalanced_keys = DetermineKeys(*read, unbalanced);
  EXPECT_FALSE(unbalanced_keys.ok());
  EXPECT_EQ(unbalanced_keys.status().message(),
            "Unbalanced 'xread' list of streams: for each stream key an ID "
            "or '$' must be specified.");

  const CommandSpec* group_read = FindCommand("xreadgroup");
  ASSERT_NE(group_read, nullptr);
  const std::vector<std::string> group_args = {
      "XREADGROUP", "GROUP", "group",  "STREAMS", "COUNT", "1",
      "STREAMS",    "first", "second", "0",       ">"};
  auto group_keys = DetermineKeys(*group_read, group_args);
  ASSERT_TRUE(group_keys.ok()) << group_keys.status();
  EXPECT_EQ(group_keys->first_, 7);
  EXPECT_EQ(group_keys->last_, 8);

  std::vector<std::string> oversized{"XREAD", "STREAMS"};
  constexpr std::size_t kTooManyStreamKeys =
      static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max());
  oversized.reserve(2 + 2 * kTooManyStreamKeys);
  for (std::size_t i = 0; i < kTooManyStreamKeys; ++i)
    oversized.push_back("key");
  for (std::size_t i = 0; i < kTooManyStreamKeys; ++i) oversized.push_back("0");
  EXPECT_FALSE(DetermineKeys(*read, oversized).ok());
}

TEST(CommandTableTest, ResolvesSortStoreDestination) {
  const CommandSpec* sort = FindCommand("sort");
  ASSERT_NE(sort, nullptr);
  const std::vector<std::string> args = {
      "SORT", "abc", "STORE", "invalid", "STORE", "stillbad", "STORE", "def"};
  auto keys = DetermineKeys(*sort, args);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_EQ(keys->first_, 1);
  EXPECT_EQ(keys->last_, 7);
  EXPECT_EQ(keys->step_, 6);
  EXPECT_EQ(keys->count(), 2);

  const std::vector<std::string> no_store = {"SORT", "abc", "BY", "nosort"};
  keys = DetermineKeys(*sort, no_store);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_EQ(keys->first_, 1);
  EXPECT_EQ(keys->last_, 1);
  EXPECT_EQ(keys->count(), 1);

  const CommandSpec* sort_ro = FindCommand("sort_ro");
  ASSERT_NE(sort_ro, nullptr);
  keys = DetermineKeys(*sort_ro,
                       std::vector<std::string>{"SORT_RO", "abc", "DESC"});
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_EQ(keys->first_, 1);
  EXPECT_EQ(keys->last_, 1);
}

TEST(CommandTableTest, AcceptsExtendedPendingIdleForm) {
  const CommandSpec* pending = FindCommand("xpending");
  ASSERT_NE(pending, nullptr);
  EXPECT_TRUE(DetermineKeys(*pending,
                            std::vector<std::string>{
                                "XPENDING", "stream", "group", "IDLE", "1000",
                                "-", "+", "10", "consumer"})
                  .ok());
}

TEST(CommandTableTest, StreamHelpHasNoKey) {
  for (const char* name : {"xgroup", "xinfo"}) {
    const CommandSpec* spec = FindCommand(name);
    ASSERT_NE(spec, nullptr);
    const std::vector<std::string> args{name, "HELP"};
    auto keys = DetermineKeys(*spec, args);
    ASSERT_TRUE(keys.ok()) << keys.status();
    EXPECT_TRUE(keys->empty());
  }
}

TEST(CommandTableTest, ResolvesSortedSetAggregateKeys) {
  const CommandSpec* read = FindCommand("zinter");
  ASSERT_NE(read, nullptr);
  const std::vector<std::string> read_args = {
      "ZINTER", "2", "a", "b", "WEIGHTS", "2", "3", "WITHSCORES"};
  auto read_keys = DetermineKeys(*read, read_args);
  ASSERT_TRUE(read_keys.ok()) << read_keys.status();
  EXPECT_EQ(read_keys->first_, 2);
  EXPECT_EQ(read_keys->last_, 3);

  const CommandSpec* store = FindCommand("zunionstore");
  ASSERT_NE(store, nullptr);
  const std::vector<std::string> store_args = {"ZUNIONSTORE", "out", "2", "a",
                                               "b"};
  auto store_keys = DetermineKeys(*store, store_args);
  ASSERT_TRUE(store_keys.ok()) << store_keys.status();
  EXPECT_EQ(store_keys->first_, 3);
  EXPECT_EQ(store_keys->last_, 4);

  for (const auto& [args, message] :
       std::vector<std::pair<std::vector<std::string>, std::string>>{
           {{"ZUNIONSTORE", "out", "not-an-integer", "a"},
            "value is not an integer or out of range"},
           {{"ZUNIONSTORE", "out", "0", "a"},
            "at least 1 input key is needed for 'zunionstore' command"},
           {{"ZUNIONSTORE", "out", "2", "a"}, "syntax error"}}) {
    auto invalid = DetermineKeys(*store, args);
    ASSERT_FALSE(invalid.ok());
    EXPECT_EQ(invalid.status().message(), message);
  }
}

TEST(CommandTableTest, ResolvesSortedSetAndGeoStoreKeys) {
  for (const char* name : {"zrangestore", "geosearchstore"}) {
    const CommandSpec* spec = FindCommand(name);
    ASSERT_NE(spec, nullptr);
    const std::vector<std::string> args =
        std::string_view(name) == "zrangestore"
            ? std::vector<std::string>{"ZRANGESTORE", "destination", "source",
                                       "0", "-1"}
            : std::vector<std::string>{
                  "GEOSEARCHSTORE", "destination", "source",
                  "FROMLONLAT",     "0",           "0",
                  "BYRADIUS",       "1",           "km"};
    auto keys = DetermineKeys(*spec, args);
    ASSERT_TRUE(keys.ok()) << keys.status();
    EXPECT_EQ(keys->first_, 1);
    EXPECT_EQ(keys->last_, 2);
  }
  const CommandSpec* radius = FindCommand("georadius");
  ASSERT_NE(radius, nullptr);
  const std::vector<std::string> args = {
      "GEORADIUS", "source", "0", "0", "1", "km", "STORE", "destination"};
  auto keys = DetermineKeys(*radius, args);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_EQ(keys->first_, 1);
  EXPECT_EQ(keys->last_, 1);
}

TEST(CommandTableTest, ResolvesMovableListPopKeys) {
  const CommandSpec* lmpop = FindCommand("lmpop");
  ASSERT_NE(lmpop, nullptr);
  const std::vector<std::string> lm_args{"LMPOP", "2",     "first", "second",
                                         "LEFT",  "COUNT", "3"};
  auto lm_keys = DetermineKeys(*lmpop, lm_args);
  ASSERT_TRUE(lm_keys.ok()) << lm_keys.status();
  EXPECT_EQ(lm_keys->first_, 2);
  EXPECT_EQ(lm_keys->last_, 3);
  EXPECT_EQ(lm_keys->count(), 2);

  const CommandSpec* blmpop = FindCommand("blmpop");
  ASSERT_NE(blmpop, nullptr);
  const std::vector<std::string> blm_args{"BLMPOP", "1", "3",    "a",
                                          "b",      "c", "RIGHT"};
  auto blm_keys = DetermineKeys(*blmpop, blm_args);
  ASSERT_TRUE(blm_keys.ok()) << blm_keys.status();
  EXPECT_EQ(blm_keys->first_, 3);
  EXPECT_EQ(blm_keys->last_, 5);
  EXPECT_EQ(blm_keys->count(), 3);

  const std::vector<std::string> invalid{"LMPOP", "3", "only-one", "LEFT"};
  EXPECT_FALSE(DetermineKeys(*lmpop, invalid).ok());

  std::vector<std::string> overflowing;
  overflowing.reserve(65538);
  overflowing.push_back("LMPOP");
  overflowing.push_back("65535");
  for (std::size_t i = 0; i < 65535; ++i) overflowing.push_back("key");
  overflowing.push_back("LEFT");
  EXPECT_FALSE(DetermineKeys(*lmpop, overflowing).ok());
}

TEST(CommandTableTest, ResolvesMovableSortedSetPopKeys) {
  const CommandSpec* zmpop = FindCommand("zmpop");
  ASSERT_NE(zmpop, nullptr);
  const std::vector<std::string> zm_args{"ZMPOP", "2",     "first", "second",
                                         "MIN",   "COUNT", "3"};
  auto zm_keys = DetermineKeys(*zmpop, zm_args);
  ASSERT_TRUE(zm_keys.ok()) << zm_keys.status();
  EXPECT_EQ(zm_keys->first_, 2);
  EXPECT_EQ(zm_keys->last_, 3);

  const std::vector<std::string> noncanonical{"ZMPOP", "02", "first", "second",
                                              "MIN"};
  auto noncanonical_keys = DetermineKeys(*zmpop, noncanonical);
  ASSERT_FALSE(noncanonical_keys.ok());
  EXPECT_EQ(noncanonical_keys.status().message(),
            "numkeys should be greater than 0");

  const CommandSpec* bzmpop = FindCommand("bzmpop");
  ASSERT_NE(bzmpop, nullptr);
  const std::vector<std::string> bzm_args{"BZMPOP", "1", "3",  "a",
                                          "b",      "c", "MAX"};
  auto bzm_keys = DetermineKeys(*bzmpop, bzm_args);
  ASSERT_TRUE(bzm_keys.ok()) << bzm_keys.status();
  EXPECT_EQ(bzm_keys->first_, 3);
  EXPECT_EQ(bzm_keys->last_, 5);

  const CommandSpec* bzpop = FindCommand("bzpopmin");
  ASSERT_NE(bzpop, nullptr);
  auto bz_keys = DetermineKeys(*bzpop, std::size_t{4});
  ASSERT_TRUE(bz_keys.ok()) << bz_keys.status();
  EXPECT_EQ(bz_keys->first_, 1);
  EXPECT_EQ(bz_keys->last_, 2);
}

TEST(CommandTableTest, RedisGlobTrailingHyphenIsRangeEndpoint) {
  EXPECT_TRUE(keylane::RedisGlobMatch("[a-]", "]"));
  EXPECT_TRUE(keylane::RedisGlobMatch("[a-]", "_"));
  EXPECT_TRUE(keylane::RedisGlobMatch("[a-]", "a"));
  EXPECT_FALSE(keylane::RedisGlobMatch("[a-]", "-"));
  // The endpoint ']' is consumed by the range. The following '*' remains
  // inside the now-unterminated class instead of matching the rest of text.
  EXPECT_FALSE(keylane::RedisGlobMatch("[b-]*", "bbba"));
  EXPECT_TRUE(keylane::RedisGlobMatch("[b-]*", "b"));
}

TEST(CommandTableTest, RedisGlobOnlyCaretNegatesAndEscapesPrecedeRanges) {
  EXPECT_TRUE(keylane::RedisGlobMatch("[!a]", "!"));
  EXPECT_TRUE(keylane::RedisGlobMatch("[!a]", "a"));
  EXPECT_FALSE(keylane::RedisGlobMatch("[!a]", "b"));

  EXPECT_TRUE(keylane::RedisGlobMatch("[\\a-z]", "a"));
  EXPECT_TRUE(keylane::RedisGlobMatch("[\\a-z]", "-"));
  EXPECT_TRUE(keylane::RedisGlobMatch("[\\a-z]", "z"));
  EXPECT_FALSE(keylane::RedisGlobMatch("[\\a-z]", "b"));

  EXPECT_TRUE(keylane::RedisGlobMatch("[abc", "a"));
  EXPECT_TRUE(keylane::RedisGlobMatch("[abc", "c"));
  EXPECT_FALSE(keylane::RedisGlobMatch("[abc", "["));
  EXPECT_FALSE(keylane::RedisGlobMatch("[abc", "d"));
}

TEST(CommandTableTest, RedisGlobMatchesValkeyReference) {
  constexpr std::string_view alphabet = "*?[]^-\\abc";
  std::mt19937_64 random(0x4b45594c414e45ULL);
  for (std::size_t iteration = 0; iteration < 200000; ++iteration) {
    std::string pattern(random() % 9, '\0');
    std::string text(random() % 9, '\0');
    for (char& byte : pattern) byte = alphabet[random() % alphabet.size()];
    for (char& byte : text) byte = alphabet[random() % alphabet.size()];
    ASSERT_EQ(keylane::RedisGlobMatch(pattern, text),
              ValkeyGlobReference(pattern, text))
        << "pattern=" << pattern << " text=" << text;
  }
}

TEST(CommandTableTest, ResolvesSInterCardKeysAndRedis72Errors) {
  const CommandSpec* spec = FindCommand("sintercard");
  ASSERT_NE(spec, nullptr);
  const std::vector<std::string> valid{"SINTERCARD", "2",     "first",
                                       "second",     "LIMIT", "1"};
  auto keys = DetermineKeys(*spec, valid);
  ASSERT_TRUE(keys.ok()) << keys.status();
  EXPECT_EQ(keys->first_, 2);
  EXPECT_EQ(keys->last_, 3);
  EXPECT_EQ(keys->count(), 2);

  const std::vector<std::string> zero{"SINTERCARD", "0", "set"};
  auto zero_keys = DetermineKeys(*spec, zero);
  ASSERT_FALSE(zero_keys.ok());
  EXPECT_EQ(zero_keys.status().message(), "numkeys should be greater than 0");

  const std::vector<std::string> too_many{"SINTERCARD", "2", "set"};
  auto too_many_keys = DetermineKeys(*spec, too_many);
  ASSERT_FALSE(too_many_keys.ok());
  EXPECT_EQ(too_many_keys.status().message(),
            "Number of keys can't be greater than number of args");

  const std::vector<std::string> trailing{"SINTERCARD", "1", "set", "bad"};
  auto trailing_keys = DetermineKeys(*spec, trailing);
  ASSERT_FALSE(trailing_keys.ok());
  EXPECT_EQ(trailing_keys.status().message(), "syntax error");

  const std::vector<std::string> repeated{"SINTERCARD", "1",     "set", "LIMIT",
                                          "1",          "LIMIT", "2"};
  EXPECT_TRUE(DetermineKeys(*spec, repeated).ok());
}
