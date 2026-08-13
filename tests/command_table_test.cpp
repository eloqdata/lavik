#include "keylane/command_table.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "keylane/command.h"

namespace {

using keylane::CommandKind;
using keylane::CommandSpec;
using keylane::DetermineKeys;
using keylane::FindCommand;
using keylane::KeyIndexView;

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
  CheckKind("LPUSH", CommandKind::kLPush);
  CheckKind("DEL", CommandKind::kDel);
  CheckKind("EXISTS", CommandKind::kExists);
  CheckKind("INCR", CommandKind::kIncr);
  CheckKind("STRLEN", CommandKind::kStrlen);
  CheckKind("EXPIRE", CommandKind::kExpire);
  CheckKind("PEXPIRE", CommandKind::kPExpire);
  CheckKind("PERSIST", CommandKind::kPersist);
  CheckKind("TTL", CommandKind::kTtl);
  CheckKind("PTTL", CommandKind::kPttl);
  CheckKind("PING", CommandKind::kPing);
  CheckKind("ECHO", CommandKind::kEcho);
  CheckKind("SELECT", CommandKind::kSelect);
  CheckKind("DBSIZE", CommandKind::kDbSize);
  CheckKind("SCAN", CommandKind::kScan);
  CheckKind("FLUSHDB", CommandKind::kFlushDb);
  CheckKind("FLUSHALL", CommandKind::kFlushAll);
  CheckKind("TOMBRAIDER", CommandKind::kTombRaider);
  CheckKind("DEFRAG", CommandKind::kDefrag);
  EXPECT_CHECK(FindCommand("NOPE") == nullptr,
               "unknown command should not resolve");
  EXPECT_CHECK(FindCommand("") == nullptr, "empty name should not resolve");
  EXPECT_CHECK(FindCommand("GETT") == nullptr,
               "prefix collision should not resolve");

  // Flag consistency: the write set must match the read-only replica check,
  // the gate set must match today's uses_db list, and NoKeys <=> first_key==0.
  const char* write_cmds[] = {"set",     "lpush",   "incr",
                              "del",     "expire",  "pexpire",
                              "persist", "flushdb", "flushall"};
  const char* read_cmds[] = {"get",    "strlen", "ttl", "pttl",
                             "exists", "dbsize", "scan"};
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
    const char* gated[] = {"dbsize",  "scan",    "del",   "exists", "get",
                           "strlen",  "set",     "lpush", "incr",   "expire",
                           "pexpire", "persist", "ttl",   "pttl"};
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
    const char* no_keys[] = {"ping", "echo",    "select",   "dbsize",
                             "scan", "flushdb", "flushall", "tombraider"};
    for (const char* name : no_keys) {
      const CommandSpec* spec = FindCommand(name);
      EXPECT_CHECK(spec != nullptr &&
                       (spec->flags_ & keylane::kCmdNoKeys) != 0 &&
                       spec->first_key_ == 0,
                   std::string(name) + " should be keyless");
    }
    const char* keyed[] = {"get",     "set",     "lpush",  "del",
                           "exists",  "incr",    "strlen", "expire",
                           "pexpire", "persist", "ttl",    "pttl"};
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
  CheckArity("dbsize", 1, true);
  CheckArity("dbsize", 2, false);
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
    KeyIndexView view = Keys("del", 5);  // DEL k1 k2 k3 k4
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 4 && view.step_ == 1 &&
                     view.count() == 4,
                 "DEL key view mismatch");
  }
  {
    KeyIndexView view = Keys("exists", 2);
    EXPECT_CHECK(view.first_ == 1 && view.last_ == 1 && view.count() == 1,
                 "EXISTS single-key view mismatch");
  }
  {
    KeyIndexView view = Keys("ping", 1);
    EXPECT_CHECK(view.empty() && view.count() == 0,
                 "PING view should be empty");
  }
}

TEST(CommandTableTest, ResolvesMovableListPopKeys) {
  const CommandSpec* lmpop = FindCommand("lmpop");
  ASSERT_NE(lmpop, nullptr);
  const std::vector<std::string> lm_args{
      "LMPOP", "2", "first", "second", "LEFT", "COUNT", "3"};
  auto lm_keys = DetermineKeys(*lmpop, lm_args);
  ASSERT_TRUE(lm_keys.ok()) << lm_keys.status();
  EXPECT_EQ(lm_keys->first_, 2);
  EXPECT_EQ(lm_keys->last_, 3);
  EXPECT_EQ(lm_keys->count(), 2);

  const CommandSpec* blmpop = FindCommand("blmpop");
  ASSERT_NE(blmpop, nullptr);
  const std::vector<std::string> blm_args{
      "BLMPOP", "1", "3", "a", "b", "c", "RIGHT"};
  auto blm_keys = DetermineKeys(*blmpop, blm_args);
  ASSERT_TRUE(blm_keys.ok()) << blm_keys.status();
  EXPECT_EQ(blm_keys->first_, 3);
  EXPECT_EQ(blm_keys->last_, 5);
  EXPECT_EQ(blm_keys->count(), 3);

  const std::vector<std::string> invalid{
      "LMPOP", "3", "only-one", "LEFT"};
  EXPECT_FALSE(DetermineKeys(*lmpop, invalid).ok());
}
