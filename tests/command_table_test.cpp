#include "keylane/command_table.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <string_view>

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
  EXPECT_CHECK(FindCommand("NOPE") == nullptr,
               "unknown command should not resolve");
  EXPECT_CHECK(FindCommand("") == nullptr, "empty name should not resolve");
  EXPECT_CHECK(FindCommand("GETT") == nullptr,
               "prefix collision should not resolve");

  // Flag consistency: the write set must match the read-only replica check,
  // the gate set must match today's uses_db list, and NoKeys <=> first_key==0.
  const char* write_cmds[] = {"set",     "incr",    "del",    "expire",
                              "pexpire", "persist", "flushdb"};
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
    const char* gated[] = {"dbsize",  "scan", "del",  "exists", "get",
                           "strlen",  "set",  "incr", "expire", "pexpire",
                           "persist", "ttl",  "pttl"};
    const char* ungated[] = {"ping", "select", "flushdb"};
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
    const char* no_keys[] = {"ping",   "echo", "select",
                             "dbsize", "scan", "flushdb"};
    for (const char* name : no_keys) {
      const CommandSpec* spec = FindCommand(name);
      EXPECT_CHECK(spec != nullptr &&
                       (spec->flags_ & keylane::kCmdNoKeys) != 0 &&
                       spec->first_key_ == 0,
                   std::string(name) + " should be keyless");
    }
    const char* keyed[] = {"get",     "set",    "del",    "exists",
                           "incr",    "strlen", "expire", "pexpire",
                           "persist", "ttl",    "pttl"};
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
