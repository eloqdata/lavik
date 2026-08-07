#include <cstddef>
#include <iostream>
#include <string>
#include <string_view>

#include "keylane/command.h"
#include "keylane/command_table.h"

namespace {

using keylane::CommandKind;
using keylane::CommandSpec;
using keylane::DetermineKeys;
using keylane::FindCommand;
using keylane::KeyIndexView;

int failures = 0;

void Check(bool condition, const std::string& message) {
  if (!condition) {
    std::cerr << "FAIL: " << message << '\n';
    ++failures;
  }
}

void CheckKind(std::string_view name, CommandKind kind) {
  const CommandSpec* spec = FindCommand(name);
  Check(spec != nullptr, std::string(name) + " should resolve");
  if (spec != nullptr) {
    Check(spec->kind == kind, std::string(name) + " kind mismatch");
  }
}

void CheckArity(std::string_view name, std::size_t argc, bool ok) {
  const CommandSpec* spec = FindCommand(name);
  Check(spec != nullptr, std::string(name) + " should resolve");
  if (spec == nullptr) {
    return;
  }
  auto keys = DetermineKeys(*spec, argc);
  Check(keys.ok() == ok, std::string(name) + " argc=" + std::to_string(argc) +
                             (ok ? " should pass" : " should fail"));
  if (!ok && !keys.ok()) {
    const std::string expected = "wrong number of arguments for '" +
                                 std::string(spec->name) + "' command";
    Check(keys.status().message() == expected,
          std::string(name) + " arity error message mismatch: " +
              keys.status().message());
  }
}

KeyIndexView Keys(std::string_view name, std::size_t argc) {
  const CommandSpec* spec = FindCommand(name);
  Check(spec != nullptr, std::string(name) + " should resolve");
  if (spec == nullptr) {
    return {};
  }
  auto keys = DetermineKeys(*spec, argc);
  Check(keys.ok(), std::string(name) + " DetermineKeys should pass");
  return keys.ok() ? *keys : KeyIndexView{};
}

}  // namespace

int main() {
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
  Check(FindCommand("NOPE") == nullptr, "unknown command should not resolve");
  Check(FindCommand("") == nullptr, "empty name should not resolve");
  Check(FindCommand("GETT") == nullptr, "prefix collision should not resolve");

  // Flag consistency: the write set must match the read-only replica check,
  // the gate set must match today's uses_db list, and NoKeys <=> first_key==0.
  const char* write_cmds[] = {"set",     "incr",    "del",    "expire",
                              "pexpire", "persist", "flushdb"};
  const char* read_cmds[] = {"get", "strlen", "ttl",    "pttl",
                             "exists", "dbsize", "scan"};
  for (const char* name : write_cmds) {
    const CommandSpec* spec = FindCommand(name);
    Check(spec != nullptr && (spec->flags & keylane::kCmdWrite) != 0,
          std::string(name) + " should have kCmdWrite");
    Check(spec != nullptr && (spec->flags & keylane::kCmdReadOnly) == 0,
          std::string(name) + " should not have kCmdReadOnly");
  }
  for (const char* name : read_cmds) {
    const CommandSpec* spec = FindCommand(name);
    Check(spec != nullptr && (spec->flags & keylane::kCmdReadOnly) != 0,
          std::string(name) + " should have kCmdReadOnly");
    Check(spec != nullptr && (spec->flags & keylane::kCmdWrite) == 0,
          std::string(name) + " should not have kCmdWrite");
  }
  {
    const char* gated[] = {"dbsize", "scan",   "del",    "exists", "get",
                           "strlen", "set",    "incr",   "expire", "pexpire",
                           "persist", "ttl",   "pttl"};
    const char* ungated[] = {"ping", "select", "flushdb"};
    for (const char* name : gated) {
      const CommandSpec* spec = FindCommand(name);
      Check(spec != nullptr && (spec->flags & keylane::kCmdUsesDbGate) != 0,
            std::string(name) + " should have kCmdUsesDbGate");
    }
    for (const char* name : ungated) {
      const CommandSpec* spec = FindCommand(name);
      Check(spec != nullptr && (spec->flags & keylane::kCmdUsesDbGate) == 0,
            std::string(name) + " should not have kCmdUsesDbGate");
    }
  }
  {
    const char* no_keys[] = {"ping", "echo", "select", "dbsize", "scan",
                             "flushdb"};
    for (const char* name : no_keys) {
      const CommandSpec* spec = FindCommand(name);
      Check(spec != nullptr && (spec->flags & keylane::kCmdNoKeys) != 0 &&
                spec->first_key == 0,
            std::string(name) + " should be keyless");
    }
    const char* keyed[] = {"get", "set", "del", "exists", "incr", "strlen",
                           "expire", "pexpire", "persist", "ttl", "pttl"};
    for (const char* name : keyed) {
      const CommandSpec* spec = FindCommand(name);
      Check(spec != nullptr && (spec->flags & keylane::kCmdNoKeys) == 0 &&
                spec->first_key == 1,
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
    Check(view.first == 1 && view.last == 1 && view.step == 1 &&
              view.count() == 1,
          "GET key view mismatch");
  }
  {
    KeyIndexView view = Keys("set", 5);  // SET k v EX 10
    Check(view.first == 1 && view.last == 1 && view.count() == 1,
          "SET key view must cover only the key");
  }
  {
    KeyIndexView view = Keys("del", 5);  // DEL k1 k2 k3 k4
    Check(view.first == 1 && view.last == 4 && view.step == 1 &&
              view.count() == 4,
          "DEL key view mismatch");
  }
  {
    KeyIndexView view = Keys("exists", 2);
    Check(view.first == 1 && view.last == 1 && view.count() == 1,
          "EXISTS single-key view mismatch");
  }
  {
    KeyIndexView view = Keys("ping", 1);
    Check(view.empty() && view.count() == 0, "PING view should be empty");
  }

  if (failures != 0) {
    std::cerr << failures << " check(s) failed\n";
    return 1;
  }
  std::cout << "command_table_test passed\n";
  return 0;
}
