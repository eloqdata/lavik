#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "keylane/command.h"

namespace keylane {

// Static command metadata. One CommandSpec per supported command; the table is
// the single source of truth for write/read classification, DB-gate
// participation, and key positions.
enum CommandFlag : std::uint32_t {
  kCmdWrite = 1u << 0,  // mutates the keyspace; rejected on read-only replicas
  kCmdReadOnly = 1u << 1,    // never mutates the keyspace
  kCmdNoKeys = 1u << 2,      // takes no key arguments
  kCmdMultiShard = 1u << 3,  // key set may span multiple shard owners
  kCmdGlobal = 1u << 4,      // fans out to every worker
  kCmdUsesDbGate = 1u << 5,  // holds a DbOperationGuard while executing
};

// Key positions follow the Redis key-spec convention: `first_key` is the
// argument index of the first key (0 = no keys), `last_key` is the index of
// the last key with negative values counting from the end (-1 = last arg),
// `key_step` is the distance between consecutive keys (MSET = 2). Arity is an
// inclusive [min_args, max_args] range over the full argument vector including
// the command name; max_args == 0 means unbounded (option parsing enforces the
// rest).
struct CommandSpec {
  std::string_view name_;  // lowercase canonical spelling
  CommandKind kind_ = CommandKind::kUnknown;
  std::uint8_t min_args_ = 1;
  std::uint8_t max_args_ = 0;
  std::uint8_t first_key_ = 0;
  std::int8_t last_key_ = 0;
  std::uint8_t key_step_ = 1;
  std::uint32_t flags_ = 0;
};

// Case-insensitive lookup; nullptr when the command is unknown.
const CommandSpec* FindCommand(std::string_view name);

// Key argument positions resolved against a concrete argc. Keys sit at
// indices first, first + step, ..., last (inclusive).
struct KeyIndexView {
  std::uint16_t first_ = 0;
  std::uint16_t last_ = 0;
  std::uint8_t step_ = 1;

  bool empty() const { return first_ == 0; }
  std::size_t count() const {
    return empty() ? 0 : (last_ - first_) / step_ + 1;
  }
};

// Validates argc against the spec's arity range and resolves key positions.
// On arity mismatch returns kInvalidArgument with the canonical Redis message
// ("wrong number of arguments for '<name>' command"; callers prepend "ERR ").
// Commands with key_step > 1 (MSET) must additionally validate key/value
// pairing in their handler; this only resolves positions.
absl::StatusOr<KeyIndexView> DetermineKeys(const CommandSpec& spec,
                                           std::size_t argc);

}  // namespace keylane
