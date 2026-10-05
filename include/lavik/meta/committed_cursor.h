/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0 */
#pragma once

#include <cstdint>

namespace lavik::meta {

// One state-machine cut. Advance moves applied without changing store contents;
// command apply and snapshot Install also move state_change. Neither Advance
// nor Install emits a command event, so an event cursor is not a state cursor.
class MetaCommittedCursor {
 public:
  MetaCommittedCursor(std::uint64_t applied_index = 0,
                      std::uint64_t state_change_index = 0)
      : applied_index_(applied_index),
        state_change_index_(state_change_index) {}

  std::uint64_t applied_index() const { return applied_index_; }
  std::uint64_t state_change_index() const { return state_change_index_; }

 private:
  std::uint64_t applied_index_;
  std::uint64_t state_change_index_;
};

}  // namespace lavik::meta
