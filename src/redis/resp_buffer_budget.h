/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lavik/command_args.h"
#include "lavik/memory.h"

namespace lavik {

// A Data worker admits the embedded argument slot of each incoming connection.
// Account allocator size classes so rounding cannot exceed the worker cap.
class alignas(64) RespBufferBudget {
 public:
  static constexpr std::size_t kConnectionLimit = 4 * 1024;
  static constexpr std::size_t kWorkerLimit = 4 * 1024 * 1024;

  // Called after a socket read round, before dispatch can release the handle.
  // Keeping the credit while the same capacity is reused avoids admission or
  // allocator queries on steady-state commands.
  void Admit(CommandArgsStorage& slot) noexcept {
    const auto capacity = slot.bytes.capacity();
    if (capacity == slot.retained_byte_capacity_) return;
    Release(slot);
    if (capacity <= CommandArgs::kInlineBytes || capacity > kConnectionLimit)
      return;
    const auto bytes = AllocatorUsableSizeForRequest(capacity);
    if (bytes > kConnectionLimit || bytes > kWorkerLimit - retained_) return;
    auto reservation = TryReserveMemory(bytes);
    if (!reservation) return;
    reservation->Commit(bytes);
    retained_ += bytes;
    slot.retained_byte_capacity_ = capacity;
  }

  // Drop the credit on capacity change or connection close. The slot owns
  // the allocation until its current handle returns or the slot is destroyed.
  void Release(CommandArgsStorage& slot) noexcept {
    if (slot.retained_byte_capacity_ == CommandArgs::kInlineBytes) return;
    const auto bytes =
        AllocatorUsableSizeForRequest(slot.retained_byte_capacity_);
    assert(bytes <= retained_);
    retained_ -= bytes;
    ReleaseRetainedMemory(CurrentMemoryAccountingShard(), bytes);
    slot.retained_byte_capacity_ = CommandArgs::kInlineBytes;
  }

 private:
  std::size_t retained_ = 0;
};

}  // namespace lavik
