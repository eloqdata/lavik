/*
 * Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lavik/command_args.h"
#include "lavik/memory.h"

namespace lavik {

// Only the embedded request slot is recycled. Independently owned pipeline or
// MULTI requests can outlive the connection and never reference its spare.
class alignas(64) RespByteSpare {
 public:
  CommandByteSpare buffer_;

  // Inspect only the connection-owned slot after a socket parsing round.
  // Grant before its handle can return the allocation. Standalone protocol
  // users do not link the Data memory-accounting implementation.
  void AdmitFor(const CommandArgs& args) noexcept {
    constexpr std::size_t kMaxBytes = 4 * 1024;
    if (credit_.bytes() || args.bytes.capacity() <= CommandArgs::kInlineBytes ||
        args.bytes.capacity() > kMaxBytes)
      return;
    auto reservation = TryReserveMemory(kMaxBytes);
    if (!reservation) return;
    credit_.Adopt(&*reservation, kMaxBytes);
    buffer_.Enable(kMaxBytes);
  }

  // A full maintenance interval without a return releases the idle budget.
  void Tick() noexcept {
    if (credit_.bytes() &&
        (!buffer_.ConsumeReturnFlag() || WouldExceedMemoryLimit(0))) {
      buffer_.Disable();
      credit_.Reset();
    }
  }

 private:
  RetainedMemoryCharge credit_;
};

}  // namespace lavik
