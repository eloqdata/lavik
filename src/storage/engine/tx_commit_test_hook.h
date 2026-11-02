/* Copyright (C) 2026 EloqData Inc.
 * SPDX-License-Identifier: Apache-2.0
 */
#pragma once

#include "lavik/fault_injection.h"

#if LAVIK_FAULTS_ENABLED
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>

#include "absl/status/status.h"
#include "lavik/error_detail.h"
#include "spdlog/spdlog.h"

namespace lavik::storage {

inline bool TxCommitTestGate(std::string_view suffix) {
  const char* base = std::getenv("LAVIK_TX_COMMIT_TEST_GATE");
  return base != nullptr &&
         ::access((std::string(base) + std::string(suffix)).c_str(), F_OK) == 0;
}

inline void TraceTxCommitForTest(std::uint64_t txid, std::string_view phase) {
  if (std::getenv("LAVIK_TX_COMMIT_TEST_GATE") != nullptr)
    spdlog::info("tx commit test {} txid={} tick_ns={}", phase, txid,
                 std::chrono::duration_cast<std::chrono::nanoseconds>(
                     std::chrono::steady_clock::now().time_since_epoch())
                     .count());
}

inline absl::Status TxCommitAppendFaultForTest(
    std::uint64_t txid, absl::string_view exhaustion_type) {
  TraceTxCommitForTest(txid, "attempt");
  const char* target = std::getenv("LAVIK_TX_COMMIT_TEST_TXID");
  if (target != nullptr && std::strtoull(target, nullptr, 10) != txid)
    return absl::OkStatus();
  if (TxCommitTestGate(".terminal"))
    return absl::InternalError("injected commit append failure");
  if (!TxCommitTestGate(".full")) return absl::OkStatus();
  auto exhausted =
      WithErrorDetail(absl::ResourceExhaustedError("out of disk space"),
                      ErrorDetail::kDiskSpace);
  exhausted.SetPayload(exhaustion_type, absl::Cord("foreground"));
  return exhausted;
}

}  // namespace lavik::storage
#endif
