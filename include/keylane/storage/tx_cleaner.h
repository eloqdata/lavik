#pragma once

#include <cstdint>

namespace keylane::storage::internal {

struct TxGenerationReadiness {
  std::uint64_t active_transactions_ = 0;
  std::uint64_t live_tagged_bytes_ = 0;
  std::uint64_t dependency_pins_ = 0;
  bool sealed_and_durable_ = false;
};

constexpr bool CanReclaimTxGeneration(
    const TxGenerationReadiness& readiness) noexcept {
  return readiness.active_transactions_ == 0 &&
         readiness.live_tagged_bytes_ == 0 && readiness.dependency_pins_ == 0 &&
         readiness.sealed_and_durable_;
}

}  // namespace keylane::storage::internal
