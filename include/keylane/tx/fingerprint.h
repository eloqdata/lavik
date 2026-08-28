#pragma once

#include <cstddef>
#include <cstdint>

#include "keylane/storage/format.h"

namespace keylane::tx {

// Lock fingerprint: the key's process-random SipHash digest, which the engine
// computes for index lookups anyway. Collisions are correctness-neutral (two
// keys sharing a lock entry only causes false contention): execution order is
// arbitrated by the per-shard TxQueue and data access still compares the key.
using LockFp = std::uint64_t;

inline LockFp FingerprintOf(const storage::Digest& digest) noexcept {
  return digest.value_;
}

// The fingerprint is already uniformly distributed; hash maps keyed by it use
// the identity function.
struct LockFpIdentityHash {
  std::size_t operator()(LockFp fp) const noexcept { return fp; }
};

enum class LockMode : std::uint8_t {
  kShared,
  kExclusive,
};

// One key of a transaction's lock set: fingerprint, requested mode, and the
// logical database whose lock table arbitrates it. Carrying the database per
// key lets one transaction span databases (SELECT inside MULTI).
struct KeyRef {
  LockFp fp_ = 0;
  LockMode mode_ = LockMode::kShared;
  std::uint8_t db_ = 0;
};

}  // namespace keylane::tx
