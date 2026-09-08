#pragma once

#include <optional>

#include "keylane/memory.h"
#include "keylane/storage/detail/collection_compact_stream.h"
#include "keylane/storage/engine.h"
#include "keylane/tx/tx_shard.h"

namespace keylane::storage {

// A native full-sync target keeps this state on the partition owner across
// transport frames. The key hold prevents observing an intermediate root;
// the outer decision independently prevents accepting it after a crash.
// Destruction is not an abort operation: callers must settle the undo/decision
// before releasing the stage, even when a transport/session is cancelled.
struct ReplicaCollectionStage {
  RetainedMemoryCharge decoded_charge_;
  RetainedMemoryCharge undo_charge_;
  std::optional<CollectionCompactDecoder> decoder_;
  TxShardWrites writes_;
  tx::TxShard::Guard key_hold_;
  std::uint64_t received_bytes_ = 0;
  std::uint64_t applied_count_ = 0;
  std::size_t undo_overhead_bytes_ = 0;
  bool skip_ = false;
  bool settled_ = false;
};

}  // namespace keylane::storage
