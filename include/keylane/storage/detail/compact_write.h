#pragma once

#include <cstddef>
#include <cstdint>

namespace keylane::storage {

// Copied population identity for collection read/modify/write that releases
// worker store state while retaining exclusive key intent and database
// admission. Compact writes retain a RecordLocation separately; grouped
// writers reuse this population token with an immutable logical-root handle.
// Creation uses the same population token and revalidates logical absence at
// the original command time; expired/tombstone records need not stay physical.
// Physical coordinates are deliberately not a CAS token because GC may
// relocate the same value while preparation is suspended.
struct CompactWriteSnapshot {
  std::uint64_t index_generation_ = 0;
  std::uint64_t db_epoch_ = 0;
  std::uint64_t replication_epoch_ = 0;
};

// Celer frames guarantee ordinary allocation alignment, not cacheline
// alignment. This copied control state must remain safe inside such a frame.
static_assert(alignof(CompactWriteSnapshot) <= alignof(std::max_align_t));

}  // namespace keylane::storage
