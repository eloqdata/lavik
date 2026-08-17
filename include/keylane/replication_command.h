#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "keylane/storage/engine.h"

namespace keylane {

// A committed, deterministic Redis command. The database is carried on every
// record so replay does not depend on connection-local SELECT state.
struct ReplicatedCommand {
  std::uint8_t db_id_ = 0;
  std::vector<std::string> args_;
};

// Streams one encoded command into the disk-backed replication log without
// flattening large arguments into another contiguous allocation. Replication
// log frames may split this byte stream, but the receiver still observes one
// logical command and one LSN.
class ReplicationCommandPayloadSource final
    : public storage::ReplicationLogPayloadSource {
 public:
  static absl::StatusOr<ReplicationCommandPayloadSource> Create(
      std::uint8_t db_id, std::span<const std::string_view> args);

  ReplicationCommandPayloadSource(ReplicationCommandPayloadSource&&) noexcept =
      default;
  ReplicationCommandPayloadSource& operator=(
      ReplicationCommandPayloadSource&&) noexcept = default;

  std::uint64_t size() const noexcept override { return size_; }
  celer::Task<absl::Status> Read(std::uint64_t offset,
                                 std::span<std::byte> output) override;

 private:
  ReplicationCommandPayloadSource() = default;

  std::string header_;
  std::vector<std::string_view> args_;
  std::uint64_t size_ = 0;
};

// Decodes one complete logical command after all transport frames for its LSN
// have arrived. The format is native to Keylane and deliberately independent
// of the source worker count and the target's physical value layout.
absl::StatusOr<ReplicatedCommand> DecodeReplicationCommand(
    std::string_view encoded);

}  // namespace keylane
