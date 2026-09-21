/*
 * Copyright (C) 2026 EloqData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "absl/status/statusor.h"
#include "lavik/storage/engine.h"

namespace lavik {

inline constexpr std::string_view kReplicatedExecCommand = "__LAVIK_EXEC_V1";
// Fragmentation changes only transport granularity. Every producer and
// receiver applies this limit to the complete canonical event.
inline constexpr std::uint64_t kMaxNativeReplicationEventBytes =
    1024ULL * 1024 * 1024;

// A committed, deterministic Redis command. The database is carried on every
// record so replay does not depend on connection-local SELECT state.
struct ReplicatedCommand {
  std::uint8_t db_id_ = 0;
  std::vector<std::string> args_;
};

// Native V1 cross-flow transaction identity. The encoded form uses a bitmap
// so its size tracks the source worker range rather than repeating decimal
// participant IDs in every flow-local marker.
struct ReplicationTransactionEnvelope {
  std::uint64_t id_ = 0;
  unsigned payload_flow_ = 0;
  std::vector<unsigned> participants_;
};

// Encodes the metadata argument shared by every participant record. The
// canonical command, when present, follows this argument only on payload_flow.
absl::StatusOr<std::string> EncodeReplicationTransactionEnvelope(
    const ReplicationTransactionEnvelope& envelope);

// Decodes one complete V1 metadata argument and rejects non-canonical bitmaps.
absl::StatusOr<ReplicationTransactionEnvelope>
DecodeReplicationTransactionEnvelope(std::string_view encoded);

// Returns true for the V1 binary transaction magic, including malformed
// records that must be routed to the strict decoder instead of command replay.
bool IsReplicationTransactionEnvelope(std::string_view encoded) noexcept;

// Streams one encoded command into the runtime-only in-memory replication log
// without flattening large arguments into another contiguous allocation.
// Replication log frames may split this byte stream, but the receiver still
// observes one logical command and one LSN.
class ReplicationCommandPayloadSource final
    : public storage::ReplicationLogPayloadSource {
 public:
  static absl::StatusOr<ReplicationCommandPayloadSource> Create(
      std::uint8_t db_id, std::span<const std::string_view> args);
  // Convenience ownership boundary for queued vector<string> payloads. The
  // returned source keeps only views, so args must outlive the source.
  static absl::StatusOr<ReplicationCommandPayloadSource> Create(
      std::uint8_t db_id, std::span<const std::string> args);

  ReplicationCommandPayloadSource(ReplicationCommandPayloadSource&&) noexcept =
      default;
  ReplicationCommandPayloadSource& operator=(
      ReplicationCommandPayloadSource&&) noexcept = default;

  std::uint64_t size() const noexcept override { return size_; }
  bycorf::Task<absl::Status> Read(std::uint64_t offset,
                                  std::span<std::byte> output) override;

 private:
  ReplicationCommandPayloadSource() = default;

  std::string header_;
  std::vector<std::string_view> args_;
  std::uint64_t size_ = 0;
};

// Decodes one complete logical command after all transport frames for its LSN
// have arrived. The format is native to Lavik and deliberately independent
// of the source worker count and the target's physical value layout.
absl::StatusOr<ReplicatedCommand> DecodeReplicationCommand(
    std::string_view encoded);

// Identifies a well-formed PUBLISH or a nonempty V1 EXEC containing only
// PUBLISH children. These effects need no partition state during native FULL
// replay. Malformed envelopes and any storage effects return false; ordinary
// replay still validates enabled databases and command execution.
bool IsPublishOnlyReplicationCommand(const ReplicatedCommand& command) noexcept;

// Turns one journaled mutation into a strict replicated EXEC and appends the
// exact committed after-image for the key's expiration metadata. A past
// absolute deadline intentionally deletes the value on a delayed replica.
void AppendReplicationExpirationEffect(std::vector<std::string>* args,
                                       std::uint8_t command_db_id,
                                       std::uint8_t effect_db_id,
                                       std::string_view key, bool exists,
                                       std::uint64_t expire_at_ms);

}  // namespace lavik
