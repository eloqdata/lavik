#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/task.h"

namespace keylane {

enum class BlockingWakeReason : std::uint8_t {
  kWaiting,
  kReady,
  kTimeout,
  kCancelled,
};

enum class BlockingQueuePolicy : std::uint8_t { kFifo, kBroadcast };

// A wait lane distinguishes independent consumers of the same physical key.
// List waits use an empty lane, XREAD uses a connection-unique lane, and
// XREADGROUP uses the group name so consumers in one group remain FIFO.
struct BlockingWaitSpec {
  std::string key_;
  std::string lane_;
  BlockingQueuePolicy policy_ = BlockingQueuePolicy::kFifo;
  std::optional<std::pair<std::uint64_t, std::uint64_t>> stream_after_;
};

class BlockingWaitHandle {
 public:
  BlockingWaitHandle(BlockingWaitHandle&&) noexcept;
  BlockingWaitHandle& operator=(BlockingWaitHandle&&) noexcept;
  ~BlockingWaitHandle();

  BlockingWaitHandle(const BlockingWaitHandle&) = delete;
  BlockingWaitHandle& operator=(const BlockingWaitHandle&) = delete;

 private:
  struct Impl;
  explicit BlockingWaitHandle(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;

  friend celer::Task<
      absl::StatusOr<std::unique_ptr<BlockingWaitHandle>>>
  RegisterBlockingWait(
      std::uint8_t, std::vector<BlockingWaitSpec>,
      std::optional<std::chrono::steady_clock::time_point>);
  friend celer::Task<BlockingWakeReason> WaitForBlockingReady(
      BlockingWaitHandle&);
  friend BlockingWakeReason BlockingWaitState(const BlockingWaitHandle&);
  friend bool ResetBlockingReady(BlockingWaitHandle&);
  friend void FinishBlockingWait(BlockingWaitHandle&);
};

celer::Task<absl::StatusOr<std::unique_ptr<BlockingWaitHandle>>>
RegisterBlockingWait(std::uint8_t db_id, std::vector<BlockingWaitSpec> specs,
                     std::optional<std::chrono::steady_clock::time_point>
                         deadline = std::nullopt);
celer::Task<BlockingWakeReason> WaitForBlockingReady(
    BlockingWaitHandle& handle);
BlockingWakeReason BlockingWaitState(const BlockingWaitHandle& handle);
bool ResetBlockingReady(BlockingWaitHandle& handle);
void FinishBlockingWait(BlockingWaitHandle& handle);

void NotifyListBlockingKey(std::uint8_t db_id, std::string_view key);
void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key,
                             std::uint64_t id_ms, std::uint64_t id_seq);
void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key);
celer::Task<absl::Status> NotifyBlockingDb(std::uint8_t db_id);

}  // namespace keylane
