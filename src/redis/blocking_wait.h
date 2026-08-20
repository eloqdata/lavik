#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/task.h"
#include "keylane/command.h"

namespace keylane {

enum class BlockingWakeReason : std::uint8_t {
  kWaiting,
  kReady,
  kTimeout,
  kCancelled,
};

enum class BlockingQueuePolicy : std::uint8_t { kFifo, kBroadcast };

enum class BlockingAttemptState : std::uint8_t { kUnavailable, kComplete };

struct BlockingAttemptResult {
  BlockingAttemptState state_ = BlockingAttemptState::kUnavailable;
  CommandReply reply_;
};

using BlockingAttempt =
    std::function<celer::Task<BlockingAttemptResult>()>;
using BlockingReplyFactory = std::function<CommandReply()>;
using BlockingStatusReplyFactory =
    std::function<CommandReply(const absl::Status&)>;

absl::StatusOr<std::optional<std::chrono::steady_clock::time_point>>
BlockingDeadlineFromSeconds(double timeout_seconds);

// Blocking readiness is type-specific. A write of a different Redis type to
// the same physical key must not wake a waiter and turn an otherwise valid
// block into a spurious WRONGTYPE reply.
enum class BlockingValueType : std::uint8_t { kList, kSortedSet, kStream };

// A wait lane distinguishes independent consumers of the same physical key.
// List waits use an empty lane, XREAD uses a connection-unique lane, and
// XREADGROUP uses the group name so consumers in one group remain FIFO.
struct BlockingWaitSpec {
  std::string key_;
  std::string lane_;
  BlockingValueType value_type_ = BlockingValueType::kList;
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

  friend celer::Task<absl::StatusOr<std::unique_ptr<BlockingWaitHandle>>>
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

// Runs the common check/register/recheck/wait state machine used by blocking
// collection commands. The attempt callback explicitly distinguishes an
// unavailable value from a completed command, so reply encodings never become
// control-flow signals.
celer::Task<CommandReply> ExecuteBlockingWaitLoop(
    std::uint8_t db_id, std::vector<BlockingWaitSpec> specs,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::string cancellation_message, BlockingAttempt attempt,
    BlockingReplyFactory timeout_reply,
    BlockingStatusReplyFactory status_reply);

void InitBlockingWaitStorage(storage::StorageEngine* engine);
void NotifyListBlockingKey(std::uint8_t db_id, std::string_view key);
void NotifyListBlockingKey(const CommandRequest& request, std::string_view key);
void NotifyZSetBlockingKey(std::uint8_t db_id, std::string_view key);
void NotifyZSetBlockingKey(const CommandRequest& request, std::string_view key);
void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key,
                             std::uint64_t id_ms, std::uint64_t id_seq);
void NotifyStreamBlockingKey(const CommandRequest& request,
                             std::string_view key, std::uint64_t id_ms,
                             std::uint64_t id_seq);
void NotifyStreamBlockingKey(std::uint8_t db_id, std::string_view key);
void NotifyStreamBlockingKey(const CommandRequest& request,
                             std::string_view key);
celer::Task<absl::Status> FlushBlockingNotifications(
    BlockingNotificationCapture& capture);
celer::Task<absl::Status> NotifyBlockingDb(std::uint8_t db_id);

}  // namespace keylane
