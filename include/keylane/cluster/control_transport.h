#pragma once

// Streaming transport adapter for the Meta <-> Data control protocol. It is
// deliberately separate from control_protocol: framing/codecs stay usable in
// pure tests, while this module owns Celer socket reads and writes.

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/net/tcp_stream.h"
#include "celer/runtime/task.h"
#include "keylane/cluster/control_protocol.h"

namespace keylane::cluster::control {

// One worker-local deadline task that can be repeatedly armed without
// accumulating a sleeping coroutine for every successful protocol event.
// The callback runs on the owning worker and must not synchronously destroy
// the watchdog. Disarm returns whether the most recent arm expired.
class ControlDeadlineWatchdog {
 public:
  using ExpireCallback = std::function<void()>;

  ControlDeadlineWatchdog(celer::Worker& worker,
                          ExpireCallback expire_callback);
  ~ControlDeadlineWatchdog();

  ControlDeadlineWatchdog(const ControlDeadlineWatchdog&) = delete;
  ControlDeadlineWatchdog& operator=(const ControlDeadlineWatchdog&) = delete;

  absl::Status Arm(std::chrono::nanoseconds timeout);
  bool Disarm() noexcept;

 private:
  friend class ControlDeadlineWatchdogTestPeer;
  struct State;

  static celer::Task<absl::Status> Watch(std::shared_ptr<State> state);
  std::uint64_t TaskStartsForTest() const noexcept;

  std::shared_ptr<State> state_;
};

// Transport-selected payload size for object chunks. It deliberately leaves
// room for the TransferChunk envelope inside one protocol frame; receivers
// accept any legal chunk size, so this is not a wire-format constant.
inline constexpr std::size_t kControlTransferChunkBytes = 12u * 1024u;

enum class MessagePriority : std::uint8_t {
  kAuthority = 0,
  kReliable = 1,
  kBulk = 2,
  kSoft = 3,
};

// Bounded scheduling queue used by a session's sole writer. Authority cannot
// be trapped behind bulk transfer data: dequeue always checks the four lanes
// in priority order. The bound includes each frame header plus its encoded
// payload, and an oversized enqueue is rejected without evicting reliable
// work.
class ControlWriteQueue {
 public:
  explicit ControlWriteQueue(std::size_t max_bytes) : max_bytes_(max_bytes) {}

  absl::Status Enqueue(MessagePriority priority, WireMessage message);
  std::optional<std::pair<MessagePriority, WireMessage>> Pop();
  std::size_t queued_bytes() const noexcept { return queued_bytes_; }
  bool empty() const noexcept { return queued_bytes_ == 0; }

 private:
  struct Item {
    WireMessage message_;
    std::size_t bytes_ = 0;
  };

  static constexpr std::size_t Lane(MessagePriority priority) {
    return static_cast<std::size_t>(priority);
  }

  const std::size_t max_bytes_;
  std::size_t queued_bytes_ = 0;
  std::array<std::deque<Item>, 4> lanes_;
};

// One instance belongs to one TCP session and owns both directional frame
// sequence counters. Call Read/Write from exactly one reader and one writer
// coroutine respectively. The transport reads a fixed header followed by the
// declared payload, so frame boundaries never depend on TCP packetization.
class ControlFrameStream {
 public:
  explicit ControlFrameStream(celer::TcpStream& stream,
                              std::chrono::nanoseconds write_progress_timeout =
                                  std::chrono::seconds(10));
  ~ControlFrameStream();

  ControlFrameStream(const ControlFrameStream&) = delete;
  ControlFrameStream& operator=(const ControlFrameStream&) = delete;

  // Must run before TLS/application reads. It disables Celer's multishot
  // prefetch so the bounded protocol state is also the socket ingress bound.
  absl::Status Prepare() noexcept;

  celer::Task<absl::StatusOr<Frame>> ReadFrame();
  celer::Task<absl::StatusOr<WireMessage>> ReadMessage();

  // before_write runs after framing and immediately before the first
  // WriteAll. Lease challenges use it to capture the only valid sent_at.
  celer::Task<absl::Status> WriteMessage(
      const WireMessage& message,
      std::function<void()> before_write = std::function<void()>{});

 private:
  struct WriteDeadlineState;
  static celer::Task<absl::Status> WatchWriteDeadline(
      std::shared_ptr<WriteDeadlineState> state);
  absl::Status ArmWriteDeadline();
  bool DisarmWriteDeadline() noexcept;

  celer::Task<absl::Status> ReadExactly(std::span<std::byte> destination);
  celer::Task<absl::Status> WriteEncoded(std::string encoded,
                                         std::function<void()> before_write);

  celer::TcpStream& stream_;
  const std::chrono::nanoseconds write_progress_timeout_;
  std::shared_ptr<WriteDeadlineState> write_deadline_;
  FrameEncoder encoder_;
  FrameDecoder decoder_;
};

// Worker-local, multi-producer session writer. Every Write call first reserves
// bounded queue capacity, then either drives or joins the single writer
// coroutine. A transfer keeps one frame-sized reservation for its lifetime
// and is re-scheduled after every frame, so authority/reliable work may pass
// between bulk chunks but bytes already submitted to the socket are never
// interrupted.
//
// The writer and its ControlFrameStream must outlive all awaited Write calls.
// It creates no detached coroutine: one of the producer coroutines owns the
// drain loop until every item admitted in that drain epoch has completed.
class ControlSessionWriter {
 public:
  using WriteFunction = std::function<celer::Task<absl::Status>(
      WireMessage, std::function<void()>)>;

  ControlSessionWriter(ControlFrameStream& frames, std::size_t max_queue_bytes);
  // Injectable frame sink for focused transport tests and non-socket
  // adapters. The sink must invoke before_write immediately before its first
  // attempted write, matching ControlFrameStream::WriteMessage.
  ControlSessionWriter(WriteFunction write_frame, std::size_t max_queue_bytes);
  ~ControlSessionWriter();

  ControlSessionWriter(const ControlSessionWriter&) = delete;
  ControlSessionWriter& operator=(const ControlSessionWriter&) = delete;

  celer::Task<absl::Status> Write(
      MessagePriority priority, WireMessage message,
      std::function<void()> before_write = std::function<void()>{});

  // Sends canonical EncodeFullDesiredState output through the reliable lane
  // when it fits one frame. Only an oversized object enters the serialized
  // Start/Chunk/End transfer path, where bulk chunks remain preemptible by
  // authority and reliable frames. Direct decode derives the same object hash
  // that the transfer envelope protects for streamed objects.
  celer::Task<absl::Status> WriteFullDesiredState(
      std::shared_ptr<const std::string> encoded);

  // The immutable owner is retained by the scheduled request. This keeps a
  // queued transfer valid even if its producer coroutine is cancelled, while
  // allowing FullDesiredState to use an aliasing shared_ptr without copying a
  // potentially 512 MiB object. Transfers are serialized as complete
  // Start/Chunk/End sequences; ordinary authority/reliable messages may still
  // pass between chunks.
  celer::Task<absl::Status> WriteTransfer(
      TransferKind kind, WireId128 object_id,
      std::shared_ptr<const std::string> bytes);

  // Includes the frame currently being written as well as queued frames.
  std::size_t outstanding_bytes() const noexcept;
  bool failed() const noexcept;

 private:
  struct Request;
  struct Impl;

  celer::Task<absl::Status> Drive();
  std::unique_ptr<Impl> impl_;
};

}  // namespace keylane::cluster::control
