#include "keylane/cluster/control_transport.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/io/storage.h"
#include "celer/runtime/worker.h"

namespace keylane::cluster::control {
namespace {

std::span<const std::byte> Bytes(std::string_view value) {
  return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

std::uint64_t TransferLimit(TransferKind kind) {
  switch (kind) {
    case TransferKind::kFullDesiredState:
    case TransferKind::kNodeControlUpdate:
      return kMaxFullDesiredStateBytes;
    case TransferKind::kDirectivePayload:
      return kMaxDirectiveTransferBytes;
    case TransferKind::kDirectiveResult:
      return kMaxDirectiveResultTransferBytes;
  }
  return 0;
}

}  // namespace

struct ControlDeadlineWatchdog::State {
  celer::Worker* worker_ = nullptr;
  ExpireCallback expire_callback_;
  std::chrono::steady_clock::time_point deadline_{};
  celer::TimerCancelHandle cancel_;
  std::uint64_t generation_ = 0;
  std::uint64_t task_starts_ = 0;
  bool armed_ = false;
  bool running_ = false;
  bool expired_ = false;
};

ControlDeadlineWatchdog::ControlDeadlineWatchdog(
    celer::Worker& worker, ExpireCallback expire_callback)
    : state_(std::make_shared<State>()) {
  state_->worker_ = &worker;
  state_->expire_callback_ = std::move(expire_callback);
}

ControlDeadlineWatchdog::~ControlDeadlineWatchdog() {
  // A detached timer retains only this small shared state. Invalidate the
  // owner callback before session-local objects disappear and cancel the
  // kernel timer so the sole dormant coroutine retires promptly.
  ++state_->generation_;
  state_->armed_ = false;
  state_->expire_callback_ = {};
  state_->cancel_.Cancel();
}

celer::Task<absl::Status> ControlDeadlineWatchdog::Watch(
    std::shared_ptr<State> state) {
  while (state->armed_) {
    const std::uint64_t generation = state->generation_;
    const auto now = std::chrono::steady_clock::now();
    if (now >= state->deadline_) {
      state->expired_ = true;
      state->armed_ = false;
      if (state->expire_callback_) state->expire_callback_();
      break;
    }
    auto timer = celer::CancellableSleepFor(*state->worker_,
                                            state->deadline_ - now);
    state->cancel_ = timer.CancelHandle();
    const absl::Status slept = co_await timer;
    state->cancel_ = {};
    if (!state->armed_) break;
    if (generation != state->generation_ ||
        slept.code() == absl::StatusCode::kCancelled) {
      continue;
    }
    if (!slept.ok()) {
      // Failing open would silently remove a control-session safety bound.
      // The owner callback closes/fails its session for both timeout and
      // timer-backend failure; the transport error remains observable in the
      // worker logs.
      state->expired_ = true;
      state->armed_ = false;
      if (state->expire_callback_) state->expire_callback_();
      break;
    }
  }
  state->running_ = false;
  co_return absl::OkStatus();
}

absl::Status ControlDeadlineWatchdog::Arm(
    std::chrono::nanoseconds timeout) {
  if (timeout.count() <= 0) {
    return absl::InvalidArgumentError("control deadline must be positive");
  }
  if (celer::ThisWorker().self_ != state_->worker_) {
    return absl::FailedPreconditionError(
        "control deadline must be armed on its owning worker");
  }
  state_->cancel_.Cancel();
  ++state_->generation_;
  state_->deadline_ = std::chrono::steady_clock::now() + timeout;
  state_->expired_ = false;
  state_->armed_ = true;
  if (!state_->running_) {
    state_->running_ = true;
    ++state_->task_starts_;
    state_->worker_->Spawn(Watch(state_));
  }
  return absl::OkStatus();
}

bool ControlDeadlineWatchdog::Disarm() noexcept {
  ++state_->generation_;
  state_->armed_ = false;
  state_->cancel_.Cancel();
  return state_->expired_;
}

std::uint64_t ControlDeadlineWatchdog::TaskStartsForTest() const noexcept {
  return state_->task_starts_;
}

struct ControlFrameStream::WriteDeadlineState {
  celer::TcpStream* stream_ = nullptr;
  celer::Worker* worker_ = nullptr;
  std::chrono::steady_clock::time_point deadline_{};
  bool armed_ = false;
  bool running_ = false;
  bool expired_ = false;
};

ControlFrameStream::ControlFrameStream(
    celer::TcpStream& stream, std::chrono::nanoseconds write_progress_timeout)
    : stream_(stream),
      write_progress_timeout_(write_progress_timeout),
      write_deadline_(std::make_shared<WriteDeadlineState>()) {
  write_deadline_->stream_ = &stream_;
}

ControlFrameStream::~ControlFrameStream() {
  // A detached timer owns the shared state, never this frame stream. Clearing
  // the stream pointer before this object goes away makes a late timer fire a
  // harmless no-op instead of touching a session-local TcpStream.
  write_deadline_->armed_ = false;
  write_deadline_->stream_ = nullptr;
}

celer::Task<absl::Status> ControlFrameStream::WatchWriteDeadline(
    std::shared_ptr<WriteDeadlineState> state) {
  while (state->armed_ && state->stream_ != nullptr) {
    const auto now = std::chrono::steady_clock::now();
    if (now < state->deadline_) {
      const absl::Status slept =
          co_await celer::SleepFor(*state->worker_, state->deadline_ - now);
      if (!slept.ok()) {
        state->running_ = false;
        co_return absl::OkStatus();
      }
      continue;
    }
    state->expired_ = true;
    state->armed_ = false;
    (void)state->stream_->Close();
  }
  state->running_ = false;
  co_return absl::OkStatus();
}

absl::Status ControlFrameStream::ArmWriteDeadline() {
  if (write_progress_timeout_.count() <= 0) return absl::OkStatus();
  celer::Worker* worker = celer::ThisWorker().self_;
  if (worker == nullptr) {
    return absl::FailedPreconditionError(
        "control writes must run on a Celer worker");
  }
  write_deadline_->worker_ = worker;
  write_deadline_->deadline_ =
      std::chrono::steady_clock::now() + write_progress_timeout_;
  write_deadline_->expired_ = false;
  write_deadline_->armed_ = true;
  if (!write_deadline_->running_) {
    write_deadline_->running_ = true;
    worker->Spawn(WatchWriteDeadline(write_deadline_));
  }
  return absl::OkStatus();
}

bool ControlFrameStream::DisarmWriteDeadline() noexcept {
  write_deadline_->armed_ = false;
  return write_deadline_->expired_;
}

absl::Status ControlWriteQueue::Enqueue(MessagePriority priority,
                                        WireMessage message) {
  auto payload = EncodeMessage(message);
  if (!payload.ok()) return payload.status();
  if (payload->size() > kMaxFramePayloadBytes) {
    return absl::ResourceExhaustedError(
        "control message requires an object transfer");
  }
  const std::size_t bytes = kFrameHeaderBytes + payload->size();
  if (bytes > max_bytes_ || queued_bytes_ > max_bytes_ - bytes) {
    return absl::ResourceExhaustedError("control writer queue is full");
  }
  lanes_[Lane(priority)].push_back(
      Item{.message_ = std::move(message), .bytes_ = bytes});
  queued_bytes_ += bytes;
  return absl::OkStatus();
}

std::optional<std::pair<MessagePriority, WireMessage>>
ControlWriteQueue::Pop() {
  for (std::size_t lane = 0; lane < lanes_.size(); ++lane) {
    if (lanes_[lane].empty()) continue;
    Item item = std::move(lanes_[lane].front());
    lanes_[lane].pop_front();
    queued_bytes_ -= item.bytes_;
    return std::pair{static_cast<MessagePriority>(lane),
                     std::move(item.message_)};
  }
  return std::nullopt;
}

absl::Status ControlFrameStream::Prepare() noexcept {
  return stream_.SetReadAhead(false);
}

celer::Task<absl::Status> ControlFrameStream::ReadExactly(
    std::span<std::byte> destination) {
  std::size_t read_bytes = 0;
  while (read_bytes < destination.size()) {
    auto read = co_await stream_.ReadSome(destination.subspan(read_bytes));
    if (!read.ok()) co_return read.status();
    if (*read == 0) {
      co_return absl::UnavailableError("control peer closed the connection");
    }
    read_bytes += *read;
  }
  co_return absl::OkStatus();
}

celer::Task<absl::StatusOr<Frame>> ControlFrameStream::ReadFrame() {
  std::array<std::byte, kFrameHeaderBytes> header_bytes{};
  if (absl::Status read = co_await ReadExactly(header_bytes); !read.ok()) {
    co_return read;
  }
  const std::string_view header(
      reinterpret_cast<const char*>(header_bytes.data()), header_bytes.size());
  auto parsed = ParseFrameHeader(header);
  if (!parsed.ok()) co_return parsed.status();

  std::string encoded(kFrameHeaderBytes + parsed->payload_length, '\0');
  std::memcpy(encoded.data(), header.data(), header.size());
  if (parsed->payload_length != 0) {
    std::span<std::byte> payload(
        reinterpret_cast<std::byte*>(encoded.data() + kFrameHeaderBytes),
        parsed->payload_length);
    if (absl::Status read = co_await ReadExactly(payload); !read.ok()) {
      co_return read;
    }
  }
  co_return decoder_.Decode(encoded);
}

celer::Task<absl::StatusOr<WireMessage>> ControlFrameStream::ReadMessage() {
  auto frame = co_await ReadFrame();
  if (!frame.ok()) co_return frame.status();
  co_return DecodeMessage(frame->type, frame->payload);
}

celer::Task<absl::Status> ControlFrameStream::WriteEncoded(
    std::string encoded, std::function<void()> before_write) {
  const std::span<const std::byte> bytes = Bytes(encoded);
  std::size_t written = 0;
  bool first_write = true;
  while (written < bytes.size()) {
    if (absl::Status armed = ArmWriteDeadline(); !armed.ok()) {
      co_return armed;
    }
    if (first_write && before_write) before_write();
    first_write = false;
    auto result = co_await stream_.WriteSome(bytes.subspan(written));
    const bool expired = DisarmWriteDeadline();
    if (expired) {
      co_return absl::DeadlineExceededError(
          "control write made no progress before its deadline");
    }
    if (!result.ok()) co_return result.status();
    if (*result == 0) {
      co_return absl::InternalError("control write made zero progress");
    }
    written += *result;
  }
  co_return absl::OkStatus();
}

celer::Task<absl::Status> ControlFrameStream::WriteMessage(
    const WireMessage& message, std::function<void()> before_write) {
  auto payload = EncodeMessage(message);
  if (!payload.ok()) co_return payload.status();
  auto encoded = encoder_.Encode(MessageTypeOf(message), *payload);
  if (!encoded.ok()) co_return encoded.status();
  co_return co_await WriteEncoded(std::move(*encoded), std::move(before_write));
}

// --------------------------------------------------------------------------
// ControlSessionWriter
// --------------------------------------------------------------------------

struct ControlSessionWriter::Request {
  enum class Kind : std::uint8_t { kMessage, kTransfer };
  enum class TransferPhase : std::uint8_t { kStart, kChunk, kEnd };

  Kind kind_ = Kind::kMessage;
  std::size_t reservation_bytes_ = 0;
  std::function<void()> before_write_;

  TransferKind transfer_kind_ = TransferKind::kFullDesiredState;
  WireId128 object_id_{};
  std::shared_ptr<const std::string> transfer_bytes_;
  TransferPhase transfer_phase_ = TransferPhase::kStart;
  std::size_t next_offset_ = 0;
  std::size_t queued_chunk_bytes_ = 0;

  // A producer may be reclaimed while it is suspended (session shutdown
  // destroys the entire coroutine chain). Keeping the waiter in an awaiter
  // that unregisters from its destructor prevents a later write completion
  // from enqueueing a dead coroutine handle. AsyncNotification intentionally
  // does not provide that cancellation contract.
  class CompletionAwaiter {
   public:
    explicit CompletionAwaiter(std::shared_ptr<Request> request)
        : request_(std::move(request)) {}

    bool await_ready() const noexcept { return request_->complete_; }
    bool await_suspend(std::coroutine_handle<> awaiting) noexcept {
      awaiting_ = awaiting;
      request_->waiter_ = awaiting;
      return true;
    }
    void await_resume() const noexcept {}

    ~CompletionAwaiter() {
      if (awaiting_ && request_->waiter_ == awaiting_) {
        request_->waiter_ = {};
      }
    }

   private:
    std::shared_ptr<Request> request_;
    std::coroutine_handle<> awaiting_{};
  };

  CompletionAwaiter Wait(const std::shared_ptr<Request>& self) {
    return CompletionAwaiter(self);
  }

  std::coroutine_handle<> waiter_{};
  absl::Status result_ = absl::UnknownError("control write is incomplete");
  bool complete_ = false;
};

struct ControlSessionWriter::Impl {
  struct Scheduled {
    WireMessage message_;
    std::shared_ptr<Request> request_;
  };

  Impl(WriteFunction write_frame, std::size_t max_queue_bytes)
      : write_frame_(std::move(write_frame)),
        max_queue_bytes_(max_queue_bytes),
        frames_(max_queue_bytes) {}

  static constexpr std::size_t Lane(MessagePriority priority) {
    return static_cast<std::size_t>(priority);
  }

  absl::Status BindWorker() {
    celer::Worker* current = celer::ThisWorker().self_;
    if (current == nullptr) {
      return absl::FailedPreconditionError(
          "control session writes require a Celer worker");
    }
    if (owner_worker_ == nullptr) {
      owner_worker_ = current;
    } else if (owner_worker_ != current) {
      return absl::FailedPreconditionError(
          "control session writer used from more than one worker");
    }
    return absl::OkStatus();
  }

  absl::Status Reserve(std::size_t bytes) {
    if (bytes > max_queue_bytes_ ||
        outstanding_bytes_ > max_queue_bytes_ - bytes) {
      return absl::ResourceExhaustedError(
          "control session writer queue is full");
    }
    outstanding_bytes_ += bytes;
    return absl::OkStatus();
  }

  absl::Status QueueFrame(MessagePriority priority, WireMessage message,
                          const std::shared_ptr<Request>& request) {
    const absl::Status queued = frames_.Enqueue(priority, std::move(message));
    if (!queued.ok()) return queued;
    requests_[Lane(priority)].push_back(request);
    return absl::OkStatus();
  }

  absl::StatusOr<Scheduled> PopFrame() {
    auto frame = frames_.Pop();
    if (!frame.has_value()) {
      return absl::InternalError("control writer scheduler lost a frame");
    }
    auto& owners = requests_[Lane(frame->first)];
    if (owners.empty()) {
      return absl::InternalError("control writer scheduler lost its owner");
    }
    std::shared_ptr<Request> request = std::move(owners.front());
    owners.pop_front();
    return Scheduled{.message_ = std::move(frame->second),
                     .request_ = std::move(request)};
  }

  void Complete(const std::shared_ptr<Request>& request, absl::Status status) {
    if (request->complete_) return;
    request->result_ = std::move(status);
    request->complete_ = true;
    outstanding_bytes_ -= request->reservation_bytes_;
    const std::coroutine_handle<> waiter = std::exchange(request->waiter_, {});
    if (waiter) owner_worker_->Enqueue(waiter);
  }

  void FailQueued(const absl::Status& status) {
    while (!frames_.empty()) {
      auto scheduled = PopFrame();
      if (!scheduled.ok()) break;
      Complete(scheduled->request_, status);
    }
  }

  void FailPendingTransfers(const absl::Status& status) {
    if (active_transfer_ != nullptr) {
      Complete(active_transfer_, status);
    }
    for (const std::shared_ptr<Request>& request : pending_transfers_) {
      Complete(request, status);
    }
    pending_transfers_.clear();
    active_transfer_.reset();
  }

  // Drive normally lives as a child of the producer that first found an idle
  // writer. If that producer is cancelled, coroutine destruction runs this
  // path and resolves every other admitted producer instead of stranding its
  // waiter behind a permanently-set driving_ bit.
  void AbortDrive() {
    if (!driving_) return;
    if (!terminal_error_.has_value()) {
      terminal_error_ =
          absl::CancelledError("control session writer was cancelled");
    }
    if (active_request_) {
      Complete(active_request_, *terminal_error_);
      active_request_.reset();
    }
    FailQueued(*terminal_error_);
    FailPendingTransfers(*terminal_error_);
    driving_ = false;
  }

  absl::Status QueueTransferStart(const std::shared_ptr<Request>& request) {
    const std::string& bytes = *request->transfer_bytes_;
    return QueueFrame(MessagePriority::kReliable,
                      WireMessage(TransferStart{
                          .kind = request->transfer_kind_,
                          .object_id = request->object_id_,
                          .total_length = bytes.size(),
                      }),
                      request);
  }

  absl::Status PromoteNextTransfer() {
    active_transfer_.reset();
    if (pending_transfers_.empty()) return absl::OkStatus();
    active_transfer_ = std::move(pending_transfers_.front());
    pending_transfers_.pop_front();
    return QueueTransferStart(active_transfer_);
  }

  absl::Status QueueNextTransfer(const std::shared_ptr<Request>& request) {
    using Phase = Request::TransferPhase;
    const std::string& bytes = *request->transfer_bytes_;
    if (request->transfer_phase_ == Phase::kStart) {
      if (bytes.empty()) {
        request->transfer_phase_ = Phase::kEnd;
        return QueueFrame(MessagePriority::kReliable,
                          WireMessage(TransferEnd{request->object_id_}),
                          request);
      }
      request->transfer_phase_ = Phase::kChunk;
    } else if (request->transfer_phase_ == Phase::kChunk) {
      request->next_offset_ += request->queued_chunk_bytes_;
      request->queued_chunk_bytes_ = 0;
      if (request->next_offset_ == bytes.size()) {
        request->transfer_phase_ = Phase::kEnd;
        return QueueFrame(MessagePriority::kReliable,
                          WireMessage(TransferEnd{request->object_id_}),
                          request);
      }
    } else {
      return absl::FailedPreconditionError(
          "completed transfer has no next frame");
    }

    const std::size_t count = std::min(kControlTransferChunkBytes,
                                       bytes.size() - request->next_offset_);
    request->queued_chunk_bytes_ = count;
    return QueueFrame(MessagePriority::kBulk,
                      WireMessage(TransferChunk{
                          .object_id = request->object_id_,
                          .offset = request->next_offset_,
                          .bytes = bytes.substr(request->next_offset_, count),
                      }),
                      request);
  }

  WriteFunction write_frame_;
  const std::size_t max_queue_bytes_;
  ControlWriteQueue frames_;
  std::array<std::deque<std::shared_ptr<Request>>, 4> requests_;
  // Only one complete transfer may be on the frame lanes at a time. Pending
  // transfers retain their bounded reservation and immutable payload owner,
  // but do not enqueue Start until the preceding End completes.
  std::shared_ptr<Request> active_transfer_;
  std::deque<std::shared_ptr<Request>> pending_transfers_;
  celer::Worker* owner_worker_ = nullptr;
  std::shared_ptr<Request> active_request_;
  std::optional<absl::Status> terminal_error_;
  std::size_t outstanding_bytes_ = 0;
  bool driving_ = false;
};

ControlSessionWriter::ControlSessionWriter(ControlFrameStream& frames,
                                           std::size_t max_queue_bytes)
    : ControlSessionWriter(
          [&frames](WireMessage message, std::function<void()> before_write)
              -> celer::Task<absl::Status> {
            co_return co_await frames.WriteMessage(message,
                                                   std::move(before_write));
          },
          max_queue_bytes) {}

ControlSessionWriter::ControlSessionWriter(WriteFunction write_frame,
                                           std::size_t max_queue_bytes)
    : impl_(std::make_unique<Impl>(std::move(write_frame), max_queue_bytes)) {}

ControlSessionWriter::~ControlSessionWriter() = default;

celer::Task<absl::Status> ControlSessionWriter::Write(
    MessagePriority priority, WireMessage message,
    std::function<void()> before_write) {
  if (absl::Status bound = impl_->BindWorker(); !bound.ok()) co_return bound;
  if (impl_->terminal_error_.has_value()) {
    co_return *impl_->terminal_error_;
  }
  auto encoded = EncodeMessage(message);
  if (!encoded.ok()) co_return encoded.status();
  if (encoded->size() > kMaxFramePayloadBytes) {
    co_return absl::ResourceExhaustedError(
        "control message requires an object transfer");
  }
  const std::size_t reservation = kFrameHeaderBytes + encoded->size();
  if (absl::Status reserved = impl_->Reserve(reservation); !reserved.ok()) {
    co_return reserved;
  }
  auto request = std::make_shared<Request>();
  request->kind_ = Request::Kind::kMessage;
  request->reservation_bytes_ = reservation;
  request->before_write_ = std::move(before_write);
  if (absl::Status queued =
          impl_->QueueFrame(priority, std::move(message), request);
      !queued.ok()) {
    impl_->outstanding_bytes_ -= reservation;
    co_return queued;
  }

  if (!impl_->driving_) {
    impl_->driving_ = true;
    (void)co_await Drive();
  } else if (!request->complete_) {
    co_await request->Wait(request);
  }
  co_return request->result_;
}

celer::Task<absl::Status> ControlSessionWriter::WriteFullDesiredState(
    std::shared_ptr<const std::string> encoded) {
  if (encoded == nullptr) {
    co_return absl::InvalidArgumentError(
        "FullDesiredState requires an immutable encoded payload owner");
  }
  if (encoded->size() > kMaxFullDesiredStateBytes) {
    co_return absl::ResourceExhaustedError(
        "FullDesiredState exceeds its object-size limit");
  }
  if (encoded->size() <= kMaxFramePayloadBytes) {
    auto desired = DecodeFullDesiredState(*encoded);
    if (!desired.ok()) co_return desired.status();
    co_return co_await Write(MessagePriority::kReliable,
                             WireMessage(std::move(*desired)));
  }

  auto object_id = GenerateId128();
  if (!object_id.ok()) co_return object_id.status();
  co_return co_await WriteTransfer(TransferKind::kFullDesiredState, *object_id,
                                   std::move(encoded));
}

celer::Task<absl::Status> ControlSessionWriter::WriteTransfer(
    TransferKind kind, WireId128 object_id,
    std::shared_ptr<const std::string> bytes) {
  if (absl::Status bound = impl_->BindWorker(); !bound.ok()) co_return bound;
  if (impl_->terminal_error_.has_value()) {
    co_return *impl_->terminal_error_;
  }
  if (bytes == nullptr) {
    co_return absl::InvalidArgumentError(
        "control transfer requires an immutable payload owner");
  }
  const std::uint64_t limit = TransferLimit(kind);
  if (limit == 0) {
    co_return absl::InvalidArgumentError("unknown control transfer kind");
  }
  if (bytes->size() > limit) {
    co_return absl::ResourceExhaustedError(
        "control transfer exceeds its object-size limit");
  }
  if (absl::Status reserved = impl_->Reserve(kMaxFrameBytes); !reserved.ok()) {
    co_return reserved;
  }

  auto request = std::make_shared<Request>();
  request->kind_ = Request::Kind::kTransfer;
  request->reservation_bytes_ = kMaxFrameBytes;
  request->transfer_kind_ = kind;
  request->object_id_ = std::move(object_id);
  request->transfer_bytes_ = std::move(bytes);
  if (impl_->active_transfer_ == nullptr) {
    impl_->active_transfer_ = request;
    if (absl::Status queued = impl_->QueueTransferStart(request);
        !queued.ok()) {
      impl_->active_transfer_.reset();
      impl_->outstanding_bytes_ -= request->reservation_bytes_;
      co_return queued;
    }
  } else {
    impl_->pending_transfers_.push_back(request);
  }

  if (!impl_->driving_) {
    impl_->driving_ = true;
    (void)co_await Drive();
  } else if (!request->complete_) {
    co_await request->Wait(request);
  }
  co_return request->result_;
}

celer::Task<absl::Status> ControlSessionWriter::Drive() {
  struct DriveGuard {
    explicit DriveGuard(Impl* impl) : impl_(impl) {}
    ~DriveGuard() {
      if (armed_) impl_->AbortDrive();
    }
    void Dismiss() noexcept { armed_ = false; }

    Impl* impl_;
    bool armed_ = true;
  } guard(impl_.get());

  while (!impl_->frames_.empty()) {
    auto scheduled = impl_->PopFrame();
    if (!scheduled.ok()) {
      impl_->terminal_error_ = scheduled.status();
      impl_->FailQueued(*impl_->terminal_error_);
      impl_->FailPendingTransfers(*impl_->terminal_error_);
      impl_->driving_ = false;
      guard.Dismiss();
      co_return *impl_->terminal_error_;
    }
    const std::shared_ptr<Request>& request = scheduled->request_;
    impl_->active_request_ = request;
    std::function<void()> before_write;
    if (request->kind_ == Request::Kind::kMessage) {
      before_write = std::move(request->before_write_);
    }
    absl::Status written = co_await impl_->write_frame_(
        std::move(scheduled->message_), std::move(before_write));
    impl_->active_request_.reset();
    if (!written.ok()) {
      impl_->terminal_error_ = written;
      impl_->Complete(request, written);
      impl_->FailQueued(written);
      impl_->FailPendingTransfers(written);
      impl_->driving_ = false;
      guard.Dismiss();
      co_return written;
    }

    if (request->kind_ == Request::Kind::kMessage) {
      impl_->Complete(request, absl::OkStatus());
      continue;
    }
    if (request->transfer_phase_ == Request::TransferPhase::kEnd) {
      impl_->Complete(request, absl::OkStatus());
      if (absl::Status promoted = impl_->PromoteNextTransfer();
          !promoted.ok()) {
        impl_->terminal_error_ = promoted;
        impl_->FailQueued(promoted);
        impl_->FailPendingTransfers(promoted);
        impl_->driving_ = false;
        guard.Dismiss();
        co_return promoted;
      }
      continue;
    }
    if (absl::Status queued = impl_->QueueNextTransfer(request); !queued.ok()) {
      impl_->terminal_error_ = queued;
      impl_->Complete(request, queued);
      impl_->FailQueued(queued);
      impl_->FailPendingTransfers(queued);
      impl_->driving_ = false;
      guard.Dismiss();
      co_return queued;
    }
  }
  impl_->driving_ = false;
  guard.Dismiss();
  co_return absl::OkStatus();
}

std::size_t ControlSessionWriter::outstanding_bytes() const noexcept {
  return impl_->outstanding_bytes_;
}

bool ControlSessionWriter::failed() const noexcept {
  return impl_->terminal_error_.has_value();
}

}  // namespace keylane::cluster::control
