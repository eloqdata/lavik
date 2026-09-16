#include "keylane/cluster/control_transport.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "celer/runtime/runtime.h"
#include "celer/runtime/worker.h"
#include "gtest/gtest.h"

namespace keylane::cluster::control {

class ControlDeadlineWatchdogTestPeer {
 public:
  static std::uint64_t TaskStarts(
      const ControlDeadlineWatchdog& watchdog) noexcept {
    return watchdog.TaskStartsForTest();
  }
};

}  // namespace keylane::cluster::control

namespace {

namespace control = keylane::cluster::control;
using namespace std::chrono_literals;

control::WireMessage Hello(char node_id_digit) {
  return control::ClientHello{
      .node_id = std::string(40, node_id_digit),
      .boot_id = std::string(40, 'b'),
      .replication_history_id = std::string(40, 'c'),
      .replication_flow_count = 3,
  };
}

control::WireMessage OversizedDirective() {
  return control::Directive{
      .recipient_node_id = std::string(40, '1'),
      .recipient_boot_id = std::string(40, '2'),
      .target_node_id = std::string(40, '1'),
      .target_boot_id = std::string(40, '2'),
      .source_node_id = std::string(40, '3'),
      .source_assignment_id = control::WireId128{7},
      .source_boot_id = std::string(40, '4'),
      .source_replication_history_id = std::string(40, '5'),
      .kind = control::WireDirectiveKind::kRebuild,
      .payload = std::string(control::kMaxFramePayloadBytes, 'x'),
  };
}

absl::StatusOr<std::string> FullStatePayload(std::size_t padding_bytes) {
  control::FullDesiredState state;
  state.source_meta_applied_index = 1;
  state.authority_lease_duration_ms = 3000;
  if (padding_bytes != 0) {
    control::WireManifestDocument manifest;
    manifest.revision = 1;
    const std::size_t entry_count = std::min<std::size_t>(
        padding_bytes / 10 + 1, control::kMaxManifestEntries);
    manifest.entries.reserve(entry_count);
    for (std::size_t i = 0; i < entry_count; ++i) {
      manifest.entries.push_back(
          {.partition_id = static_cast<std::uint16_t>(i), .logical_epoch = 1});
    }
    state.manifests.push_back(std::move(manifest));
  }
  auto directives =
      control::ComputeDirectiveSetDigest(state.current_directives);
  if (!directives.ok()) return directives.status();
  state.directive_set_digest = *directives;
  auto projection = control::ComputeProjectionHash(state);
  if (!projection.ok()) return projection.status();
  state.projection_hash = *projection;
  return control::EncodeFullDesiredState(state);
}

using WriterScenario = std::function<celer::Task<absl::Status>(celer::Worker&)>;

celer::Task<absl::Status> CompleteWriterScenario(
    std::shared_ptr<WriterScenario> scenario, celer::Worker* worker,
    std::shared_ptr<std::promise<absl::Status>> completed) {
  absl::Status status = co_await (*scenario)(*worker);
  completed->set_value(status);
  co_return absl::OkStatus();
}

absl::Status RunWriterScenario(WriterScenario scenario) {
  celer::Runtime runtime;
  auto initialized = std::make_shared<std::promise<absl::Status>>();
  std::future<absl::Status> init_result = initialized->get_future();
  auto completed = std::make_shared<std::promise<absl::Status>>();
  std::future<absl::Status> scenario_result = completed->get_future();
  auto owned_scenario = std::make_shared<WriterScenario>(std::move(scenario));
  runtime.Start(
      1,
      [initialized, completed, owned_scenario](unsigned,
                                               celer::Worker& worker) {
        const absl::Status status = worker.Init();
        initialized->set_value(status);
        if (!status.ok()) return 1;
        worker.Spawn(
            CompleteWriterScenario(owned_scenario, &worker, completed));
        worker.Run();
        worker.Shutdown();
        worker.DestroyDetachedTasks();
        return 0;
      },
      false);
  const absl::Status initialized_status = init_result.get();
  if (!initialized_status.ok()) {
    runtime.RequestStop();
    runtime.WaitUntilStopped();
    return initialized_status;
  }
  if (scenario_result.wait_for(10s) != std::future_status::ready) {
    runtime.RequestStop();
    runtime.WaitUntilStopped();
    return absl::DeadlineExceededError("writer scenario did not complete");
  }
  absl::Status result = scenario_result.get();
  runtime.RequestStop();
  runtime.WaitUntilStopped();
  if (runtime.exit_code() != 0 && result.ok()) {
    return absl::InternalError("writer scenario runtime failed");
  }
  return result;
}

struct DeadlineWatchdogScenario {
  celer::Task<absl::Status> Run(celer::Worker& worker) {
    {
      control::ControlDeadlineWatchdog watchdog(
          worker, [this] { ++expiration_callbacks_; });
      for (std::size_t i = 0; i < 1000; ++i) {
        if (absl::Status armed = watchdog.Arm(20ms); !armed.ok()) {
          co_return armed;
        }
        (void)watchdog.Disarm();
      }
      if (absl::Status armed = watchdog.Arm(20ms); !armed.ok()) {
        co_return armed;
      }
      starts_during_rearm_ =
          control::ControlDeadlineWatchdogTestPeer::TaskStarts(watchdog);
      if (absl::Status slept = co_await celer::SleepFor(worker, 50ms);
          !slept.ok()) {
        co_return slept;
      }
      callbacks_after_expiry_ = expiration_callbacks_;

      if (absl::Status armed = watchdog.Arm(20ms); !armed.ok()) {
        co_return armed;
      }
      (void)watchdog.Disarm();
      if (absl::Status slept = co_await celer::SleepFor(worker, 50ms);
          !slept.ok()) {
        co_return slept;
      }
      callbacks_after_disarm_ = expiration_callbacks_;
    }

    {
      control::ControlDeadlineWatchdog rearmed(
          worker, [this] { ++rearmed_expiration_callbacks_; });
      if (absl::Status armed = rearmed.Arm(20ms); !armed.ok()) {
        co_return armed;
      }
      // Let the manager install the first native timer before cancelling it.
      // This exercises the generation check on the in-flight cancellation,
      // not just repeated Arm calls made before Watch first runs.
      co_await celer::Yield(worker);
      if (absl::Status armed = rearmed.Arm(100ms); !armed.ok()) {
        co_return armed;
      }
      rearmed_starts_ =
          control::ControlDeadlineWatchdogTestPeer::TaskStarts(rearmed);
      if (absl::Status slept = co_await celer::SleepFor(worker, 50ms);
          !slept.ok()) {
        co_return slept;
      }
      rearmed_callbacks_after_old_deadline_ = rearmed_expiration_callbacks_;
      if (absl::Status slept = co_await celer::SleepFor(worker, 100ms);
          !slept.ok()) {
        co_return slept;
      }
      rearmed_callbacks_after_new_deadline_ = rearmed_expiration_callbacks_;
    }

    {
      control::ControlDeadlineWatchdog destroyed(
          worker, [this] { ++expiration_callbacks_; });
      if (absl::Status armed = destroyed.Arm(20ms); !armed.ok()) {
        co_return armed;
      }
      co_await celer::Yield(worker);
    }
    if (absl::Status slept = co_await celer::SleepFor(worker, 50ms);
        !slept.ok()) {
      co_return slept;
    }
    callbacks_after_destroy_ = expiration_callbacks_;
    co_return absl::OkStatus();
  }

  std::uint64_t starts_during_rearm_ = 0;
  std::size_t expiration_callbacks_ = 0;
  std::size_t callbacks_after_expiry_ = 0;
  std::size_t callbacks_after_disarm_ = 0;
  std::size_t callbacks_after_destroy_ = 0;
  std::uint64_t rearmed_starts_ = 0;
  std::size_t rearmed_expiration_callbacks_ = 0;
  std::size_t rearmed_callbacks_after_new_deadline_ = 0;
  std::size_t rearmed_callbacks_after_old_deadline_ = 0;
};

struct PriorityWriterScenario {
  explicit PriorityWriterScenario(std::size_t queue_bytes)
      : queue_bytes_(queue_bytes) {}

  celer::Task<absl::Status> SendAuthority() {
    authority_status_ = co_await writer_->Write(
        control::MessagePriority::kAuthority, Hello('d'));
    authority_done_ = true;
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> WriteFrame(control::WireMessage message,
                                       std::function<void()> before_write) {
    if (before_write) before_write();
    const control::MessageType type = control::MessageTypeOf(message);
    trace_.push_back(type);
    if (type == control::MessageType::kTransferChunk && !injected_) {
      injected_ = true;
      worker_->Spawn(SendAuthority());
      co_await celer::Yield(*worker_);
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> Run(celer::Worker& worker) {
    worker_ = &worker;
    control::ControlSessionWriter writer(
        [this](control::WireMessage message,
               std::function<void()> before_write) {
          return WriteFrame(std::move(message), std::move(before_write));
        },
        queue_bytes_);
    writer_ = &writer;
    auto bytes = std::make_shared<const std::string>(24u * 1024u, 'x');
    transfer_status_ = co_await writer.WriteTransfer(
        control::TransferKind::kObservationEvidence, control::WireId128{0x42},
        std::move(bytes));
    for (unsigned attempt = 0; !authority_done_ && attempt < 100; ++attempt) {
      co_await celer::Yield(worker);
    }
    writer_ = nullptr;
    if (!authority_done_) {
      co_return absl::DeadlineExceededError(
          "authority producer did not finish");
    }
    co_return absl::OkStatus();
  }

  const std::size_t queue_bytes_;
  celer::Worker* worker_ = nullptr;
  control::ControlSessionWriter* writer_ = nullptr;
  std::vector<control::MessageType> trace_;
  absl::Status transfer_status_ = absl::UnknownError("not run");
  absl::Status authority_status_ = absl::UnknownError("not run");
  bool injected_ = false;
  bool authority_done_ = false;
};

struct ErrorWriterScenario {
  celer::Task<absl::Status> SendQueued() {
    queued_ = co_await writer_->Write(control::MessagePriority::kAuthority,
                                      Hello('2'));
    queued_done_ = true;
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> WriteFrame(control::WireMessage,
                                       std::function<void()> before_write) {
    ++sink_calls_;
    if (before_write) before_write();
    if (!injected_) {
      injected_ = true;
      worker_->Spawn(SendQueued());
      co_await celer::Yield(*worker_);
    }
    co_return absl::UnavailableError("injected terminal write failure");
  }

  celer::Task<absl::Status> Run(celer::Worker& worker) {
    worker_ = &worker;
    control::ControlSessionWriter writer(
        [this](control::WireMessage message,
               std::function<void()> before_write) {
          return WriteFrame(std::move(message), std::move(before_write));
        },
        4 * control::kMaxFrameBytes);
    writer_ = &writer;
    first_ = co_await writer.Write(control::MessagePriority::kReliable,
                                   Hello('1'), [this] { ++before_calls_; });
    for (unsigned attempt = 0; !queued_done_ && attempt < 100; ++attempt) {
      co_await celer::Yield(worker);
    }
    if (!queued_done_) {
      co_return absl::DeadlineExceededError(
          "queued writer producer did not finish");
    }
    future_ =
        co_await writer.Write(control::MessagePriority::kReliable, Hello('3'));
    failed_ = writer.failed();
    outstanding_bytes_ = writer.outstanding_bytes();
    writer_ = nullptr;
    co_return absl::OkStatus();
  }

  celer::Worker* worker_ = nullptr;
  control::ControlSessionWriter* writer_ = nullptr;
  absl::Status first_ = absl::UnknownError("not run");
  absl::Status queued_ = absl::UnknownError("not run");
  absl::Status future_ = absl::UnknownError("not run");
  std::size_t sink_calls_ = 0;
  std::size_t before_calls_ = 0;
  std::size_t outstanding_bytes_ = 1;
  bool injected_ = false;
  bool queued_done_ = false;
  bool failed_ = false;
};

struct SerializedTransfersScenario {
  struct TraceEntry {
    control::MessageType type_;
    std::uint8_t object_tag_ = 0;

    friend bool operator==(const TraceEntry&, const TraceEntry&) = default;
  };

  celer::Task<absl::Status> SendSecond(
      std::shared_ptr<const std::string> payload) {
    second_status_ = co_await writer_->WriteTransfer(
        control::TransferKind::kObservationEvidence, control::WireId128{0x22},
        std::move(payload));
    second_done_ = true;
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> WriteFrame(control::WireMessage message,
                                       std::function<void()> before_write) {
    if (before_write) before_write();
    const control::MessageType type = control::MessageTypeOf(message);
    std::uint8_t object_tag = 0;
    if (const auto* start = std::get_if<control::TransferStart>(&message)) {
      object_tag = start->object_id[0];
    } else if (const auto* chunk =
                   std::get_if<control::TransferChunk>(&message)) {
      object_tag = chunk->object_id[0];
    } else if (const auto* end = std::get_if<control::TransferEnd>(&message)) {
      object_tag = end->object_id[0];
    }
    trace_.push_back(TraceEntry{type, object_tag});

    if (type == control::MessageType::kTransferStart && object_tag == 0x11 &&
        !injected_) {
      injected_ = true;
      auto second = std::make_shared<const std::string>("second");
      second_owner_ = second;
      worker_->Spawn(SendSecond(std::move(second)));
      co_await celer::Yield(*worker_);
      owner_retained_while_queued_ = !second_owner_.expired();
    }
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> Run(celer::Worker& worker) {
    worker_ = &worker;
    control::ControlSessionWriter writer(
        [this](control::WireMessage message,
               std::function<void()> before_write) {
          return WriteFrame(std::move(message), std::move(before_write));
        },
        4 * control::kMaxFrameBytes);
    writer_ = &writer;
    auto first = std::make_shared<const std::string>(24u * 1024u, 'x');
    first_status_ = co_await writer.WriteTransfer(
        control::TransferKind::kObservationEvidence, control::WireId128{0x11},
        std::move(first));
    for (unsigned attempt = 0; !second_done_ && attempt < 100; ++attempt) {
      co_await celer::Yield(worker);
    }
    writer_ = nullptr;
    if (!second_done_) {
      co_return absl::DeadlineExceededError(
          "second transfer producer did not finish");
    }
    co_return absl::OkStatus();
  }

  celer::Worker* worker_ = nullptr;
  control::ControlSessionWriter* writer_ = nullptr;
  std::vector<TraceEntry> trace_;
  std::weak_ptr<const std::string> second_owner_;
  absl::Status first_status_ = absl::UnknownError("not run");
  absl::Status second_status_ = absl::UnknownError("not run");
  bool injected_ = false;
  bool owner_retained_while_queued_ = false;
  bool second_done_ = false;
};

struct OversizedWriterScenario {
  celer::Task<absl::Status> WriteFrame(control::WireMessage,
                                       std::function<void()>) {
    ++sink_calls_;
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> Run(celer::Worker&) {
    control::ControlSessionWriter writer(
        [this](control::WireMessage message,
               std::function<void()> before_write) {
          return WriteFrame(std::move(message), std::move(before_write));
        },
        4 * control::kMaxFrameBytes);
    status_ = co_await writer.Write(control::MessagePriority::kReliable,
                                    OversizedDirective());
    failed_ = writer.failed();
    co_return absl::OkStatus();
  }

  absl::Status status_ = absl::UnknownError("not run");
  std::size_t sink_calls_ = 0;
  bool failed_ = true;
};

struct FullStateWriterScenario {
  celer::Task<absl::Status> WriteFrame(control::WireMessage message,
                                       std::function<void()> before_write) {
    if (before_write) before_write();
    frames_.push_back(std::move(message));
    co_return absl::OkStatus();
  }

  celer::Task<absl::Status> Run(celer::Worker&) {
    auto small = FullStatePayload(0);
    if (!small.ok()) co_return small.status();
    auto large = FullStatePayload(control::kMaxFramePayloadBytes);
    if (!large.ok()) co_return large.status();
    if (small->size() > control::kMaxFramePayloadBytes ||
        large->size() <= control::kMaxFramePayloadBytes) {
      co_return absl::InternalError(
          "test fixtures do not straddle the frame payload limit");
    }
    small_bytes_ = *small;
    large_bytes_ = *large;

    control::ControlSessionWriter writer(
        [this](control::WireMessage message,
               std::function<void()> before_write) {
          return WriteFrame(std::move(message), std::move(before_write));
        },
        4 * control::kMaxFrameBytes);
    small_status_ = co_await writer.WriteFullDesiredState(
        std::make_shared<const std::string>(std::move(*small)));
    large_status_ = co_await writer.WriteFullDesiredState(
        std::make_shared<const std::string>(std::move(*large)));
    co_return absl::OkStatus();
  }

  std::vector<control::WireMessage> frames_;
  std::string small_bytes_;
  std::string large_bytes_;
  absl::Status small_status_ = absl::UnknownError("not run");
  absl::Status large_status_ = absl::UnknownError("not run");
};

TEST(ControlWriteQueueTest, AuthorityOvertakesEarlierBulkAndSoftWork) {
  control::ControlWriteQueue queue(4096);
  ASSERT_TRUE(queue.Enqueue(control::MessagePriority::kSoft, Hello('1')).ok());
  ASSERT_TRUE(queue.Enqueue(control::MessagePriority::kBulk, Hello('2')).ok());
  ASSERT_TRUE(
      queue.Enqueue(control::MessagePriority::kAuthority, Hello('3')).ok());
  ASSERT_TRUE(
      queue.Enqueue(control::MessagePriority::kReliable, Hello('4')).ok());

  const auto authority = queue.Pop();
  const auto reliable = queue.Pop();
  const auto bulk = queue.Pop();
  const auto soft = queue.Pop();
  ASSERT_TRUE(authority.has_value());
  ASSERT_TRUE(reliable.has_value());
  ASSERT_TRUE(bulk.has_value());
  ASSERT_TRUE(soft.has_value());
  EXPECT_EQ(authority->first, control::MessagePriority::kAuthority);
  EXPECT_EQ(reliable->first, control::MessagePriority::kReliable);
  EXPECT_EQ(bulk->first, control::MessagePriority::kBulk);
  EXPECT_EQ(soft->first, control::MessagePriority::kSoft);
  EXPECT_TRUE(queue.empty());
}

TEST(ControlWriteQueueTest, RejectsOverflowWithoutDiscardingQueuedMessage) {
  control::WireMessage first = Hello('a');
  auto payload = control::EncodeMessage(first);
  ASSERT_TRUE(payload.ok()) << payload.status();
  control::ControlWriteQueue queue(control::kFrameHeaderBytes +
                                   payload->size());
  ASSERT_TRUE(
      queue.Enqueue(control::MessagePriority::kReliable, std::move(first))
          .ok());
  const std::size_t before = queue.queued_bytes();
  const absl::Status rejected =
      queue.Enqueue(control::MessagePriority::kAuthority,
                    control::ClientHello{
                        .node_id = std::string(40, 'd'),
                        .boot_id = std::string(40, 'e'),
                        .replication_history_id = std::string(40, 'f'),
                        .replication_flow_count = 3,
                    });
  EXPECT_EQ(rejected.code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(queue.queued_bytes(), before);
  ASSERT_TRUE(queue.Pop().has_value());
  EXPECT_TRUE(queue.empty());
}

TEST(ControlWriteQueueTest, RejectsMessageThatRequiresObjectTransfer) {
  control::ControlWriteQueue queue(2 * control::kMaxFrameBytes);
  const absl::Status rejected =
      queue.Enqueue(control::MessagePriority::kReliable, OversizedDirective());
  EXPECT_EQ(rejected.code(), absl::StatusCode::kResourceExhausted);
  EXPECT_TRUE(queue.empty());
}

TEST(ControlDeadlineWatchdogTest,
     RearmUsesOneLiveTaskAndDisarmOrDestructionSuppressesExpiry) {
  auto scenario = std::make_shared<DeadlineWatchdogScenario>();
  const absl::Status run = RunWriterScenario(
      [scenario](celer::Worker& worker) { return scenario->Run(worker); });
  ASSERT_TRUE(run.ok()) << run;
  EXPECT_EQ(scenario->starts_during_rearm_, 1u);
  EXPECT_EQ(scenario->callbacks_after_expiry_, 1u);
  EXPECT_EQ(scenario->callbacks_after_disarm_, 1u);
  EXPECT_EQ(scenario->callbacks_after_destroy_, 1u);
  EXPECT_EQ(scenario->rearmed_starts_, 1u);
  EXPECT_EQ(scenario->rearmed_callbacks_after_old_deadline_, 0u);
  EXPECT_EQ(scenario->rearmed_callbacks_after_new_deadline_, 1u);
}

TEST(ControlSessionWriterTest, AuthorityOvertakesTransferBetweenBulkChunks) {
  auto scenario =
      std::make_shared<PriorityWriterScenario>(4 * control::kMaxFrameBytes);
  const absl::Status run = RunWriterScenario(
      [scenario](celer::Worker& worker) { return scenario->Run(worker); });
  ASSERT_TRUE(run.ok()) << run;
  EXPECT_TRUE(scenario->transfer_status_.ok()) << scenario->transfer_status_;
  EXPECT_TRUE(scenario->authority_status_.ok()) << scenario->authority_status_;
  const std::vector expected{
      control::MessageType::kTransferStart,
      control::MessageType::kTransferChunk,
      control::MessageType::kClientHello,
      control::MessageType::kTransferChunk,
      control::MessageType::kTransferEnd,
  };
  EXPECT_EQ(scenario->trace_, expected);
}

TEST(ControlSessionWriterTest, TransferReservationMakesQueueBoundReal) {
  auto scenario =
      std::make_shared<PriorityWriterScenario>(control::kMaxFrameBytes);
  const absl::Status run = RunWriterScenario(
      [scenario](celer::Worker& worker) { return scenario->Run(worker); });
  ASSERT_TRUE(run.ok()) << run;
  EXPECT_TRUE(scenario->transfer_status_.ok()) << scenario->transfer_status_;
  EXPECT_EQ(scenario->authority_status_.code(),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(std::count(scenario->trace_.begin(), scenario->trace_.end(),
                       control::MessageType::kClientHello),
            0);
}

TEST(ControlSessionWriterTest,
     SerializesTransfersAndRetainsPendingPayloadOwner) {
  auto scenario = std::make_shared<SerializedTransfersScenario>();
  const absl::Status run = RunWriterScenario(
      [scenario](celer::Worker& worker) { return scenario->Run(worker); });
  ASSERT_TRUE(run.ok()) << run;
  EXPECT_TRUE(scenario->first_status_.ok()) << scenario->first_status_;
  EXPECT_TRUE(scenario->second_status_.ok()) << scenario->second_status_;
  EXPECT_TRUE(scenario->owner_retained_while_queued_);
  const std::vector<SerializedTransfersScenario::TraceEntry> expected{
      {control::MessageType::kTransferStart, 0x11},
      {control::MessageType::kTransferChunk, 0x11},
      {control::MessageType::kTransferChunk, 0x11},
      {control::MessageType::kTransferEnd, 0x11},
      {control::MessageType::kTransferStart, 0x22},
      {control::MessageType::kTransferChunk, 0x22},
      {control::MessageType::kTransferEnd, 0x22},
  };
  EXPECT_EQ(scenario->trace_, expected);
}

TEST(ControlSessionWriterTest, TerminalWriteErrorPoisonsQueuedAndFutureWork) {
  auto scenario = std::make_shared<ErrorWriterScenario>();
  const absl::Status run = RunWriterScenario(
      [scenario](celer::Worker& worker) { return scenario->Run(worker); });
  ASSERT_TRUE(run.ok()) << run;
  EXPECT_EQ(scenario->first_.code(), absl::StatusCode::kUnavailable);
  EXPECT_EQ(scenario->queued_.code(), absl::StatusCode::kUnavailable);
  EXPECT_EQ(scenario->future_.code(), absl::StatusCode::kUnavailable);
  EXPECT_EQ(scenario->first_.message(), scenario->queued_.message());
  EXPECT_EQ(scenario->first_.message(), scenario->future_.message());
  EXPECT_EQ(scenario->sink_calls_, 1u);
  EXPECT_EQ(scenario->before_calls_, 1u);
  EXPECT_TRUE(scenario->failed_);
  EXPECT_EQ(scenario->outstanding_bytes_, 0u);
}

TEST(ControlSessionWriterTest,
     OversizedSingleFrameFailsWithoutPoisoningWriter) {
  auto scenario = std::make_shared<OversizedWriterScenario>();
  const absl::Status run = RunWriterScenario(
      [scenario](celer::Worker& worker) { return scenario->Run(worker); });
  ASSERT_TRUE(run.ok()) << run;
  EXPECT_EQ(scenario->status_.code(), absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(scenario->sink_calls_, 0u);
  EXPECT_FALSE(scenario->failed_);
}

TEST(ControlSessionWriterTest,
     FullDesiredStateUsesOneFrameUntilItsPayloadRequiresStreaming) {
  auto scenario = std::make_shared<FullStateWriterScenario>();
  const absl::Status run = RunWriterScenario(
      [scenario](celer::Worker& worker) { return scenario->Run(worker); });
  ASSERT_TRUE(run.ok()) << run;
  ASSERT_TRUE(scenario->small_status_.ok()) << scenario->small_status_;
  ASSERT_TRUE(scenario->large_status_.ok()) << scenario->large_status_;

  ASSERT_GE(scenario->frames_.size(), 5u);
  const auto* direct =
      std::get_if<control::FullDesiredState>(&scenario->frames_.front());
  ASSERT_NE(direct, nullptr);
  EXPECT_EQ(direct->object_hash,
            control::ComputeSha256(scenario->small_bytes_));

  const auto* start =
      std::get_if<control::TransferStart>(&scenario->frames_[1]);
  ASSERT_NE(start, nullptr);
  EXPECT_EQ(start->kind, control::TransferKind::kFullDesiredState);
  EXPECT_EQ(start->total_length, scenario->large_bytes_.size());
  EXPECT_EQ(start->sha256, control::ComputeSha256(scenario->large_bytes_));
  EXPECT_TRUE(
      std::holds_alternative<control::TransferEnd>(scenario->frames_.back()));
  EXPECT_TRUE(std::all_of(
      scenario->frames_.begin() + 2, scenario->frames_.end() - 1,
      [](const control::WireMessage& frame) {
        return std::holds_alternative<control::TransferChunk>(frame);
      }));
}

}  // namespace
