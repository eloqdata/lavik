/* Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 */
#include "replication_internal.h"

namespace lavik {
namespace detail {
void NativeSessionObservation::BeginFull(std::uint64_t now,
                                         ReplicationSide side, std::uint64_t id,
                                         std::string_view history) {
  std::uint64_t unset = 0;
  if (full_started_.compare_exchange_strong(unset, now,
                                            std::memory_order_release))
    spdlog::info("native FULL started side={} session={} history={}",
                 ReplicationSideName(side), id, history);
}

void NativeFlowObservation::Fail(const absl::Status& status,
                                 ReplicationFailure reason) {
  if (failure_.ok() && !status.ok()) {
    failure_ = status;
    failure_phase_ = phase_;
    failure_at_ = SteadyNanos();
    failure_reason_ = reason == ReplicationFailure::kNone
                          ? ClassifyReplicationFailure(status)
                          : reason;
  }
}

void NativeFlowObservation::Phase(FullSyncPhase phase, std::uint64_t now) {
  if (phase_ == phase && phase_started_ != 0) return;
  phase_ = phase;
  phase_started_ = now;
  spdlog::info("native flow side={} session={} history={} flow={} phase={}",
               ReplicationSideName(side_), session_id_, history_id_, flow_id_,
               FullSyncPhaseName(phase));
}

ReplicationFailure ClassifyReplicationFailure(const absl::Status& status) {
  switch (status.code()) {
    case absl::StatusCode::kOk:
      return ReplicationFailure::kNone;
    case absl::StatusCode::kUnavailable:
      return ReplicationFailure::kNetwork;
    case absl::StatusCode::kDeadlineExceeded:
      return ReplicationFailure::kTimeout;
    case absl::StatusCode::kUnauthenticated:
    case absl::StatusCode::kPermissionDenied:
      return ReplicationFailure::kAuthentication;
    case absl::StatusCode::kInvalidArgument:
    case absl::StatusCode::kOutOfRange:
    case absl::StatusCode::kDataLoss:
      return ReplicationFailure::kProtocol;
    case absl::StatusCode::kResourceExhausted:
      return ReplicationFailure::kResource;
    case absl::StatusCode::kCancelled:
      return ReplicationFailure::kCancelled;
    default:
      return ReplicationFailure::kOther;
  }
}
}  // namespace detail

void ReplicationManager::ReplicationGroup::StartProgressMonitor() {
  AssertStateOwner();
  if (progress_monitor_started_ || replication_shutdown_requested_) return;
  progress_monitor_started_ = true;
  observation_totals_.totals_.resize(2 * storage_->worker_count());
  for (unsigned side = 0; side != 2; ++side) {
    for (unsigned worker = 0; worker != storage_->worker_count(); ++worker) {
      auto& total =
          observation_totals_.totals_[side * storage_->worker_count() + worker];
      total.side_ = static_cast<ReplicationSide>(side);
      total.worker_ = worker;
    }
  }
  bycorf::ThisWorker().self_->Spawn(MonitorProgress());
}

Task<absl::Status> ReplicationManager::ReplicationGroup::MonitorProgress() {
  while (!replication_shutdown_requested_) {
    (void)co_await ObserveProgress(true);
    if (replication_shutdown_requested_) break;
    auto timer = bycorf::CancellableSleepFor(*bycorf::ThisWorker().self_,
                                             std::chrono::seconds(1));
    progress_monitor_timer_ = timer.CancelHandle();
    auto slept = co_await timer;
    progress_monitor_timer_ = {};
    if (!slept.ok()) break;
  }
  progress_monitor_started_ = false;
  co_return absl::OkStatus();
}

Task<ReplicationObservabilitySnapshot>
ReplicationManager::ReplicationGroup::ObserveProgress(
    bool sample, const std::atomic<bool>* serve_stale) {
  if (bycorf::ThisWorker().id_ != 0)
    co_return co_await bycorf::SubmitTaskTo(0, [this, sample, serve_stale] {
      return ObserveProgress(sample, serve_stale);
    });
  AssertStateOwner();
  const auto epoch = role_epoch_.load(std::memory_order_acquire);
  const auto active = active_replica_session_;
  const auto history = history_id_;
  const auto sources = observation_sources_;
  const auto targets = observation_targets_;
  ReplicationObservabilitySnapshot view;
  view.available_ = true;
  const auto now = SteadyNanos();

  // Only the sampler mutates clocks/totals; scrapes obtain fresh owner-local
  // data while using those sampled ages. No task keeps an owner reference
  // without retaining the corresponding session and flow allocation.
  auto append = [&](detail::NativeSessionObservation& observation,
                    ReplicationSide side, std::uint64_t session_id,
                    const std::string& session_history, const std::string& peer,
                    unsigned flow, const detail::NativeFlowObservation& local,
                    bool current, std::optional<std::uint64_t> published,
                    std::optional<std::uint64_t> applied, std::uint64_t sent,
                    std::uint64_t ack) {
    const unsigned owner = flow % storage_->worker_count();
    if (sample && observation.samples_.size() <= flow)
      observation.samples_.resize(flow + 1);
    detail::NativeFlowSample fallback;
    auto& previous = flow < observation.samples_.size()
                         ? observation.samples_[flow]
                         : fallback;
    if (sample && !observation_totals_.totals_.empty()) {
      auto& total =
          observation_totals_
              .totals_[static_cast<unsigned>(side) * storage_->worker_count() +
                       owner];
      total.bytes_ += local.bytes_ - previous.bytes_;
      total.events_ += local.events_ - previous.events_;
      total.full_items_ += local.full_items_ - previous.full_items_;
      total.partitions_ += local.partitions_ - previous.partitions_;
      previous.bytes_ = local.bytes_;
      previous.events_ = local.events_;
      previous.full_items_ = local.full_items_;
      previous.partitions_ = local.partitions_;
      if (side == ReplicationSide::kTarget && local.online_ && applied) {
        const auto before =
            previous.applied_ ? previous.applied_ : local.initial_lsn_;
        if (before && *applied >= before)
          total.applied_events_ += *applied - before;
        previous.applied_ = *applied;
      }
    }
    const auto started =
        observation.full_started_.load(std::memory_order_acquire);
    const bool full = started != 0 && observation.full_succeeded_.load(
                                          std::memory_order_acquire) == 0;
    ReplicationFlowObservation row;
    row.side_ = side;
    row.worker_ = owner;
    row.flow_ = flow;
    row.session_id_ = session_id;
    row.history_id_ = session_history;
    row.peer_id_ = peer;
    row.connected_ = current && local.connected_;
    row.phase_ = local.phase_;
    row.full_ = full;
    row.phase_seconds_ = local.phase_started_ && now >= local.phase_started_
                             ? (now - local.phase_started_) / 1e9
                             : 0;
    row.full_seconds_ = full && now >= started ? (now - started) / 1e9 : 0;
    row.bytes_ = local.bytes_;
    row.events_ = local.events_;
    row.full_items_ = local.full_items_;
    row.partitions_ = local.partitions_;
    // A fresh ONLINE flow may not have sent an event yet. Its accepted ACK
    // cursor is already the lower bound of sent work.
    sent = std::max(sent, ack);
    if (row.connected_ && local.online_) {
      if (side == ReplicationSide::kSource && published && ack != 0 &&
          ack <= *published && sent <= *published) {
        row.comparable_ = true;
        row.published_next_lsn_ = published;
        row.ack_next_lsn_ = ack;
        row.pending_events_ = *published - ack;
        row.pending_[0] = *published > sent;
        row.pending_[2] = sent > ack;
      } else if (side == ReplicationSide::kTarget && applied &&
                 local.received_ >= *applied && *applied >= local.ack_sent_) {
        row.comparable_ = true;
        row.received_next_lsn_ = local.received_;
        row.applied_next_lsn_ = applied;
        // Zero is the local "no successful ACK write yet" sentinel, not
        // an observed next-LSN. Pending ACK work is still known below.
        if (local.ack_sent_ != 0) row.ack_next_lsn_ = local.ack_sent_;
        row.pending_events_ = local.received_ - *applied;
        row.pending_[1] = local.received_ > *applied;
        row.pending_[2] = *applied > local.ack_sent_;
      }
    }
    if (full && row.phase_ == FullSyncPhase::kOnline)
      row.phase_ = FullSyncPhase::kActivate;
    row.pending_[3] = row.connected_ && full;
    const std::array<std::uint64_t, 4> progress{
        local.bytes_, applied.value_or(0),
        side == ReplicationSide::kSource ? ack : local.ack_sent_,
        local.bytes_ + local.partitions_ + static_cast<unsigned>(local.phase_)};
    for (unsigned step = 0; step != progress.size(); ++step) {
      if (sample)
        previous.clocks_[step].Sample(row.pending_[step], progress[step], now);
      row.no_progress_seconds_[step] =
          previous.clocks_[step].Age(row.pending_[step], now);
    }
    if (current) view.flows_.push_back(std::move(row));
  };

  auto finish = [&](detail::NativeSessionObservation& observation,
                    ReplicationSide side, std::uint64_t id,
                    const std::string& session_history, const std::string& peer,
                    bool ended,
                    const std::vector<detail::NativeFlowObservation>& flows,
                    bool expected_cancel) {
    if (!sample || observation.result_recorded_) return;
    const auto started =
        observation.full_started_.load(std::memory_order_acquire);
    const auto succeeded =
        observation.full_succeeded_.load(std::memory_order_acquire);
    if (!started || (!ended && !succeeded)) return;
    FullSyncObservationResult result;
    result.side_ = side;
    result.session_id_ = id;
    result.history_id_ = session_history;
    result.peer_id_ = peer;
    if (side == ReplicationSide::kTarget)
      result.promotion_complete_ =
          observation.promotion_complete_.load(std::memory_order_acquire);
    result.seconds_ =
        (std::max(started, succeeded ? succeeded : now) - started) / 1e9;
    result.result_ = succeeded         ? FullSyncResult::kSuccess
                     : expected_cancel ? FullSyncResult::kCancelled
                                       : FullSyncResult::kFailure;
    absl::Status failure =
        ended ? observation.control_failure_ : absl::OkStatus();
    const detail::NativeFlowObservation* first_failure = nullptr;
    for (const auto& flow : flows) {
      if (flow.phase_ > result.phase_) result.phase_ = flow.phase_;
      if (!flow.failure_.ok() && !absl::IsCancelled(flow.failure_) &&
          (!first_failure || flow.failure_at_ < first_failure->failure_at_))
        first_failure = &flow;
    }
    if (first_failure) {
      failure = first_failure->failure_;
      result.phase_ = first_failure->failure_phase_;
    }
    if (!succeeded) {
      if (expected_cancel)
        failure = absl::CancelledError("replication relationship retired");
      else if (failure.ok())
        failure = absl::UnavailableError(
            "replication connection closed before FULL completion");
      result.reason_ = first_failure && !expected_cancel
                           ? first_failure->failure_reason_
                           : detail::ClassifyReplicationFailure(failure);
      if (result.phase_ == FullSyncPhase::kPromote &&
          result.reason_ == ReplicationFailure::kOther)
        result.reason_ = ReplicationFailure::kStorage;
      result.detail_ = std::string(failure.message().substr(0, 512));
    }
    auto& totals =
        observation_totals_.full_totals_[static_cast<unsigned>(side)];
    ++totals.results_[static_cast<unsigned>(result.result_)];
    if (result.result_ == FullSyncResult::kFailure)
      ++totals.errors_[static_cast<unsigned>(result.reason_)];
    const auto bucket =
        std::lower_bound(kFullSyncDurationBounds.begin(),
                         kFullSyncDurationBounds.end(), result.seconds_);
    ++totals.duration_bins_[bucket - kFullSyncDurationBounds.begin()];
    totals.duration_sum_ += result.seconds_;
    observation.result_recorded_ = true;
    const auto finished = succeeded ? succeeded : now;
    if (finished >= last_full_finished_at_) {
      observation_totals_.last_full_ = result;
      last_full_finished_at_ = finished;
    }
    spdlog::info(
        "native FULL side={} session={} history={} result={} phase={} "
        "seconds={} reason={} detail={}",
        ReplicationSideName(side), id, session_history,
        FullSyncResultName(result.result_), FullSyncPhaseName(result.phase_),
        result.seconds_, ReplicationFailureName(result.reason_),
        result.detail_);
  };

  for (const auto& session : sources) {
    auto& observation = session->observation_;
    const auto flows = observation.flows_;
    std::vector<detail::NativeFlowObservation> copies;
    if (!flows) continue;
    copies.reserve(flows->size());
    for (unsigned flow = 0; flow != flows->size(); ++flow) {
      struct Cut {
        detail::NativeFlowObservation flow_;
        std::uint64_t published_ = 0, sent_ = 0, ack_ = 0;
        bool active_ = false;
      };
      const auto cut =
          co_await bycorf::SubmitTo(flow, [this, session, flows, flow] {
            const auto log = storage_->LocalReplicationLogInfo();
            const auto progress = session->Progress(flow);
            const auto sent = session->HighestSentNextLsn(flow);
            return Cut{(*flows)[flow], log.tail_lsn_ + 1, sent,
                       progress ? progress->lsn_ : 0,
                       log.state_ == storage::ReplicationLogState::kActive};
          });
      copies.push_back(cut.flow_);
      const bool current = !session->cancelled() &&
                           session->source_history_id_ == history_id_ &&
                           !replication_shutdown_requested_;
      append(observation, ReplicationSide::kSource, session->id_,
             session->source_history_id_, session->node_id_, flow, cut.flow_,
             current,
             cut.active_ ? std::optional(cut.published_) : std::nullopt,
             std::nullopt, cut.sent_, cut.ack_);
    }
    const bool ended =
        !session->control_active() && session->connected_flows() == 0;
    finish(observation, ReplicationSide::kSource, session->id_,
           session->source_history_id_, session->node_id_, ended, copies,
           replication_shutdown_requested_ ||
               observation.expected_cancel_.load(std::memory_order_acquire));
  }
  for (const auto& session : targets) {
    auto& observation = session->observation_;
    const auto flows = observation.flows_;
    std::vector<detail::NativeFlowObservation> copies;
    if (flows) {
      copies.reserve(flows->size());
      for (unsigned flow = 0; flow != flows->size(); ++flow) {
        copies.push_back(co_await bycorf::SubmitTo(
            flow % storage_->worker_count(),
            [flows, flow] { return (*flows)[flow]; }));
      }
    }
    const auto frontier = session->applied_frontier_;
    auto applied = frontier ? frontier->TrySnapshot()
                            : absl::StatusOr<std::vector<std::uint64_t>>(
                                  absl::UnavailableError("no native frontier"));
    const bool current = active_replica_session_ == session &&
                         !session->cancelled() && flows == observation.flows_;
    for (unsigned flow = 0; flow != copies.size(); ++flow) {
      const auto cursor = applied.ok() && flow < applied->size()
                              ? std::optional((*applied)[flow])
                              : std::nullopt;
      append(observation, ReplicationSide::kTarget, session->session_id_,
             session->source_history_id_, observation.peer_id_, flow,
             copies[flow], current, std::nullopt, cursor, 0, 0);
    }
    const bool ended =
        observation.control_done_ &&
        session->active_flows_.load(std::memory_order_acquire) == 0 &&
        session->active_transaction_applies_.load(std::memory_order_acquire) ==
            0;
    finish(observation, ReplicationSide::kTarget, session->session_id_,
           session->source_history_id_, observation.peer_id_, ended, copies,
           observation.expected_cancel_.load(std::memory_order_acquire));
  }
  if (sample) {
    std::erase_if(observation_sources_, [](const auto& session) {
      return !session->control_active() && session->connected_flows() == 0;
    });
    std::erase_if(observation_targets_, [](const auto& session) {
      return session->observation_.control_done_ &&
             session->active_flows_.load(std::memory_order_acquire) == 0 &&
             session->active_transaction_applies_.load(
                 std::memory_order_acquire) == 0;
    });
  }
  view.following_ =
      upstream_.has_value() && !redis_psync_.load(std::memory_order_acquire);
  view.connected_ = view.following_ && role_.load(std::memory_order_acquire) ==
                                           ReplicationRole::kOnline;
  view.population_ready_ =
      native_dataset_valid_.load(std::memory_order_acquire) ||
      (!meta_managed_ && !upstream_.has_value() &&
       !storage_->ReplicaRecoveryFenced());
  view.catalog_ready_ =
      view.population_ready_ || storage_->ReplicaCatalogReady();
  view.failed_stopped_ = failed_stopped_.load(std::memory_order_acquire);
  view.loading_ =
      (view.following_ && !view.population_ready_) || view.failed_stopped_;
  view.waiting_ = view.following_ && !view.connected_ &&
                  (last_retry_reason_ == ReplicationFailure::kFullBusy ||
                   last_retry_reason_ == ReplicationFailure::kAuthority);
  view.wait_reason_ =
      view.waiting_ ? last_retry_reason_ : ReplicationFailure::kNone;
  if (active &&
      active->observation_.full_started_.load(std::memory_order_acquire) &&
      !active->observation_.full_succeeded_.load(std::memory_order_acquire))
    view.full_promotion_complete_ =
        active->observation_.promotion_complete_.load(
            std::memory_order_acquire);
  view.failure_detail_ = failure_reason_;
  if (serve_stale != nullptr)
    view.readable_ =
        dataset_read_state(*serve_stale) == DatasetReadState::kReadable;
  // Process totals never decrease when a failed attempt is replaced. A retry
  // or fresh connection alone must not reset the population recovery clock.
  std::uint64_t recovery_progress = 0;
  for (const auto& total : observation_totals_.totals_)
    if (total.side_ == ReplicationSide::kTarget)
      recovery_progress += total.bytes_ + total.events_ + total.partitions_;
  if (sample)
    loading_progress_clock_.Sample(view.loading_ && !view.waiting_,
                                   recovery_progress, now);
  view.loading_no_progress_seconds_ =
      loading_progress_clock_.Age(view.loading_ && !view.waiting_, now);
  if (epoch != role_epoch_.load(std::memory_order_acquire) ||
      history != history_id_ || active != active_replica_session_ ||
      (active && active->cancelled())) {
    view.available_ = false;
    view.flows_.clear();
  }
  // Source control/flow cancellation need not change the group role epoch.
  // A peer retired during an asynchronous scrape must disappear immediately.
  std::erase_if(view.flows_, [&](const auto& row) {
    return row.side_ == ReplicationSide::kSource &&
           std::any_of(
               sources.begin(), sources.end(), [&](const auto& session) {
                 return session->id_ == row.session_id_ && session->cancelled();
               });
  });
  view.totals_ = observation_totals_.totals_;
  view.full_totals_ = observation_totals_.full_totals_;
  view.retries_ = observation_totals_.retries_;
  view.last_full_ = observation_totals_.last_full_;
  co_return view;
}
}  // namespace lavik
