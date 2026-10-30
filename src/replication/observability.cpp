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
    bool sample, const std::atomic<bool>* serve_stale,
    ReplicationObservationDetail detail) {
  if (bycorf::ThisWorker().id_ != 0)
    co_return co_await bycorf::SubmitTaskTo(0, [this, sample, serve_stale, detail] {
      return ObserveProgress(sample, serve_stale, detail);
    });
  AssertStateOwner();
  const auto epoch = role_epoch_.load(std::memory_order_acquire);
  const auto active = active_replica_session_;
  const auto history = history_id_;
  ReplicationObservabilitySnapshot view;
  view.available_ = true;
  const auto now = SteadyNanos();
  const bool metrics = !sample && detail == ReplicationObservationDetail::kMetrics;
  std::size_t source_metric_rows = 0;

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
    const bool connected = current && local.connected_;
    bool comparable = false;
    std::array<bool, 4> pending{};
    // A fresh ONLINE flow may not have sent an event yet. Its accepted ACK
    // cursor is already the lower bound of sent work.
    sent = std::max(sent, ack);
    if (connected && local.online_) {
      if (side == ReplicationSide::kSource && published && ack != 0 &&
          ack <= *published && sent <= *published) {
        comparable = true;
        pending[0] = *published > sent;
        pending[2] = sent > ack;
      } else if (side == ReplicationSide::kTarget && applied &&
                 local.received_ >= *applied && *applied >= local.ack_sent_) {
        comparable = true;
        pending[1] = local.received_ > *applied;
        pending[2] = *applied > local.ack_sent_;
      }
    }
    pending[3] = connected && full;
    const std::array<std::uint64_t, 4> progress{
        local.bytes_, applied.value_or(0),
        side == ReplicationSide::kSource ? ack : local.ack_sent_,
        local.bytes_ + local.partitions_ + static_cast<unsigned>(local.phase_)};
    if (sample) {
      for (unsigned step = 0; step != progress.size(); ++step)
        previous.clocks_[step].Sample(pending[step], progress[step], now);
      return;
    }
    if (!current) return;
    // Metrics merge each numeric row immediately instead of retaining an
    // INFO row (and two allocated identity strings) for every downstream peer.
    ReplicationFlowObservation metric_row;
    auto& row = metrics ? metric_row : view.flows_.emplace_back();
    row.side_ = side;
    row.worker_ = owner;
    row.flow_ = flow;
    if (!metrics) {
      row.session_id_ = session_id;
      row.history_id_ = session_history;
      row.peer_id_ = peer;
    }
    row.connected_ = connected;
    row.comparable_ = comparable;
    row.pending_ = pending;
    row.phase_ = full && local.phase_ == FullSyncPhase::kOnline
                     ? FullSyncPhase::kActivate
                     : local.phase_;
    row.full_ = full;
    row.phase_seconds_ = local.phase_started_ && now >= local.phase_started_
                             ? (now - local.phase_started_) / 1e9
                             : 0;
    row.full_seconds_ = full && now >= started ? (now - started) / 1e9 : 0;
    if (!metrics) {
      row.bytes_ = local.bytes_;
      row.events_ = local.events_;
      row.full_items_ = local.full_items_;
      row.partitions_ = local.partitions_;
    }
    if (comparable) {
      if (side == ReplicationSide::kSource) {
        row.published_next_lsn_ = published;
        row.ack_next_lsn_ = ack;
        row.pending_events_ = *published - ack;
      } else {
        row.received_next_lsn_ = local.received_;
        row.applied_next_lsn_ = applied;
        // Zero means no successful ACK write, not an observed next-LSN.
        if (local.ack_sent_ != 0) row.ack_next_lsn_ = local.ack_sent_;
        row.pending_events_ = local.received_ - *applied;
      }
    }
    for (unsigned step = 0; step != progress.size(); ++step)
      row.no_progress_seconds_[step] =
          previous.clocks_[step].Age(pending[step], now);
    if (metrics) {
      // Sources are processed before the single current target. Each session
      // has contiguous logical flow IDs; no map or per-peer output is needed.
      const auto index =
          (side == ReplicationSide::kSource ? 0 : source_metric_rows) + flow;
      if (view.metric_flows_.size() <= index)
        view.metric_flows_.resize(index + 1);
      auto& aggregate = view.metric_flows_[index];
      aggregate.side_ = side;
      aggregate.worker_ = owner;
      aggregate.flow_ = flow;
      aggregate.Add(row);
    }
  };

  auto finish = [&](detail::NativeSessionObservation& observation,
                    ReplicationSide side, std::uint64_t id,
                    const std::string& session_history, const std::string& peer,
                    bool ended, const auto& flows, bool expected_cancel) {
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
    for (const auto& cut : flows) {
      const auto& flow = cut.local_;
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

  struct FlowCut {
    detail::NativeFlowObservation local_;
    std::uint64_t sent_ = 0, ack_ = 0;
  };
  struct SourceCut {
    std::shared_ptr<MasterSession> session_;
    std::shared_ptr<std::vector<detail::NativeFlowObservation>> owners_;
    std::vector<FlowCut> flows_;
    bool drained_ = false;
  };
  struct TargetCut {
    std::shared_ptr<ReplicaSession> session_;
    std::shared_ptr<std::vector<detail::NativeFlowObservation>> owners_;
    std::vector<FlowCut> flows_;
    bool drained_ = false;
  };
  std::vector<SourceCut> source_cuts;
  std::vector<TargetCut> target_cuts;
  source_cuts.reserve(observation_sources_.size());
  target_cuts.reserve(observation_targets_.size());
  std::size_t flow_count = 0;
  std::size_t source_flow_count = 0, target_flow_count = 0;
  unsigned owner_count = 0;
  for (const auto& session : observation_sources_) {
    // Only sampling needs retired sessions' final deltas/results. Scrapes and
    // INFO still revalidate selected relationships after collection below.
    if (!sample && (session->cancelled() ||
                    session->source_history_id_ != history ||
                    replication_shutdown_requested_))
      continue;
    const auto owners = session->observation_.flows_;
    if (!owners) continue;
    flow_count += owners->size();
    source_flow_count = std::max(source_flow_count, owners->size());
    owner_count = std::max(owner_count, static_cast<unsigned>(owners->size()));
    source_cuts.push_back(
        {session, owners, std::vector<FlowCut>(owners->size()),
         !session->control_active() && session->connected_flows() == 0});
  }
  for (const auto& session : observation_targets_) {
    if (!sample && (session != active || session->cancelled())) continue;
    const auto owners = session->observation_.flows_;
    const auto count = owners ? owners->size() : 0;
    flow_count += count;
    target_flow_count = std::max(target_flow_count, count);
    owner_count = std::max(owner_count, static_cast<unsigned>(count));
    target_cuts.push_back(
        {session, owners, std::vector<FlowCut>(count),
         session->observation_.control_done_ &&
             session->active_flows_.load(std::memory_order_acquire) == 0 &&
             session->active_transaction_applies_.load(
                 std::memory_order_acquire) == 0});
  }
  if (metrics)
    view.metric_flows_.reserve(source_flow_count + target_flow_count);
  else if (!sample)
    view.flows_.reserve(flow_count);
  const unsigned workers = storage_->worker_count();
  owner_count = std::min(owner_count, workers);
  std::vector<std::optional<std::uint64_t>> published(
      source_cuts.empty() ? 0 : owner_count);
  if (flow_count != 0) {
    // Retain each session and its exact flow allocation until all submissions
    // return. Containers are sized before yielding; callbacks write only the
    // entries owned by that worker. One submission collects every peer and
    // every target flow on that owner, including mismatched worker layouts.
    for (unsigned worker = 0; worker != owner_count; ++worker) {
      (void)co_await bycorf::SubmitTo(worker, [&, worker] {
        if (!source_cuts.empty()) {
          const auto log = storage_->LocalReplicationLogInfo();
          if (log.state_ == storage::ReplicationLogState::kActive)
            published[worker] = log.tail_lsn_ + 1;
        }
        for (auto& source : source_cuts) {
          if (worker >= source.flows_.size()) continue;
          auto& cut = source.flows_[worker];
          cut.local_ = (*source.owners_)[worker];
          cut.sent_ = source.session_->HighestSentNextLsn(worker);
          const auto progress = source.session_->Progress(worker);
          cut.ack_ = progress ? progress->lsn_ : 0;
        }
        for (auto& target : target_cuts)
          for (unsigned flow = worker; flow < target.flows_.size();
               flow += workers)
            target.flows_[flow].local_ = (*target.owners_)[flow];
        return true;
      });
    }
  }

  // All asynchronous collection precedes validation and rendering. Worker zero
  // cannot replace a relationship while these final checks/rows run, so each
  // source is validated once instead of searching all peers again for each row.
  for (const auto& cut : source_cuts) {
    const auto& session = cut.session_;
    auto& observation = session->observation_;
    const bool current = !session->cancelled() &&
                         session->source_history_id_ == history_id_ &&
                         !replication_shutdown_requested_;
    for (unsigned flow = 0; flow != cut.flows_.size(); ++flow) {
      const auto& local = cut.flows_[flow];
      append(observation, ReplicationSide::kSource, session->id_,
             session->source_history_id_, session->node_id_, flow, local.local_,
             current, published[flow], std::nullopt, local.sent_, local.ack_);
    }
    finish(observation, ReplicationSide::kSource, session->id_,
           session->source_history_id_, session->node_id_, cut.drained_,
           cut.flows_,
           replication_shutdown_requested_ ||
               observation.expected_cancel_.load(std::memory_order_acquire));
    if (sample) observation.drained_sampled_ = cut.drained_;
  }
  source_metric_rows = view.metric_flows_.size();
  for (const auto& cut : target_cuts) {
    const auto& session = cut.session_;
    auto& observation = session->observation_;
    const auto frontier = session->applied_frontier_;
    auto applied = frontier ? frontier->TrySnapshot()
                            : absl::StatusOr<std::vector<std::uint64_t>>(
                                  absl::UnavailableError("no native frontier"));
    const bool current = active_replica_session_ == session &&
                         !session->cancelled() &&
                         cut.owners_ == observation.flows_;
    for (unsigned flow = 0; flow != cut.flows_.size(); ++flow) {
      const auto cursor = applied.ok() && flow < applied->size()
                              ? std::optional((*applied)[flow])
                              : std::nullopt;
      append(observation, ReplicationSide::kTarget, session->session_id_,
             session->source_history_id_, observation.peer_id_, flow,
             cut.flows_[flow].local_, current, std::nullopt, cursor, 0, 0);
    }
    finish(observation, ReplicationSide::kTarget, session->session_id_,
           session->source_history_id_, observation.peer_id_, cut.drained_,
           cut.flows_,
           observation.expected_cancel_.load(std::memory_order_acquire));
    if (sample) observation.drained_sampled_ = cut.drained_;
  }
  if (sample) {
    std::erase_if(observation_sources_, [](const auto& session) {
      return session->observation_.drained_sampled_;
    });
    std::erase_if(observation_targets_, [](const auto& session) {
      return session->observation_.drained_sampled_;
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
  if (!sample && !metrics) view.failure_detail_ = failure_reason_;
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
  // Sampling maintains clocks and terminal results, but has no snapshot
  // consumer.
  if (sample) co_return view;
  view.loading_no_progress_seconds_ =
      loading_progress_clock_.Age(view.loading_ && !view.waiting_, now);
  if (epoch != role_epoch_.load(std::memory_order_acquire) ||
      history != history_id_ || active != active_replica_session_ ||
      (active && active->cancelled())) {
    view.available_ = false;
    view.flows_.clear();
    view.metric_flows_.clear();
  }
  if (metrics) {
    // Counters are monotonic across relationships. Copy only rows that have
    // ever done work; the exporter reconstructs the omitted zero series.
    view.worker_count_ = workers;
    const auto& totals = observation_totals_.totals_;
    const auto count = std::count_if(totals.begin(), totals.end(),
                                    [](const auto& total) { return !total.empty(); });
    view.totals_.reserve(count);
    for (const auto& total : totals)
      if (!total.empty()) view.totals_.push_back(total);
  } else {
    view.totals_ = observation_totals_.totals_;
    view.last_full_ = observation_totals_.last_full_;
  }
  view.full_totals_ = observation_totals_.full_totals_;
  view.retries_ = observation_totals_.retries_;
  co_return view;
}
}  // namespace lavik
