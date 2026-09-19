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

#include "lavik/meta/membership_reconciler.h"

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <future>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "bycorf/io/storage.h"
#include "bycorf/runtime/worker.h"
#include "lavik/cluster/control_protocol.h"
#include "lavik/fault_injection.h"
#include "lavik/meta/identity_verifier.h"
#include "lavik/meta/proposal_executor.h"
#include "lavik/meta/raft.h"
#include "lavik/meta/state_machine.h"
#include "lavik/numeric_endpoint.h"
#include "spdlog/spdlog.h"

namespace lavik::meta {
namespace {
bool Terminal(const MetaOperationRecord& op) {
  return op.lifecycle_ == MetaOperationLifecycle::kCompleted ||
         op.lifecycle_ == MetaOperationLifecycle::kAborted;
}
std::string Id(const MetaOperationId& id) {
  return absl::BytesToHexString(
      std::string_view(reinterpret_cast<const char*>(id.data()), id.size()));
}
using Plan = absl::StatusOr<std::optional<MetaMembershipStep>>;
template <typename T>
Plan Command(T command) {
  return MetaMembershipStep(MetaCommand(std::move(command)));
}
Plan Phase(const MetaOperationRecord& op, std::string phase) {
  TransitionOperationPhase c;
  c.operation_id_ = op.operation_id_;
  c.expected_revision_ = op.revision_;
  c.kind_phase_blob_ = std::move(phase);
  return Command(std::move(c));
}
absl::Status Conflict(std::string_view reason) {
  return absl::FailedPreconditionError(std::string(reason));
}
void WritePeer(MetaWriter& w, const MetaMembershipPeer& p) {
  w.WriteU32(p.id_);
  w.WriteString(p.endpoint_);
  w.WriteString(p.principal_);
  w.WriteString(p.data_control_endpoint_);
  w.WriteString(p.ctl_endpoint_);
  w.WriteU32(std::bit_cast<std::uint32_t>(p.dc_id_));
  w.WriteU32(std::bit_cast<std::uint32_t>(p.priority_));
  w.WriteBool(p.learner_);
  w.WriteBool(p.new_joiner_);
}
absl::StatusOr<MetaMembershipPeer> ReadPeer(MetaReader& r) {
  auto id = r.ReadU32();
  auto endpoint = r.ReadString(kMaxMetaEndpointBytes);
  auto principal = r.ReadString(kMaxMetaPrincipalBytes);
  auto data_control = r.ReadString(kMaxMetaEndpointBytes);
  auto ctl = r.ReadString(kMaxMetaEndpointBytes);
  auto dc = r.ReadU32();
  auto priority = r.ReadU32();
  auto learner = r.ReadBool("invalid learner");
  auto joining = r.ReadBool("invalid new-joiner");
  if (!id.ok() || !endpoint.ok() || !principal.ok() || !data_control.ok() ||
      !ctl.ok() || !dc.ok() || !priority.ok() || !learner.ok() || !joining.ok())
    return Conflict("invalid membership peer encoding");
  if (*id == 0 || *id > INT32_MAX ||
      *principal != absl::StrCat("lavik://meta/", *id) ||
      !lavik::ParseNumericEndpoint(*endpoint) ||
      !lavik::ParseNumericEndpoint(*data_control) ||
      !lavik::ParseNumericEndpoint(*ctl))
    return Conflict("invalid membership peer identity");
  return MetaMembershipPeer{*id,
                            std::string(*endpoint),
                            std::string(*principal),
                            std::string(*data_control),
                            std::string(*ctl),
                            std::bit_cast<std::int32_t>(*dc),
                            std::bit_cast<std::int32_t>(*priority),
                            *learner,
                            *joining};
}
void WriteBinding(MetaWriter& w, const MetaMemberRecord& p) {
  w.WriteU32(p.server_id_);
  w.WriteString(p.principal_);
  w.WriteString(p.data_control_endpoint_);
  w.WriteOptional(p.ctl_endpoint_,
                  [](auto& out, const auto& s) { out.WriteString(s); });
}
absl::StatusOr<MetaMemberRecord> ReadBinding(MetaReader& r) {
  auto id = r.ReadU32();
  auto principal = r.ReadString(kMaxMetaPrincipalBytes);
  auto data = r.ReadString(kMaxMetaEndpointBytes);
  auto ctl =
      r.ReadOptional<std::string>([](auto& in) -> absl::StatusOr<std::string> {
        auto s = in.ReadString(kMaxMetaEndpointBytes);
        if (!s.ok()) return s.status();
        return std::string(*s);
      });
  if (!id.ok() || !principal.ok() || !data.ok() || !ctl.ok())
    return Conflict("invalid membership binding encoding");
  return MetaMemberRecord{*id, std::string(*principal), std::string(*data),
                          *ctl, false};
}
}  // namespace

absl::StatusOr<std::vector<MetaMembershipPeer>> CaptureMembershipConfig(
    const std::shared_ptr<MetaRaftConfig>& config) {
  if (!config || config->is_async_replication() ||
      !config->get_user_ctx().empty())
    return Conflict("unsupported or missing membership configuration");
  std::vector<MetaMembershipPeer> peers;
  for (const auto& p : config->get_servers()) {
    if (!p) return Conflict("null membership peer");
    auto identity = MetaMemberIdentity::DecodeAux(p->get_aux());
    if (!identity.ok() || identity->server_id_ != p->get_id())
      return Conflict("invalid membership aux identity");
    peers.push_back({static_cast<std::uint32_t>(p->get_id()), p->get_endpoint(),
                     identity->principal_, identity->data_control_endpoint_,
                     identity->ctl_endpoint_, p->get_dc_id(), p->get_priority(),
                     p->is_learner(), p->is_new_joiner()});
  }
  std::sort(peers.begin(), peers.end(),
            [](const auto& a, const auto& b) { return a.id_ < b.id_; });
  if (peers.empty() || peers.size() > kMaxMetaNodes ||
      std::adjacent_find(peers.begin(), peers.end(),
                         [](const auto& a, const auto& b) {
                           return a.id_ == b.id_;
                         }) != peers.end())
    return Conflict("invalid membership peer set");
  return peers;
}

absl::StatusOr<std::optional<BindMetaMember>> PlanInitialMetaBindings(
    const MetaCommittedView& view,
    const std::vector<MetaMembershipPeer>& config, bool initial_config) {
  for (const auto& peer : config) {
    if (initial_config && (peer.dc_id_ != 0 || peer.priority_ != 1 ||
                           peer.learner_ || peer.new_joiner_)) {
      return Conflict("initial Meta config contains a non-voter descriptor");
    }
    const MetaMemberRecord expected{peer.id_, peer.principal_,
                                    peer.data_control_endpoint_,
                                    peer.ctl_endpoint_, false};
    const auto actual = view.identity().FindMetaMember(peer.id_);
    if (actual.has_value()) {
      if (*actual != expected) {
        return Conflict(
            "Meta config descriptor conflicts with identity binding");
      }
      continue;
    }
    if (!initial_config) {
      return Conflict("post-genesis Meta config has no identity binding");
    }
    BindMetaMember bind;
    bind.server_id_ = peer.id_;
    bind.principal_ = peer.principal_;
    bind.data_control_endpoint_ = peer.data_control_endpoint_;
    bind.ctl_endpoint_ = peer.ctl_endpoint_;
    return std::optional(std::move(bind));
  }
  if (initial_config) {
    for (const auto& binding : view.identity().MetaMembers()) {
      const auto peer = std::find_if(
          config.begin(), config.end(), [&](const auto& candidate) {
            return candidate.id_ == binding.server_id_;
          });
      if (peer == config.end()) {
        return Conflict("initial identity binding is absent from Meta config");
      }
    }
  }
  return std::nullopt;
}

absl::StatusOr<std::string> EncodeMembershipIntent(
    const MetaMembershipIntent& plan) {
  if (plan.before_.size() > kMaxMetaNodes ||
      plan.bindings_.size() > kMaxMetaNodes)
    return Conflict("membership intent exceeds member bound");
  MetaWriter w;
  w.WriteU16(kMetaFormatVersion);
  w.WriteBool(plan.add_);
  WritePeer(w, plan.target_);
  WriteBinding(w, plan.binding_);
  w.WriteList(plan.before_, WritePeer);
  w.WriteList(plan.bindings_, WriteBinding);
  if (w.buffer().size() > kMaxMetaPayloadBytes)
    return Conflict("membership intent too large");
  auto checked = DecodeMembershipIntent(w.buffer());
  if (!checked.ok()) return checked.status();
  if (*checked != plan) return Conflict("membership intent is not canonical");
  return w.TakeBuffer();
}

absl::StatusOr<MetaMembershipIntent> DecodeMembershipIntent(
    std::string_view bytes) {
  if (bytes.size() > kMaxMetaPayloadBytes)
    return Conflict("membership intent too large");
  MetaReader r(bytes);
  auto version = r.ReadU16();
  auto add = r.ReadBool("invalid membership action");
  auto target = ReadPeer(r);
  auto binding = ReadBinding(r);
  auto before = r.ReadList<MetaMembershipPeer>(kMaxMetaNodes, ReadPeer);
  auto bindings = r.ReadList<MetaMemberRecord>(kMaxMetaNodes, ReadBinding);
  if (!version.ok() || *version != kMetaFormatVersion || !add.ok() ||
      !target.ok() || !binding.ok() || !before.ok() || !bindings.ok() ||
      !r.Finish().ok())
    return Conflict("invalid membership intent encoding");
  if (before->empty() || binding->server_id_ != target->id_ ||
      binding->principal_ != target->principal_ ||
      binding->data_control_endpoint_ != target->data_control_endpoint_ ||
      binding->ctl_endpoint_ != std::optional(target->ctl_endpoint_) ||
      bindings->size() != before->size())
    return Conflict("inconsistent membership intent");
  MetaIdentityStore identities;
  for (std::size_t i = 0; i < before->size(); ++i) {
    const auto& peer = (*before)[i];
    const auto& record = (*bindings)[i];
    if ((i && (*before)[i - 1].id_ >= peer.id_) ||
        record.server_id_ != peer.id_ || record.principal_ != peer.principal_)
      return Conflict("membership baseline is not canonical");
    if (record.data_control_endpoint_ != peer.data_control_endpoint_ ||
        record.ctl_endpoint_ != std::optional(peer.ctl_endpoint_)) {
      return Conflict("membership baseline descriptor differs from binding");
    }
    BindMetaMember c;
    c.server_id_ = record.server_id_;
    c.principal_ = record.principal_;
    c.data_control_endpoint_ = record.data_control_endpoint_;
    c.ctl_endpoint_ = record.ctl_endpoint_;
    if (!identities.Apply(c).ok()) return Conflict("invalid baseline binding");
    if (identities.FindMetaMember(record.server_id_) != std::optional(record))
      return Conflict("noncanonical baseline binding");
  }
  const auto old =
      std::find_if(before->begin(), before->end(),
                   [&](const auto& p) { return p.id_ == target->id_; });
  if (*add ? old != before->end()
           : (old == before->end() || *old != *target || before->size() < 2))
    return Conflict("invalid membership target relative to baseline");
  BindMetaMember c;
  c.server_id_ = binding->server_id_;
  c.principal_ = binding->principal_;
  c.data_control_endpoint_ = binding->data_control_endpoint_;
  c.ctl_endpoint_ = binding->ctl_endpoint_;
  if (!identities.Apply(c).ok()) return Conflict("invalid target binding");
  if (identities.FindMetaMember(binding->server_id_) !=
          std::optional(*binding) ||
      (*add && target->new_joiner_))
    return Conflict("noncanonical target binding");
  return MetaMembershipIntent{*add, *target, *binding, *before, *bindings};
}

Plan PlanMembershipStep(const MetaCommittedView& view,
                        const MetaOperationRecord& op,
                        const std::vector<MetaMembershipPeer>& config,
                        std::uint32_t local_id) {
  if (op.kind_ != kMetaMembershipOperationKind || Terminal(op))
    return std::nullopt;
  auto intent = DecodeMembershipIntent(op.intent_);
  if (!intent.ok()) return intent.status();
  const auto& p = *intent;
  auto after = p.before_;
  if (p.add_)
    after.push_back(p.target_);
  else
    std::erase_if(after,
                  [&](const auto& peer) { return peer.id_ == p.target_.id_; });
  std::sort(after.begin(), after.end(),
            [](const auto& a, const auto& b) { return a.id_ < b.id_; });
  const bool done = config == after;
  auto learner_stage = after;
  if (p.add_ && !p.target_.learner_) {
    for (auto& peer : learner_stage) {
      if (peer.id_ == p.target_.id_) peer.learner_ = true;
    }
  }
  // Adding a voter has one committed intermediate configuration. Keeping the
  // exact descriptor/baseline checks makes this stage recoverable by a new
  // leader without accepting an unrelated membership change as progress.
  const bool catching_up =
      p.add_ && !p.target_.learner_ && config == learner_stage;
  if (!done && !catching_up && config != p.before_)
    return Conflict("membership configuration diverged from intent");
  for (const auto& expected : p.bindings_) {
    auto current = view.identity().FindMetaMember(expected.server_id_);
    if (!p.add_ && expected.server_id_ == p.target_.id_ && done && current &&
        current->retired_)
      current->retired_ = false;
    if (current != std::optional(expected))
      return Conflict("membership identity baseline changed");
  }
  auto binding = view.identity().FindMetaMember(p.target_.id_);
  auto expected = p.binding_;
  if (!p.add_ && done && binding && binding->retired_) expected.retired_ = true;
  if ((binding && *binding != expected) || (!p.add_ && !binding))
    return Conflict("membership target binding changed");
  const auto& phase = op.kind_phase_blob_;
  if (phase.empty()) return Phase(op, p.add_ ? "bind-member" : "change-config");
  if (phase == "bind-member" && p.add_) {
    if (!binding) {
      BindMetaMember c;
      c.server_id_ = p.binding_.server_id_;
      c.principal_ = p.binding_.principal_;
      c.data_control_endpoint_ = p.binding_.data_control_endpoint_;
      c.ctl_endpoint_ = p.binding_.ctl_endpoint_;
      return Command(std::move(c));
    }
    return Phase(op, "change-config");
  }
  if (p.add_ && !binding) return Conflict("membership binding disappeared");
  if (phase == "change-config") {
    // Raft's accepted invite/leave reply is not a committed configuration.
    // Only the exact post-state authorizes identity retirement/completion.
    if (done) return Phase(op, "config-committed");
    if (!p.add_ && local_id == p.target_.id_)
      return MetaMembershipStep(MetaMembershipRaftAction::kYieldLeadership);
    return MetaMembershipStep(p.add_ ? MetaMembershipRaftAction::kAdd
                                     : MetaMembershipRaftAction::kRemove);
  }
  if (!done) return Conflict("completed membership configuration regressed");
  if (phase == "config-committed") {
    if (!p.add_ && !binding->retired_) {
      RetireMetaMember c;
      c.server_id_ = p.target_.id_;
      return Command(c);
    }
    return Phase(op, "identity-complete");
  }
  if (phase == "identity-complete") {
    if (!p.add_ && !binding->retired_)
      return Conflict("membership retirement regressed");
    CompleteOperation c;
    c.operation_id_ = op.operation_id_;
    c.expected_revision_ = op.revision_;
    c.result_ = p.add_ ? "member-added" : "member-removed";
    return Command(c);
  }
  return Conflict("unknown membership workflow phase");
}

struct MetaMembershipReconciler::Core {
  bycorf::ForeignExecutor executor_;
  MetaProposalExecutor* proposals_;
  std::shared_ptr<MetaRaft> server_;
  std::shared_ptr<MetaStateMachine> state_machine_;
  std::shared_ptr<MetaMembershipGate> gate_;
  bool running_ = false, cancelled_ = true, shutdown_ = false;
  std::atomic<bool> stopping_{false}, stopped_{false};
  std::vector<std::shared_ptr<std::promise<void>>> waiters_;
  struct Attempt {
    std::atomic<bool> entered_{false};
    std::atomic<bool> replied_{false};
    std::atomic<int> code_{0};
  };
};
MetaMembershipReconciler::MetaMembershipReconciler(
    bycorf::ForeignExecutor executor, MetaProposalExecutor& proposals,
    std::shared_ptr<MetaRaft> server, std::shared_ptr<MetaStateMachine> machine,

    std::shared_ptr<MetaMembershipGate> gate)
    : core_(std::make_shared<Core>()) {
  core_->executor_ = std::move(executor);
  core_->proposals_ = &proposals;
  core_->server_ = std::move(server);
  core_->state_machine_ = std::move(machine);
  core_->gate_ = std::move(gate);
}
MetaMembershipReconciler::~MetaMembershipReconciler() { Shutdown(); }
void MetaMembershipReconciler::Start(MetaLeaderContext& context) {
  auto core = core_;
  if (!core->executor_.Notify([core, context = &context]() noexcept {
        if (core->shutdown_) return;
        if (core->running_) std::terminate();
        core->running_ = true;
        core->cancelled_ = false;
        bycorf::ThisWorker().self_->Spawn(Run(core, context));
      }))
    std::terminate();
}
void MetaMembershipReconciler::Stop(bool permanent) {
  auto core = core_;
  if (core->stopped_.load(std::memory_order_acquire)) return;
  if (permanent) core->stopping_.store(true, std::memory_order_release);
  auto waiter = std::make_shared<std::promise<void>>();
  auto done = waiter->get_future();
  if (!core->executor_.Notify([core, waiter, permanent]() noexcept {
        core->shutdown_ |= permanent;
        core->cancelled_ = true;
        if (core->running_)
          core->waiters_.push_back(waiter);
        else
          waiter->set_value();
      })) {
    if (core->stopped_.load(std::memory_order_acquire)) return;
    std::terminate();
  }
  done.wait();
  if (permanent) core->stopped_.store(true, std::memory_order_release);
}
void MetaMembershipReconciler::CancelAndWait() { Stop(false); }
void MetaMembershipReconciler::Shutdown() { Stop(true); }
bool MetaMembershipReconciler::accepting() const {
  return !core_->stopping_.load(std::memory_order_acquire);
}

bycorf::Task<absl::Status> MetaMembershipReconciler::Run(
    std::shared_ptr<Core> core, MetaLeaderContext* context) {
  std::unique_ptr<MetaMembershipGate::Lease> lease;
  std::shared_ptr<Core::Attempt> attempt;
  auto retry_at = std::chrono::steady_clock::now();
  auto changed = std::make_shared<std::atomic<bool>>(false);
  auto subscribe = [&] {
    return context->SubscribeCommitted([changed](const MetaCommitEvent&) {
      changed->store(true, std::memory_order_release);
    });
  };
  auto subscribed = subscribe();
  std::string last_cut;
  std::optional<MetaOperationId> last_operation;
  while (!core->cancelled_) {
    // A committed effect may become visible before the local API returns.
    // Finish that bounded entry before discarding its lifetime/join handle.
    if (attempt && !attempt->entered_.load(std::memory_order_acquire)) {
      auto slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                             std::chrono::milliseconds(5));
      if (!slept.ok()) break;
      continue;
    }
    if (subscribed.subscription_->needs_resync()) subscribed = subscribe();
    if (changed->exchange(false, std::memory_order_acq_rel))
      subscribed.view_ = context->CommittedView();
    const auto& view = subscribed.view_;
    auto operations =
        view.operation().HasActiveKind(kMetaMembershipOperationKind)
            ? view.operation().LiveOperations()
            : std::vector<MetaOperationRecord>{};
    auto op = std::find_if(
        operations.begin(), operations.end(), [](const auto& item) {
          return item.kind_ == kMetaMembershipOperationKind && !Terminal(item);
        });
    const auto loaded_config = core->server_->get_config();
    auto configured = CaptureMembershipConfig(loaded_config);
    if (op == operations.end()) {
      auto initial_binding =
          configured.ok() ? PlanInitialMetaBindings(
                                view, *configured,
                                core->server_->initial_bindings_pending())
                          : absl::StatusOr<std::optional<BindMetaMember>>(
                                configured.status());
      if (!initial_binding.ok() || initial_binding->has_value()) {
        if (!lease) lease = core->gate_->TryAcquire();
        if (!initial_binding.ok()) {
          if (last_cut != initial_binding.status().message()) {
            spdlog::critical("initial Meta identity reconciliation blocked: {}",
                             initial_binding.status().message());
            last_cut = std::string(initial_binding.status().message());
          }
        } else {
          bool paused = false;
          std::size_t active_binding_count = 0;
          LAVIK_FAULT_INJECT(
              const auto bindings = view.identity().MetaMembers();
              active_binding_count = static_cast<std::size_t>(std::count_if(
                  bindings.begin(), bindings.end(),
                  [](const auto& binding) { return !binding.retired_; }));
              paused =
                  LAVIK_FAULT_MATCHES("LAVIK_TEST_PAUSE_INITIAL_BINDINGS_AFTER",
                                      std::to_string(active_binding_count)););
          if (paused) {
            const std::string cut = absl::StrCat(
                "initial-bindings-paused-after-", active_binding_count);
            if (last_cut != cut) {
              spdlog::info(
                  "initial Meta identity reconciliation paused after {} "
                  "bindings",
                  active_binding_count);
              last_cut = cut;
            }
          } else if (lease && core->server_->is_leader() &&
                     core->server_->is_leader_alive() &&
                     core->server_->is_leader_sm_fully_caught_up()) {
            MetaCommand command(std::move(**initial_binding));
            auto request = cluster::control::GenerateId128();
            if (!request.ok()) std::terminate();
            std::visit([&](auto& value) { value.request_id_ = *request; },
                       command);
            auto result = co_await context->Propose(std::move(command));
            changed->store(true, std::memory_order_release);
            if (result.ok() &&
                result->verdict_ == MetaAuditVerdict::kAccepted) {
              continue;
            }
          }
        }
        auto slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                               std::chrono::milliseconds(25));
        if (!slept.ok()) break;
        continue;
      }
    }
    if (op == operations.end()) {
      lease.reset();
      attempt.reset();
      last_operation.reset();
    } else {
      if (last_operation != std::optional(op->operation_id_)) {
        // Completion follows committed config, not callback delivery. A late
        // result from a previous task must never stall the next admitted one.
        attempt.reset();
        last_operation = op->operation_id_;
        last_cut.clear();
        retry_at = std::chrono::steady_clock::now();
      }
      if (!lease) lease = core->gate_->TryAcquire();
      if (lease && !op->kind_phase_blob_.starts_with("recovery-required:")) {
        const std::string cut =
            op->kind_phase_blob_.empty() ? "submitted" : op->kind_phase_blob_;
        if (cut != last_cut) {
          spdlog::info("membership {} phase={}", Id(op->operation_id_), cut);
          last_cut = cut;
        }
        bool paused = false;
        LAVIK_FAULT_INJECT(
            auto intent = DecodeMembershipIntent(op->intent_);
            paused =
                intent.ok() &&
                LAVIK_FAULT_MATCHES("LAVIK_TEST_PAUSE_MEMBERSHIP_PHASE", cut) &&
                LAVIK_FAULT_MATCHES("LAVIK_TEST_PAUSE_MEMBERSHIP_TARGET",
                                    std::to_string(intent->target_.id_)) &&
                LAVIK_FAULT_MATCHES("LAVIK_TEST_PAUSE_MEMBERSHIP_ACTION",
                                    intent->add_ ? "add" : "remove"););
        auto config = CaptureMembershipConfig(core->server_->get_config());
        if (!paused && core->server_->is_leader() &&
            core->server_->is_leader_alive() &&
            core->server_->is_leader_sm_fully_caught_up()) {
          auto plan = config.ok() ? PlanMembershipStep(view, *op, *config,
                                                       core->server_->get_id())
                                  : Plan(config.status());
          if (!plan.ok()) {
            spdlog::warn("membership {} requires recovery: {}",
                         Id(op->operation_id_), plan.status().message());
            plan = Phase(*op, absl::StrCat("recovery-required:",
                                           plan.status().message()));
          }
          if (plan.ok() && plan->has_value()) {
            if (auto* command = std::get_if<MetaCommand>(&**plan)) {
              auto request = cluster::control::GenerateId128();
              if (!request.ok()) std::terminate();
              std::visit([&](auto& c) { c.request_id_ = *request; }, *command);
              auto result = co_await context->Propose(std::move(*command));
              changed->store(true, std::memory_order_release);
              if (result.ok() &&
                  result->verdict_ == MetaAuditVerdict::kAccepted)
                continue;
            } else if (std::chrono::steady_clock::now() >= retry_at &&
                       (!attempt || (attempt->entered_.load() &&
                                     attempt->replied_.load()))) {
              auto intent = DecodeMembershipIntent(op->intent_);
              if (!intent.ok()) std::terminate();
              const auto action = std::get<MetaMembershipRaftAction>(**plan);
              attempt = std::make_shared<Core::Attempt>();
              const auto server = core->server_;
              const auto machine = core->state_machine_;
              const auto term = server->get_term();
              const auto id = op->operation_id_;
              const auto revision = op->revision_;
              // Check the retained operation/leadership again at executor
              // entry. No callback owns the reconciler/context or resumes a
              // cancelled coroutine. An accepted Raft change may commit after
              // demotion; its next owner observes the configuration before
              // retrying.
              auto submitted = core->proposals_->Submit([attempt, server,
                                                         machine, term, id,
                                                         revision,
                                                         expected = *config,
                                                         intent = *intent,
                                                         action] {
                auto current = machine->FindOperation(id);
                auto config_now = CaptureMembershipConfig(server->get_config());
                if (!server->is_leader() || server->get_term() != term ||
                    !current || Terminal(*current) ||
                    current->revision_ != revision || !config_now.ok() ||
                    *config_now != expected) {
                  attempt->replied_ = true;
                  attempt->entered_ = true;
                  return;
                }
                try {
                  if (action == MetaMembershipRaftAction::kYieldLeadership) {
                    server->yield_leadership();
                    attempt->replied_ = true;
                  } else {
                    std::shared_ptr<MetaRaftResult> result;
                    if (action == MetaMembershipRaftAction::kAdd) {
                      const auto& t = intent.target_;
                      MetaRaftMember peer(
                          t.id_, t.dc_id_, t.endpoint_,
                          MetaMemberIdentity{
                              static_cast<int>(t.id_), t.principal_,
                              t.data_control_endpoint_, t.ctl_endpoint_}
                              .EncodeAux(),
                          t.learner_, t.priority_);
                      result = server->add_srv(peer);
                    } else
                      result = server->remove_srv(intent.target_.id_);
                    if (result) {
                      decltype(result)::element_type::handler_type2 callback =
                          [attempt](auto& reply, auto&) {
                            attempt->code_ =
                                static_cast<int>(reply.get_result_code());
                            attempt->replied_ = true;
                          };
                      result->when_ready(callback);
                    } else
                      attempt->replied_ = true;
                  }
                } catch (...) {
                  attempt->replied_ = true;
                }
                attempt->entered_.store(true, std::memory_order_release);
              });
              if (!submitted.ok()) {
                attempt->entered_ = true;
                attempt->replied_ = true;
              }
              retry_at = std::chrono::steady_clock::now() +
                         std::chrono::milliseconds(250);
            }
          }
        }
      }
    }
    auto slept = co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                           std::chrono::milliseconds(25));
    if (!slept.ok()) break;
  }
  // Join only queued/local API entry, never the remote membership result.
  while (attempt && !attempt->entered_.load(std::memory_order_acquire))
    (void)co_await bycorf::SleepFor(*bycorf::ThisWorker().self_,
                                    std::chrono::milliseconds(5));
  lease.reset();
  core->running_ = false;
  for (const auto& waiter : core->waiters_) waiter->set_value();
  core->waiters_.clear();
  co_return absl::OkStatus();
}
}  // namespace lavik::meta
