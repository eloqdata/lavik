#include <algorithm>
#include <array>
#include <iterator>
#include <map>
#include <set>
#include <string_view>
#include <tuple>

#include "absl/strings/str_cat.h"
#include "tests/cluster/reference_model.h"

namespace keylane::test::cluster {
namespace {

bool CanExerciseAuthority(const AuthorityObservation& observation) {
  return observation.can_admit_write_ || observation.can_complete_inflight_ ||
         observation.can_mutate_in_background_ ||
         observation.can_decide_success_;
}

std::optional<Finding> CheckAuthority(const ClusterSnapshot& snapshot) {
  using AuthorityIdentity = std::tuple<NodeId, BootId, GroupTerm, GrantId>;
  std::map<GroupId, std::set<AuthorityIdentity>> capable_authorities;
  for (const AuthorityObservation& authority : snapshot.authorities_) {
    if (CanExerciseAuthority(authority)) {
      capable_authorities[authority.group_].emplace(
          authority.node_, authority.boot_, authority.term_, authority.grant_);
    }
  }
  for (const auto& [group, authorities] : capable_authorities) {
    if (authorities.size() <= 1) continue;
    const auto& [first_node, first_boot, first_term, first_grant] =
        *authorities.begin();
    const auto& [second_node, second_boot, second_term, second_grant] =
        *std::next(authorities.begin());
    return Finding{
        .invariant_id_ = "authority.single-writer",
        .witness_ = absl::StrCat(
            "group=", group.value_, ",first=", first_node.value_, "/",
            first_boot.value_, "/", first_term.value_, "/", first_grant.value_,
            ",second=", second_node.value_, "/", second_boot.value_, "/",
            second_term.value_, "/", second_grant.value_),
    };
  }
  return std::nullopt;
}

std::optional<Finding> CheckPromotion(const ClusterSnapshot& snapshot) {
  const PromotionObservation& promotion = snapshot.promotion_;
  if (promotion.candidate_activated_ &&
      (!promotion.candidate_selected_ ||
       !promotion.durability_barrier_complete_ ||
       !promotion.child_history_ready_)) {
    return Finding{.invariant_id_ = "promotion.safe-activation",
                   .witness_ = "candidate-active-before-full-barrier"};
  }
  if (promotion.write_gate_open_ && !promotion.durability_barrier_complete_) {
    return Finding{.invariant_id_ = "promotion.durable-before-write-authority",
                   .witness_ = "write-gate-open-before-barrier"};
  }
  if (promotion.write_gate_open_ && !promotion.child_history_ready_) {
    return Finding{.invariant_id_ = "history.child-ready-before-write",
                   .witness_ = "write-gate-open-before-child-history"};
  }
  if (promotion.another_replica_reset_ &&
      (!promotion.candidate_activated_ ||
       !promotion.durability_barrier_complete_ ||
       !promotion.child_history_ready_)) {
    return Finding{.invariant_id_ = "promotion.keep-replicas-until-activation",
                   .witness_ = "replica-reset-before-candidate-active"};
  }
  return std::nullopt;
}

std::optional<Finding> CheckResume(const ClusterSnapshot& snapshot) {
  const ResumeObservation& resume = snapshot.resume_;
  if (!resume.partial_resume_selected_) return std::nullopt;
  if (resume.source_boot_ != resume.evidence_source_boot_ ||
      resume.target_boot_ != resume.evidence_target_boot_) {
    return Finding{.invariant_id_ = "replication.restart-invalidates-evidence",
                   .witness_ = "partial-resume-used-stale-boot"};
  }
  if (!resume.compatible_history_ || !resume.compatible_population_) {
    return Finding{.invariant_id_ = "replication.compatible-resume-domain",
                   .witness_ = "partial-resume-domain-mismatch"};
  }
  if (!resume.exact_cursor_ || !resume.retained_events_contiguous_ ||
      !resume.transaction_boundary_complete_) {
    return Finding{
        .invariant_id_ = "replication.reparent-requires-complete-history",
        .witness_ = "partial-resume-crossed-history-gap",
    };
  }
  return std::nullopt;
}

std::optional<Finding> CheckPopulation(const ClusterSnapshot& snapshot) {
  const PopulationObservation& population = snapshot.population_;
  if (population.staging_visible_) {
    return Finding{.invariant_id_ = "population.staging-hidden",
                   .witness_ = "staging-population-visible"};
  }
  if (population.active_ &&
      (!population.complete_ || !population.activation_durable_)) {
    return Finding{.invariant_id_ = "population.atomic-activation",
                   .witness_ = "partial-population-active"};
  }
  return std::nullopt;
}

std::optional<Finding> CheckCandidate(const ClusterSnapshot& snapshot) {
  if (!snapshot.candidate_.has_value() ||
      !snapshot.candidate_->candidate_claimed_not_behind_) {
    return std::nullopt;
  }
  const VectorOrder order = CompareAppliedVectors(
      snapshot.candidate_->candidate_, snapshot.candidate_->peer_);
  if (order != VectorOrder::kGreater && order != VectorOrder::kEqual) {
    return Finding{.invariant_id_ = "candidate.componentwise-applied-order",
                   .witness_ = "candidate-not-componentwise-ahead"};
  }
  return std::nullopt;
}

std::optional<Finding> CheckMeta(const ClusterSnapshot& snapshot) {
  const MetaObservation& meta = snapshot.meta_;
  if (meta.serving_state_published_ &&
      (meta.installed_topology_ < meta.committed_topology_ ||
       meta.installed_term_ < meta.committed_term_)) {
    return Finding{.invariant_id_ = "meta.committed-state-monotonic",
                   .witness_ = "serving-state-regressed"};
  }
  if (meta.directive_applied_ &&
      (meta.current_operation_ != meta.directive_operation_ ||
       meta.current_boot_ != meta.evidence_boot_)) {
    return Finding{.invariant_id_ = "meta.directive-evidence-scoped",
                   .witness_ = "stale-directive-evidence-applied"};
  }
  return std::nullopt;
}

std::optional<Finding> CheckMigration(const ClusterSnapshot& snapshot) {
  const MigrationObservation& migration = snapshot.migration_;
  if (migration.source_serves_ && migration.target_serves_) {
    return Finding{.invariant_id_ = "migration.single-owner",
                   .witness_ = "source-and-target-both-serving"};
  }
  if (migration.target_serves_ && (!migration.ownership_committed_ ||
                                   !migration.target_population_complete_)) {
    return Finding{.invariant_id_ = "migration.complete-before-serving",
                   .witness_ = "target-serving-before-commit"};
  }
  return std::nullopt;
}

std::optional<Finding> CheckClientHistory(const ClusterSnapshot& snapshot) {
  using AuthorityIdentity = std::tuple<NodeId, BootId, GroupTerm, GrantId>;
  std::map<OperationId, AuthorityIdentity> operations;
  for (const ClientOperationObservation& operation : snapshot.client_history_) {
    const AuthorityIdentity identity{
        operation.authority_, operation.authority_boot_,
        operation.authority_term_, operation.authority_grant_};
    const auto [found, inserted] =
        operations.try_emplace(operation.operation_, identity);
    if (!inserted && found->second != identity) {
      return Finding{
          .invariant_id_ = "client.operation-single-authority",
          .witness_ = absl::StrCat("operation=", operation.operation_.value_)};
    }
    if (operation.mutates_ && operation.admitted_ &&
        !operation.admitted_with_valid_authority_) {
      return Finding{
          .invariant_id_ = "client.valid-authority-at-admission",
          .witness_ = absl::StrCat("operation=", operation.operation_.value_)};
    }
    if (operation.outcome_ == ClientOutcome::kSuccess &&
        (!operation.admitted_ ||
         !operation.authority_valid_at_success_decision_ ||
         (operation.mutates_ && !operation.mutation_durable_))) {
      return Finding{
          .invariant_id_ = "client.safe-success-decision",
          .witness_ = absl::StrCat("operation=", operation.operation_.value_)};
    }
  }
  return std::nullopt;
}

bool IsBoundedDecimal(std::string_view text, std::uint32_t maximum,
                      bool allow_zero) {
  if (text.empty()) return false;
  std::uint32_t value = 0;
  for (const char byte : text) {
    if (byte < '0' || byte > '9') return false;
    const std::uint32_t digit = static_cast<std::uint32_t>(byte - '0');
    if (value > (maximum - digit) / 10) return false;
    value = value * 10 + digit;
  }
  return allow_zero || value != 0;
}

bool IsPublicEndpoint(std::string_view endpoint) {
  const std::size_t colon = endpoint.rfind(':');
  if (colon == std::string_view::npos || colon == 0 ||
      colon + 1 == endpoint.size()) {
    return false;
  }
  const std::string_view host = endpoint.substr(0, colon);
  const std::string_view port = endpoint.substr(colon + 1);
  if (!IsBoundedDecimal(port, 65535, false)) return false;
  return std::all_of(host.begin(), host.end(), [](const char byte) {
    return (byte >= 'a' && byte <= 'z') || (byte >= 'A' && byte <= 'Z') ||
           (byte >= '0' && byte <= '9') || byte == '.' || byte == '-' ||
           byte == '_' || byte == ':' || byte == '[' || byte == ']' ||
           byte == '%';
  });
}

bool IsPublicRedirect(std::string_view reply, std::string_view prefix) {
  if (reply.size() <= prefix.size() + 2 || reply[prefix.size() + 1] != ' ') {
    return false;
  }
  const std::string_view arguments = reply.substr(prefix.size() + 2);
  const std::size_t separator = arguments.find(' ');
  if (separator == std::string_view::npos || separator == 0 ||
      separator + 1 == arguments.size() ||
      arguments.find(' ', separator + 1) != std::string_view::npos) {
    return false;
  }
  const std::string_view slot = arguments.substr(0, separator);
  const std::string_view endpoint = arguments.substr(separator + 1);
  return IsBoundedDecimal(slot, 16383, true) && IsPublicEndpoint(endpoint);
}

std::optional<Finding> CheckWire(const ClusterSnapshot& snapshot) {
  if (!snapshot.server_control_reply_.has_value()) return std::nullopt;
  const std::string_view reply = *snapshot.server_control_reply_;
  if (reply.empty() || reply.front() != '-' ||
      std::any_of(reply.begin(), reply.end(), [](char byte) {
        const unsigned char value = static_cast<unsigned char>(byte);
        return value < 0x20 || value == 0x7f;
      })) {
    return Finding{.invariant_id_ = "redis.control-error-shape",
                   .witness_ = "invalid-control-error-frame"};
  }
  const std::size_t end = reply.find(' ');
  const std::string_view prefix = reply.substr(
      1, end == std::string_view::npos ? reply.size() - 1 : end - 1);
  constexpr std::array<std::string_view, 5> allowed{"MOVED", "ASK", "TRYAGAIN",
                                                    "CLUSTERDOWN", "LOADING"};
  if (std::find(allowed.begin(), allowed.end(), prefix) == allowed.end()) {
    return Finding{.invariant_id_ = "redis.compatible-control-error",
                   .witness_ = absl::StrCat("private-prefix=", prefix)};
  }
  constexpr std::array<std::string_view, 3> public_messages{
      "-TRYAGAIN cluster state is changing", "-CLUSTERDOWN no safe owner",
      "-LOADING Redis is loading the dataset in memory"};
  if (std::find(public_messages.begin(), public_messages.end(), reply) !=
          public_messages.end() ||
      ((prefix == "MOVED" || prefix == "ASK") &&
       IsPublicRedirect(reply, prefix))) {
    return std::nullopt;
  }
  return Finding{.invariant_id_ = "redis.private-control-state-hidden",
                 .witness_ = absl::StrCat("non-public-", prefix, "-shape")};
}

}  // namespace

VectorOrder CompareAppliedVectors(const AppliedVector& left,
                                  const AppliedVector& right) {
  if (left.domain_ != right.domain_) return VectorOrder::kIncompatible;
  bool left_less = false;
  bool left_greater = false;
  std::map<FlowId, std::uint64_t> all = left.cursors_;
  for (const auto& [flow, cursor] : right.cursors_) {
    all.try_emplace(flow, cursor);
  }
  for (const auto& [flow, ignored] : all) {
    (void)ignored;
    const auto left_found = left.cursors_.find(flow);
    const auto right_found = right.cursors_.find(flow);
    const std::uint64_t left_cursor =
        left_found == left.cursors_.end() ? 0 : left_found->second;
    const std::uint64_t right_cursor =
        right_found == right.cursors_.end() ? 0 : right_found->second;
    left_less = left_less || left_cursor < right_cursor;
    left_greater = left_greater || left_cursor > right_cursor;
  }
  if (left_less && left_greater) return VectorOrder::kIncomparable;
  if (left_less) return VectorOrder::kLess;
  if (left_greater) return VectorOrder::kGreater;
  return VectorOrder::kEqual;
}

std::optional<Finding> CheckClusterInvariants(const ClusterSnapshot& snapshot) {
  if (auto finding = CheckAuthority(snapshot); finding.has_value()) {
    return finding;
  }
  if (auto finding = CheckPromotion(snapshot); finding.has_value()) {
    return finding;
  }
  if (auto finding = CheckResume(snapshot); finding.has_value()) return finding;
  if (auto finding = CheckPopulation(snapshot); finding.has_value()) {
    return finding;
  }
  if (auto finding = CheckCandidate(snapshot); finding.has_value()) {
    return finding;
  }
  if (auto finding = CheckMeta(snapshot); finding.has_value()) return finding;
  if (auto finding = CheckMigration(snapshot); finding.has_value()) {
    return finding;
  }
  if (auto finding = CheckClientHistory(snapshot); finding.has_value()) {
    return finding;
  }
  return CheckWire(snapshot);
}

}  // namespace keylane::test::cluster
