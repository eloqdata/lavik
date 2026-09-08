#include "keylane/storage/detail/grouped_sorted_rewrite.h"

#include <bit>
#include <cmath>
#include <limits>

namespace keylane::storage {
namespace {

bool EqualEntry(const OrderedCollectionEntry& left,
                const OrderedCollectionEntry& right) {
  return left.value_ == right.value_ &&
         std::bit_cast<std::uint64_t>(left.score_) ==
             std::bit_cast<std::uint64_t>(right.score_);
}

absl::Status ValidateOrder(std::span<const OrderedCollectionEntry> entries) {
  for (std::size_t i = 0; i < entries.size(); ++i) {
    if (std::isnan(entries[i].score_) ||
        entries[i].value_.size() > kMaxStringBytes ||
        (i != 0 && !OrderedEntryLess(entries[i - 1], entries[i]))) {
      return absl::InvalidArgumentError(
          "invalid Sorted Set logical after-image");
    }
  }
  return absl::OkStatus();
}

struct Draft {
  OrderedGroupSnapshot page_;
  bool changed_ = true;
};

}  // namespace

absl::StatusOr<OrderedCollectionMutationPlan> PlanSortedSetRewrite(
    const OrderedGroupDirectory& directory,
    std::span<const OrderedCollectionEntry> before,
    std::vector<OrderedCollectionEntry> after, std::size_t target_bytes) {
  const auto& root = directory.root();
  if (root.kind_ != OrderedCollectionKind::kSortedSet ||
      before.size() != root.item_count_ || before.empty() ||
      after.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::InvalidArgumentError(
        "Sorted Set rewrite has wrong source/count");
  }
  auto valid = ValidateOrder(before);
  if (!valid.ok()) return valid;
  valid = ValidateOrder(after);
  if (!valid.ok()) return valid;
  OrderedCollectionMutationPlan plan{
      .root_ = root, .expected_sequence_ = directory.sequence(), .writes_ = {}};
  plan.root_.item_count_ = after.size();
  if (after.empty()) {
    plan.changed_ = true;
    plan.delete_key_ = true;
    plan.root_.first_group_ = plan.root_.last_group_ = 0;
    plan.root_.group_count_ = 0;
    return plan;
  }
  std::vector<Draft> drafts;
  std::vector<OrderedGroupSnapshot> retired;
  std::size_t old_offset = 0;
  std::size_t new_offset = 0;
  auto next_id = root.next_group_id_;
  for (const auto& metadata : directory.groups()) {
    if (metadata.item_count_ == 0 ||
        metadata.item_count_ > before.size() - old_offset) {
      return absl::DataLossError(
          "Sorted Set directory count exceeds before-image");
    }
    const auto old_end = old_offset + metadata.item_count_;
    const auto begin = new_offset;
    // The final page also owns the unbounded high end. Every other page owns
    // the interval ending at its original last (score, member), even if that
    // member is deleted or moves to another page in the after-image.
    while (new_offset != after.size() &&
           (old_end == before.size() ||
            !OrderedEntryLess(before[old_end - 1], after[new_offset]))) {
      ++new_offset;
    }
    if (begin == new_offset) {
      retired.push_back({.kind_ = root.kind_,
                         .incarnation_ = root.incarnation_,
                         .id_ = metadata.id_,
                         .retired_ = true,
                         .entries_ = {}});
      old_offset = old_end;
      continue;
    }
    bool unchanged = new_offset - begin == metadata.item_count_;
    if (unchanged) {
      for (std::size_t i = 0; i < metadata.item_count_; ++i) {
        if (!EqualEntry(before[old_offset + i], after[begin + i])) {
          unchanged = false;
          break;
        }
      }
    }
    OrderedGroupSnapshot page{.kind_ = root.kind_,
                              .incarnation_ = root.incarnation_,
                              .id_ = metadata.id_,
                              .previous_ = metadata.previous_,
                              .next_ = metadata.next_,
                              .entries_ = {}};
    page.entries_.reserve(new_offset - begin);
    for (std::size_t i = begin; i < new_offset; ++i) {
      page.entries_.push_back(std::move(after[i]));
    }
    if (unchanged) {
      drafts.push_back({.page_ = std::move(page), .changed_ = false});
    } else {
      auto split = SplitOrderedGroup(std::move(page), next_id, target_bytes);
      if (!split.ok()) return split.status();
      next_id = split->next_group_id_;
      for (auto& part : split->groups_) {
        drafts.push_back({.page_ = std::move(part)});
      }
    }
    old_offset = old_end;
  }
  if (old_offset != before.size() || new_offset != after.size() ||
      drafts.empty() ||
      drafts.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::DataLossError(
        "Sorted Set rewrite did not cover its logical image");
  }
  plan.root_.first_group_ = drafts.front().page_.id_;
  plan.root_.last_group_ = drafts.back().page_.id_;
  plan.root_.group_count_ = drafts.size();
  plan.root_.next_group_id_ = next_id;
  for (std::size_t i = 0; i < drafts.size(); ++i) {
    auto& draft = drafts[i];
    const auto previous = i == 0 ? 0 : drafts[i - 1].page_.id_;
    const auto next = i + 1 == drafts.size() ? 0 : drafts[i + 1].page_.id_;
    if (draft.page_.previous_ != previous || draft.page_.next_ != next) {
      draft.changed_ = true;
      draft.page_.previous_ = previous;
      draft.page_.next_ = next;
    }
    if (draft.changed_) plan.writes_.push_back(std::move(draft.page_));
  }
  for (auto& page : retired) plan.writes_.push_back(std::move(page));
  plan.changed_ = !plan.writes_.empty();
  return plan;
}

}  // namespace keylane::storage
