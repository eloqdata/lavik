#include "keylane/storage/detail/grouped_hash.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace keylane::storage {
namespace {

constexpr std::uint64_t kRootMagic = 0x31544f4f5248474bULL;   // KGHROOT1
constexpr std::uint64_t kGroupMagic = 0x3150554f5247484bULL;  // KHGROUP1
// Version also fixes SipHash-1-2/high-prefix routing. A future hash change is
// a representation change, not a process-wide lookup optimization.
constexpr std::uint32_t kVersion = 1;
constexpr std::size_t kRootBytes = kGroupedHashRootBytes;
constexpr std::size_t kGroupHeaderBytes = kHashGroupHeaderBytes;
constexpr std::size_t kCompactHeaderBytes = kHashValueHeaderBytes;

std::uint64_t Mask(unsigned bits) noexcept {
  return bits == 0 ? 0
                   : std::numeric_limits<std::uint64_t>::max() << (64 - bits);
}

void Store(std::string& bytes, std::size_t offset, std::uint64_t value,
           unsigned width) {
  for (unsigned i = 0; i < width; ++i) {
    bytes[offset + i] = static_cast<char>(value >> (i * 8));
  }
}

template <std::size_t N>
void Store(std::array<char, N>& bytes, std::size_t offset, std::uint64_t value,
           unsigned width) {
  for (unsigned i = 0; i < width; ++i)
    bytes[offset + i] = static_cast<char>(value >> (i * 8));
}

std::uint64_t Load(std::string_view bytes, std::size_t offset, unsigned width) {
  std::uint64_t value = 0;
  for (unsigned i = 0; i < width; ++i) {
    value |= static_cast<std::uint64_t>(
                 static_cast<unsigned char>(bytes[offset + i]))
             << (i * 8);
  }
  return value;
}

bool ValidRoot(const GroupedHashRoot& root) {
  return root.incarnation_ != 0 && root.revision_ != 0 &&
         root.group_count_ != 0 && root.field_count_ != 0 &&
         root.field_count_ <= std::numeric_limits<std::uint32_t>::max();
}

absl::StatusOr<std::size_t> PayloadBytes(const HashValue& value) {
  if (value.entries_.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::OutOfRangeError("Hash group contains too many fields");
  }
  std::uint64_t bytes = value.entries_.empty() ? 0 : kCompactHeaderBytes;
  for (const auto& entry : value.entries_) {
    auto next =
        AppendHashEntrySize(bytes, entry.field_.size(), entry.value_.size(),
                            kHashGroupPayloadLimit);
    if (!next.ok()) return next.status();
    bytes = *next;
  }
  return static_cast<std::size_t>(bytes);
}

absl::Status ValidateFields(const HashGroupSnapshot& group,
                            const DigestSeed* seed) {
  if (group.incarnation_ == 0 || !group.id_.valid() ||
      (group.retired_ && !group.value_.entries_.empty())) {
    return absl::InvalidArgumentError("invalid Hash group identity or state");
  }
  absl::flat_hash_set<std::string_view> fields;
  fields.reserve(group.value_.entries_.size());
  for (const auto& entry : group.value_.entries_) {
    if (!fields.insert(entry.field_).second) {
      return absl::InvalidArgumentError("duplicate field in Hash group");
    }
    if (seed != nullptr &&
        !group.id_.contains(ComputeDigest(entry.field_, *seed).value_)) {
      return absl::InvalidArgumentError("field is outside its Hash group");
    }
  }
  return absl::OkStatus();
}

}  // namespace

bool HashGroupId::valid() const noexcept {
  return bits_ <= 64 && (prefix_ & ~Mask(bits_)) == 0;
}

bool HashGroupId::contains(std::uint64_t hash) const noexcept {
  return valid() && (hash & Mask(bits_)) == prefix_;
}

std::uint64_t HashGroupId::last() const noexcept {
  return valid() ? prefix_ | ~Mask(bits_) : 0;
}

absl::StatusOr<std::string> EncodeGroupedHashRoot(const GroupedHashRoot& root) {
  if (!ValidRoot(root)) {
    return absl::InvalidArgumentError("invalid grouped Hash root");
  }
  std::string bytes(kRootBytes, '\0');
  Store(bytes, 0, kRootMagic, 8);
  Store(bytes, 8, kVersion, 4);
  Store(bytes, 12, kRootBytes, 4);
  Store(bytes, 16, root.incarnation_, 8);
  for (std::size_t i = 0; i < root.seed_.size(); ++i) {
    bytes[24 + i] = static_cast<char>(root.seed_[i]);
  }
  Store(bytes, 40, root.field_count_, 8);
  Store(bytes, 48, root.group_count_, 4);
  Store(bytes, 56, root.revision_, 8);
  return bytes;
}

absl::StatusOr<GroupedHashRoot> DecodeGroupedHashRoot(std::string_view bytes) {
  if (bytes.size() != kRootBytes || Load(bytes, 0, 8) != kRootMagic ||
      Load(bytes, 8, 4) != kVersion || Load(bytes, 12, 4) != kRootBytes ||
      Load(bytes, 52, 4) != 0) {
    return absl::DataLossError("invalid grouped Hash root encoding");
  }
  GroupedHashRoot root{
      .incarnation_ = Load(bytes, 16, 8),
      .field_count_ = Load(bytes, 40, 8),
      .group_count_ = static_cast<std::uint32_t>(Load(bytes, 48, 4)),
      .revision_ = Load(bytes, 56, 8),
  };
  for (std::size_t i = 0; i < root.seed_.size(); ++i) {
    root.seed_[i] = static_cast<std::uint8_t>(bytes[24 + i]);
  }
  if (!ValidRoot(root)) {
    return absl::DataLossError("invalid grouped Hash root metadata");
  }
  return root;
}

absl::StatusOr<HashGroupEncoder> HashGroupEncoder::Create(
    const HashGroupSnapshot& group) {
  auto valid = ValidateFields(group, nullptr);
  if (!valid.ok()) return valid;
  auto size = PayloadBytes(group.value_);
  if (!size.ok()) return size.status();
  HashGroupEncoder cursor;
  cursor.group_ = &group;
  cursor.encoded_bytes_ = kGroupHeaderBytes + *size;
  auto& bytes = cursor.header_;
  Store(bytes, 0, kGroupMagic, 8);
  Store(bytes, 8, kVersion, 4);
  Store(bytes, 12, kGroupHeaderBytes, 4);
  Store(bytes, 16, group.incarnation_, 8);
  Store(bytes, 24, group.id_.prefix_, 8);
  Store(bytes, 32, group.value_.entries_.size(), 4);
  Store(bytes, 36, *size, 4);
  Store(bytes, 40, group.id_.bits_, 1);
  Store(bytes, 41, group.retired_, 1);
  if (!group.value_.entries_.empty()) {
    Store(bytes, 48, kHashValueMagic, 8);
    Store(bytes, 56, kStorageFormatVersion, 4);
    Store(bytes, 60, kHashValueHeaderBytes, 4);
    Store(bytes, 64, group.value_.entries_.size(), 4);
    Store(bytes, 72, *size, 8);
  }
  return cursor;
}

std::optional<std::string_view> HashGroupEncoder::Next() noexcept {
  if (group_ == nullptr) return std::nullopt;
  if (phase_ == 0) {
    phase_ = 1;
    return std::string_view(header_.data(), group_->value_.entries_.empty()
                                                ? kGroupHeaderBytes
                                                : header_.size());
  }
  if (entry_ == group_->value_.entries_.size()) return std::nullopt;
  const auto& entry = group_->value_.entries_[entry_];
  if (phase_ == 1) {
    Store(lengths_, 0, entry.field_.size(), 4);
    Store(lengths_, 4, entry.value_.size(), 4);
    phase_ = 2;
    return std::string_view(lengths_.data(), lengths_.size());
  }
  if (phase_ == 2) {
    phase_ = 3;
    return entry.field_;
  }
  phase_ = 1;
  ++entry_;
  return entry.value_;
}

absl::StatusOr<std::string> EncodeHashGroup(const HashGroupSnapshot& group) {
  auto cursor = HashGroupEncoder::Create(group);
  if (!cursor.ok()) return cursor.status();
  std::string bytes;
  bytes.reserve(cursor->encoded_bytes());
  while (auto span = cursor->Next()) bytes.append(*span);
  return bytes;
}

absl::StatusOr<HashGroupMetadata> DecodeHashGroupMetadata(
    std::string_view bytes, std::size_t encoded_bytes) {
  if (bytes.size() < kGroupHeaderBytes || encoded_bytes < kGroupHeaderBytes ||
      encoded_bytes - kGroupHeaderBytes > kHashGroupPayloadLimit ||
      Load(bytes, 0, 8) != kGroupMagic || Load(bytes, 8, 4) != kVersion ||
      Load(bytes, 12, 4) != kGroupHeaderBytes ||
      Load(bytes, 36, 4) != encoded_bytes - kGroupHeaderBytes ||
      Load(bytes, 41, 1) > 1 || Load(bytes, 42, 6) != 0) {
    return absl::DataLossError("invalid Hash group encoding");
  }
  HashGroupMetadata metadata{
      .incarnation_ = Load(bytes, 16, 8),
      .id_ = {.prefix_ = Load(bytes, 24, 8),
              .bits_ = static_cast<std::uint8_t>(Load(bytes, 40, 1))},
      .field_count_ = static_cast<std::uint32_t>(Load(bytes, 32, 4)),
      .retired_ = Load(bytes, 41, 1) != 0,
  };
  const std::size_t payload_bytes = encoded_bytes - kGroupHeaderBytes;
  if (metadata.incarnation_ == 0 || !metadata.id_.valid() ||
      (metadata.retired_ && metadata.field_count_ != 0) ||
      (metadata.field_count_ == 0 && payload_bytes != 0) ||
      (metadata.field_count_ != 0 &&
       (payload_bytes < kCompactHeaderBytes ||
        metadata.field_count_ > (payload_bytes - kCompactHeaderBytes) / 8))) {
    return absl::DataLossError("invalid Hash group envelope metadata");
  }
  return metadata;
}

absl::StatusOr<HashGroupSnapshot> DecodeHashGroup(std::string_view bytes) {
  auto metadata = DecodeHashGroupMetadata(bytes, bytes.size());
  if (!metadata.ok()) return metadata.status();
  HashGroupSnapshot group{
      .incarnation_ = metadata->incarnation_,
      .id_ = metadata->id_,
      .retired_ = metadata->retired_,
      .value_ = {},
  };
  if (bytes.size() != kGroupHeaderBytes) {
    // Page admission is based on the outer count. The compact payload has
    // its own count: compare them before DecodeHashValue reserves a vector,
    // so a corrupt inner header cannot exceed that admitted metadata budget.
    if (Load(bytes, kGroupHeaderBytes + 16, 4) != metadata->field_count_) {
      return absl::DataLossError(
          "Hash group inner count disagrees with envelope");
    }
    auto decoded = DecodeHashValue(bytes.substr(kGroupHeaderBytes),
                                   kHashGroupPayloadLimit);
    if (!decoded.ok()) return absl::DataLossError(decoded.status().message());
    group.value_ = std::move(*decoded);
  }
  if (group.value_.entries_.size() != metadata->field_count_) {
    return absl::DataLossError("Hash group count does not match its payload");
  }
  auto valid = ValidateFields(group, nullptr);
  if (!valid.ok()) return absl::DataLossError(valid.message());
  return group;
}

absl::StatusOr<std::vector<HashGroupSnapshot>> SplitHashGroup(
    HashGroupSnapshot group, const DigestSeed& seed, std::size_t target_bytes) {
  if (target_bytes < kCompactHeaderBytes || group.retired_) {
    return absl::InvalidArgumentError("invalid Hash group split request");
  }
  auto valid = ValidateFields(group, &seed);
  if (!valid.ok()) return valid;
  // Check each indivisible entry, not the aggregate compact encoding: a
  // collection may exceed one record's envelope as long as each resulting
  // group fits. Splitting must not first encode the big value. In particular,
  // a valid 512 MiB value still needs room for its name and framing bytes.
  for (const auto& entry : group.value_.entries_) {
    auto size =
        AppendHashEntrySize(kCompactHeaderBytes, entry.field_.size(),
                            entry.value_.size(), kHashGroupPayloadLimit);
    if (!size.ok()) return size.status();
  }
  std::vector<HashGroupSnapshot> pending;
  std::vector<HashGroupSnapshot> leaves;
  pending.push_back(std::move(group));
  while (!pending.empty()) {
    HashGroupSnapshot current = std::move(pending.back());
    pending.pop_back();
    auto bytes = PayloadBytes(current.value_);
    if ((bytes.ok() && *bytes <= target_bytes) ||
        current.value_.entries_.size() <= 1 || current.id_.bits_ == 64) {
      if (!bytes.ok()) return bytes.status();
      leaves.push_back(std::move(current));
      continue;
    }
    const auto child_bits = static_cast<std::uint8_t>(current.id_.bits_ + 1);
    const std::uint64_t branch_bit = std::uint64_t{1} << (64 - child_bits);
    HashGroupSnapshot left{
        .incarnation_ = current.incarnation_,
        .id_ = {.prefix_ = current.id_.prefix_, .bits_ = child_bits},
        .value_ = {},
    };
    HashGroupSnapshot right{
        .incarnation_ = current.incarnation_,
        .id_ = {.prefix_ = current.id_.prefix_ | branch_bit,
                .bits_ = child_bits},
        .value_ = {},
    };
    for (auto& entry : current.value_.entries_) {
      auto& destination =
          (ComputeDigest(entry.field_, seed).value_ & branch_bit) ? right.value_
                                                                  : left.value_;
      destination.entries_.push_back(std::move(entry));
    }
    pending.push_back(std::move(right));
    pending.push_back(std::move(left));
  }
  return leaves;
}

absl::StatusOr<std::vector<HashGroupSnapshot>> GroupHashValue(
    HashValue value, std::uint64_t incarnation, const DigestSeed& seed,
    std::size_t target_bytes) {
  if (value.entries_.empty()) {
    return absl::InvalidArgumentError("an empty Hash is a key tombstone");
  }
  return SplitHashGroup(HashGroupSnapshot{.incarnation_ = incarnation,
                                          .value_ = std::move(value)},
                        seed, target_bytes);
}

absl::StatusOr<HashGroupDirectory> HashGroupDirectory::Recover(
    const GroupedHashRoot& root, std::uint64_t root_sequence,
    std::span<const RecoveredHashGroup> candidates,
    const absl::flat_hash_set<std::uint64_t>& committed_txids) {
  if (!ValidRoot(root) || root_sequence == 0) {
    return absl::DataLossError("invalid grouped Hash recovery root");
  }
  std::map<HashGroupId, RecoveredHashGroup> winners;
  for (const auto& candidate : candidates) {
    if (candidate.incarnation_ != root.incarnation_ ||
        candidate.sequence_ > root.revision_ ||
        (candidate.txid_ != 0 && !committed_txids.contains(candidate.txid_)) ||
        (candidate.batch_txid_ != 0 &&
         !committed_txids.contains(candidate.batch_txid_))) {
      continue;
    }
    if (!candidate.id_.valid() || candidate.sequence_ == 0 ||
        (candidate.retired_ && candidate.field_count_ != 0) ||
        candidate.field_count_ > std::numeric_limits<std::uint32_t>::max()) {
      return absl::DataLossError("invalid recovered Hash group metadata");
    }
    auto [it, inserted] = winners.try_emplace(candidate.id_, candidate);
    if (!inserted) {
      auto& previous = it->second;
      // Relocation copies of one logical version must agree on its logical
      // contents even when the cleaner cleared a committed transaction tag.
      if (candidate.sequence_ == previous.sequence_ &&
          (candidate.field_count_ != previous.field_count_ ||
           candidate.retired_ != previous.retired_)) {
        return absl::DataLossError("conflicting Hash group relocation copies");
      }
      if (candidate.sequence_ > previous.sequence_ ||
          (candidate.sequence_ == previous.sequence_ &&
           candidate.lsn_ > previous.lsn_)) {
        previous = candidate;
      }
    }
  }
  HashGroupDirectory directory;
  directory.root_ = root;
  directory.sequence_ = root.revision_;
  directory.command_sequence_ = root_sequence;
  std::uint64_t count = 0;
  for (const auto& [id, candidate] : winners) {
    if (candidate.retired_) {
      auto inserted = directory.retired_.Set(id, candidate);
      if (!inserted.ok()) return inserted;
      continue;
    }
    if (candidate.field_count_ > root.field_count_ - count ||
        directory.groups_.find(id.prefix_) != directory.groups_.end()) {
      return absl::DataLossError("overlapping Hash groups or invalid count");
    }
    auto inserted = directory.groups_.Set(id.prefix_, candidate);
    if (!inserted.ok()) return inserted;
    count += candidate.field_count_;
  }
  if (count != root.field_count_ ||
      directory.groups_.size() != root.group_count_) {
    return absl::DataLossError("grouped Hash recovery aggregate mismatch");
  }
  std::uint64_t next = 0;
  bool complete = false;
  for (const auto& [prefix, candidate] : directory.groups_) {
    if (complete || prefix != next) {
      return absl::DataLossError("grouped Hash routing has gaps or overlaps");
    }
    const std::uint64_t last = candidate.id_.last();
    complete = last == std::numeric_limits<std::uint64_t>::max();
    if (!complete) next = last + 1;
  }
  if (!complete) {
    return absl::DataLossError("grouped Hash routing is incomplete");
  }
  return directory;
}

const RecoveredHashGroup* HashGroupDirectory::Find(
    std::string_view field) const noexcept {
  const std::uint64_t hash = ComputeDigest(field, root_.seed_).value_;
  const auto* group = groups_.Floor(hash);
  return group && group->id_.contains(hash) ? group : nullptr;
}

absl::StatusOr<HashGroupDirectory> HashGroupDirectory::Apply(
    const GroupedHashRoot& root, std::uint64_t sequence,
    std::span<const RecoveredHashGroup> changes) const {
  if (!ValidRoot(root) || root.incarnation_ != root_.incarnation_ ||
      root.seed_ != root_.seed_ || sequence < command_sequence_ ||
      root.revision_ <= sequence_ || changes.empty()) {
    return absl::FailedPreconditionError("invalid grouped directory update");
  }
  HashGroupDirectory next = *this;
  next.root_ = root;
  next.sequence_ = root.revision_;
  next.command_sequence_ = sequence;
  std::map<HashGroupId, const RecoveredHashGroup*> writes;
  std::uint64_t count = root_.field_count_;
  // Hash-prefix leaves partition a 64-bit domain. A wider scratch accumulator
  // verifies total coverage after local replacements, without scanning every
  // unchanged route. Pairwise overlap checks below make equal coverage imply
  // there are no gaps either.
  __uint128_t coverage = static_cast<__uint128_t>(1) << 64;
  for (const auto& change : changes) {
    if (!change.id_.valid() || change.incarnation_ != root.incarnation_ ||
        change.sequence_ != root.revision_ ||
        change.field_count_ > std::numeric_limits<std::uint32_t>::max() ||
        (change.retired_ && change.field_count_ != 0) ||
        !writes.emplace(change.id_, &change).second) {
      return absl::DataLossError("invalid grouped directory mutation record");
    }
    const auto current = groups_.find(change.id_.prefix_);
    if (current != groups_.end() && current->second.id_ == change.id_) {
      count -= current->second.field_count_;
      coverage -= static_cast<__uint128_t>(1) << (64 - change.id_.bits_);
      const auto erased = next.groups_.Erase(change.id_.prefix_);
      if (!erased.ok()) return erased;
    } else if (change.retired_) {
      return absl::DataLossError("retiring a non-current Hash group");
    }
    if (change.retired_) {
      const auto retired = next.retired_.Set(change.id_, change);
      if (!retired.ok()) return retired;
    }
  }
  for (const auto& [id, changed] : writes) {
    const auto& change = *changed;
    if (change.retired_) continue;
    if (next.retired_.find(id) != next.retired_.end()) {
      return absl::DataLossError("resurrecting a retired Hash group");
    }
    const auto* floor = next.groups_.Floor(id.prefix_);
    if (floor && floor->id_.last() >= id.prefix_) {
      return absl::DataLossError("group update overlaps an earlier route");
    }
    const auto inserted = next.groups_.Set(id.prefix_, change);
    if (!inserted.ok()) return inserted;
    auto after = next.groups_.find(id.prefix_);
    ++after;
    if (after != next.groups_.end() && after->first <= id.last()) {
      return absl::DataLossError("group update overlaps a later route");
    }
    count += change.field_count_;
    coverage += static_cast<__uint128_t>(1) << (64 - id.bits_);
  }
  if (count != root.field_count_ || next.groups_.size() != root.group_count_ ||
      coverage != (static_cast<__uint128_t>(1) << 64)) {
    return absl::DataLossError(
        "group directory update has gaps or count mismatch");
  }
  return next;
}

absl::StatusOr<HashGroupMutationPlan> PlanHashGroupMutation(
    const HashGroupDirectory& directory,
    std::vector<LoadedHashGroup> loaded_groups, HashGroupMutationKind kind,
    std::span<const std::string_view> fields,
    std::span<const std::string_view> values, std::size_t target_bytes) {
  const bool deleting = kind == HashGroupMutationKind::kDelete;
  if ((!deleting && fields.size() != values.size()) ||
      (deleting && !values.empty()) || target_bytes < kCompactHeaderBytes ||
      (kind != HashGroupMutationKind::kSet &&
       kind != HashGroupMutationKind::kSetIfAbsent && !deleting)) {
    return absl::InvalidArgumentError("invalid grouped Hash mutation operands");
  }
  std::map<HashGroupId, LoadedHashGroup*> loaded_by_id;
  for (auto& loaded : loaded_groups) {
    const auto& group = loaded.snapshot_;
    auto current = directory.groups().find(group.id_.prefix_);
    if (group.retired_ || group.incarnation_ != directory.root().incarnation_ ||
        current == directory.groups().end() ||
        current->second.id_ != group.id_ ||
        loaded.sequence_ != current->second.sequence_ ||
        group.value_.entries_.size() != current->second.field_count_ ||
        !loaded_by_id.emplace(group.id_, &loaded).second) {
      return absl::FailedPreconditionError(
          "stale or duplicate loaded Hash group");
    }
    auto valid = ValidateFields(group, &directory.root().seed_);
    if (!valid.ok()) return valid;
  }

  HashGroupMutationPlan plan{
      .root_ = directory.root(),
      .expected_sequence_ = directory.sequence(),
      .writes_ = {},
  };
  std::map<HashGroupId, bool> changed;
  for (std::size_t i = 0; i < fields.size(); ++i) {
    if (fields[i].size() > kMaxStringBytes ||
        (!deleting && values[i].size() > kMaxStringBytes)) {
      return absl::OutOfRangeError("Hash field or value exceeds Redis limit");
    }
    const auto* selected = directory.Find(fields[i]);
    if (selected == nullptr) {
      return absl::DataLossError("grouped Hash directory has no field route");
    }
    auto found = loaded_by_id.find(selected->id_);
    if (found == loaded_by_id.end()) {
      return absl::FailedPreconditionError(
          "mutation is missing an affected Hash group");
    }
    auto& entries = found->second->snapshot_.value_.entries_;
    auto entry = std::find_if(entries.begin(), entries.end(),
                              [&](const HashEntry& candidate) {
                                return candidate.field_ == fields[i];
                              });
    if (deleting) {
      if (entry != entries.end()) {
        entries.erase(entry);
        ++plan.affected_fields_;
        changed[selected->id_] = true;
      }
    } else if (entry == entries.end()) {
      entries.push_back({.digest_ = ComputeDigest(fields[i]),
                         .field_ = std::string(fields[i]),
                         .value_ = std::string(values[i])});
      ++plan.affected_fields_;
      changed[selected->id_] = true;
    } else if (kind == HashGroupMutationKind::kSet &&
               entry->value_ != values[i]) {
      entry->value_ = values[i];
      changed[selected->id_] = true;
    }
  }
  plan.changed_ = !changed.empty();
  if (!plan.changed_) return plan;
  if (deleting) {
    plan.root_.field_count_ -= plan.affected_fields_;
  } else {
    if (plan.affected_fields_ >
        std::numeric_limits<std::uint32_t>::max() - plan.root_.field_count_) {
      return absl::OutOfRangeError(
          "grouped Hash cardinality exceeds storage limit");
    }
    plan.root_.field_count_ += plan.affected_fields_;
  }
  if (plan.root_.field_count_ == 0) {
    plan.delete_key_ = true;
    plan.root_.group_count_ = 0;
    return plan;
  }
  for (const auto& [id, was_changed] : changed) {
    (void)was_changed;
    auto replacements =
        SplitHashGroup(std::move(loaded_by_id.at(id)->snapshot_),
                       directory.root().seed_, target_bytes);
    if (!replacements.ok()) return replacements.status();
    if (replacements->size() > 1) {
      if (replacements->size() - 1 >
          std::numeric_limits<std::uint32_t>::max() - plan.root_.group_count_) {
        return absl::OutOfRangeError(
            "grouped Hash directory exceeds storage limit");
      }
      plan.root_.group_count_ += replacements->size() - 1;
      plan.writes_.push_back({.incarnation_ = plan.root_.incarnation_,
                              .id_ = id,
                              .retired_ = true,
                              .value_ = {}});
    }
    for (auto& replacement : *replacements) {
      plan.writes_.push_back(std::move(replacement));
    }
  }
  return plan;
}

}  // namespace keylane::storage
