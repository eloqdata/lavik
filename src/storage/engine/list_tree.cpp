#include "impl.h"

namespace keylane::storage {

namespace {

constexpr std::string_view kListMagic = "KLL1";
constexpr std::size_t kListHeaderBytes = 8;
constexpr std::size_t kCompactPromoteBytes = 64 * 1024;
constexpr std::size_t kCompactDemoteBytes = 32 * 1024;
constexpr std::size_t kSegmentMaxBytes = 1024 * 1024;
constexpr std::size_t kSegmentSplitTargetBytes = 512 * 1024;
constexpr std::size_t kSegmentMergeTriggerBytes = 256 * 1024;
constexpr std::size_t kSegmentMergeMaxBytes = 768 * 1024;
constexpr std::size_t kDirectoryBytes = 32 * 1024;
constexpr std::size_t kLeafMaxEntries =
    (kDirectoryBytes - sizeof(ListDirectoryHeader)) /
    sizeof(ListDirectorySegment);
constexpr std::size_t kInternalMaxEntries =
    (kDirectoryBytes - sizeof(ListDirectoryHeader)) /
    sizeof(ListDirectoryChild);

void AppendU32(std::string* output, std::uint32_t value) {
  output->push_back(static_cast<char>(value));
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value >> 16));
  output->push_back(static_cast<char>(value >> 24));
}

bool ReadU32(std::string_view input, std::size_t* offset,
             std::uint32_t* value) {
  if (*offset > input.size() || input.size() - *offset < sizeof(*value)) {
    return false;
  }
  const auto* bytes =
      reinterpret_cast<const unsigned char*>(input.data()) + *offset;
  *value = static_cast<std::uint32_t>(bytes[0]) |
           (static_cast<std::uint32_t>(bytes[1]) << 8) |
           (static_cast<std::uint32_t>(bytes[2]) << 16) |
           (static_cast<std::uint32_t>(bytes[3]) << 24);
  *offset += sizeof(*value);
  return true;
}

absl::StatusOr<std::vector<std::string>> DecodeSegment(
    std::string_view encoded, std::uint64_t expected_count) {
  if (expected_count > std::numeric_limits<std::uint32_t>::max() ||
      encoded.size() < kListHeaderBytes ||
      encoded.substr(0, kListMagic.size()) != kListMagic) {
    return absl::InternalError("invalid persisted List segment");
  }
  std::size_t offset = kListMagic.size();
  std::uint32_t count = 0;
  if (!ReadU32(encoded, &offset, &count) || count != expected_count) {
    return absl::InternalError("List segment count does not match directory");
  }
  std::vector<std::string> elements;
  elements.reserve(count);
  for (std::uint32_t index = 0; index < count; ++index) {
    std::uint32_t bytes = 0;
    if (!ReadU32(encoded, &offset, &bytes) || offset > encoded.size() ||
        bytes > encoded.size() - offset) {
      return absl::InternalError("persisted List segment is truncated");
    }
    elements.emplace_back(encoded.substr(offset, bytes));
    offset += bytes;
  }
  if (offset != encoded.size()) {
    return absl::InternalError("persisted List segment has trailing bytes");
  }
  return elements;
}

absl::StatusOr<std::string> EncodeSegment(
    std::span<const std::string> elements) {
  if (elements.empty() ||
      elements.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::OutOfRangeError("invalid List segment element count");
  }
  std::uint64_t bytes = kListHeaderBytes;
  for (const std::string& element : elements) {
    if (element.size() > kMaxStringBytes ||
        bytes > kMaxStringBytes - sizeof(std::uint32_t) ||
        element.size() > kMaxStringBytes - bytes - sizeof(std::uint32_t)) {
      return absl::OutOfRangeError("List element exceeds storage limits");
    }
    bytes += sizeof(std::uint32_t) + element.size();
  }
  std::string output;
  output.reserve(static_cast<std::size_t>(bytes));
  output.append(kListMagic);
  AppendU32(&output, static_cast<std::uint32_t>(elements.size()));
  for (const std::string& element : elements) {
    AppendU32(&output, static_cast<std::uint32_t>(element.size()));
    output.append(element);
  }
  return output;
}

std::uint64_t EncodedBytes(std::span<const std::string> elements) {
  std::uint64_t bytes = kListHeaderBytes;
  for (const std::string& element : elements) {
    bytes += sizeof(std::uint32_t) + element.size();
  }
  return bytes;
}

std::optional<std::uint64_t> NormalizeIndex(std::int64_t index,
                                            std::uint64_t size) {
  if (index < 0) {
    const std::uint64_t magnitude =
        index == std::numeric_limits<std::int64_t>::min()
            ? std::uint64_t{1} << 63
            : static_cast<std::uint64_t>(-index);
    if (magnitude > size) return std::nullopt;
    return size - magnitude;
  }
  const std::uint64_t converted = static_cast<std::uint64_t>(index);
  return converted < size ? std::optional(converted) : std::nullopt;
}

std::pair<std::uint64_t, std::uint64_t> NormalizeRange(std::int64_t start,
                                                       std::int64_t stop,
                                                       std::uint64_t size) {
  if (size == 0) return {0, 0};
  auto position = [size](std::int64_t value) -> std::int64_t {
    if (value >= 0) return value;
    if (value == std::numeric_limits<std::int64_t>::min()) {
      return std::numeric_limits<std::int64_t>::min();
    }
    if (size > static_cast<std::uint64_t>(
                   std::numeric_limits<std::int64_t>::max())) {
      return value;
    }
    return static_cast<std::int64_t>(size) + value;
  };
  std::int64_t first = position(start);
  std::int64_t last = position(stop);
  if (first < 0) first = 0;
  if (last < 0 || static_cast<std::uint64_t>(first) >= size || first > last) {
    return {size, size};
  }
  const std::uint64_t begin = static_cast<std::uint64_t>(first);
  const std::uint64_t end =
      static_cast<std::uint64_t>(last) >= size - 1
          ? size
          : static_cast<std::uint64_t>(last) + 1;
  return begin < end ? std::pair(begin, end) : std::pair(size, size);
}

std::uint64_t UnsignedMagnitude(std::int64_t value) {
  // -(INT64_MIN) is undefined. Moving one step toward zero before negating
  // keeps the calculation representable and produces the exact magnitude.
  return value < 0
             ? static_cast<std::uint64_t>(-(value + 1)) + 1
             : static_cast<std::uint64_t>(value);
}

struct ListRemovePlan {
  bool forward_ = true;
  std::uint64_t limit_ = 0;
};

ListRemovePlan NormalizeListRemoveLimit(std::int64_t count) {
  return ListRemovePlan{
      .forward_ = count >= 0,
      .limit_ = count == 0 ? std::numeric_limits<std::uint64_t>::max()
                           : UnsignedMagnitude(count),
  };
}

struct ListPositionPlan {
  bool reverse_ = false;
  std::uint64_t wanted_rank_ = 0;
  std::uint64_t return_limit_ = 0;
  std::uint64_t comparison_limit_ = 0;
};

ListPositionPlan NormalizeListPosition(const ListOperation& operation) {
  return ListPositionPlan{
      .reverse_ = operation.rank_ < 0,
      .wanted_rank_ = UnsignedMagnitude(operation.rank_),
      .return_limit_ =
          !operation.count_provided_
              ? 1
              : (operation.count_ == 0
                     ? std::numeric_limits<std::uint64_t>::max()
                     : operation.count_),
      .comparison_limit_ =
          operation.max_length_provided_ && operation.max_length_ != 0
              ? operation.max_length_
              : std::numeric_limits<std::uint64_t>::max(),
  };
}

struct PathStep {
  DirectRecordRef reference_;
  ListDirectoryNode node_;
  std::size_t selected_ = 0;
};

struct Path {
  std::vector<PathStep> steps_;
  std::uint64_t segment_start_ = 0;
};

struct NodeLink {
  DirectRecordRef reference_;
  std::uint64_t element_count_ = 0;
  std::uint64_t segment_count_ = 0;
  std::uint64_t encoded_bytes_ = 0;
  std::uint32_t level_ = 0;
};

struct FlatTree {
  std::vector<ListSegmentMeta> segments_;
  std::vector<DirectRecordRef> directories_;
};

void Recount(ListDirectoryNode* node) {
  node->element_count_ = 0;
  node->segment_count_ = 0;
  node->encoded_bytes_ = 0;
  if (node->leaf()) {
    for (const ListSegmentMeta& segment : node->segments_) {
      node->element_count_ += segment.element_count_;
      ++node->segment_count_;
      node->encoded_bytes_ += segment.encoded_bytes_;
    }
  } else {
    for (const ListDirectoryChildMeta& child : node->children_) {
      node->element_count_ += child.element_count_;
      node->segment_count_ += child.segment_count_;
      node->encoded_bytes_ += child.encoded_bytes_;
    }
  }
}

ListDirectoryChildMeta ChildOf(const NodeLink& link) {
  return ListDirectoryChildMeta{
      .child_ = link.reference_,
      .element_count_ = link.element_count_,
      .segment_count_ = link.segment_count_,
      .encoded_bytes_ = link.encoded_bytes_,
  };
}

}  // namespace

Task<absl::StatusOr<std::vector<RetiredRecord>>>
StorageEngine::Impl::CollectListTreeRetirements(
    WorkerStore& store, const ListState& state) {
  std::vector<RetiredRecord> retired;
  if (!state.directory_root_.valid()) co_return retired;
  std::vector<DirectRecordRef> directories{state.directory_root_};
  while (!directories.empty()) {
    DirectRecordRef reference = directories.back();
    directories.pop_back();
    auto cached = co_await LoadListDirectoryMeta(store, reference);
    if (!cached.ok()) co_return cached.status();
    absl::StatusOr<ListDirectoryNode> node =
        absl::InternalError("List directory metadata is missing");
    if (cached->has_value()) {
      node = std::move(**cached);
    } else {
      auto payload =
          co_await LoadCollectionObject(store, reference, ValueType::kList);
      if (!payload.ok()) co_return payload.status();
      node = DecodeListDirectory(std::as_bytes(
          std::span<const char>(payload->data(), payload->size())));
      if (!node.ok()) co_return node.status();
    }
    auto directory_retirement =
        co_await ResolveCollectionRetirement(store, reference);
    if (!directory_retirement.ok()) co_return directory_retirement.status();
    retired.push_back(std::move(*directory_retirement));
    if (node->leaf()) {
      for (const ListSegmentMeta& segment : node->segments_) {
        auto segment_retirement =
            co_await ResolveCollectionRetirement(store, segment.segment_);
        if (!segment_retirement.ok()) co_return segment_retirement.status();
        retired.push_back(std::move(*segment_retirement));
      }
    } else {
      for (const ListDirectoryChildMeta& child : node->children_) {
        directories.push_back(child.child_);
      }
    }
  }
  co_return retired;
}

Task<absl::StatusOr<std::string>>
StorageEngine::Impl::MaterializeListValueLocked(WorkerStore& store,
                                                const ListState& state) {
  if (!state.directory_root_.valid() ||
      state.element_count_ > std::numeric_limits<std::uint32_t>::max()) {
    co_return absl::InternalError("invalid segmented List state");
  }
  if (state.encoded_bytes_ > std::numeric_limits<std::size_t>::max()) {
    co_return absl::ResourceExhaustedError(
        "segmented List is too large to materialize for replication");
  }

  std::string portable;
  portable.reserve(static_cast<std::size_t>(state.encoded_bytes_));
  portable.append(kListMagic);
  AppendU32(&portable, static_cast<std::uint32_t>(state.element_count_));

  std::vector<DirectRecordRef> pending{state.directory_root_};
  std::uint64_t seen_elements = 0;
  while (!pending.empty()) {
    const DirectRecordRef reference = pending.back();
    pending.pop_back();
    auto cached = co_await LoadListDirectoryMeta(store, reference);
    if (!cached.ok()) co_return cached.status();
    absl::StatusOr<ListDirectoryNode> node =
        absl::InternalError("List directory metadata is missing");
    if (cached->has_value()) {
      node = std::move(**cached);
    } else {
      auto payload =
          co_await LoadCollectionObject(store, reference, ValueType::kList);
      if (!payload.ok()) co_return payload.status();
      node = DecodeListDirectory(std::as_bytes(
          std::span<const char>(payload->data(), payload->size())));
      if (!node.ok()) co_return node.status();
    }
    if (!node->leaf()) {
      for (auto child = node->children_.rbegin();
           child != node->children_.rend(); ++child) {
        pending.push_back(child->child_);
      }
      continue;
    }
    for (const ListSegmentMeta& segment : node->segments_) {
      auto encoded = co_await LoadCollectionObject(
          store, segment.segment_, ValueType::kList);
      if (!encoded.ok()) co_return encoded.status();
      auto elements = DecodeSegment(*encoded, segment.element_count_);
      if (!elements.ok()) co_return elements.status();
      for (const std::string& element : *elements) {
        AppendU32(&portable, static_cast<std::uint32_t>(element.size()));
        portable.append(element);
        ++seen_elements;
      }
    }
  }
  // state.encoded_bytes_ is the sum of independently encoded segments and
  // therefore includes one KLL1 header per segment. The portable image has a
  // single header, so only cardinality (plus every decoded segment's own
  // validated byte count above) is representation-independent here.
  if (seen_elements != state.element_count_) {
    co_return absl::InternalError(
        "segmented List totals changed while materializing");
  }
  co_return portable;
}

Task<absl::StatusOr<ListResult>> StorageEngine::Impl::ExecuteListLocked(
    std::uint8_t db_id, std::string_view key, const Digest& digest,
    const ListOperation& operation, TxShardWrites* tx) {
  assert(db_id < kLogicalDatabaseCount);
  WorkerStore& store = CurrentStore();
  auto& partition = PartitionForKey(store, key);
  co_await store.store_state_mutex_.Lock();
  UnlockGuard unlock(&store.store_state_mutex_, store.worker_);

  auto& index = partition.indexes_[db_id];
  auto* found = index.Find(digest, key);
  if (found != nullptr && !found->key_complete()) [[unlikely]] {
    auto resolved = co_await FindVerifiedEntry(store, index, digest, key);
    if (!resolved.ok()) co_return resolved.status();
    found = *resolved;
  }
  const bool exists = found != nullptr &&
                      found->value_.kind_ == RecordKind::kValue &&
                      !IsExpired(found->value_, UnixTimeMillis());
  if (exists && found->value_.value_type_ != ValueType::kList) {
    co_return absl::InvalidArgumentError(
        "WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  const bool segmented =
      exists && found->value_.logical_size_ == kSegmentedCollection;
  std::optional<ListState> original_state =
      segmented ? ListStateFor(store, found) : std::optional<ListState>{};
  if (segmented && !original_state.has_value()) {
    co_return absl::InternalError("segmented List side state is missing");
  }

  const bool read_only = operation.kind_ == ListOperationKind::kLength ||
                         operation.kind_ == ListOperationKind::kIndex ||
                         operation.kind_ == ListOperationKind::kRange ||
                         operation.kind_ == ListOperationKind::kPosition;

  ListResult result;
  result.key_exists_ = exists;
  result.length_ = segmented
                       ? original_state->element_count_
                       : (exists ? found->value_.logical_size_ : 0);
  if (operation.kind_ == ListOperationKind::kLength) co_return result;

  // The key lock fixes the logical root for the operation. Physical object
  // readers pin each block before suspending, so read-only traversal need not
  // serialize unrelated store metadata and allocation work for its full IO.
  if (read_only) unlock.Unlock();

  const std::uint64_t expire_at_ms = exists ? found->value_.expire_at_ms_ : 0;
  ListState working = segmented ? *original_state : ListState{};
  bool allocated_owner = false;
  auto ensure_owner = [&]() -> absl::Status {
    if (working.owner_id_ == 0) {
      const std::uint64_t stride = worker_count_;
      const std::uint64_t owner_id = store.next_list_owner_id_;
      if (owner_id == 0) {
        return absl::ResourceExhaustedError(
            "runtime List owner id space is exhausted");
      }
      store.next_list_owner_id_ =
          owner_id > std::numeric_limits<std::uint64_t>::max() - stride
              ? 0
              : owner_id + stride;
      working.owner_id_ = owner_id;
      allocated_owner = true;
    }
    store.list_owners_.insert_or_assign(
        working.owner_id_,
        ListOwnerRuntime{.db_id_ = db_id,
                         .key_ = std::string(key),
                         .digest_ = digest,
                         .index_generation_ =
                             store.index_generations_[db_id]});
    return absl::OkStatus();
  };
  if (segmented) {
    absl::Status bound = ensure_owner();
    if (!bound.ok()) co_return bound;
  }
  std::vector<DirectRecordRef> new_objects;
  std::vector<DirectRecordRef> retire_after_publish;

  auto load_node = [&](const DirectRecordRef& reference)
      -> Task<absl::StatusOr<ListDirectoryNode>> {
    auto cached = co_await LoadListDirectoryMeta(store, reference);
    if (!cached.ok()) co_return cached.status();
    if (cached->has_value()) co_return std::move(**cached);
    auto payload =
        co_await LoadCollectionObject(store, reference, ValueType::kList);
    if (!payload.ok()) co_return payload.status();
    co_return DecodeListDirectory(std::as_bytes(
        std::span<const char>(payload->data(), payload->size())));
  };

  auto load_segment = [&](const ListSegmentMeta& segment)
      -> Task<absl::StatusOr<std::vector<std::string>>> {
    auto payload = co_await LoadCollectionObject(store, segment.segment_,
                                                 ValueType::kList);
    if (!payload.ok()) co_return payload.status();
    if (payload->size() != segment.encoded_bytes_) {
      co_return absl::InternalError("List segment byte count mismatch");
    }
    co_return DecodeSegment(*payload, segment.element_count_);
  };

  auto flatten = [&](const ListState& state)
      -> Task<absl::StatusOr<FlatTree>> {
    FlatTree flat;
    std::vector<DirectRecordRef> pending{state.directory_root_};
    while (!pending.empty()) {
      DirectRecordRef reference = pending.back();
      pending.pop_back();
      auto decoded = co_await load_node(reference);
      if (!decoded.ok()) co_return decoded.status();
      flat.directories_.push_back(reference);
      if (decoded->leaf()) {
        flat.segments_.insert(flat.segments_.end(), decoded->segments_.begin(),
                              decoded->segments_.end());
      } else {
        for (auto it = decoded->children_.rbegin();
             it != decoded->children_.rend(); ++it) {
          pending.push_back(it->child_);
        }
      }
    }
    co_return flat;
  };

  auto find_path = [&](const ListState& state, std::uint64_t position,
                       bool allow_end) -> Task<absl::StatusOr<Path>> {
    if (state.element_count_ == 0 ||
        position > state.element_count_ ||
        (position == state.element_count_ && !allow_end)) {
      co_return absl::OutOfRangeError("List index out of range");
    }
    Path path;
    DirectRecordRef next = state.directory_root_;
    std::uint64_t remaining = position;
    std::uint64_t prefix = 0;
    while (true) {
      auto decoded = co_await load_node(next);
      if (!decoded.ok()) co_return decoded.status();
      if (decoded->leaf()) {
        std::size_t selected = 0;
        for (; selected < decoded->segments_.size(); ++selected) {
          const auto count = decoded->segments_[selected].element_count_;
          if (remaining < count ||
              (allow_end && remaining == count &&
               selected + 1 == decoded->segments_.size())) {
            break;
          }
          remaining -= count;
          prefix += count;
        }
        if (selected == decoded->segments_.size()) {
          co_return absl::InternalError("List directory totals are corrupt");
        }
        path.steps_.push_back(
            PathStep{next, std::move(*decoded), selected});
        path.segment_start_ = prefix;
        co_return path;
      }
      std::size_t selected = 0;
      for (; selected < decoded->children_.size(); ++selected) {
        const auto count = decoded->children_[selected].element_count_;
        if (remaining < count ||
            (allow_end && remaining == count &&
             selected + 1 == decoded->children_.size())) {
          break;
        }
        remaining -= count;
        prefix += count;
      }
      if (selected == decoded->children_.size()) {
        co_return absl::InternalError("List directory totals are corrupt");
      }
      DirectRecordRef child = decoded->children_[selected].child_;
      path.steps_.push_back(PathStep{next, std::move(*decoded), selected});
      next = child;
    }
  };

  auto write_directory_nodes =
      [&](const ListDirectoryNode& source)
      -> Task<absl::StatusOr<std::vector<NodeLink>>> {
    const std::size_t count =
        source.leaf() ? source.segments_.size() : source.children_.size();
    if (count == 0) co_return std::vector<NodeLink>{};
    const std::size_t maximum =
        source.leaf() ? kLeafMaxEntries : kInternalMaxEntries;
    std::vector<NodeLink> output;
    for (std::size_t begin = 0; begin < count; begin += maximum) {
      const std::size_t end = std::min(count, begin + maximum);
      ListDirectoryNode node;
      node.level_ = source.level_;
      if (source.leaf()) {
        node.segments_.assign(source.segments_.begin() + begin,
                              source.segments_.begin() + end);
      } else {
        node.children_.assign(source.children_.begin() + begin,
                              source.children_.begin() + end);
      }
      Recount(&node);
      const std::string encoded = EncodeListDirectory(node);
      auto written = co_await WriteCollectionObjectLocked(
          store, db_id, ValueType::kList, encoded, tx, 0,
          working.owner_id_);
      if (!written.ok()) co_return written.status();
      new_objects.push_back(*written);
      output.push_back(NodeLink{
          .reference_ = *written,
          .element_count_ = node.element_count_,
          .segment_count_ = node.segment_count_,
          .encoded_bytes_ = node.encoded_bytes_,
          .level_ = node.level_,
      });
    }
    co_return output;
  };

  auto replace_path =
      [&](Path path, std::size_t replace_begin, std::size_t replace_count,
          std::vector<ListSegmentMeta> replacements) -> Task<absl::Status> {
    if (path.steps_.empty() || !path.steps_.back().node_.leaf()) {
      co_return absl::InternalError("invalid List update path");
    }
    ListDirectoryNode leaf = path.steps_.back().node_;
    if (replace_begin > leaf.segments_.size() ||
        replace_count > leaf.segments_.size() - replace_begin) {
      co_return absl::InternalError("invalid List segment replacement");
    }
    for (std::size_t i = 0; i < replace_count; ++i) {
      retire_after_publish.push_back(
          leaf.segments_[replace_begin + i].segment_);
    }
    leaf.segments_.erase(leaf.segments_.begin() + replace_begin,
                         leaf.segments_.begin() + replace_begin + replace_count);
    leaf.segments_.insert(leaf.segments_.begin() + replace_begin,
                          replacements.begin(), replacements.end());
    Recount(&leaf);
    retire_after_publish.push_back(path.steps_.back().reference_);
    auto links = co_await write_directory_nodes(leaf);
    if (!links.ok()) co_return links.status();

    for (std::size_t depth = path.steps_.size() - 1; depth > 0; --depth) {
      PathStep& parent_step = path.steps_[depth - 1];
      ListDirectoryNode parent = parent_step.node_;
      const std::size_t selected = parent_step.selected_;
      parent.children_.erase(parent.children_.begin() + selected);
      std::vector<ListDirectoryChildMeta> children;
      children.reserve(links->size());
      for (const NodeLink& link : *links) children.push_back(ChildOf(link));
      parent.children_.insert(parent.children_.begin() + selected,
                              children.begin(), children.end());
      Recount(&parent);
      retire_after_publish.push_back(parent_step.reference_);
      if (depth == 1 && parent.children_.size() == 1) {
        const auto& child = parent.children_.front();
        links = std::vector<NodeLink>{NodeLink{
            .reference_ = child.child_,
            .element_count_ = child.element_count_,
            .segment_count_ = child.segment_count_,
            .encoded_bytes_ = child.encoded_bytes_,
            .level_ = parent.level_ - 1,
        }};
      } else {
        links = co_await write_directory_nodes(parent);
        if (!links.ok()) co_return links.status();
      }
    }

    while (links->size() > 1) {
      ListDirectoryNode parent;
      parent.level_ = links->front().level_ + 1;
      parent.children_.reserve(links->size());
      for (const NodeLink& link : *links) {
        parent.children_.push_back(ChildOf(link));
      }
      Recount(&parent);
      links = co_await write_directory_nodes(parent);
      if (!links.ok()) co_return links.status();
    }
    if (links->empty()) {
      const std::uint64_t owner_id = working.owner_id_;
      working = ListState{};
      working.owner_id_ = owner_id;
    } else {
      const NodeLink& root = links->front();
      working = ListState{
          .element_count_ = root.element_count_,
          .encoded_bytes_ = root.encoded_bytes_,
          .segment_count_ = static_cast<std::uint32_t>(root.segment_count_),
          .tree_height_ = root.level_ + 1,
          .directory_root_ = root.reference_,
          .owner_id_ = working.owner_id_,
      };
    }
    co_return absl::OkStatus();
  };

  auto split_and_write =
      [&](std::span<const std::string> elements)
      -> Task<absl::StatusOr<std::vector<ListSegmentMeta>>> {
    std::vector<ListSegmentMeta> segments;
    std::size_t begin = 0;
    while (begin < elements.size()) {
      std::size_t end = begin;
      std::uint64_t bytes = kListHeaderBytes;
      while (end < elements.size()) {
        const std::uint64_t addition =
            sizeof(std::uint32_t) + elements[end].size();
        if (end != begin &&
            (bytes + addition > kSegmentMaxBytes ||
             bytes >= kSegmentSplitTargetBytes)) {
          break;
        }
        bytes += addition;
        ++end;
      }
      auto encoded = EncodeSegment(elements.subspan(begin, end - begin));
      if (!encoded.ok()) co_return encoded.status();
      auto written = co_await WriteCollectionObjectLocked(
          store, db_id, ValueType::kList, *encoded, tx, end - begin,
          working.owner_id_);
      if (!written.ok()) co_return written.status();
      new_objects.push_back(*written);
      segments.push_back(ListSegmentMeta{
          .segment_ = *written,
          .element_count_ = end - begin,
          .encoded_bytes_ = encoded->size(),
      });
      begin = end;
    }
    co_return segments;
  };

  auto rewrite_segment =
      [&](Path path, std::vector<std::string> elements,
          bool allow_merge) -> Task<absl::Status> {
    auto& leaf = path.steps_.back().node_;
    std::size_t begin = path.steps_.back().selected_;
    std::size_t count = 1;
    if (allow_merge && !elements.empty() &&
        EncodedBytes(elements) < kSegmentMergeTriggerBytes) {
      std::optional<std::size_t> neighbor;
      if (begin > 0) {
        neighbor = begin - 1;
      } else if (begin + 1 < leaf.segments_.size()) {
        neighbor = begin + 1;
      }
      if (neighbor.has_value()) {
        auto adjacent = co_await load_segment(leaf.segments_[*neighbor]);
        if (!adjacent.ok()) co_return adjacent.status();
        std::vector<std::string> merged;
        if (*neighbor < begin) {
          merged = *adjacent;
          merged.insert(merged.end(), elements.begin(), elements.end());
        } else {
          merged = elements;
          merged.insert(merged.end(), adjacent->begin(), adjacent->end());
        }
        if (EncodedBytes(merged) <= kSegmentMergeMaxBytes) {
          if (*neighbor < begin) begin = *neighbor;
          elements = std::move(merged);
          count = 2;
        }
      }
    }
    auto replacements = co_await split_and_write(elements);
    if (!replacements.ok()) co_return replacements.status();
    co_return co_await replace_path(std::move(path), begin, count,
                                    std::move(*replacements));
  };

  auto build_tree = [&](std::vector<ListSegmentMeta> segments)
      -> Task<absl::StatusOr<ListState>> {
    ListDirectoryNode leaves;
    leaves.level_ = 0;
    leaves.segments_ = std::move(segments);
    Recount(&leaves);
    auto links = co_await write_directory_nodes(leaves);
    if (!links.ok()) co_return links.status();
    while (links->size() > 1) {
      ListDirectoryNode parent;
      parent.level_ = links->front().level_ + 1;
      for (const NodeLink& link : *links) {
        parent.children_.push_back(ChildOf(link));
      }
      Recount(&parent);
      links = co_await write_directory_nodes(parent);
      if (!links.ok()) co_return links.status();
    }
    if (links->empty()) co_return absl::InternalError("empty List tree");
    const NodeLink& root = links->front();
    co_return ListState{
        .element_count_ = root.element_count_,
        .encoded_bytes_ = root.encoded_bytes_,
        .segment_count_ = static_cast<std::uint32_t>(root.segment_count_),
        .tree_height_ = root.level_ + 1,
        .directory_root_ = root.reference_,
        .owner_id_ = working.owner_id_,
    };
  };

  auto materialize = [&](const ListState& state)
      -> Task<absl::StatusOr<std::vector<std::string>>> {
    auto flat = co_await flatten(state);
    if (!flat.ok()) co_return flat.status();
    std::vector<std::string> all;
    if (state.element_count_ <= std::numeric_limits<std::size_t>::max()) {
      all.reserve(static_cast<std::size_t>(state.element_count_));
    }
    for (const ListSegmentMeta& segment : flat->segments_) {
      auto decoded = co_await load_segment(segment);
      if (!decoded.ok()) co_return decoded.status();
      all.insert(all.end(), std::make_move_iterator(decoded->begin()),
                 std::make_move_iterator(decoded->end()));
    }
    co_return all;
  };

  auto collect_current_graph = [&]() -> Task<absl::Status> {
    if (working.element_count_ == 0) co_return absl::OkStatus();
    auto flat = co_await flatten(working);
    if (!flat.ok()) co_return flat.status();
    retire_after_publish.insert(retire_after_publish.end(),
                                flat->directories_.begin(),
                                flat->directories_.end());
    for (const ListSegmentMeta& segment : flat->segments_) {
      retire_after_publish.push_back(segment.segment_);
    }
    co_return absl::OkStatus();
  };

  auto cleanup_new_objects = [&]() -> absl::Status {
    absl::Status first = absl::OkStatus();
    for (const DirectRecordRef& reference : new_objects) {
      absl::Status dead = MarkRecordDeadLocal(
          store.worker_->id(), RetiredRecordOf(reference));
      if (first.ok() && !dead.ok()) first = dead;
    }
    if (tx != nullptr) {
      auto journal = store.tx_new_collection_objects_.find(tx->txid_);
      if (journal != store.tx_new_collection_objects_.end()) {
        std::erase_if(journal->second, [&](const RetiredRecord& record) {
          return std::any_of(
              new_objects.begin(), new_objects.end(),
              [&](const DirectRecordRef& reference) {
                return record.block_id_ == reference.block_id_ &&
                       record.allocation_epoch_ ==
                           reference.allocation_epoch_ &&
                       record.total_disk_bytes_ == reference.total_disk_bytes_;
              });
        });
        if (journal->second.empty()) {
          store.tx_new_collection_objects_.erase(journal);
        }
      }
    }
    if (allocated_owner) {
      store.list_owners_.erase(working.owner_id_);
    }
    return first;
  };
  bool root_published = false;
  struct NewObjectCleanupGuard {
    decltype(cleanup_new_objects)* cleanup_;
    bool* published_;
    ~NewObjectCleanupGuard() {
      if (!*published_) cleanup_->operator()().IgnoreError();
    }
  } new_object_cleanup{&cleanup_new_objects, &root_published};

  auto resolve_retirements = [&]()
      -> Task<absl::StatusOr<std::vector<RetiredRecord>>> {
    std::vector<RetiredRecord> records;
    records.reserve(retire_after_publish.size());
    for (const DirectRecordRef& reference : retire_after_publish) {
      auto retired = co_await ResolveCollectionRetirement(store, reference);
      if (!retired.ok()) co_return retired.status();
      records.push_back(std::move(*retired));
    }
    co_return records;
  };

  auto attach_retirements =
      [&](std::vector<RetiredRecord> resolved)
      -> std::shared_ptr<std::vector<RetiredRecord>> {
    auto records = std::make_shared<std::vector<RetiredRecord>>(
        std::move(resolved));
    if (tx != nullptr) {
      for (const RetiredRecord& record : *records) {
        tx->retirements_.push_back(TxShardWrites::Retired{
            .block_id_ = record.block_id_,
            .allocation_epoch_ = record.allocation_epoch_,
            .total_disk_bytes_ = record.total_disk_bytes_,
            .block_owner_ = record.block_owner_,
            .record_offset_ = record.record_offset_,
            .collection_object_ = true,
            .dependent_extents_ = record.dependent_extents_,
            .immediate_extents_ = nullptr,
        });
      }
      return nullptr;
    }
    return records;
  };

  auto publish = [&](std::optional<std::vector<std::string>> compact)
      -> Task<absl::Status> {
    std::string payload;
    RecordKind kind = RecordKind::kValue;
    ValueType type = ValueType::kList;
    std::uint64_t logical_size = kSegmentedCollection;
    std::optional<ListState> next_state;
    if (compact.has_value()) {
      if (compact->empty()) {
        kind = RecordKind::kTombstone;
        type = ValueType::kNone;
        logical_size = 0;
      } else {
        auto encoded = EncodeSegment(*compact);
        if (!encoded.ok()) co_return encoded.status();
        payload = std::move(*encoded);
        logical_size = compact->size();
      }
    } else {
      payload = EncodeListRoot(working);
      next_state = working;
      // Every referenced anonymous object precedes the root in the append
      // stream. A crash here must recover the old root and ignore this whole
      // unpublished COW path.
      KEYLANE_MAYBE_CRASH_AT("list-segments-durable");
    }

    auto resolved_retirements = co_await resolve_retirements();
    if (!resolved_retirements.ok()) co_return resolved_retirements.status();
    auto retirements = attach_retirements(std::move(*resolved_retirements));
    absl::Status written = co_await AppendLocked(
        store, partition, db_id, key, payload, kind, type,
        kind == RecordKind::kValue ? expire_at_ms : 0, tx, logical_size,
        std::move(retirements));
    if (!written.ok()) co_return written;
    root_published = true;
    RecordIndex::Entry* current = index.Find(digest, key);
    if (current == nullptr) {
      co_return absl::InternalError("published List root was not indexed");
    }
    if (next_state.has_value()) {
      store.list_states_.insert_or_assign(current, std::move(*next_state));
      store.list_owners_.insert_or_assign(
          working.owner_id_,
          ListOwnerRuntime{.db_id_ = db_id,
                           .key_ = std::string(key),
                           .digest_ = digest,
                           .index_generation_ =
                               store.index_generations_[db_id]});
    } else {
      store.list_states_.erase(current);
      if (working.owner_id_ != 0) {
        store.list_owners_.erase(working.owner_id_);
      }
    }
    KEYLANE_MAYBE_CRASH_AT("list-root-published");
    co_return absl::OkStatus();
  };

  // Compact Lists are bounded by 64 KiB, so materializing them is cheap. The
  // first mutation that crosses the promotion boundary builds the immutable
  // tree; segmented Lists below never take this path.
  if (!segmented) {
    std::vector<std::string> elements;
    if (exists) {
      auto loaded = co_await LoadValue(store, partition, db_id, key, digest,
                                       found->value_, ExtentsFor(store, found));
      if (!loaded.ok()) co_return loaded.status();
      const auto bytes = loaded->value();
      auto decoded = DecodeSegment(
          std::string_view(reinterpret_cast<const char*>(bytes.data()),
                           bytes.size()),
          found->value_.logical_size_);
      if (!decoded.ok()) co_return decoded.status();
      elements = std::move(*decoded);
    }

    switch (operation.kind_) {
      case ListOperationKind::kPushLeft:
      case ListOperationKind::kPushRight:
      case ListOperationKind::kPushLeftIfExists:
      case ListOperationKind::kPushRightIfExists: {
        const bool only_if_exists =
            operation.kind_ == ListOperationKind::kPushLeftIfExists ||
            operation.kind_ == ListOperationKind::kPushRightIfExists;
        if (!exists && only_if_exists) co_return result;
        const bool left = operation.kind_ == ListOperationKind::kPushLeft ||
                          operation.kind_ ==
                              ListOperationKind::kPushLeftIfExists;
        for (std::string_view value : operation.values_) {
          if (value.size() > kMaxStringBytes) {
            co_return absl::OutOfRangeError(
                "List element exceeds Redis-compatible 512 MiB limit");
          }
          if (left) {
            elements.insert(elements.begin(), std::string(value));
          } else {
            elements.emplace_back(value);
          }
        }
        result.changed_ = !operation.values_.empty();
        result.key_exists_ = true;
        break;
      }
      case ListOperationKind::kPopLeft:
      case ListOperationKind::kPopRight: {
        const std::size_t count = static_cast<std::size_t>(
            std::min<std::uint64_t>(operation.count_, elements.size()));
        const bool left = operation.kind_ == ListOperationKind::kPopLeft;
        for (std::size_t i = 0; i < count; ++i) {
          if (left) {
            result.values_.push_back(std::move(elements.front()));
            elements.erase(elements.begin());
          } else {
            result.values_.push_back(std::move(elements.back()));
            elements.pop_back();
          }
        }
        result.changed_ = count != 0;
        break;
      }
      case ListOperationKind::kIndex: {
        if (auto position = NormalizeIndex(operation.first_, elements.size())) {
          result.values_.push_back(elements[*position]);
        }
        co_return result;
      }
      case ListOperationKind::kRange: {
        const auto [begin, end] =
            NormalizeRange(operation.first_, operation.second_, elements.size());
        for (std::uint64_t i = begin; i < end; ++i) {
          result.values_.push_back(elements[i]);
        }
        co_return result;
      }
      case ListOperationKind::kSet: {
        if (!exists) co_return absl::NotFoundError("no such key");
        auto position = NormalizeIndex(operation.first_, elements.size());
        if (!position) co_return absl::OutOfRangeError("index out of range");
        elements[*position] = operation.value_;
        result.changed_ = true;
        break;
      }
      case ListOperationKind::kInsertBefore:
      case ListOperationKind::kInsertAfter: {
        if (!exists) co_return result;
        auto it = std::find(elements.begin(), elements.end(), operation.pivot_);
        if (it == elements.end()) {
          result.integer_ = -1;
          co_return result;
        }
        if (operation.kind_ == ListOperationKind::kInsertAfter) ++it;
        elements.insert(it, std::string(operation.value_));
        result.changed_ = true;
        result.integer_ = elements.size();
        break;
      }
      case ListOperationKind::kRemove: {
        const ListRemovePlan plan =
            NormalizeListRemoveLimit(operation.first_);
        std::uint64_t removed = 0;
        if (plan.forward_) {
          for (auto it = elements.begin();
               it != elements.end() && removed < plan.limit_;) {
            if (*it == operation.value_) {
              it = elements.erase(it);
              ++removed;
            } else {
              ++it;
            }
          }
        } else {
          for (std::size_t i = elements.size();
               i > 0 && removed < plan.limit_;) {
            --i;
            if (elements[i] == operation.value_) {
              elements.erase(elements.begin() + i);
              ++removed;
            }
          }
        }
        result.integer_ = removed;
        result.changed_ = removed != 0;
        break;
      }
      case ListOperationKind::kTrim: {
        const auto [begin, end] =
            NormalizeRange(operation.first_, operation.second_, elements.size());
        std::vector<std::string> kept;
        for (std::uint64_t i = begin; i < end; ++i) {
          kept.push_back(std::move(elements[i]));
        }
        result.changed_ = kept.size() != elements.size();
        elements = std::move(kept);
        break;
      }
      case ListOperationKind::kPosition: {
        const ListPositionPlan plan = NormalizeListPosition(operation);
        std::uint64_t matches = 0;
        const std::uint64_t inspect = std::min<std::uint64_t>(
            plan.comparison_limit_, elements.size());
        for (std::uint64_t step = 0;
             step < inspect &&
             result.positions_.size() < plan.return_limit_;
             ++step) {
          const std::size_t position =
              plan.reverse_ ? elements.size() - 1 - step : step;
          if (elements[position] == operation.value_ &&
              ++matches >= plan.wanted_rank_) {
            result.positions_.push_back(position);
          }
        }
        co_return result;
      }
      case ListOperationKind::kMoveWithin: {
        if (elements.empty()) co_return result;
        const bool source_left = operation.first_ != 0;
        const bool destination_left = operation.second_ != 0;
        std::string moved = source_left ? std::move(elements.front())
                                        : std::move(elements.back());
        if (source_left) {
          elements.erase(elements.begin());
        } else {
          elements.pop_back();
        }
        if (destination_left) {
          elements.insert(elements.begin(), moved);
        } else {
          elements.push_back(moved);
        }
        result.values_.push_back(std::move(moved));
        result.changed_ = source_left != destination_left && elements.size() > 1;
        break;
      }
      case ListOperationKind::kLength:
        co_return result;
    }

    result.length_ = elements.size();
    if (!result.changed_) co_return result;
    if (elements.empty() || EncodedBytes(elements) <= kCompactPromoteBytes) {
      absl::Status status = co_await publish(std::move(elements));
      if (!status.ok()) co_return status;
      co_return result;
    }
    absl::Status owner_status = ensure_owner();
    if (!owner_status.ok()) co_return owner_status;
    auto segments = co_await split_and_write(elements);
    if (!segments.ok()) {
      co_return segments.status();
    }
    auto tree = co_await build_tree(std::move(*segments));
    if (!tree.ok()) {
      co_return tree.status();
    }
    working = *tree;
    absl::Status status = co_await publish(std::nullopt);
    if (!status.ok()) {
      co_return status;
    }
    co_return result;
  }

  auto read_at = [&](std::uint64_t position)
      -> Task<absl::StatusOr<std::string>> {
    auto path = co_await find_path(working, position, false);
    if (!path.ok()) co_return path.status();
    const auto& segment =
        path->steps_.back().node_.segments_[path->steps_.back().selected_];
    auto elements = co_await load_segment(segment);
    if (!elements.ok()) co_return elements.status();
    co_return std::move((*elements)[position - path->segment_start_]);
  };

  auto pop_edge = [&](bool left, std::uint64_t requested,
                      std::vector<std::string>* returned)
      -> Task<absl::Status> {
    std::uint64_t remaining = std::min(requested, working.element_count_);
    while (remaining != 0) {
      const std::uint64_t position = left ? 0 : working.element_count_ - 1;
      auto path = co_await find_path(working, position, false);
      if (!path.ok()) co_return path.status();
      const auto& meta =
          path->steps_.back().node_.segments_[path->steps_.back().selected_];
      auto elements = co_await load_segment(meta);
      if (!elements.ok()) co_return elements.status();
      const std::size_t take = static_cast<std::size_t>(
          std::min<std::uint64_t>(remaining, elements->size()));
      for (std::size_t i = 0; i < take; ++i) {
        if (left) {
          if (returned != nullptr)
            returned->push_back(std::move(elements->front()));
          elements->erase(elements->begin());
        } else {
          if (returned != nullptr)
            returned->push_back(std::move(elements->back()));
          elements->pop_back();
        }
      }
      absl::Status rewritten =
          co_await rewrite_segment(std::move(*path), std::move(*elements), true);
      if (!rewritten.ok()) co_return rewritten;
      remaining -= take;
    }
    co_return absl::OkStatus();
  };

  switch (operation.kind_) {
    case ListOperationKind::kPushLeft:
    case ListOperationKind::kPushRight:
    case ListOperationKind::kPushLeftIfExists:
    case ListOperationKind::kPushRightIfExists: {
      const bool left = operation.kind_ == ListOperationKind::kPushLeft ||
                        operation.kind_ == ListOperationKind::kPushLeftIfExists;
      auto path = co_await find_path(
          working, left ? 0 : working.element_count_, !left);
      if (!path.ok()) co_return path.status();
      const auto& meta =
          path->steps_.back().node_.segments_[path->steps_.back().selected_];
      auto elements = co_await load_segment(meta);
      if (!elements.ok()) co_return elements.status();
      for (std::string_view value : operation.values_) {
        if (left) {
          elements->insert(elements->begin(), std::string(value));
        } else {
          elements->emplace_back(value);
        }
      }
      result.changed_ = !operation.values_.empty();
      if (result.changed_) {
        absl::Status status = co_await rewrite_segment(
            std::move(*path), std::move(*elements), false);
        if (!status.ok()) co_return status;
      }
      break;
    }
    case ListOperationKind::kPopLeft:
    case ListOperationKind::kPopRight: {
      absl::Status status = co_await pop_edge(
          operation.kind_ == ListOperationKind::kPopLeft, operation.count_,
          &result.values_);
      if (!status.ok()) co_return status;
      result.changed_ = !result.values_.empty();
      break;
    }
    case ListOperationKind::kIndex: {
      auto position = NormalizeIndex(operation.first_, working.element_count_);
      if (position) {
        auto value = co_await read_at(*position);
        if (!value.ok()) co_return value.status();
        result.values_.push_back(std::move(*value));
      }
      co_return result;
    }
    case ListOperationKind::kRange: {
      const auto [begin, end] = NormalizeRange(
          operation.first_, operation.second_, working.element_count_);
      if (begin == end) co_return result;
      auto flat = co_await flatten(working);
      if (!flat.ok()) co_return flat.status();
      std::uint64_t base = 0;
      for (const ListSegmentMeta& meta : flat->segments_) {
        const std::uint64_t segment_end = base + meta.element_count_;
        if (segment_end > begin && base < end) {
          auto elements = co_await load_segment(meta);
          if (!elements.ok()) co_return elements.status();
          const std::uint64_t first = begin > base ? begin - base : 0;
          const std::uint64_t last = std::min(end, segment_end) - base;
          for (std::uint64_t i = first; i < last; ++i) {
            result.values_.push_back(std::move((*elements)[i]));
          }
        }
        base = segment_end;
        if (base >= end) break;
      }
      co_return result;
    }
    case ListOperationKind::kSet: {
      auto position = NormalizeIndex(operation.first_, working.element_count_);
      if (!position) co_return absl::OutOfRangeError("index out of range");
      auto path = co_await find_path(working, *position, false);
      if (!path.ok()) co_return path.status();
      const auto& meta =
          path->steps_.back().node_.segments_[path->steps_.back().selected_];
      auto elements = co_await load_segment(meta);
      if (!elements.ok()) co_return elements.status();
      (*elements)[*position - path->segment_start_] = operation.value_;
      absl::Status status = co_await rewrite_segment(
          std::move(*path), std::move(*elements), true);
      if (!status.ok()) co_return status;
      result.changed_ = true;
      break;
    }
    case ListOperationKind::kInsertBefore:
    case ListOperationKind::kInsertAfter: {
      auto flat = co_await flatten(working);
      if (!flat.ok()) co_return flat.status();
      std::uint64_t base = 0;
      bool inserted = false;
      for (const ListSegmentMeta& meta : flat->segments_) {
        auto elements = co_await load_segment(meta);
        if (!elements.ok()) co_return elements.status();
        auto it = std::find(elements->begin(), elements->end(), operation.pivot_);
        if (it == elements->end()) {
          base += meta.element_count_;
          continue;
        }
        std::uint64_t position = base + (it - elements->begin());
        auto path = co_await find_path(working, position, false);
        if (!path.ok()) co_return path.status();
        if (operation.kind_ == ListOperationKind::kInsertAfter) ++it;
        elements->insert(it, std::string(operation.value_));
        absl::Status status = co_await rewrite_segment(
            std::move(*path), std::move(*elements), false);
        if (!status.ok()) co_return status;
        inserted = true;
        break;
      }
      if (!inserted) {
        result.integer_ = -1;
        co_return result;
      }
      result.changed_ = true;
      result.integer_ = working.element_count_;
      break;
    }
    case ListOperationKind::kRemove: {
      const ListRemovePlan plan =
          NormalizeListRemoveLimit(operation.first_);
      std::uint64_t removed = 0;
      if (plan.forward_) {
        std::uint64_t cursor = 0;
        while (cursor < working.element_count_ && removed < plan.limit_) {
          auto path = co_await find_path(working, cursor, false);
          if (!path.ok()) co_return path.status();
          const std::uint64_t segment_start = path->segment_start_;
          const auto& meta = path->steps_.back().node_.segments_[
              path->steps_.back().selected_];
          auto elements = co_await load_segment(meta);
          if (!elements.ok()) co_return elements.status();
          const auto before = elements->size();
          for (auto it = elements->begin();
               it != elements->end() && removed < plan.limit_;) {
            if (*it == operation.value_) {
              it = elements->erase(it);
              ++removed;
            } else {
              ++it;
            }
          }
          if (elements->size() != before) {
            const std::size_t remain = elements->size();
            absl::Status status = co_await rewrite_segment(
                std::move(*path), std::move(*elements), true);
            if (!status.ok()) co_return status;
            cursor = segment_start + remain;
          } else {
            cursor = segment_start + meta.element_count_;
          }
        }
      } else {
        std::uint64_t cursor = working.element_count_;
        while (cursor != 0 && removed < plan.limit_) {
          auto path = co_await find_path(working, cursor - 1, false);
          if (!path.ok()) co_return path.status();
          const auto& meta = path->steps_.back().node_.segments_[
              path->steps_.back().selected_];
          auto elements = co_await load_segment(meta);
          if (!elements.ok()) co_return elements.status();
          const auto before = elements->size();
          for (std::size_t i = elements->size();
               i > 0 && removed < plan.limit_;) {
            --i;
            if ((*elements)[i] == operation.value_) {
              elements->erase(elements->begin() + i);
              ++removed;
            }
          }
          const std::uint64_t next = path->segment_start_;
          if (elements->size() != before) {
            absl::Status status = co_await rewrite_segment(
                std::move(*path), std::move(*elements), true);
            if (!status.ok()) co_return status;
          }
          cursor = next;
        }
      }
      result.integer_ = removed;
      result.changed_ = removed != 0;
      break;
    }
    case ListOperationKind::kTrim: {
      const auto [begin, end] = NormalizeRange(
          operation.first_, operation.second_, working.element_count_);
      const std::uint64_t old_size = working.element_count_;
      if (begin != 0) {
        absl::Status status = co_await pop_edge(true, begin, nullptr);
        if (!status.ok()) co_return status;
      }
      const std::uint64_t kept = end > begin ? end - begin : 0;
      if (working.element_count_ > kept) {
        absl::Status status = co_await pop_edge(
            false, working.element_count_ - kept, nullptr);
        if (!status.ok()) co_return status;
      }
      result.changed_ = working.element_count_ != old_size;
      break;
    }
    case ListOperationKind::kPosition: {
      auto flat = co_await flatten(working);
      if (!flat.ok()) co_return flat.status();
      const ListPositionPlan plan = NormalizeListPosition(operation);
      std::uint64_t matches = 0;
      std::uint64_t inspected = 0;
      std::vector<std::uint64_t> bases(flat->segments_.size());
      for (std::size_t i = 1; i < bases.size(); ++i) {
        bases[i] = bases[i - 1] + flat->segments_[i - 1].element_count_;
      }
      for (std::size_t step = 0;
           step < flat->segments_.size() &&
           result.positions_.size() < plan.return_limit_;
           ++step) {
        const std::size_t segment_index =
            plan.reverse_ ? flat->segments_.size() - 1 - step : step;
        auto elements = co_await load_segment(flat->segments_[segment_index]);
        if (!elements.ok()) co_return elements.status();
        for (std::size_t item_step = 0;
             item_step < elements->size() &&
             result.positions_.size() < plan.return_limit_;
             ++item_step) {
          if (inspected++ >= plan.comparison_limit_) {
            co_return result;
          }
          const std::size_t item =
              plan.reverse_ ? elements->size() - 1 - item_step : item_step;
          if ((*elements)[item] != operation.value_ ||
              ++matches < plan.wanted_rank_) {
            continue;
          }
          result.positions_.push_back(
              static_cast<std::int64_t>(bases[segment_index] + item));
        }
      }
      co_return result;
    }
    case ListOperationKind::kMoveWithin: {
      if (working.element_count_ == 0) co_return result;
      const bool source_left = operation.first_ != 0;
      const bool destination_left = operation.second_ != 0;
      if (source_left == destination_left || working.element_count_ == 1) {
        auto value = co_await read_at(source_left ? 0 : working.element_count_ - 1);
        if (!value.ok()) co_return value.status();
        result.values_.push_back(std::move(*value));
        co_return result;
      }
      std::vector<std::string> moved;
      absl::Status popped = co_await pop_edge(source_left, 1, &moved);
      if (!popped.ok()) co_return popped;
      auto path = co_await find_path(
          working, destination_left ? 0 : working.element_count_,
          !destination_left);
      if (!path.ok()) co_return path.status();
      const auto& meta = path->steps_.back().node_.segments_[
          path->steps_.back().selected_];
      auto elements = co_await load_segment(meta);
      if (!elements.ok()) co_return elements.status();
      if (destination_left) {
        elements->insert(elements->begin(), moved.front());
      } else {
        elements->push_back(moved.front());
      }
      absl::Status pushed = co_await rewrite_segment(
          std::move(*path), std::move(*elements), false);
      if (!pushed.ok()) co_return pushed;
      result.values_.push_back(std::move(moved.front()));
      result.changed_ = true;
      break;
    }
    case ListOperationKind::kLength:
      co_return result;
  }

  result.length_ = working.element_count_;
  if (!result.changed_) co_return result;

  std::optional<std::vector<std::string>> compact;
  if (working.element_count_ == 0) {
    compact.emplace();
  } else if (working.segment_count_ == 1 &&
             working.encoded_bytes_ <= kCompactDemoteBytes) {
    auto all = co_await materialize(working);
    if (!all.ok()) {
      co_return all.status();
    }
    absl::Status collected = co_await collect_current_graph();
    if (!collected.ok()) {
      co_return collected;
    }
    compact = std::move(*all);
  }
  absl::Status status = co_await publish(std::move(compact));
  if (!status.ok()) {
    co_return status;
  }
  co_return result;
}

}  // namespace keylane::storage
