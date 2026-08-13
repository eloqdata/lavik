#include "impl.h"

namespace keylane::storage {

namespace {

constexpr std::size_t kListEncodingHeaderBytes = 8;

}  // namespace

absl::StatusOr<ListState> DecodeListRoot(
    std::span<const std::byte> payload) {
  if (payload.size() != sizeof(ListRootHeader)) {
    return absl::InternalError("segmented List root has an invalid size");
  }
  ListRootHeader header{};
  std::memcpy(&header, payload.data(), sizeof(header));
  if (header.magic_ != kListRootMagic ||
      header.version_ != kStorageFormatVersion ||
      header.header_bytes_ != sizeof(ListRootHeader) ||
      header.element_count_ == 0 || header.encoded_bytes_ == 0 ||
      header.segment_count_ == 0 || header.tree_height_ == 0 ||
      !header.directory_root_.valid() || header.reserved_ != 0) {
    return absl::InternalError("invalid segmented List root");
  }
  return ListState{
      .element_count_ = header.element_count_,
      .encoded_bytes_ = header.encoded_bytes_,
      .segment_count_ = header.segment_count_,
      .tree_height_ = header.tree_height_,
      .directory_root_ = header.directory_root_,
      .owner_id_ = 0,
  };
}

std::string EncodeListRoot(const ListState& state) {
  const ListRootHeader header{
      .magic_ = kListRootMagic,
      .version_ = kStorageFormatVersion,
      .header_bytes_ = sizeof(ListRootHeader),
      .element_count_ = state.element_count_,
      .encoded_bytes_ = state.encoded_bytes_,
      .segment_count_ = state.segment_count_,
      .tree_height_ = state.tree_height_,
      .directory_root_ = state.directory_root_,
      .reserved_ = 0,
  };
  return std::string(reinterpret_cast<const char*>(&header), sizeof(header));
}

absl::StatusOr<ListDirectoryNode> DecodeListDirectory(
    std::span<const std::byte> payload) {
  if (payload.size() < sizeof(ListDirectoryHeader)) {
    return absl::InternalError("List directory is truncated");
  }
  ListDirectoryHeader header{};
  std::memcpy(&header, payload.data(), sizeof(header));
  const bool leaf = header.kind_ == ListDirectoryKind::kLeaf;
  const std::size_t entry_bytes =
      leaf ? sizeof(ListDirectorySegment) : sizeof(ListDirectoryChild);
  if (header.magic_ != kListDirectoryMagic ||
      header.version_ != kStorageFormatVersion ||
      header.header_bytes_ != sizeof(ListDirectoryHeader) ||
      (header.kind_ != ListDirectoryKind::kLeaf &&
       header.kind_ != ListDirectoryKind::kInternal) ||
      leaf != (header.level_ == 0) || header.entry_count_ == 0 ||
      header.reserved_ != 0 ||
      payload.size() != sizeof(header) + header.entry_count_ * entry_bytes) {
    return absl::InternalError("invalid List directory header");
  }
  ListDirectoryNode node;
  node.level_ = header.level_;
  std::size_t offset = sizeof(header);
  for (std::uint32_t index = 0; index < header.entry_count_; ++index) {
    if (leaf) {
      ListDirectorySegment encoded{};
      std::memcpy(&encoded, payload.data() + offset, sizeof(encoded));
      offset += sizeof(encoded);
      if (!encoded.segment_.valid() || encoded.element_count_ == 0 ||
          encoded.encoded_bytes_ < kListEncodingHeaderBytes) {
        return absl::InternalError("invalid List segment entry");
      }
      node.segments_.push_back(ListSegmentMeta{
          .segment_ = encoded.segment_,
          .element_count_ = encoded.element_count_,
          .encoded_bytes_ = encoded.encoded_bytes_,
      });
      node.element_count_ += encoded.element_count_;
      ++node.segment_count_;
      node.encoded_bytes_ += encoded.encoded_bytes_;
    } else {
      ListDirectoryChild encoded{};
      std::memcpy(&encoded, payload.data() + offset, sizeof(encoded));
      offset += sizeof(encoded);
      if (!encoded.child_.valid() || encoded.subtree_element_count_ == 0 ||
          encoded.subtree_segment_count_ == 0 ||
          encoded.subtree_encoded_bytes_ == 0) {
        return absl::InternalError("invalid List directory child");
      }
      node.children_.push_back(ListDirectoryChildMeta{
          .child_ = encoded.child_,
          .element_count_ = encoded.subtree_element_count_,
          .segment_count_ = encoded.subtree_segment_count_,
          .encoded_bytes_ = encoded.subtree_encoded_bytes_,
      });
      node.element_count_ += encoded.subtree_element_count_;
      node.segment_count_ += encoded.subtree_segment_count_;
      node.encoded_bytes_ += encoded.subtree_encoded_bytes_;
    }
  }
  if (node.element_count_ != header.element_count_ ||
      node.segment_count_ != header.segment_count_ ||
      node.encoded_bytes_ != header.encoded_bytes_) {
    return absl::InternalError("List directory totals do not match");
  }
  return node;
}

std::string EncodeListDirectory(const ListDirectoryNode& node) {
  const std::size_t entry_count =
      node.leaf() ? node.segments_.size() : node.children_.size();
  const std::size_t entry_bytes =
      node.leaf() ? sizeof(ListDirectorySegment)
                  : sizeof(ListDirectoryChild);
  const ListDirectoryHeader header{
      .magic_ = kListDirectoryMagic,
      .version_ = kStorageFormatVersion,
      .header_bytes_ = sizeof(ListDirectoryHeader),
      .kind_ = node.leaf() ? ListDirectoryKind::kLeaf
                           : ListDirectoryKind::kInternal,
      .level_ = node.level_,
      .entry_count_ = static_cast<std::uint32_t>(entry_count),
      .reserved_ = 0,
      .element_count_ = node.element_count_,
      .segment_count_ = node.segment_count_,
      .encoded_bytes_ = node.encoded_bytes_,
  };
  std::string output(sizeof(header) + entry_count * entry_bytes, '\0');
  std::memcpy(output.data(), &header, sizeof(header));
  std::size_t offset = sizeof(header);
  if (node.leaf()) {
    for (const ListSegmentMeta& segment : node.segments_) {
      const ListDirectorySegment encoded{
          .segment_ = segment.segment_,
          .element_count_ = segment.element_count_,
          .encoded_bytes_ = segment.encoded_bytes_,
      };
      std::memcpy(output.data() + offset, &encoded, sizeof(encoded));
      offset += sizeof(encoded);
    }
  } else {
    for (const ListDirectoryChildMeta& child : node.children_) {
      const ListDirectoryChild encoded{
          .child_ = child.child_,
          .subtree_element_count_ = child.element_count_,
          .subtree_segment_count_ = child.segment_count_,
          .subtree_encoded_bytes_ = child.encoded_bytes_,
      };
      std::memcpy(output.data() + offset, &encoded, sizeof(encoded));
      offset += sizeof(encoded);
    }
  }
  return output;
}

}  // namespace keylane::storage
