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

#include <thread>

#include "absl/strings/str_cat.h"
#include "device_affinity.h"
#include "impl.h"

namespace lavik::storage {
namespace {

std::vector<std::uint64_t> InitialEpochValues() {
  std::vector<std::uint64_t> values(kEpochValueCount, 1);
  std::fill(values.begin() + kCheckpointGenerationIndex, values.end(), 0);
  return values;
}

struct MirroredSystemStateRoot {
  std::uint64_t generation_ = 0;
  SystemStateRoot root_{};
  std::array<std::byte, sizeof(SystemStateRoot)> payload_{};
};

absl::StatusOr<std::vector<MirroredSystemStateRoot>>
ReadSystemStateRootCandidates(const std::string& path) {
  std::vector<MirroredSystemStateRoot> candidates;
  bool saw_nonzero = false;
  for (unsigned slot = 0; slot < 2; ++slot) {
    std::array<std::byte, kDirectIoAlignment> page{};
    absl::Status read = ReadExactlyAt(
        path, page,
        MetadataPageSlotOffset(kSystemStateMetadataOffset, 0, slot));
    if (!read.ok()) return read;
    if (IsZero(page)) continue;
    saw_nonzero = true;
    MirroredSystemStateRoot candidate;
    if (!DecodeMetadataPage(page, MetadataPageKind::kSystemState, 0,
                            &candidate.generation_, candidate.payload_) ||
        !DecodeSystemStateRoot(candidate.payload_, &candidate.root_) ||
        candidate.root_.generation_ != candidate.generation_) {
      continue;
    }
    candidates.push_back(candidate);
  }
  if (saw_nonzero && candidates.empty()) {
    return absl::InternalError(
        "both fixed system-state root slots are corrupt: " + path);
  }
  return candidates;
}

absl::StatusOr<std::optional<MirroredSystemStateRoot>>
SelectCommonSystemStateRoot(const std::vector<std::string>& paths) {
  if (paths.empty()) return std::optional<MirroredSystemStateRoot>{};
  std::vector<std::vector<MirroredSystemStateRoot>> candidates;
  candidates.reserve(paths.size());
  bool saw_any = false;
  for (const std::string& path : paths) {
    auto loaded = ReadSystemStateRootCandidates(path);
    if (!loaded.ok()) return loaded.status();
    saw_any |= !loaded->empty();
    candidates.push_back(std::move(*loaded));
  }
  if (!saw_any) return std::optional<MirroredSystemStateRoot>{};
  std::optional<MirroredSystemStateRoot> selected;
  for (const MirroredSystemStateRoot& candidate : candidates.front()) {
    bool common = true;
    for (std::size_t device = 1; device < candidates.size() && common;
         ++device) {
      common =
          std::any_of(candidates[device].begin(), candidates[device].end(),
                      [&](const MirroredSystemStateRoot& other) {
                        return other.generation_ == candidate.generation_ &&
                               other.root_ == candidate.root_;
                      });
    }
    if (common && (!selected.has_value() ||
                   candidate.generation_ > selected->generation_)) {
      selected = candidate;
    }
  }
  if (!selected.has_value()) {
    return absl::FailedPreconditionError(
        "existing devices have no common valid system-state generation");
  }
  return selected;
}

absl::Status InstallSystemStateRoot(
    const std::string& path,
    const std::optional<MirroredSystemStateRoot>& root) {
  std::array<std::byte, kDirectIoAlignment> zero{};
  for (unsigned slot = 0; slot < 2; ++slot) {
    absl::Status cleared = WriteExactlyAt(
        path, zero, MetadataPageSlotOffset(kSystemStateMetadataOffset, 0, slot),
        false);
    if (!cleared.ok()) return cleared;
  }
  if (!root.has_value()) {
    return WriteExactlyAt(
        path, zero, MetadataPageSlotOffset(kSystemStateMetadataOffset, 0, 1),
        true);
  }
  std::array<std::byte, kDirectIoAlignment> page{};
  EncodeMetadataPage(MetadataPageKind::kSystemState, 0, root->generation_,
                     root->payload_, page);
  const unsigned slot = static_cast<unsigned>((root->generation_ - 1) & 1);
  return WriteExactlyAt(
      path, page, MetadataPageSlotOffset(kSystemStateMetadataOffset, 0, slot),
      true);
}

absl::Status ResetStorageMetadata(const std::string& path,
                                  std::uint64_t capacity_blocks) {
  constexpr std::size_t kResetChunkBytes = 128 * 1024;
  const std::uint64_t bytes =
      static_cast<std::uint64_t>(DataBlockBegin(capacity_blocks)) *
      kStorageBlockBytes;
  std::vector<std::byte> zero(kResetChunkBytes, std::byte{0});

  if (bycorf::IsSpdkStoragePath(path)) {
    for (std::uint64_t offset = 0; offset < bytes; offset += kResetChunkBytes) {
      const std::size_t chunk = static_cast<std::size_t>(
          std::min<std::uint64_t>(kResetChunkBytes, bytes - offset));
      absl::Status status = bycorf::WriteSpdkStorage(
          path, std::span<const std::byte>(zero.data(), chunk), offset,
          offset + chunk == bytes);
      if (!status.ok()) return status;
    }
    return absl::OkStatus();
  }

  const int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    return absl::InternalError(
        absl::StrCat("open storage path for reset failed: ", path, ": ",
                     std::strerror(errno)));
  }
  absl::Status status = absl::OkStatus();
  for (std::uint64_t offset = 0; offset < bytes; offset += kResetChunkBytes) {
    const std::size_t chunk = static_cast<std::size_t>(
        std::min<std::uint64_t>(kResetChunkBytes, bytes - offset));
    status = WriteExactlyAt(fd, std::span<const std::byte>(zero.data(), chunk),
                            offset);
    if (!status.ok()) break;
  }
  if (status.ok() && ::fdatasync(fd) != 0) {
    status = absl::InternalError(absl::StrCat(
        "storage reset fdatasync failed: ", path, ": ", std::strerror(errno)));
  }
  const int close_error = ::close(fd);
  if (status.ok() && close_error != 0) {
    status = absl::InternalError(
        absl::StrCat("close storage path after reset failed: ", path));
  }
  return status;
}

absl::Status InitializeAddedDeviceMetadata(
    const std::string& path, std::uint64_t capacity_blocks,
    const std::vector<std::uint64_t>& epoch_values) {
  std::array<std::byte, kDirectIoAlignment> zero{};
  absl::Status status = WriteExactlyAt(path, zero, kDeviceLabelOffset, true);
  if (!status.ok()) {
    return status;
  }

  for (std::size_t page_index = 0; page_index < kEpochMetadataPageCount;
       ++page_index) {
    const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
    const std::size_t payload_bytes =
        std::min(kMetadataPagePayloadBytes, kEpochMetadataBytes - byte_offset);
    status = WriteExactlyAt(
        path, zero, MetadataPageSlotOffset(kEpochMetadataOffset, page_index, 1),
        false);
    if (!status.ok()) {
      return status;
    }
    std::array<std::byte, kDirectIoAlignment> page{};
    EncodeMetadataPage(
        MetadataPageKind::kEpochs, static_cast<std::uint32_t>(page_index), 1,
        std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(epoch_values.data()) +
                byte_offset,
            payload_bytes),
        page);
    status = WriteExactlyAt(
        path, page, MetadataPageSlotOffset(kEpochMetadataOffset, page_index, 0),
        true);
    if (!status.ok()) {
      return status;
    }
  }

  const std::size_t bitmap_pages = ScanBitmapPageCount(capacity_blocks);
  for (std::size_t page_index = 0; page_index < bitmap_pages; ++page_index) {
    status = WriteExactlyAt(
        path, zero,
        MetadataPageSlotOffset(kScanBitmapMetadataOffset, page_index, 0),
        false);
    if (!status.ok()) {
      return status;
    }
    status = WriteExactlyAt(
        path, zero,
        MetadataPageSlotOffset(kScanBitmapMetadataOffset, page_index, 1), true);
    if (!status.ok()) {
      return status;
    }
  }
  const std::uint64_t checkpoint_bitmap_offset =
      CheckpointBitmapMetadataOffset(capacity_blocks);
  for (std::size_t page_index = 0; page_index < bitmap_pages; ++page_index) {
    status = WriteExactlyAt(
        path, zero,
        MetadataPageSlotOffset(checkpoint_bitmap_offset, page_index, 0), false);
    if (!status.ok()) {
      return status;
    }
    status = WriteExactlyAt(
        path, zero,
        MetadataPageSlotOffset(checkpoint_bitmap_offset, page_index, 1), true);
    if (!status.ok()) {
      return status;
    }
  }
  for (unsigned slot = 0; slot < 2; ++slot) {
    status = WriteExactlyAt(
        path, zero, MetadataPageSlotOffset(kSystemStateMetadataOffset, 0, slot),
        slot == 1);
    if (!status.ok()) return status;
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status StorageEngine::Impl::Prepare(unsigned worker_count) {
  if (options_.database_count_ == 0 ||
      options_.database_count_ > kLogicalDatabaseCount) {
    return absl::InvalidArgumentError("invalid runtime database count");
  }
  if (worker_count == 0 || options_.data_files_.empty()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "storage requires workers and at least one data file");
  }
  if (worker_count > kLogicalStorageShards) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "storage worker count exceeds logical storage shards");
  }
  if (options_.inline_key_max_bytes_ == 0 ||
      options_.inline_key_max_bytes_ > MaxInlineKeyBytes()) {
    return absl::Status(absl::StatusCode::kInvalidArgument,
                        "inline key limit must be between 1 and " +
                            std::to_string(MaxInlineKeyBytes()) + " bytes");
  }
  if (options_.defrag_max_active_per_device_ == 0 ||
      options_.defrag_max_active_per_device_ > kDefragReserveBlocksPerDevice) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "defrag concurrency must be between 1 and the per-device reserve");
  }
  if (options_.data_files_.size() > std::numeric_limits<std::uint16_t>::max()) {
    return absl::Status(absl::StatusCode::kOutOfRange, "too many data files");
  }

  std::size_t direct_io_alignment = 1;
  std::vector<StoragePathInfo> path_info;
  path_info.reserve(options_.data_files_.size());
  std::vector<std::optional<DeviceLabel>> labels;
  for (const std::string& path : options_.data_files_) {
    auto probed = ProbeStoragePath(path);
    if (!probed.ok()) {
      return probed.status();
    }
    if (probed->size_bytes_ < 2 * kStorageBlockBytes) {
      return absl::Status(
          absl::StatusCode::kOutOfRange,
          "storage path is too small to hold metadata and data: " + path);
    }
    direct_io_alignment = std::max(direct_io_alignment, probed->io_alignment_);
    path_info.push_back(*probed);
  }

  labels.resize(options_.data_files_.size());
  if (options_.reset_data_files_) {
    // Validate the complete target set before the first destructive write.
    // The normal preparation below repeats these checks while constructing
    // device state, but this preflight prevents avoidable partial resets.
    for (std::size_t i = 0; i < path_info.size(); ++i) {
      for (std::size_t previous = 0; previous < i; ++previous) {
        if (options_.data_files_[previous] == options_.data_files_[i]) {
          return absl::FailedPreconditionError(
              "duplicate storage path configured for reset: " +
              options_.data_files_[i]);
        }
      }
      const StoragePathInfo& probed = path_info[i];
      if (!probed.is_block_device_ &&
          probed.size_bytes_ % kStorageBlockBytes != 0) {
        return absl::InvalidArgumentError(
            "new regular storage file size must be a multiple of 8 MiB: " +
            options_.data_files_[i]);
      }
      const std::uint64_t capacity_blocks =
          probed.size_bytes_ / kStorageBlockBytes;
      if (capacity_blocks > kLocalBlockIdLimit) {
        return absl::OutOfRangeError(
            "each data file or device is limited to 1 PiB: " +
            options_.data_files_[i]);
      }
      const std::uint32_t data_block_begin = DataBlockBegin(capacity_blocks);
      if (data_block_begin >= capacity_blocks ||
          capacity_blocks - data_block_begin <= kDefragReserveBlocksPerDevice) {
        return absl::OutOfRangeError(
            "storage path has no foreground block after its per-device "
            "defrag reserve; each device must be at least 80 MiB: " +
            options_.data_files_[i]);
      }
    }
    for (std::size_t i = 0; i < path_info.size(); ++i) {
      spdlog::warn("resetting all Lavik data on storage path {}",
                   options_.data_files_[i]);
      absl::Status reset =
          ResetStorageMetadata(options_.data_files_[i],
                               path_info[i].size_bytes_ / kStorageBlockBytes);
      if (!reset.ok()) return reset;
    }
  } else {
    for (std::size_t i = 0; i < options_.data_files_.size(); ++i) {
      auto label = ReadDeviceLabel(options_.data_files_[i]);
      if (!label.ok()) return label.status();
      labels[i] = std::move(*label);
    }
  }

  std::uint64_t storage_set_id = 0;
  std::uint32_t minimum_device_count = 0;
  std::uint32_t maximum_device_count = 0;
  bool has_existing_device = false;
  bool has_empty_device = false;
  absl::flat_hash_map<std::uint64_t, std::size_t> seen_device_ids;
  for (std::size_t i = 0; i < labels.size(); ++i) {
    if (!labels[i].has_value()) {
      has_empty_device = true;
      continue;
    }
    has_existing_device = true;
    const DeviceLabel& label = *labels[i];
    if (storage_set_id == 0) {
      storage_set_id = label.storage_set_id_;
    } else if (storage_set_id != label.storage_set_id_) {
      return absl::Status(
          absl::StatusCode::kFailedPrecondition,
          "configured devices belong to different storage sets");
    }
    minimum_device_count =
        minimum_device_count == 0
            ? label.device_count_
            : std::min(minimum_device_count, label.device_count_);
    maximum_device_count = std::max(maximum_device_count, label.device_count_);
    if (!seen_device_ids.try_emplace(label.device_id_, i).second) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "duplicate device id in configured storage files");
    }
  }

  const std::uint32_t configured_device_count =
      static_cast<std::uint32_t>(labels.size());
  std::vector<std::uint64_t> assigned_device_ids(labels.size(),
                                                 kInvalidBlockId);
  std::uint32_t previous_device_count = 0;
  if (has_existing_device) {
    previous_device_count = minimum_device_count;
    if (maximum_device_count > configured_device_count ||
        (maximum_device_count != previous_device_count &&
         maximum_device_count != configured_device_count)) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "configured devices disagree on storage-set size");
    }
    for (const auto& [id, path_index] : seen_device_ids) {
      const DeviceLabel& label = *labels[path_index];
      if (id >= configured_device_count ||
          (label.device_count_ != previous_device_count &&
           label.device_count_ != configured_device_count)) {
        return absl::Status(absl::StatusCode::kFailedPrecondition,
                            "invalid interrupted storage-set expansion state");
      }
      assigned_device_ids[path_index] = id;
    }
    // The complete old set must be present. This prevents an empty replacement
    // path from silently hiding a lost initialized device.
    for (std::uint64_t id = 0; id < previous_device_count; ++id) {
      if (!seen_device_ids.contains(id)) {
        return absl::Status(absl::StatusCode::kFailedPrecondition,
                            "configured storage set is missing existing "
                            "device id " +
                                std::to_string(id));
      }
    }
    if (configured_device_count == previous_device_count && has_empty_device) {
      return absl::Status(absl::StatusCode::kFailedPrecondition,
                          "an empty path cannot replace a missing initialized "
                          "storage device");
    }
    std::uint64_t next_id = previous_device_count;
    for (std::size_t i = 0; i < labels.size(); ++i) {
      if (labels[i].has_value()) {
        continue;
      }
      while (seen_device_ids.contains(next_id)) {
        ++next_id;
      }
      if (next_id >= configured_device_count) {
        return absl::Status(absl::StatusCode::kFailedPrecondition,
                            "storage expansion has no device id for new path");
      }
      assigned_device_ids[i] = next_id;
      seen_device_ids.emplace(next_id, i);
      ++next_id;
    }
    for (std::uint64_t id = 0; id < configured_device_count; ++id) {
      if (!seen_device_ids.contains(id)) {
        return absl::Status(absl::StatusCode::kFailedPrecondition,
                            "configured storage set is missing device id " +
                                std::to_string(id));
      }
    }
  } else {
    auto generated = RandomStorageSetId();
    if (!generated.ok()) {
      return generated.status();
    }
    storage_set_id = *generated;
    for (std::size_t i = 0; i < labels.size(); ++i) {
      assigned_device_ids[i] = i;
    }
  }

  devices_.clear();
  devices_.reserve(options_.data_files_.size());
  std::vector<std::uint64_t> capacity_by_path(labels.size(), 0);
  for (std::size_t i = 0; i < labels.size(); ++i) {
    const StoragePathInfo& probed = path_info[i];
    std::uint64_t capacity_blocks = 0;
    const std::uint64_t device_id = assigned_device_ids[i];
    if (labels[i].has_value()) {
      const DeviceLabel& label = *labels[i];
      capacity_blocks = label.capacity_blocks_;
      const std::uint64_t required_bytes = capacity_blocks * kStorageBlockBytes;
      if (probed.size_bytes_ < required_bytes) {
        return absl::Status(
            absl::StatusCode::kFailedPrecondition,
            "storage path is smaller than its persisted capacity: " +
                options_.data_files_[i]);
      }
      if (probed.size_bytes_ > required_bytes) {
        spdlog::info(
            "storage path {} has {} trailing bytes beyond its persisted "
            "capacity; ignoring them",
            options_.data_files_[i], probed.size_bytes_ - required_bytes);
      }
    } else {
      if (!probed.is_block_device_ &&
          probed.size_bytes_ % kStorageBlockBytes != 0) {
        return absl::Status(
            absl::StatusCode::kInvalidArgument,
            "new regular storage file size must be a multiple of 8 MiB: " +
                options_.data_files_[i]);
      }
      capacity_blocks = probed.size_bytes_ / kStorageBlockBytes;
      if (capacity_blocks > kLocalBlockIdLimit) {
        return absl::Status(absl::StatusCode::kOutOfRange,
                            "each data file or device is limited to 1 PiB: " +
                                options_.data_files_[i]);
      }
      const std::uint64_t ignored_bytes =
          probed.size_bytes_ - capacity_blocks * kStorageBlockBytes;
      if (ignored_bytes != 0) {
        spdlog::info(
            "block device {} has {} tail bytes outside a complete 8 MiB "
            "block; ignoring them",
            options_.data_files_[i], ignored_bytes);
      }
    }
    const std::uint32_t data_block_begin = DataBlockBegin(capacity_blocks);
    if (capacity_blocks > kLocalBlockIdLimit) {
      return absl::Status(
          absl::StatusCode::kOutOfRange,
          "persisted device capacity exceeds the 1 PiB limit: " +
              options_.data_files_[i]);
    }
    if (data_block_begin >= capacity_blocks) {
      return absl::Status(
          absl::StatusCode::kOutOfRange,
          "fixed metadata leaves no data blocks: " + options_.data_files_[i]);
    }
    if (capacity_blocks - data_block_begin <= kDefragReserveBlocksPerDevice) {
      return absl::Status(
          absl::StatusCode::kOutOfRange,
          "storage path has no foreground block after its per-device "
          "defrag reserve; each device must be at least 80 MiB: " +
              options_.data_files_[i]);
    }
    capacity_by_path[i] = capacity_blocks;
    devices_.push_back(StorageDevice{
        .path_ = options_.data_files_[i],
        .controller_id_ = probed.controller_id_,
        .id_ = device_id,
        .capacity_blocks_ = capacity_blocks,
        .io_queue_count_ = probed.io_queue_count_,
        .data_block_begin_ = data_block_begin,
        .data_block_count_ = capacity_blocks - data_block_begin,
        .file_index_ = static_cast<std::uint32_t>(i),
        .is_block_device_ = path_info[i].is_block_device_,
    });
  }
  std::sort(devices_.begin(), devices_.end(),
            [](const StorageDevice& left, const StorageDevice& right) {
              return left.id_ < right.id_;
            });

  ConfigureDefragReserves();
  active_defrags_by_device_ =
      std::make_unique<std::atomic<unsigned>[]>(devices_.size());
  defrag_ready_by_device_ =
      std::make_unique<moodycamel::ConcurrentQueue<std::uint16_t>[]>(
          devices_.size());
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    active_defrags_by_device_[device_index].store(0, std::memory_order_relaxed);
  }
  total_data_blocks_ = 0;
  std::uint64_t foreground_blocks = 0;
  // Sized once and never resized: BlockState pointers are held across
  // suspension points, so entries must not move.
  device_block_states_.resize(devices_.size());
  block_state_lookup_.resize(devices_.size());
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    device_block_states_[device_index] =
        std::vector<BlockState>(static_cast<std::size_t>(
            device.capacity_blocks_ - device.data_block_begin_));
    block_state_lookup_[device_index] = BlockStateLookup{
        .states_ = device_block_states_[device_index].data(),
        .local_begin_ = device.data_block_begin_,
        .local_end_ = static_cast<std::uint32_t>(device.capacity_blocks_),
    };
    total_data_blocks_ += device.data_block_count_;
    const std::size_t reserve = DefragReserveForDevice(device_index);
    if (device.data_block_count_ > reserve) {
      foreground_blocks += device.data_block_count_ - reserve;
    }
  }
  if (foreground_blocks == 0) {
    return absl::Status(
        absl::StatusCode::kOutOfRange,
        "storage set has no foreground blocks after the defrag reserve; "
        "a single-device storage set must be at least 80 MiB");
  }

  if (has_existing_device && configured_device_count > previous_device_count) {
    std::vector<std::uint64_t> canonical_epochs = InitialEpochValues();
    std::vector<std::string> existing_paths;
    for (std::size_t i = 0; i < labels.size(); ++i) {
      if (!labels[i].has_value()) {
        continue;
      }
      existing_paths.push_back(options_.data_files_[i]);
      for (std::size_t page_index = 0; page_index < kEpochMetadataPageCount;
           ++page_index) {
        const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
        const std::size_t payload_bytes = std::min(
            kMetadataPagePayloadBytes, kEpochMetadataBytes - byte_offset);
        auto loaded = ReadMetadataPagePair(
            options_.data_files_[i], kEpochMetadataOffset,
            MetadataPageKind::kEpochs, static_cast<std::uint32_t>(page_index),
            payload_bytes);
        if (!loaded.ok()) {
          return loaded.status();
        }
        const std::size_t first_value = byte_offset / sizeof(std::uint64_t);
        const std::size_t value_count = payload_bytes / sizeof(std::uint64_t);
        for (std::size_t value_index = 0; value_index < value_count;
             ++value_index) {
          std::uint64_t value = 0;
          std::memcpy(
              &value,
              loaded->payload_.data() + value_index * sizeof(std::uint64_t),
              sizeof(value));
          if (first_value + value_index < kCheckpointGenerationIndex) {
            canonical_epochs[first_value + value_index] =
                std::max(canonical_epochs[first_value + value_index], value);
          }
        }
      }
    }
    auto inherited_system_state = SelectCommonSystemStateRoot(existing_paths);
    if (!inherited_system_state.ok()) {
      return inherited_system_state.status();
    }
    for (std::size_t i = 0; i < labels.size(); ++i) {
      if (labels[i].has_value()) {
        continue;
      }
      absl::Status initialized = InitializeAddedDeviceMetadata(
          options_.data_files_[i], capacity_by_path[i], canonical_epochs);
      if (!initialized.ok()) {
        return initialized;
      }
      absl::Status system_state_installed = InstallSystemStateRoot(
          options_.data_files_[i], *inherited_system_state);
      if (!system_state_installed.ok()) return system_state_installed;
      DeviceLabel label{
          .magic_ = kDeviceLabelMagic,
          .version_ = kStorageFormatVersion,
          .header_bytes_ = kDirectIoAlignment,
          .storage_set_id_ = storage_set_id,
          .device_id_ = assigned_device_ids[i],
          .capacity_blocks_ = capacity_by_path[i],
          .device_count_ = configured_device_count,
          .block_bytes_ = kStorageBlockBytes,
      };
      absl::Status written = WriteDeviceLabel(options_.data_files_[i], label);
      if (!written.ok()) {
        return written;
      }
      labels[i] = label;
    }
    // Publish all new labels before changing an old label's member count. An
    // interrupted run therefore remains recognizable and safe to retry.
    for (std::size_t i = 0; i < labels.size(); ++i) {
      if (labels[i]->device_count_ == configured_device_count) {
        continue;
      }
      labels[i]->device_count_ = configured_device_count;
      absl::Status written =
          WriteDeviceLabel(options_.data_files_[i], *labels[i]);
      if (!written.ok()) {
        return written;
      }
    }
    spdlog::info("expanded storage set from {} to {} devices",
                 previous_device_count, configured_device_count);
  } else if (!has_existing_device) {
    for (std::size_t i = 0; i < labels.size(); ++i) {
      DeviceLabel label{
          .magic_ = kDeviceLabelMagic,
          .version_ = kStorageFormatVersion,
          .header_bytes_ = kDirectIoAlignment,
          .storage_set_id_ = storage_set_id,
          .device_id_ = assigned_device_ids[i],
          .capacity_blocks_ = capacity_by_path[i],
          .device_count_ = configured_device_count,
          .block_bytes_ = kStorageBlockBytes,
      };
      absl::Status written = WriteDeviceLabel(options_.data_files_[i], label);
      if (!written.ok()) {
        return written;
      }
      labels[i] = label;
    }
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    const std::size_t reserve = DefragReserveForDevice(device_index);
    spdlog::info(
        "storage device id={} path={} capacity-bytes={} data-blocks={} "
        "foreground-blocks={} defrag-reserve-blocks={} data-bytes={}",
        device.id_, device.path_, device.capacity_blocks_ * kStorageBlockBytes,
        device.data_block_count_, device.data_block_count_ - reserve, reserve,
        device.data_block_count_ * kStorageBlockBytes);
  }
  spdlog::info(
      "storage capacity: data-blocks={} foreground-blocks={} "
      "defrag-reserve-blocks={}",
      total_data_blocks_, foreground_blocks,
      total_data_blocks_ - foreground_blocks);
  direct_io_alignment_ = direct_io_alignment;
  if (options_.flush_size_bytes_ < direct_io_alignment_ ||
      options_.flush_size_bytes_ > kStorageBlockBytes ||
      (options_.flush_size_bytes_ & (options_.flush_size_bytes_ - 1)) != 0 ||
      options_.flush_size_bytes_ % direct_io_alignment_ != 0) {
    return absl::Status(
        absl::StatusCode::kInvalidArgument,
        "flush size must be a power of two between the direct-I/O alignment "
        "and the 8 MiB storage block size");
  }
  spdlog::info("storage direct-I/O alignment={} bytes", direct_io_alignment_);
  spdlog::info("storage flush submission size={} bytes",
               options_.flush_size_bytes_);

  absl::Status system_state = LoadSystemState();
  if (!system_state.ok()) return system_state;

  worker_count_ = worker_count;
  epoch_values_ = InitialEpochValues();
  std::vector<CheckpointRoot> loaded_checkpoint_roots;
  loaded_checkpoint_roots.reserve(devices_.size());
  device_allocators_.clear();
  device_allocators_.reserve(devices_.size());
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    const std::size_t bitmap_bytes = ScanBitmapBytes(device.capacity_blocks_);
    const std::size_t bitmap_page_count =
        ScanBitmapPageCount(device.capacity_blocks_);
    auto allocator = std::make_unique<DeviceAllocator>();
    allocator->owner_ =
        static_cast<bycorf::WorkerId>(device_index % worker_count_);
    allocator->data_block_begin_ = device.data_block_begin_;
    allocator->next_pristine_ = device.data_block_begin_;
    allocator->scan_bitmap_.resize(bitmap_bytes, std::byte{0});
    allocator->bitmap_pages_.resize(bitmap_page_count);
    allocator->checkpoint_bitmap_.resize(bitmap_bytes, std::byte{0});
    allocator->checkpoint_bitmap_pages_.resize(bitmap_page_count);
    allocator->epoch_pages_.resize(kEpochMetadataPageCount);
    allocator->epoch_values_ = InitialEpochValues();
    allocator->durable_epoch_values_ = InitialEpochValues();

    absl::Status load_status = absl::OkStatus();
    for (std::size_t page_index = 0; page_index < kEpochMetadataPageCount;
         ++page_index) {
      const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
      const std::size_t payload_bytes = std::min(
          kMetadataPagePayloadBytes, kEpochMetadataBytes - byte_offset);
      auto loaded = ReadMetadataPagePair(
          device.path_, kEpochMetadataOffset, MetadataPageKind::kEpochs,
          static_cast<std::uint32_t>(page_index), payload_bytes);
      if (!loaded.ok()) {
        load_status = loaded.status();
        break;
      }
      allocator->epoch_pages_[page_index] = loaded->state_;
      const std::size_t first_value = byte_offset / sizeof(std::uint64_t);
      const std::size_t value_count = payload_bytes / sizeof(std::uint64_t);
      for (std::size_t value_index = 0; value_index < value_count;
           ++value_index) {
        std::uint64_t value = 0;
        std::memcpy(
            &value,
            loaded->payload_.data() + value_index * sizeof(std::uint64_t),
            sizeof(value));
        if (first_value + value_index < kCheckpointGenerationIndex) {
          value = std::max<std::uint64_t>(value, 1);
        }
        allocator->durable_epoch_values_[first_value + value_index] = value;
        if (first_value + value_index < kCheckpointGenerationIndex) {
          epoch_values_[first_value + value_index] =
              std::max(epoch_values_[first_value + value_index], value);
        }
      }
    }
    loaded_checkpoint_roots.push_back(CheckpointRoot{
        .generation_ =
            allocator->durable_epoch_values_[kCheckpointGenerationIndex],
        .consumed_generation_ =
            allocator
                ->durable_epoch_values_[kCheckpointConsumedGenerationIndex],
        .block_count_ =
            allocator->durable_epoch_values_[kCheckpointBlockCountIndex],
        .entry_count_ =
            allocator->durable_epoch_values_[kCheckpointEntryCountIndex],
    });
    for (std::size_t page_index = 0;
         load_status.ok() && page_index < bitmap_page_count; ++page_index) {
      const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
      const std::size_t payload_bytes =
          std::min(kMetadataPagePayloadBytes, bitmap_bytes - byte_offset);
      auto loaded = ReadMetadataPagePair(
          device.path_, kScanBitmapMetadataOffset,
          MetadataPageKind::kScanBitmap, static_cast<std::uint32_t>(page_index),
          payload_bytes);
      if (!loaded.ok()) {
        load_status = loaded.status();
        break;
      }
      allocator->bitmap_pages_[page_index] = loaded->state_;
      std::memcpy(allocator->scan_bitmap_.data() + byte_offset,
                  loaded->payload_.data(), payload_bytes);
    }
    const std::uint64_t checkpoint_bitmap_offset =
        CheckpointBitmapMetadataOffset(device.capacity_blocks_);
    for (std::size_t page_index = 0; page_index < bitmap_page_count;
         ++page_index) {
      const std::size_t byte_offset = page_index * kMetadataPagePayloadBytes;
      const std::size_t payload_bytes =
          std::min(kMetadataPagePayloadBytes, bitmap_bytes - byte_offset);
      auto loaded = ReadMetadataPagePair(device.path_, checkpoint_bitmap_offset,
                                         MetadataPageKind::kCheckpointBitmap,
                                         static_cast<std::uint32_t>(page_index),
                                         payload_bytes);
      if (!loaded.ok()) {
        // Checkpoint discovery metadata is only an accelerator. A damaged page
        // disables this generation and is overwritten with zero at startup;
        // it must not make authoritative record recovery unavailable.
        allocator->checkpoint_bitmap_valid_ = false;
        continue;
      }
      allocator->checkpoint_bitmap_pages_[page_index] = loaded->state_;
      std::memcpy(allocator->checkpoint_bitmap_.data() + byte_offset,
                  loaded->payload_.data(), payload_bytes);
    }
    if (!load_status.ok()) {
      return absl::Status(
          load_status.code(),
          std::string(load_status.message()) + ": " + device.path_);
    }
    for (std::uint64_t local = device.capacity_blocks_;
         local-- > device.data_block_begin_;) {
      const std::size_t byte_index = static_cast<std::size_t>(local / 8);
      const unsigned bit_index = static_cast<unsigned>(local % 8);
      if ((std::to_integer<unsigned>(allocator->scan_bitmap_[byte_index]) &
           (1U << bit_index)) != 0) {
        allocator->next_pristine_ = local + 1;
        break;
      }
    }
    for (std::uint64_t local = device.data_block_begin_;
         local < allocator->next_pristine_; ++local) {
      const std::size_t byte_index = static_cast<std::size_t>(local / 8);
      const unsigned bit_index = static_cast<unsigned>(local % 8);
      if ((std::to_integer<unsigned>(allocator->scan_bitmap_[byte_index]) &
           (1U << bit_index)) == 0) {
        allocator->cold_free_.push_back(
            MakeBlockId(device.id_, static_cast<std::uint32_t>(local)));
      }
    }
    for (std::uint64_t local = device.data_block_begin_;
         local < device.capacity_blocks_; ++local) {
      const std::size_t byte_index = static_cast<std::size_t>(local / 8);
      const unsigned bit_index = static_cast<unsigned>(local % 8);
      if ((std::to_integer<unsigned>(allocator->scan_bitmap_[byte_index]) &
           (1U << bit_index)) != 0) {
        ++recovery_allocated_blocks_;
      }
    }
    device_allocators_.push_back(std::move(allocator));
  }
  checkpoint_root_ = {};
  for (const CheckpointRoot& root : loaded_checkpoint_roots) {
    if (root.generation_ > checkpoint_root_.generation_) {
      checkpoint_root_ = root;
    } else if (root.generation_ == checkpoint_root_.generation_ &&
               root.consumed_generation_ >
                   checkpoint_root_.consumed_generation_) {
      checkpoint_root_ = root;
    }
  }
  checkpoint_root_coherent_ = std::all_of(
      loaded_checkpoint_roots.begin(), loaded_checkpoint_roots.end(),
      [this](const CheckpointRoot& root) { return root == checkpoint_root_; });
  const bool checkpoint_root_valid =
      checkpoint_root_.generation_ != 0 &&
      checkpoint_root_.generation_ > checkpoint_root_.consumed_generation_ &&
      checkpoint_root_.block_count_ >= 2 * worker_count_ &&
      checkpoint_root_.block_count_ <= recovery_allocated_blocks_;
  const bool checkpoint_bitmaps_valid =
      std::all_of(device_allocators_.begin(), device_allocators_.end(),
                  [](const std::unique_ptr<DeviceAllocator>& allocator) {
                    return allocator->checkpoint_bitmap_valid_;
                  });
  checkpoint_active_.store(
      ShutdownCheckpointEnabled() && checkpoint_root_coherent_ &&
          checkpoint_root_valid && checkpoint_bitmaps_valid,
      std::memory_order_relaxed);
  epoch_values_[kCheckpointGenerationIndex] = checkpoint_root_.generation_;
  epoch_values_[kCheckpointConsumedGenerationIndex] =
      checkpoint_root_.consumed_generation_;
  epoch_values_[kCheckpointBlockCountIndex] = checkpoint_root_.block_count_;
  epoch_values_[kCheckpointEntryCountIndex] = checkpoint_root_.entry_count_;
  // Runtime device owners start from the canonical component-wise maximum.
  // durable_epoch_values retains what each device actually contained, so a
  // later update to the same page also repairs stale mirror fields.
  for (auto& allocator : device_allocators_) {
    allocator->epoch_values_ = epoch_values_;
  }
  recovery_device_cursors_ =
      std::make_unique<RecoveryDeviceCursor[]>(devices_.size());
  for (std::size_t i = 0; i < devices_.size(); ++i) {
    recovery_device_cursors_[i].next_local_.store(
        device_allocators_[i]->next_pristine_, std::memory_order_relaxed);
    recovery_device_cursors_[i].next_allocation_epoch_.store(
        1, std::memory_order_relaxed);
  }
  const auto recovery_start = std::chrono::steady_clock::now();
  recovery_started_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                             recovery_start.time_since_epoch())
                             .count();
  recovery_next_log_ms_.store(recovery_started_ms_ + 5000,
                              std::memory_order_relaxed);
  stores_.reserve(worker_count);
  for (unsigned i = 0; i < worker_count; ++i) {
    stores_.push_back(std::make_unique<WorkerStore>());
    WorkerStore& store = *stores_.back();
    // WriteRecordLocked reserves a new page together with any bucket growth
    // before it mutates the durable staging block. The arena must not reserve
    // the same bytes again after that point.
    store.record_index_entry_arena_ = std::make_shared<ScanHashMapEntryArena>(
        ScanHashMapEntryArena::kMaximumPageId,
        /*externally_admitted=*/true,
        /*externally_accounted=*/false,
        /*owner_shard=*/i + 1);
    const std::size_t partition_count =
        (kLogicalStorageShards + worker_count - 1 - i) / worker_count;
    const std::size_t index_count = partition_count * options_.database_count_;
    store.partitions_.reserve(partition_count);
    store.partition_indexes_.reserve(index_count);
    store.partition_grouped_objects_.reserve(index_count);
    // Construct directly with the shared arena. In particular, do not create
    // and then discard a private grouped-object arena for every empty DB.
    for (std::size_t index = 0; index < index_count; ++index) {
      store.partition_indexes_.emplace_back(store.record_index_entry_arena_);
      store.partition_grouped_objects_.emplace_back(
          store.record_index_entry_arena_);
    }
    for (std::uint32_t partition = i; partition < kLogicalStorageShards;
         partition += worker_count) {
      store.partitions_.emplace_back();
      WorkerStore::PartitionStore& partition_store = store.partitions_.back();
      partition_store.id_ = static_cast<std::uint16_t>(partition);
      const std::size_t offset =
          (store.partitions_.size() - 1) * options_.database_count_;
      partition_store.indexes_ = std::span(store.partition_indexes_)
                                     .subspan(offset, options_.database_count_);
      partition_store.grouped_objects_ =
          std::span(store.partition_grouped_objects_)
              .subspan(offset, options_.database_count_);
      partition_store.replication_epoch_ =
          epoch_values_[kLogicalDatabaseCount + partition];
      partition_store.replica_candidate_epoch_ =
          partition_store.replication_epoch_;
    }
  }
  absl::Status affinity = ConfigureWorkerDeviceAffinity();
  if (!affinity.ok()) {
    return affinity;
  }
  // Metadata probing is complete. Return its temporary controller qpairs so
  // a controller advertising exactly worker_count queues can still start.
  bycorf::ReleaseSpdkStorageMetadataQpairs();
  for (std::uint8_t db_id = 0; db_id < kLogicalDatabaseCount; ++db_id) {
    db_epochs_[db_id].store(epoch_values_[db_id], std::memory_order_relaxed);
  }
  open_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_consumed_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_capacity_loaded_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_capacity_ready_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_indexes_preallocated_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_indexes_ready_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_loaded_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_index_validated_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  metadata_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_retired_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  recovery_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  recovery_accounting_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  free_list_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  orphan_extent_barrier_ = std::make_unique<CoroutineBarrier>(worker_count);
  shutdown_recovery_ready_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  shutdown_recovery_published_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  shutdown_checkpoint_ready_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  shutdown_checkpoint_tx_cleaned_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  shutdown_checkpoint_refrozen_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  shutdown_checkpoint_built_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  shutdown_checkpoint_published_barrier_ =
      std::make_unique<CoroutineBarrier>(worker_count);
  checkpoint_shards_.resize(worker_count);
  checkpoint_load_results_.resize(worker_count);
  for (CheckpointLoadResult& result : checkpoint_load_results_) {
    result.saw_shards_.resize(worker_count, false);
    result.saw_accounting_shards_.resize(worker_count, false);
    result.capacity_chunks_by_shard_.resize(worker_count, 0);
  }
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::ApplyRecoveryLiveReferenceBatches(
    WorkerStore& store,
    std::vector<std::vector<RecoveryLiveReference>>* batches) {
  for (unsigned owner = 0; owner < worker_count_; ++owner) {
    std::vector<RecoveryLiveReference>& pending = batches->at(owner);
    if (pending.empty()) {
      continue;
    }
    std::vector<RecoveryLiveReference> references;
    std::swap(references, pending);
    auto apply_live = [this, owner,
                       references = std::move(references)]() mutable {
      WorkerStore& owner_store = *stores_[owner];
      for (const RecoveryLiveReference& reference : references) {
        BlockState* state = FindBlockState(owner_store, reference.block_id_);
        if (state == nullptr || !state->allocated_ ||
            state->allocation_epoch_ != reference.allocation_epoch_) {
          return absl::Status(absl::StatusCode::kInternal,
                              "recovery live reference has no owning block");
        }
        if (reference.extent_) {
          const auto found =
              owner_store.recovered_extents_.find(reference.block_id_);
          if (state->kind_ != BlockKind::kPayloadExtent ||
              reference.extent_payload_bytes_ == 0 ||
              state->committed_bytes_ !=
                  kBlockHeaderBytes + reference.extent_payload_bytes_ ||
              found == owner_store.recovered_extents_.end() ||
              found->second.extent_index_ != reference.extent_index_ ||
              found->second.payload_checksum_ !=
                  reference.extent_payload_checksum_) {
            return absl::Status(absl::StatusCode::kInternal,
                                "live extent header does not match manifest");
          }
        }
        if (reference.replace_live_bytes_) {
          if (reference.txid_ != 0 || state->live_bytes_ != 0) {
            return absl::Status(
                absl::StatusCode::kInternal,
                "checkpoint accounting repeats an initialized block");
          }
          state->live_bytes_ = reference.bytes_;
        } else {
          state->live_bytes_ += reference.bytes_;
        }
        if (reference.txid_ != 0) {
          const auto tx_block =
              owner_store.tx_blocks_.find(reference.block_id_);
          if (tx_block != owner_store.tx_blocks_.end()) {
            NoteTxRecordLocal(owner_store, reference.block_id_,
                              reference.allocation_epoch_,
                              tx_block->second.generation_, reference.txid_,
                              reference.bytes_, false);
          }
        }
      }
      return absl::OkStatus();
    };
    // if/else, not ?:, to keep the co_await out of a conditional expression
    // (GCC coroutine frame-slot aliasing).
    absl::Status applied = absl::OkStatus();
    if (owner == store.worker_->id()) {
      applied = apply_live();
    } else {
      applied = co_await bycorf::SubmitTo(owner, std::move(apply_live));
    }
    if (!applied.ok()) {
      co_return applied;
    }
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::InitializeWorker(Worker& worker) {
  WorkerStore& store = *stores_[worker.id()];
  store.worker_ = &worker;

  absl::Status status = store.buffers_.Init(worker, options_.buffers_);
  if (status.ok()) {
    status = worker.RegisterFixedFiles(
        static_cast<unsigned>(options_.data_files_.size()));
  }
  if (status.ok()) {
    if (bycorf::SpdkStorageEnabled()) {
      store.files_.resize(options_.data_files_.size());
      for (std::size_t i = 0; i < store.files_.size(); ++i) {
        store.files_[i] = FixedFile{.index_ = static_cast<std::uint32_t>(i)};
      }
      for (std::size_t device_index = 0; device_index < devices_.size();
           ++device_index) {
        const StorageDevice& device = devices_[device_index];
        if (!std::binary_search(device_owners_[device_index].begin(),
                                device_owners_[device_index].end(),
                                static_cast<std::uint16_t>(worker.id()))) {
          continue;
        }
        FixedFile file{.index_ = device.file_index_};
        status = co_await bycorf::OpenFixedFile(worker, device.path_,
                                                O_RDWR | O_DIRECT, 0, file);
        if (!status.ok()) {
          break;
        }
        store.files_[device.file_index_] = file;
      }
    } else {
      store.files_.reserve(options_.data_files_.size());
      for (std::size_t i = 0; i < options_.data_files_.size(); ++i) {
        FixedFile file{.index_ = static_cast<std::uint32_t>(i)};
        status = co_await bycorf::OpenFixedFile(worker, options_.data_files_[i],
                                                O_RDWR | O_DIRECT, 0, file);
        if (!status.ok()) {
          break;
        }
        store.files_.push_back(file);
      }
    }
  }
  if (!status.ok()) {
    Fail(status);
    co_return status;
  }

  status = co_await open_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }

  std::vector<RecoveryBatch> batches(worker_count_);
  std::vector<std::uint64_t> zero_blocks;
  CheckpointLoadResult& checkpoint_load = checkpoint_load_results_[worker.id()];
  if (worker.id() == 0) {
    if (!checkpoint_root_coherent_ ||
        checkpoint_root_.generation_ > checkpoint_root_.consumed_generation_) {
      CheckpointRoot consumed = checkpoint_root_;
      consumed.consumed_generation_ = consumed.generation_;
      status = co_await PersistCheckpointRoot(consumed);
      if (!status.ok()) {
        Fail(status);
        co_return status;
      }
    }
  }

  status = co_await checkpoint_consumed_barrier_->Wait(worker);
  if (!status.ok()) co_return status;

  if (checkpoint_active_.load(std::memory_order_acquire)) {
    checkpoint_load.status_ =
        co_await DiscoverCheckpoint(store, &checkpoint_load);
  }

  status = co_await checkpoint_capacity_loaded_barrier_->Wait(worker);
  if (!status.ok()) co_return status;

  if (worker.id() == 0 && checkpoint_active_.load(std::memory_order_acquire)) {
    const absl::Status prepared = co_await PrepareCheckpointIndexes();
    if (!prepared.ok()) {
      spdlog::warn(
          "shutdown checkpoint generation={} cannot prepare indexes; "
          "falling back to record scan: {}",
          checkpoint_root_.generation_, prepared.message());
      checkpoint_load_fell_back_.store(true, std::memory_order_release);
      checkpoint_active_.store(false, std::memory_order_release);
    }
  }

  status = co_await checkpoint_capacity_ready_barrier_->Wait(worker);
  if (!status.ok()) co_return status;

  if (checkpoint_active_.load(std::memory_order_acquire)) {
    checkpoint_load.status_ = PreallocateCheckpointIndexes(store);
  }

  status = co_await checkpoint_indexes_preallocated_barrier_->Wait(worker);
  if (!status.ok()) co_return status;

  if (worker.id() == 0 && checkpoint_active_.load(std::memory_order_acquire)) {
    absl::Status preallocation_status = absl::OkStatus();
    for (const CheckpointLoadResult& loaded : checkpoint_load_results_) {
      if (!loaded.status_.ok()) {
        preallocation_status = loaded.status_;
        break;
      }
    }
    if (!preallocation_status.ok()) {
      // No body has been installed yet. All workers must observe the shared
      // fallback decision before any of them starts decoding checkpoint data.
      spdlog::warn(
          "shutdown checkpoint generation={} cannot preallocate indexes; "
          "falling back to record scan: {}",
          checkpoint_root_.generation_, preallocation_status.message());
      checkpoint_load_fell_back_.store(true, std::memory_order_release);
      checkpoint_active_.store(false, std::memory_order_release);
    }
  }

  status = co_await checkpoint_indexes_ready_barrier_->Wait(worker);
  if (!status.ok()) co_return status;

  if (checkpoint_active_.load(std::memory_order_acquire)) {
    checkpoint_load.status_ = co_await LoadCheckpoint(store, &checkpoint_load);
  }

  status = co_await checkpoint_loaded_barrier_->Wait(worker);
  if (!status.ok()) co_return status;

  if (checkpoint_active_.load(std::memory_order_acquire) &&
      checkpoint_load.status_.ok()) {
    checkpoint_load.status_ = ValidateCheckpointIndexSizes(store);
  }

  status = co_await checkpoint_index_validated_barrier_->Wait(worker);
  if (!status.ok()) co_return status;

  if (worker.id() == 0) {
    if (checkpoint_active_.load(std::memory_order_acquire)) {
      absl::Status load_status = absl::OkStatus();
      std::uint64_t loaded_blocks = 0;
      std::uint64_t loaded_entries = 0;
      std::uint64_t loaded_accounting_entries = 0;
      std::vector<bool> saw_shards(worker_count_, false);
      std::vector<bool> saw_accounting_shards(worker_count_, false);
      for (const CheckpointLoadResult& loaded : checkpoint_load_results_) {
        if (load_status.ok() && !loaded.status_.ok()) {
          load_status = loaded.status_;
        }
        if (std::numeric_limits<std::uint64_t>::max() - loaded_blocks <
                loaded.blocks_.size() ||
            std::numeric_limits<std::uint64_t>::max() - loaded_entries <
                loaded.entry_count_ ||
            std::numeric_limits<std::uint64_t>::max() -
                    loaded_accounting_entries <
                loaded.accounting_entry_count_) {
          if (load_status.ok()) {
            load_status =
                absl::InternalError("checkpoint decoded totals overflow");
          }
          continue;
        }
        loaded_blocks += loaded.blocks_.size();
        loaded_entries += loaded.entry_count_;
        loaded_accounting_entries += loaded.accounting_entry_count_;
        for (unsigned shard = 0; shard < worker_count_; ++shard) {
          saw_shards[shard] = saw_shards[shard] || loaded.saw_shards_[shard];
          saw_accounting_shards[shard] = saw_accounting_shards[shard] ||
                                         loaded.saw_accounting_shards_[shard];
        }
      }
      if (load_status.ok() && loaded_blocks != checkpoint_root_.block_count_) {
        load_status = absl::InternalError("checkpoint block count mismatch");
      }
      if (load_status.ok() && loaded_entries != checkpoint_root_.entry_count_) {
        load_status = absl::InternalError("checkpoint entry count mismatch");
      }
      if (load_status.ok() && std::find(saw_shards.begin(), saw_shards.end(),
                                        false) != saw_shards.end()) {
        load_status =
            absl::InternalError("checkpoint bitmap omits a worker shard");
      }
      if (load_status.ok() &&
          std::find(saw_accounting_shards.begin(), saw_accounting_shards.end(),
                    false) != saw_accounting_shards.end()) {
        load_status = absl::InternalError(
            "checkpoint bitmap omits a worker accounting shard");
      }
      if (!load_status.ok()) {
        spdlog::warn(
            "shutdown checkpoint generation={} is unusable; falling back "
            "to record scan: {}",
            checkpoint_root_.generation_, load_status.message());
        checkpoint_load_fell_back_.store(true, std::memory_order_release);
        checkpoint_active_.store(false, std::memory_order_release);
      } else {
        checkpoint_loaded_block_count_ = loaded_blocks;
        spdlog::info(
            "loaded shutdown checkpoint generation={} entries={} "
            "accounting-entries={} blocks={} scan-workers={}",
            checkpoint_root_.generation_, loaded_entries,
            loaded_accounting_entries, loaded_blocks, worker_count_);
      }
    }

    // The discovery bitmap is one-use even if loading was disabled or failed.
    // The consumed root prevents reuse if clearing itself is interrupted.
    const absl::Status cleared = co_await PersistCheckpointBitmap({});
    if (!cleared.ok()) {
      spdlog::warn("failed to clear checkpoint bitmap: {}", cleared.message());
    }
  }

  status = co_await metadata_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }

  if (checkpoint_active_.load(std::memory_order_acquire)) {
    if (worker.id() == 0) {
      assert(recovery_allocated_blocks_ >= checkpoint_loaded_block_count_);
      recovery_allocated_blocks_ -= checkpoint_loaded_block_count_;
    }
    // Every scan worker retires the blocks it validated. Allocation-bitmap
    // updates remain serialized by each device owner, while devices and
    // independent metadata pages can progress concurrently.
    status = co_await ReturnColdBlocks(std::move(checkpoint_load.blocks_));
    if (!status.ok()) {
      Fail(status);
      co_return status;
    }
  } else {
    checkpoint_load.blocks_.clear();
    checkpoint_load.live_by_block_.clear();
    checkpoint_load.live_by_block_.rehash(0);
  }

  status = co_await checkpoint_retired_barrier_->Wait(worker);
  if (!status.ok()) co_return status;

  if (checkpoint_load_fell_back_.load(std::memory_order_acquire)) {
    // Checkpoint chunks become visible only after their CRC and structure
    // validate, but a later chunk can still invalidate the whole snapshot.
    // Mark the already-installed prefix as newer than equal-sequence disk
    // copies before the cold scan merges them. The successful path avoids
    // this O(keys) auxiliary hash table entirely.
    for (auto& partition : store.partitions_) {
      for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
        partition.indexes_[db_id].ForEach([&](RecordIndex::Entry& entry) {
          store.recovery_lsns_.insert_or_assign(
              &entry, std::numeric_limits<std::uint64_t>::max());
        });
      }
    }
  }

  absl::flat_hash_set<std::uint64_t> committed_txids;
  status = co_await ScanAssignedBlocks(store, &batches, &zero_blocks,
                                       &committed_txids);
  if (!status.ok()) {
    Fail(status);
    co_return status;
  }
  if (!committed_txids.empty()) {
    std::lock_guard<std::mutex> lock(recovery_committed_mutex_);
    recovery_committed_txids_.merge(committed_txids);
  }

  status = co_await ApplyRecoveryBatches(store, &batches);
  if (!status.ok()) {
    Fail(status);
    co_return status;
  }

  status = co_await recovery_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }

  // LSN comparisons are only between physical copies of one logical key
  // version. All publications for a key run on its current key owner. Seed
  // every worker above all durable values, then stripe by worker id so normal
  // appends need only this worker-local integer. A topology change happens
  // across this recovery boundary, where the new base again exceeds every
  // value produced by the old topology.
  const std::uint64_t recovered_max_lsn =
      recovery_max_lsn_.load(std::memory_order_relaxed);
  if (recovered_max_lsn >
      std::numeric_limits<std::uint64_t>::max() - worker_count_) {
    status = absl::Status(absl::StatusCode::kResourceExhausted,
                          "physical LSN space is exhausted");
    Fail(status);
    co_return status;
  }
  store.next_lsn_ = recovered_max_lsn + 1 + worker.id();

  // Every scan has fed the committed set by now (merges happen before the
  // barrier); decide the parked transaction-tagged records. A tagged record
  // without its commit record is a prepare whose transaction never durably
  // committed — recovery drops it, which is exactly the all-or-nothing the
  // commit protocol promises.
  for (const RecoveryRecord& parked : store.recovery_tx_records_) {
    if (recovery_committed_txids_.contains(parked.txid_)) {
      status = ApplyRecoveredRecord(store, parked);
      if (!status.ok()) {
        Fail(status);
        co_return status;
      }
    }
  }
  // Roots are selected by the ordinary key/epoch/sequence merge; only now
  // can their auxiliary graphs be adjudicated against every durable commit.
  // Do this before expiration or orphan reclamation can retire any graph.
  status = co_await RecoverGroupedObjects(store);
  if (status.ok()) status = co_await ValidateRecoveredGroups(store);
  if (!status.ok()) {
    Fail(status);
    co_return status;
  }

  // Winner selection must see expired values so a newer expired version
  // still suppresses every older version. Keep every expired winner charged
  // until an expiration authority can publish a durable tombstone: dropping
  // an unshielded winner only in memory is also unsafe if a later boot
  // observes a clock rollback. Without authority recovery leaves the winner
  // indexed (ordinary reads still hide it), so a later authority grant can
  // let active expiration retire it safely.
  struct RecoveryExpiredTombstone {
    std::uint8_t db_id_ = 0;
    Digest digest_{};
    std::string key_;
    bool shielding_ = false;
  };
  std::vector<RecoveryExpiredTombstone> expired_tombstones;
  std::uint64_t recovery_now_ms = UnixTimeMillis();
  LAVIK_FAULT_INJECT(
      // Deterministically emulate a wall-clock rollback in recovery tests. This
      // must not be used as a correctness mechanism: runtime expiration first
      // publishes a durable tombstone and only then retires a collection graph,
      // so an older root can never become the winning recoverable version
      // merely because this clock moved backwards.
      if (const char* configured = std::getenv("LAVIK_RECOVERY_NOW_MS");
          configured != nullptr) {
        const char* end = configured + std::strlen(configured);
        std::uint64_t overridden = 0;
        const auto parsed = std::from_chars(configured, end, overridden);
        if (parsed.ec == std::errc{} && parsed.ptr == end) {
          recovery_now_ms = overridden;
        }
      });
  if (expiration_authority_.load(std::memory_order_acquire)) {
    for (auto& partition : store.partitions_) {
      for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
        // Recovery rebuilds this count alongside every winning index entry.
        // A zero count proves that no value in this index carries an expiry,
        // so scanning all buckets cannot discover work. This matters
        // especially for large persistent datasets without TTLs, while
        // preserving the durable-tombstone treatment below for every index
        // that can expire.
        if (partition.expiring_key_count_[db_id] == 0) continue;
        auto& index = partition.indexes_[db_id];
        index.ForEach([&](RecordIndex::Entry& entry) {
          if (entry.value_.kind() == RecordKind::kValue &&
              IsExpired(entry, recovery_now_ms)) {
            std::string key;
            Digest digest;
            if (entry.key_complete()) {
              key = std::string(entry.key());
              digest = ComputeDigest(key);
            } else {
              const auto recovered_key =
                  store.recovery_external_keys_.find(&entry);
              if (recovered_key == store.recovery_external_keys_.end()) {
                status = absl::InternalError(
                    "expired recovered winner has no complete key");
                return;
              }
              key = recovered_key->second;
              digest = entry.external_key_digest();
            }
            expired_tombstones.push_back(RecoveryExpiredTombstone{
                .db_id_ = db_id,
                .digest_ = digest,
                .key_ = std::move(key),
                .shielding_ = entry.value_.shielding(),
            });
          }
        });
        if (!status.ok()) {
          Fail(status);
          co_return status;
        }
      }
    }
  }
  store.recovery_tx_records_.clear();
  store.recovery_tx_records_.shrink_to_fit();
  store.recovery_lsns_.clear();
  store.recovery_lsns_.rehash(0);
  if (worker.id() == 0) {
    // Seed the transaction-id counter above everything on disk so a new
    // boot's transactions can never alias a previous boot's commit records.
    tx::TxRuntime::Get()->next_txid_.store(
        std::max<std::uint64_t>(
            recovery_max_txid_.load(std::memory_order_relaxed) + 1, 1),
        std::memory_order_relaxed);
  }

  const std::size_t batch_target_bytes =
      RecoveryWorkerBatchTargetBytes(worker_count_);
  std::vector<std::vector<RecoveryLiveReference>> live_by_owner(worker_count_);
  std::size_t buffered_bytes = 0;
  // System-state blobs are not represented by key-index checkpoint entries,
  // so charge them on both checkpoint and cold-scan recovery paths.
  if (worker.id() == 0) {
    auto account_system_extents = [&](ExtentManifest extents) -> absl::Status {
      if (extents == nullptr) return absl::OkStatus();
      for (std::size_t extent_index = 0; extent_index < extents->size();
           ++extent_index) {
        const ExtentRef& extent = extents->at(extent_index);
        const std::uint16_t owner = BlockOwner(extent.block_id_);
        if (owner >= worker_count_) {
          return absl::InternalError(
              "system-state manifest references an unscanned extent");
        }
        live_by_owner[owner].push_back(RecoveryLiveReference{
            .block_id_ = extent.block_id_,
            .allocation_epoch_ = extent.allocation_epoch_,
            .bytes_ = extent.payload_bytes_,
            .expected_owner_ = owner,
            .extent_ = true,
            .extent_payload_bytes_ = extent.payload_bytes_,
            .extent_index_ = static_cast<std::uint32_t>(extent_index),
            .extent_payload_checksum_ = extent.payload_checksum_,
        });
        buffered_bytes += sizeof(RecoveryLiveReference);
      }
      return absl::OkStatus();
    };
    status = account_system_extents(system_state_.manifest_extents_);
    if (status.ok()) {
      status = account_system_extents(system_state_.catalog_extents_);
    }
    if (!status.ok()) {
      Fail(status);
      co_return status;
    }
  }

  if (checkpoint_active_.load(std::memory_order_acquire)) {
    // The checkpoint persisted one absolute live-byte value per physical
    // block. Route that bounded table now that the header scan has established
    // block owners, avoiding both per-key aggregation and a second O(keys)
    // walk over the rebuilt index.
    for (auto& [block_id, reference] : checkpoint_load.live_by_block_) {
      const std::uint16_t owner = BlockOwner(block_id);
      if (owner >= worker_count_ ||
          (reference.expected_owner_ != kUnownedBlock &&
           reference.expected_owner_ != owner)) {
        status = absl::InternalError(
            "checkpoint live reference has no matching scanned block owner");
        Fail(status);
        co_return status;
      }
      live_by_owner[owner].push_back(reference);
      buffered_bytes += sizeof(RecoveryLiveReference);
      if (buffered_bytes >= batch_target_bytes) {
        status =
            co_await ApplyRecoveryLiveReferenceBatches(store, &live_by_owner);
        if (!status.ok()) {
          Fail(status);
          co_return status;
        }
        buffered_bytes = 0;
      }
    }
    checkpoint_load.live_by_block_.clear();
    checkpoint_load.live_by_block_.rehash(0);
  } else {
    // Cold recovery can install multiple versions before choosing winners.
    // Walk the final index with an exact resumable cursor and send physical
    // accounting in bounded batches instead of retaining one reference per
    // live key until the complete pass finishes.
    for (auto& partition : store.partitions_) {
      for (std::uint8_t db_id = 0; db_id < options_.database_count_; ++db_id) {
        auto& index = partition.indexes_[db_id];
        RecordIndex::StableScanCursor cursor;
        bool exhausted = false;
        while (!exhausted) {
          exhausted = index.ScanStableWhile(
              &cursor, [&](const RecordIndex::Entry& entry) {
                const RecordLocation location = MaterializeIndexLocation(entry);
                if (location.block_owner() >= worker_count_) {
                  status = absl::InternalError(
                      "recovery live root has no scanned block owner");
                  return false;
                }
                live_by_owner[location.block_owner()].push_back(
                    RecoveryLiveReference{
                        .block_id_ = location.block_id(),
                        .allocation_epoch_ = location.allocation_epoch(),
                        .txid_ = store.recovery_txids_.contains(&entry)
                                     ? store.recovery_txids_.at(&entry)
                                     : 0,
                        .bytes_ = location.total_disk_bytes(),
                        .expected_owner_ = location.block_owner(),
                    });
                buffered_bytes += sizeof(RecoveryLiveReference);
                const ExtentManifest extents = ExtentsFor(store, &entry);
                if (location.external() && extents != nullptr) {
                  for (std::size_t extent_index = 0;
                       extent_index < extents->size(); ++extent_index) {
                    const ExtentRef& extent = extents->at(extent_index);
                    const std::uint16_t extent_owner =
                        BlockOwner(extent.block_id_);
                    if (extent_owner >= worker_count_) {
                      status = absl::InternalError(
                          "manifest references an unscanned extent");
                      return false;
                    }
                    live_by_owner[extent_owner].push_back(RecoveryLiveReference{
                        .block_id_ = extent.block_id_,
                        .allocation_epoch_ = extent.allocation_epoch_,
                        .bytes_ = extent.payload_bytes_,
                        .expected_owner_ = extent_owner,
                        .extent_ = true,
                        .extent_payload_bytes_ = extent.payload_bytes_,
                        .extent_index_ =
                            static_cast<std::uint32_t>(extent_index),
                        .extent_payload_checksum_ = extent.payload_checksum_,
                    });
                    buffered_bytes += sizeof(RecoveryLiveReference);
                  }
                }
                return buffered_bytes < batch_target_bytes;
              });
          if (!status.ok()) {
            Fail(status);
            co_return status;
          }
          if (buffered_bytes >= batch_target_bytes) {
            status = co_await ApplyRecoveryLiveReferenceBatches(store,
                                                                &live_by_owner);
            if (!status.ok()) {
              Fail(status);
              co_return status;
            }
            buffered_bytes = 0;
          }
        }
      }
    }
  }
  // Auxiliary records and their extents are independent physical owners.
  // Only complete graph reconstruction marks a candidate reachable; neither
  // an uncommitted prepare nor a superseded incarnation contributes bytes.
  // Flush per extent as well as per group, keeping a single oversized Hash
  // from defeating the worker's bounded recovery-batch memory target.
  for (const RecoveryRecord& recovered : store.recovery_hash_groups_) {
    if (!recovered.grouped_reachable_) continue;
    const RecordLocation& location = recovered.location_;
    if (location.block_owner() >= worker_count_) {
      status =
          absl::DataLossError("recovered Hash group has no physical owner");
      Fail(status);
      co_return status;
    }
    live_by_owner[location.block_owner()].push_back(RecoveryLiveReference{
        .block_id_ = location.block_id(),
        .allocation_epoch_ = location.allocation_epoch(),
        .txid_ = recovered.txid_,
        .bytes_ = location.total_disk_bytes(),
        .expected_owner_ = location.block_owner(),
    });
    buffered_bytes += sizeof(RecoveryLiveReference);
    if (recovered.extents_ != nullptr) {
      for (std::size_t index = 0; index < recovered.extents_->size(); ++index) {
        const ExtentRef& extent = recovered.extents_->at(index);
        const std::uint16_t owner = BlockOwner(extent.block_id_);
        if (owner >= worker_count_) {
          status = absl::DataLossError("Hash group has an unscanned extent");
          Fail(status);
          co_return status;
        }
        live_by_owner[owner].push_back(RecoveryLiveReference{
            .block_id_ = extent.block_id_,
            .allocation_epoch_ = extent.allocation_epoch_,
            .bytes_ = extent.payload_bytes_,
            .expected_owner_ = owner,
            .extent_ = true,
            .extent_payload_bytes_ = extent.payload_bytes_,
            .extent_index_ = static_cast<std::uint32_t>(index),
            .extent_payload_checksum_ = extent.payload_checksum_,
        });
        buffered_bytes += sizeof(RecoveryLiveReference);
        if (buffered_bytes >= batch_target_bytes) {
          status =
              co_await ApplyRecoveryLiveReferenceBatches(store, &live_by_owner);
          if (!status.ok()) {
            Fail(status);
            co_return status;
          }
          buffered_bytes = 0;
        }
      }
    }
    if (buffered_bytes >= batch_target_bytes) {
      status =
          co_await ApplyRecoveryLiveReferenceBatches(store, &live_by_owner);
      if (!status.ok()) {
        Fail(status);
        co_return status;
      }
      buffered_bytes = 0;
    }
  }
  store.recovery_hash_groups_.clear();
  store.recovery_hash_groups_.shrink_to_fit();
  status = co_await ApplyRecoveryLiveReferenceBatches(store, &live_by_owner);
  if (!status.ok()) {
    Fail(status);
    co_return status;
  }

  status = co_await recovery_accounting_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }
  // Every worker's live-reference pass has run, so no manifest still needs
  // to be matched against a recovered extent header.
  store.recovered_extents_.clear();
  store.recovery_txids_.clear();
  store.recovery_txids_.rehash(0);

  std::vector<std::vector<std::uint64_t>> free_by_device(devices_.size());
  for (std::uint64_t block_id : zero_blocks) {
    const std::size_t device_index = DeviceIndexForBlock(block_id);
    const std::uint64_t pristine =
        recovery_device_cursors_[device_index].next_local_.load(
            std::memory_order_acquire);
    if (LocalBlockId(block_id) < pristine) {
      free_by_device[device_index].push_back(block_id);
    }
  }
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const bycorf::WorkerId allocator_owner =
        device_allocators_[device_index]->owner_;
    auto apply_recovery_free = [this, device_index,
                                blocks = std::move(
                                    free_by_device[device_index])]() mutable {
      DeviceAllocator& allocator = *device_allocators_[device_index];
      allocator.next_pristine_ =
          std::max(allocator.next_pristine_,
                   recovery_device_cursors_[device_index].next_local_.load(
                       std::memory_order_acquire));
      allocator.next_allocation_epoch_ = std::max(
          allocator.next_allocation_epoch_,
          recovery_device_cursors_[device_index].next_allocation_epoch_.load(
              std::memory_order_acquire));
      allocator.ready_blocks_.insert(allocator.ready_blocks_.end(),
                                     std::make_move_iterator(blocks.begin()),
                                     std::make_move_iterator(blocks.end()));
      return absl::OkStatus();
    };
    // if/else, not ?:, to keep the co_await out of a conditional
    // expression (GCC coroutine frame-slot aliasing).
    if (allocator_owner == worker.id()) {
      status = apply_recovery_free();
    } else {
      status = co_await bycorf::SubmitTo(allocator_owner,
                                         std::move(apply_recovery_free));
    }
    if (!status.ok()) {
      Fail(status);
      co_return status;
    }
  }
  status = co_await free_list_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }
  // Reclaim orphan extents before writing recovery tombstones. On a full
  // device these may be the only reusable blocks, so deferring this pass
  // until after DeleteLocked makes every restart fail at the same point.
  auto orphan_extents = std::make_shared<std::vector<ExtentRef>>();
  ForEachOwnedBlock(store, [&](std::uint64_t block_id, BlockState& state) {
    if (state.kind_ == BlockKind::kPayloadExtent && state.live_bytes_ == 0) {
      orphan_extents->push_back(ExtentRef{
          .block_id_ = block_id,
          .allocation_epoch_ = state.allocation_epoch_,
          .payload_bytes_ = static_cast<std::uint32_t>(state.committed_bytes_ -
                                                       kBlockHeaderBytes),
          .payload_checksum_ = 0,
      });
    } else {
      MaybeQueueDefrag(store, block_id);
    }
  });
  if (!orphan_extents->empty()) {
    status = co_await ReclaimExtents(
        &store, std::shared_ptr<const std::vector<ExtentRef>>(
                    std::move(orphan_extents)));
    if (!status.ok()) {
      Fail(status);
      co_return status;
    }
  }
  status = co_await orphan_extent_barrier_->Wait(worker);
  if (!status.ok()) {
    co_return status;
  }
  // The recovered roots above have now been charged exactly once and free
  // blocks are available. Normal AppendLocked accounting can therefore
  // replace every expired winner with a tombstone without either
  // double-charging the new record or exposing a side-state-less collection
  // to clients.
  for (const RecoveryExpiredTombstone& expired : expired_tombstones) {
    auto deleted =
        co_await DeleteLocked(expired.db_id_, expired.key_, expired.digest_);
    if (!deleted.ok()) {
      if (deleted.status().code() != absl::StatusCode::kResourceExhausted ||
          expired.shielding_) {
        Fail(deleted.status());
        co_return deleted.status();
      }

      // The same full-device escape valve used by active expiration is safe
      // for an unshielded recovered winner: no older version can become
      // visible, and this record remains expired on disk. A shielding winner
      // must still publish a tombstone before it can be forgotten.
      auto& partition = PartitionForKey(store, expired.key_);
      std::optional<RetiredRecord> retired;
      ExtentManifest value_extents;
      co_await store.store_state_mutex_.Lock();
      {
        UnlockGuard unlock(&store.store_state_mutex_, store.worker_);
        auto resolved = co_await FindVerifiedEntry(
            store, partition.indexes_[expired.db_id_], expired.digest_,
            expired.key_);
        if (!resolved.ok()) {
          Fail(resolved.status());
          co_return resolved.status();
        }
        RecordIndex::Entry* current = *resolved;
        if (current == nullptr ||
            current->value_.kind() != RecordKind::kValue ||
            !IsExpired(*current, recovery_now_ms)) {
          continue;
        }
        if (current->value_.shielding()) {
          Fail(deleted.status());
          co_return deleted.status();
        }
        const RecordLocation dropped = MaterializeIndexLocation(*current);
        value_extents = ExtentsFor(store, current);
        retired = RetiredRecordOf(dropped, DependentExtentsFor(store, current));
        --partition.live_key_count_[expired.db_id_];
        --store.live_key_count_[expired.db_id_];
        --partition.expiring_key_count_[expired.db_id_];
        store.external_manifests_.erase(current);
        store.recovery_external_keys_.erase(current);
        const std::size_t logical_key_bytes = current->logical_key_size();
        const bool erased = partition.indexes_[expired.db_id_].Erase(current);
        assert(erased);
        if (erased) {
          RemoveFullSyncCoverageEntry(partition, expired.db_id_,
                                      logical_key_bytes);
        }
        if (!dropped.external() || dropped.key_external()) {
          value_extents.reset();
        }
      }
      if (value_extents != nullptr) SpawnExtentReclaim(store, value_extents);
      absl::Status dead = co_await MarkRecordDead(*retired);
      if (!dead.ok()) {
        LatchRuntimeFailure(store);
        Fail(dead);
        co_return dead;
      }
    }
  }
  store.recovery_external_keys_.clear();
  store.recovery_external_keys_.rehash(0);
  worker.SpawnRoot(PeriodicFlush(&store));
  worker.SpawnBackground(ActiveExpiration(&store));
  if (options_.expiration_authority_) {
    // One coordinator drives the whole-engine round; worker 0 hosts it.
    if (worker.id() == 0 &&
        tomb_raider_config_.mode_.load(std::memory_order_relaxed) !=
            TombRaiderMode::kOff) {
      worker.SpawnBackground(TombRaiderLoop(
          &store,
          tomb_raider_config_.generation_.load(std::memory_order_relaxed)));
    }
  }
  co_return absl::OkStatus();
}

void StorageEngine::Impl::FinalizeWorker(unsigned worker_id) noexcept {
  assert(worker_id < stores_.size());
  if (abandon_worker_state_on_finalize_.load(std::memory_order_acquire)) {
    // Worker::Shutdown has already drained IO and closed io_uring/SPDK state,
    // and detached coroutine frames were destroyed before this callback. The
    // production caller exits the process after joining these workers, so
    // traversing every index node has no correctness benefit; releasing the
    // pointer leaves the address space for the kernel to reclaim in bulk.
    (void)stores_[worker_id].release();
    return;
  }
  stores_[worker_id].reset();
}

bool StorageEngine::Impl::AbandonWorkerStateForProcessExit() noexcept {
#if LAVIK_ASAN_BUILD
  // ASan enables LeakSanitizer on supported platforms. Keep the ordinary
  // return path intact so intentional production process-exit abandonment
  // cannot hide unrelated leaks from its exit-time reachability scan.
  return false;
#else
  if (!shutdown_checkpoint_published_.load(std::memory_order_acquire)) {
    return false;
  }
  abandon_worker_state_on_finalize_.store(true, std::memory_order_release);
  return true;
#endif
}

absl::Status StorageEngine::Impl::FlushForShutdown() {
  if (RuntimeFailureLatched()) {
    return absl::FailedPreconditionError(
        "runtime storage failure forbids a clean-shutdown checkpoint");
  }
  // Shutdown must not certify a population while acknowledged transactions
  // still lack durable decisions. A bounded wait fails closed on timeout.
  const auto commit_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (active_tx_commits_.load(std::memory_order_acquire) != 0 &&
         std::chrono::steady_clock::now() < commit_deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (active_tx_commits_.load(std::memory_order_acquire) != 0) {
    return absl::DeadlineExceededError(
        "transaction commits did not drain for shutdown");
  }
  // Request admission is already closed and drained, so CONFIG can no longer
  // change this process's policy. Publish one latched decision to every worker;
  // letting workers sample the live atomic independently could split barrier
  // participation and deadlock shutdown.
  shutdown_checkpoint_for_flush_ = ShutdownCheckpointEnabled();
  shutdown_flush_requested_.store(true, std::memory_order_release);
  unsigned completed =
      shutdown_flush_completed_.load(std::memory_order_acquire);
  while (completed < worker_count_) {
    shutdown_flush_completed_.wait(completed, std::memory_order_acquire);
    completed = shutdown_flush_completed_.load(std::memory_order_acquire);
  }
  if (shutdown_flush_failed_.load(std::memory_order_acquire)) {
    return absl::Status(absl::StatusCode::kInternal,
                        "one or more workers failed to flush during shutdown");
  }
  if (RuntimeFailureLatched()) {
    return absl::FailedPreconditionError(
        "runtime storage failure raced the shutdown flush");
  }
  return absl::OkStatus();
}

void StorageEngine::Impl::Fail(const absl::Status& status) {
  open_barrier_->Abort(status);
  checkpoint_consumed_barrier_->Abort(status);
  checkpoint_capacity_loaded_barrier_->Abort(status);
  checkpoint_capacity_ready_barrier_->Abort(status);
  checkpoint_indexes_preallocated_barrier_->Abort(status);
  checkpoint_indexes_ready_barrier_->Abort(status);
  checkpoint_loaded_barrier_->Abort(status);
  checkpoint_index_validated_barrier_->Abort(status);
  metadata_barrier_->Abort(status);
  checkpoint_retired_barrier_->Abort(status);
  recovery_barrier_->Abort(status);
  recovery_accounting_barrier_->Abort(status);
  free_list_barrier_->Abort(status);
  orphan_extent_barrier_->Abort(status);
  shutdown_recovery_ready_barrier_->Abort(status);
  shutdown_recovery_published_barrier_->Abort(status);
  shutdown_checkpoint_ready_barrier_->Abort(status);
  shutdown_checkpoint_tx_cleaned_barrier_->Abort(status);
  shutdown_checkpoint_refrozen_barrier_->Abort(status);
  shutdown_checkpoint_built_barrier_->Abort(status);
  shutdown_checkpoint_published_barrier_->Abort(status);
}

absl::Status StorageEngine::Impl::ConfigureWorkerDeviceAffinity() {
  if (bycorf::SpdkStorageEnabled()) {
    struct ControllerPlan {
      std::string id_;
      unsigned io_queue_count_ = 0;
      std::uint64_t weight_ = 0;
      std::vector<std::size_t> devices_;
      std::vector<std::uint16_t> workers_;
    };

    std::map<std::string, ControllerPlan> grouped;
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const StorageDevice& device = devices_[device_index];
      if (device.controller_id_.empty() || device.io_queue_count_ == 0) {
        return absl::FailedPreconditionError(
            "SPDK controller did not report an available I/O qpair: " +
            device.path_);
      }
      auto [entry, inserted] = grouped.try_emplace(
          device.controller_id_,
          ControllerPlan{.id_ = device.controller_id_,
                         .io_queue_count_ = device.io_queue_count_,
                         .weight_ = 0,
                         .devices_ = {},
                         .workers_ = {}});
      ControllerPlan& controller = entry->second;
      if (!inserted && controller.io_queue_count_ != device.io_queue_count_) {
        return absl::FailedPreconditionError(
            "SPDK namespaces on one controller reported inconsistent qpair "
            "counts: " +
            device.controller_id_);
      }
      controller.devices_.push_back(device_index);
      controller.weight_ += ForegroundBlocksForDevice(device_index);
    }
    if (grouped.empty()) {
      return absl::FailedPreconditionError("SPDK storage has no controllers");
    }

    std::vector<ControllerPlan*> controllers;
    controllers.reserve(grouped.size());
    for (auto& [_, controller] : grouped) {
      controllers.push_back(&controller);
    }
    std::vector<ControllerAffinityInput> inputs;
    inputs.reserve(controllers.size());
    for (const ControllerPlan* controller : controllers) {
      inputs.push_back(ControllerAffinityInput{
          .id_ = controller->id_,
          .foreground_weight_ = controller->weight_,
          .io_qpair_count_ = controller->io_queue_count_,
      });
    }
    auto planned = PlanControllerAffinity(inputs, worker_count_);
    if (!planned.ok()) return planned.status();

    device_owners_.assign(devices_.size(), {});
    auto assign_controller = [this](ControllerPlan& controller,
                                    unsigned worker) {
      controller.workers_.push_back(static_cast<std::uint16_t>(worker));
      for (const std::size_t device_index : controller.devices_) {
        stores_[worker]->home_devices_.push_back(device_index);
        device_owners_[device_index].push_back(
            static_cast<std::uint16_t>(worker));
      }
    };

    for (unsigned worker = 0; worker < worker_count_; ++worker) {
      for (const std::size_t controller :
           planned->worker_controllers_[worker]) {
        assign_controller(*controllers[controller], worker);
      }
    }

    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      auto& owners = device_owners_[device_index];
      std::sort(owners.begin(), owners.end());
      assert(!owners.empty());
      device_allocators_[device_index]->owner_ =
          owners[devices_[device_index].id_ % owners.size()];
    }
    for (auto& store : stores_) {
      if (store->home_devices_.empty()) {
        return absl::InternalError(
            "SPDK controller assignment left a worker without a qpair");
      }
      store->home_device_allocations_.assign(store->home_devices_.size(), 0);
    }
    for (const ControllerPlan* controller : controllers) {
      std::string workers;
      for (const std::uint16_t worker : controller->workers_) {
        if (!workers.empty()) workers += ',';
        workers += std::to_string(worker);
      }
      spdlog::info(
          "SPDK controller={} io-qpairs={} foreground-weight={} owners=[{}]",
          controller->id_, controller->io_queue_count_, controller->weight_,
          workers);
    }
    return absl::OkStatus();
  } else {
    std::vector<std::size_t> usable_devices;
    std::uint64_t total_weight = 0;
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      const std::uint64_t weight = ForegroundBlocksForDevice(device_index);
      if (weight != 0) {
        usable_devices.push_back(device_index);
        total_weight += weight;
      }
    }
    assert(!usable_devices.empty());

    if (worker_count_ >= usable_devices.size()) {
      std::vector<unsigned> quotas(devices_.size(), 0);
      for (const std::size_t device_index : usable_devices) {
        quotas[device_index] = 1;
      }
      const unsigned remaining_workers =
          worker_count_ - static_cast<unsigned>(usable_devices.size());
      std::vector<std::pair<std::uint64_t, std::size_t>> remainders;
      remainders.reserve(usable_devices.size());
      unsigned assigned_workers = static_cast<unsigned>(usable_devices.size());
      for (const std::size_t device_index : usable_devices) {
        const std::uint64_t weighted =
            remaining_workers * ForegroundBlocksForDevice(device_index);
        quotas[device_index] += static_cast<unsigned>(weighted / total_weight);
        assigned_workers += static_cast<unsigned>(weighted / total_weight);
        remainders.emplace_back(weighted % total_weight, device_index);
      }
      std::sort(remainders.begin(), remainders.end(),
                [](const auto& left, const auto& right) {
                  if (left.first != right.first) {
                    return left.first > right.first;
                  }
                  return left.second < right.second;
                });
      for (unsigned i = assigned_workers; i < worker_count_; ++i) {
        ++quotas[remainders[i - assigned_workers].second];
      }

      std::vector<unsigned> quota_remaining = quotas;
      std::vector<std::int64_t> smooth_current(devices_.size(), 0);
      for (unsigned worker = 0; worker < worker_count_; ++worker) {
        std::size_t selected = usable_devices.front();
        bool selected_valid = false;
        for (const std::size_t device_index : usable_devices) {
          smooth_current[device_index] += quotas[device_index];
          if (quota_remaining[device_index] != 0 &&
              (!selected_valid ||
               smooth_current[device_index] > smooth_current[selected])) {
            selected = device_index;
            selected_valid = true;
          }
        }
        assert(selected_valid);
        smooth_current[selected] -= worker_count_;
        --quota_remaining[selected];
        stores_[worker]->home_devices_.push_back(selected);
      }
    } else {
      std::sort(usable_devices.begin(), usable_devices.end(),
                [this](std::size_t left, std::size_t right) {
                  return ForegroundBlocksForDevice(left) >
                         ForegroundBlocksForDevice(right);
                });
      std::vector<std::uint64_t> worker_weights(worker_count_, 0);
      for (const std::size_t device_index : usable_devices) {
        const auto lightest =
            std::min_element(worker_weights.begin(), worker_weights.end());
        const unsigned worker =
            static_cast<unsigned>(lightest - worker_weights.begin());
        stores_[worker]->home_devices_.push_back(device_index);
        *lightest += ForegroundBlocksForDevice(device_index);
      }
    }

    std::vector<unsigned> home_workers(devices_.size(), 0);
    for (auto& store : stores_) {
      assert(!store->home_devices_.empty());
      store->home_device_allocations_.assign(store->home_devices_.size(), 0);
      for (const std::size_t device_index : store->home_devices_) {
        ++home_workers[device_index];
      }
    }
    for (std::size_t device_index = 0; device_index < devices_.size();
         ++device_index) {
      spdlog::info("storage device id={} foreground-weight={} home-workers={}",
                   devices_[device_index].id_,
                   ForegroundBlocksForDevice(device_index),
                   home_workers[device_index]);
    }
    return absl::OkStatus();
  }
}

Task<absl::Status> StorageEngine::Impl::FlushWorkerForShutdown(
    WorkerStore* store) {
  // shutdown_flush_requested_ prevents a new cycle from beginning, but one
  // cycle may already have crossed that check and be suspended in a delete.
  // Join it locally before freezing append streams; doing this through the
  // global QuiesceExpiration helper would make workers submit to and wait on
  // themselves while every periodic flush owns the same shutdown barrier.
  while (store->expiry_cycle_running_) {
    absl::Status status = co_await bycorf::SleepFor(
        *store->worker_, std::chrono::milliseconds(1));
    if (!status.ok()) co_return status;
  }

  while (active_defrags_.load(std::memory_order_acquire) != 0) {
    absl::Status status = co_await bycorf::SleepFor(
        *store->worker_, std::chrono::milliseconds(1));
    if (!status.ok()) {
      co_return status;
    }
  }

  // Replication's in-memory backlog owns a separately replenished standby.
  // Shutdown prevents new refills, then joins an in-flight allocation before
  // releasing the unused 8 MiB owner.
  for (;;) {
    co_await store->replication_log_.mutex_.Lock();
    const bool pending = store->replication_log_.standby_refill_pending_;
    store->replication_log_.mutex_.Unlock(*store->worker_);
    if (!pending) break;
    absl::Status status = co_await bycorf::SleepFor(
        *store->worker_, std::chrono::milliseconds(1));
    if (!status.ok()) co_return status;
  }
  co_await store->replication_log_.mutex_.Lock();
  store->replication_log_.standby_block_.reset();
  store->replication_log_.mutex_.Unlock(*store->worker_);

  // A prefetch task may be off-worker in the device allocator. Its pending bit
  // remains set until any stale reservation has been returned, so WorkerStore
  // and the allocator cannot be torn down under that detached coroutine.
  while (true) {
    co_await store->store_state_mutex_.Lock();
    const bool prefetch_pending = store->standby_prefetch_pending_;
    store->store_state_mutex_.Unlock(*store->worker_);
    if (!prefetch_pending) break;
    absl::Status status = co_await bycorf::SleepFor(
        *store->worker_, std::chrono::milliseconds(1));
    if (!status.ok()) co_return status;
  }

  co_await store->active_block_allocation_mutex_.Lock();
  UnlockGuard allocation_guard(&store->active_block_allocation_mutex_,
                               store->worker_);
  std::optional<ReservedBlock> standby;
  co_await store->store_state_mutex_.Lock();
  {
    UnlockGuard guard(&store->store_state_mutex_, store->worker_);
    standby = std::exchange(store->standby_block_, std::nullopt);
    SealActiveBlocks(*store);
  }
  if (standby.has_value()) {
    absl::Status returned = co_await ReturnReservedBlock(*standby);
    if (!returned.ok()) co_return returned;
  }

  while (true) {
    co_await store->store_state_mutex_.Lock();
    bool done = false;
    bool failed = false;
    {
      UnlockGuard guard(&store->store_state_mutex_, store->worker_);
      // Extent reclaims are detached and hop to whichever worker owns the
      // device allocator, so one can still be mid-flight across workers
      // here. Draining the flush queue is not enough: flush completion is
      // itself what spawns them, and letting a worker tear down under one
      // frees the coroutine frame it is running on.
      done = !store->flush_running_ && store->flush_queue_.empty() &&
             active_extent_reclaims_.load(std::memory_order_acquire) == 0 &&
             active_settlements_.load(std::memory_order_acquire) == 0;
      failed = store->write_failed_ || RuntimeFailureLatched();
    }
    if (failed) {
      co_return absl::Status(
          absl::StatusCode::kInternal,
          "storage write failed while draining shutdown buffers");
    }
    if (done) {
      co_return absl::OkStatus();
    }
    absl::Status status = co_await bycorf::SleepFor(
        *store->worker_, std::chrono::milliseconds(1));
    if (!status.ok()) {
      co_return status;
    }
  }
}

void StorageEngine::Impl::CompleteShutdownFlush(const absl::Status& status) {
  if (!status.ok()) {
    spdlog::error("shutdown durability failed: {}", status.ToString());
    shutdown_flush_failed_.store(true, std::memory_order_release);
  }
  shutdown_flush_completed_.fetch_add(1, std::memory_order_acq_rel);
  shutdown_flush_completed_.notify_all();
}

}  // namespace lavik::storage
