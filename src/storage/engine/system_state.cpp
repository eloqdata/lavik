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

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdlib>
#include <limits>

#include "impl.h"

namespace lavik::storage {
namespace {

constexpr std::uint64_t kSystemStateManifestMagic =
    0x314d545353564cULL;                                            // LVSSTM1
constexpr std::uint64_t kPromotionBaseMagic = 0x3145534142564cULL;  // LVBASE1
constexpr std::size_t kSystemStateManifestHeaderBytes = 96;
constexpr std::size_t kMaxPromotionBaseBytes = 1024 * 1024;

enum SystemStateFlag : std::uint32_t {
  kCatalogPresent = 1U << 0,
  kPromotionPresent = 1U << 1,
  kFullSyncInProgress = 1U << 2,
  kCatalogReady = 1U << 3,
};

void PutU32(std::string* output, std::uint32_t value) {
  for (unsigned byte = 0; byte < 4; ++byte) {
    output->push_back(static_cast<char>(value >> (byte * 8)));
  }
}

void PutU64(std::string* output, std::uint64_t value) {
  for (unsigned byte = 0; byte < 8; ++byte) {
    output->push_back(static_cast<char>(value >> (byte * 8)));
  }
}

bool GetU32(std::string_view input, std::size_t* offset, std::uint32_t* value) {
  if (*offset > input.size() || input.size() - *offset < 4) return false;
  *value = 0;
  for (unsigned byte = 0; byte < 4; ++byte) {
    *value |= static_cast<std::uint32_t>(
                  static_cast<std::uint8_t>(input[*offset + byte]))
              << (byte * 8);
  }
  *offset += 4;
  return true;
}

bool GetU64(std::string_view input, std::size_t* offset, std::uint64_t* value) {
  if (*offset > input.size() || input.size() - *offset < 8) return false;
  *value = 0;
  for (unsigned byte = 0; byte < 8; ++byte) {
    *value |= static_cast<std::uint64_t>(
                  static_cast<std::uint8_t>(input[*offset + byte]))
              << (byte * 8);
  }
  *offset += 8;
  return true;
}

absl::Status PutString(std::string* output, std::string_view value) {
  if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
    return absl::OutOfRangeError("system-state string is too large");
  }
  PutU32(output, static_cast<std::uint32_t>(value.size()));
  output->append(value);
  return absl::OkStatus();
}

bool GetString(std::string_view input, std::size_t* offset,
               std::string* value) {
  std::uint32_t bytes = 0;
  if (!GetU32(input, offset, &bytes) || *offset > input.size() ||
      bytes > input.size() - *offset) {
    return false;
  }
  value->assign(input.substr(*offset, bytes));
  *offset += bytes;
  return true;
}

std::span<const std::byte> AsBytes(std::string_view value) {
  return {reinterpret_cast<const std::byte*>(value.data()), value.size()};
}

}  // namespace

absl::StatusOr<std::string> StorageEngine::Impl::EncodePromotionBase(
    const PromotionBase& base) {
  if (base.group_id_.empty() || base.parent_history_id_.empty() ||
      base.parent_frontier_.history_context_.empty() ||
      base.parent_frontier_.flow_cursors_.empty() ||
      base.population_token_.generation_ == 0 ||
      base.catalog_token_.catalog_generation_ == 0) {
    return absl::InvalidArgumentError("promotion base is incomplete");
  }
  if (base.parent_frontier_.flow_cursors_.size() >
      std::numeric_limits<std::uint32_t>::max()) {
    return absl::OutOfRangeError("promotion frontier has too many flows");
  }
  std::string output;
  PutU64(&output, kPromotionBaseMagic);
  PutU32(&output, kStorageFormatVersion);
  PutU32(&output, static_cast<std::uint32_t>(
                      base.parent_frontier_.flow_cursors_.size()));
  PutU64(&output, base.population_token_.generation_);
  PutU64(&output, base.population_token_.digest_);
  PutU64(&output, base.catalog_token_.catalog_generation_);
  PutU64(&output, base.catalog_token_.dump_crc64_);
  for (std::string_view value :
       {std::string_view(base.group_id_),
        std::string_view(base.parent_history_id_),
        std::string_view(base.parent_frontier_.history_context_),
        std::string_view(base.storage_accumulator_)}) {
    absl::Status appended = PutString(&output, value);
    if (!appended.ok()) return appended;
  }
  for (const std::uint64_t cursor : base.parent_frontier_.flow_cursors_) {
    if (cursor == 0) {
      return absl::InvalidArgumentError(
          "promotion frontier contains a zero cursor");
    }
    PutU64(&output, cursor);
  }
  if (output.size() > kMaxPromotionBaseBytes) {
    return absl::OutOfRangeError("promotion base exceeds the 1 MiB limit");
  }
  return output;
}

absl::StatusOr<PromotionBase> StorageEngine::Impl::DecodePromotionBase(
    std::string_view encoded) {
  std::size_t offset = 0;
  std::uint64_t magic = 0;
  std::uint32_t version = 0;
  std::uint32_t flow_count = 0;
  PromotionBase base;
  if (encoded.size() > kMaxPromotionBaseBytes ||
      !GetU64(encoded, &offset, &magic) || magic != kPromotionBaseMagic ||
      !GetU32(encoded, &offset, &version) || version != kStorageFormatVersion ||
      !GetU32(encoded, &offset, &flow_count) || flow_count == 0 ||
      !GetU64(encoded, &offset, &base.population_token_.generation_) ||
      !GetU64(encoded, &offset, &base.population_token_.digest_) ||
      !GetU64(encoded, &offset, &base.catalog_token_.catalog_generation_) ||
      !GetU64(encoded, &offset, &base.catalog_token_.dump_crc64_) ||
      !GetString(encoded, &offset, &base.group_id_) ||
      !GetString(encoded, &offset, &base.parent_history_id_) ||
      !GetString(encoded, &offset, &base.parent_frontier_.history_context_) ||
      !GetString(encoded, &offset, &base.storage_accumulator_) ||
      flow_count > (encoded.size() - offset) / sizeof(std::uint64_t)) {
    return absl::InternalError("invalid durable promotion base");
  }
  base.parent_frontier_.flow_cursors_.reserve(flow_count);
  for (std::uint32_t flow = 0; flow < flow_count; ++flow) {
    std::uint64_t cursor = 0;
    if (!GetU64(encoded, &offset, &cursor) || cursor == 0) {
      return absl::InternalError("invalid durable promotion frontier");
    }
    base.parent_frontier_.flow_cursors_.push_back(cursor);
  }
  if (offset != encoded.size() || base.group_id_.empty() ||
      base.parent_history_id_.empty() ||
      base.parent_frontier_.history_context_.empty() ||
      base.population_token_.generation_ == 0 ||
      base.catalog_token_.catalog_generation_ == 0) {
    return absl::InternalError("invalid durable promotion base fields");
  }
  return base;
}

absl::StatusOr<std::string> StorageEngine::Impl::EncodeSystemStateManifest(
    const DurableSystemState& state) {
  if (state.generation_ == 0) {
    return absl::InvalidArgumentError("system-state generation is zero");
  }
  const bool has_catalog = state.catalog_token_.catalog_generation_ != 0;
  if (has_catalog != (state.catalog_extents_ != nullptr) ||
      (has_catalog && (state.catalog_bytes_ == 0 ||
                       state.catalog_bytes_ > kMaxFunctionCatalogBytes))) {
    return absl::InvalidArgumentError("system-state catalog is incomplete");
  }
  std::string promotion;
  if (state.promotion_base_.has_value()) {
    auto encoded = EncodePromotionBase(*state.promotion_base_);
    if (!encoded.ok()) return encoded.status();
    promotion = std::move(*encoded);
  }
  const std::size_t ref_count =
      state.catalog_extents_ == nullptr ? 0 : state.catalog_extents_->size();
  if (ref_count > kMaxStringExtents ||
      ref_count > std::numeric_limits<std::uint32_t>::max()) {
    return absl::OutOfRangeError("system-state catalog has too many extents");
  }
  std::uint32_t flags = 0;
  if (has_catalog) flags |= kCatalogPresent;
  if (state.promotion_base_.has_value()) flags |= kPromotionPresent;
  if (state.full_sync_session_id_ != 0) flags |= kFullSyncInProgress;
  if (state.catalog_ready_) flags |= kCatalogReady;

  std::string output;
  output.reserve(kSystemStateManifestHeaderBytes + ref_count * 24 +
                 promotion.size());
  PutU64(&output, kSystemStateManifestMagic);
  PutU32(&output, kStorageFormatVersion);
  PutU32(&output, flags);
  PutU64(&output, state.generation_);
  PutU64(&output, state.catalog_token_.catalog_generation_);
  PutU64(&output, state.catalog_token_.dump_crc64_);
  PutU64(&output, state.catalog_bytes_);
  PutU64(&output, state.full_sync_session_id_);
  PutU64(&output, state.population_token_.generation_);
  PutU64(&output, state.population_token_.digest_);
  PutU32(&output, static_cast<std::uint32_t>(ref_count));
  PutU32(&output, static_cast<std::uint32_t>(promotion.size()));
  PutU64(&output, state.population_identity_.size());
  PutU64(&output, state.clean_shutdown_proof_.size());
  if (output.size() != kSystemStateManifestHeaderBytes) {
    return absl::InternalError("system-state header size mismatch");
  }
  if (state.catalog_extents_ != nullptr) {
    for (const ExtentRef& ref : *state.catalog_extents_) {
      PutU64(&output, ref.block_id_);
      PutU64(&output, ref.allocation_epoch_);
      PutU32(&output, ref.payload_bytes_);
      PutU32(&output, ref.payload_checksum_);
    }
  }
  output.append(promotion);
  output.append(state.population_identity_);
  output.append(state.clean_shutdown_proof_);
  if (output.size() > kExtentPayloadBytes) {
    return absl::OutOfRangeError(
        "system-state manifest exceeds one storage extent");
  }
  return output;
}

absl::StatusOr<StorageEngine::Impl::DurableSystemState>
StorageEngine::Impl::DecodeSystemStateManifest(std::string_view encoded) {
  if (encoded.size() < kSystemStateManifestHeaderBytes) {
    return absl::InternalError("durable system-state manifest is truncated");
  }
  std::size_t offset = 0;
  std::uint64_t magic = 0;
  std::uint32_t version = 0;
  std::uint32_t flags = 0;
  std::uint32_t ref_count = 0;
  std::uint32_t promotion_bytes = 0;
  std::uint64_t identity_bytes = 0;
  std::uint64_t proof_bytes = 0;
  DurableSystemState state;
  if (!GetU64(encoded, &offset, &magic) || magic != kSystemStateManifestMagic ||
      !GetU32(encoded, &offset, &version) || version != kStorageFormatVersion ||
      !GetU32(encoded, &offset, &flags) ||
      (flags & ~(kCatalogPresent | kPromotionPresent | kFullSyncInProgress |
                 kCatalogReady)) != 0 ||
      !GetU64(encoded, &offset, &state.generation_) ||
      !GetU64(encoded, &offset, &state.catalog_token_.catalog_generation_) ||
      !GetU64(encoded, &offset, &state.catalog_token_.dump_crc64_) ||
      !GetU64(encoded, &offset, &state.catalog_bytes_) ||
      !GetU64(encoded, &offset, &state.full_sync_session_id_) ||
      !GetU64(encoded, &offset, &state.population_token_.generation_) ||
      !GetU64(encoded, &offset, &state.population_token_.digest_) ||
      !GetU32(encoded, &offset, &ref_count) ||
      !GetU32(encoded, &offset, &promotion_bytes) ||
      !GetU64(encoded, &offset, &identity_bytes) ||
      !GetU64(encoded, &offset, &proof_bytes) ||
      identity_bytes > kExtentPayloadBytes ||
      proof_bytes > kExtentPayloadBytes || ref_count > kMaxStringExtents ||
      ref_count > (encoded.size() - offset) / 24) {
    return absl::InternalError("invalid durable system-state manifest");
  }
  if (ref_count != 0) {
    auto refs = std::make_shared<std::vector<ExtentRef>>();
    refs->reserve(ref_count);
    std::uint64_t catalog_bytes = 0;
    for (std::uint32_t index = 0; index < ref_count; ++index) {
      ExtentRef ref;
      if (!GetU64(encoded, &offset, &ref.block_id_) ||
          !GetU64(encoded, &offset, &ref.allocation_epoch_) ||
          !GetU32(encoded, &offset, &ref.payload_bytes_) ||
          !GetU32(encoded, &offset, &ref.payload_checksum_) ||
          ref.block_id_ == kInvalidBlockId || ref.allocation_epoch_ == 0 ||
          ref.payload_bytes_ == 0 || ref.payload_bytes_ > kExtentPayloadBytes ||
          catalog_bytes > kMaxFunctionCatalogBytes - ref.payload_bytes_) {
        return absl::InternalError(
            "invalid catalog extent in system-state manifest");
      }
      catalog_bytes += ref.payload_bytes_;
      refs->push_back(ref);
    }
    state.catalog_extents_ =
        std::shared_ptr<const std::vector<ExtentRef>>(std::move(refs));
    if (catalog_bytes != state.catalog_bytes_) {
      return absl::InternalError("durable catalog extent length mismatch");
    }
  }
  if (promotion_bytes > encoded.size() - offset ||
      encoded.size() - offset !=
          promotion_bytes + identity_bytes + proof_bytes) {
    return absl::InternalError("durable promotion base length mismatch");
  }
  if (promotion_bytes != 0) {
    auto promotion =
        DecodePromotionBase(encoded.substr(offset, promotion_bytes));
    if (!promotion.ok()) return promotion.status();
    state.promotion_base_ = std::move(*promotion);
  }
  offset += promotion_bytes;
  state.population_identity_ = encoded.substr(offset, identity_bytes);
  offset += identity_bytes;
  state.clean_shutdown_proof_ = encoded.substr(offset, proof_bytes);
  if ((!state.clean_shutdown_proof_.empty() &&
       state.population_identity_.empty()) ||
      (!state.population_identity_.empty() &&
       (state.full_sync_session_id_ != 0 || (flags & kCatalogReady) == 0 ||
        state.population_token_.generation_ == 0 ||
        state.catalog_token_.catalog_generation_ == 0))) {
    return absl::DataLossError(
        "recovery proof has no complete population/catalog");
  }
  const bool has_catalog = (flags & kCatalogPresent) != 0;
  if (state.generation_ == 0 ||
      has_catalog != (state.catalog_extents_ != nullptr) ||
      has_catalog != (state.catalog_token_.catalog_generation_ != 0) ||
      ((flags & kPromotionPresent) != 0) != state.promotion_base_.has_value() ||
      ((flags & kFullSyncInProgress) != 0) !=
          (state.full_sync_session_id_ != 0) ||
      ((flags & kCatalogReady) != 0 && !has_catalog)) {
    return absl::InternalError("durable system-state flags disagree");
  }
  state.catalog_ready_ = (flags & kCatalogReady) != 0;
  return state;
}

absl::Status StorageEngine::Impl::LoadSystemState() {
  struct Candidate {
    std::uint64_t generation_ = 0;
    SystemStateRoot root_{};
  };
  std::vector<std::vector<Candidate>> by_device(devices_.size());
  bool saw_any_root = false;
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const StorageDevice& device = devices_[device_index];
    for (unsigned slot = 0; slot < 2; ++slot) {
      std::array<std::byte, kDirectIoAlignment> page{};
      absl::Status read = ReadExactlyAt(
          device.path_, page,
          MetadataPageSlotOffset(kSystemStateMetadataOffset, 0, slot));
      if (!read.ok()) return read;
      if (IsZero(page)) continue;
      saw_any_root = true;
      std::array<std::byte, sizeof(SystemStateRoot)> payload{};
      std::uint64_t generation = 0;
      SystemStateRoot root;
      if (!DecodeMetadataPage(page, MetadataPageKind::kSystemState, 0,
                              &generation, payload) ||
          !DecodeSystemStateRoot(payload, &root) ||
          root.generation_ != generation) {
        continue;
      }
      by_device[device_index].push_back(Candidate{generation, root});
    }
  }
  if (!saw_any_root) {
    system_state_ = DurableSystemState{};
    recovered_catalog_dump_.reset();
    replica_recovery_fenced_.store(false, std::memory_order_release);
    return absl::OkStatus();
  }
  std::optional<Candidate> selected;
  for (const Candidate& candidate : by_device.front()) {
    bool common = true;
    for (std::size_t device = 1; device < by_device.size() && common;
         ++device) {
      common =
          std::any_of(by_device[device].begin(), by_device[device].end(),
                      [&](const Candidate& other) {
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
        "configured devices have no common valid system-state generation");
  }

  auto read_extent =
      [&](const ExtentRef& ref,
          std::uint32_t expected_index) -> absl::StatusOr<std::string> {
    const std::size_t device_index = DeviceIdForBlock(ref.block_id_);
    if (device_index >= devices_.size() ||
        devices_[device_index].id_ != device_index) {
      return absl::InternalError("system-state extent names a missing device");
    }
    const std::size_t read_bytes =
        AlignDirect(kBlockHeaderBytes + ref.payload_bytes_);
    std::vector<std::byte> bytes(read_bytes);
    absl::Status read = ReadExactlyAt(devices_[device_index].path_, bytes,
                                      LocalBlockOffset(ref.block_id_));
    if (!read.ok()) return read;
    BlockHeader header;
    if (!DecodeBlockHeaderPages(std::span<const std::byte, kBlockHeaderBytes>(
                                    bytes.data(), kBlockHeaderBytes),
                                &header) ||
        header.kind_ != BlockKind::kPayloadExtent ||
        header.block_id_ != ref.block_id_ ||
        header.allocation_epoch_ != ref.allocation_epoch_ ||
        header.extent_index_ != expected_index ||
        header.extent_payload_bytes_ != ref.payload_bytes_ ||
        header.extent_payload_checksum_ != ref.payload_checksum_) {
      return absl::InternalError(
          "system-state extent does not match its durable reference");
    }
    const auto payload = std::span<const std::byte>(
        bytes.data() + kBlockHeaderBytes, ref.payload_bytes_);
    if (Crc32c(payload) != ref.payload_checksum_) {
      return absl::InternalError("system-state extent checksum mismatch");
    }
    return std::string(reinterpret_cast<const char*>(payload.data()),
                       payload.size());
  };

  auto manifest = read_extent(selected->root_.manifest_, 0);
  if (!manifest.ok()) return manifest.status();
  if (manifest->size() != selected->root_.manifest_bytes_) {
    return absl::InternalError("system-state root manifest length mismatch");
  }
  auto decoded = DecodeSystemStateManifest(*manifest);
  if (!decoded.ok()) return decoded.status();
  if (decoded->generation_ != selected->generation_) {
    return absl::InternalError("system-state root and manifest disagree");
  }
  decoded->manifest_extents_ = std::make_shared<const std::vector<ExtentRef>>(
      std::vector<ExtentRef>{selected->root_.manifest_});

  std::optional<std::string> catalog;
  if (decoded->catalog_extents_ != nullptr) {
    catalog.emplace();
    catalog->reserve(static_cast<std::size_t>(decoded->catalog_bytes_));
    for (std::size_t index = 0; index < decoded->catalog_extents_->size();
         ++index) {
      auto part = read_extent(decoded->catalog_extents_->at(index),
                              static_cast<std::uint32_t>(index));
      if (!part.ok()) return part.status();
      catalog->append(*part);
    }
    if (catalog->size() != decoded->catalog_bytes_ ||
        Crc64(AsBytes(*catalog)) != decoded->catalog_token_.dump_crc64_) {
      return absl::InternalError(
          "durable Function catalog length or CRC64 mismatch");
    }
  }
  system_state_ = std::move(*decoded);
  recovered_catalog_dump_ = std::move(catalog);
  const bool fenced = system_state_.full_sync_session_id_ != 0;
  replica_recovery_fenced_.store(fenced, std::memory_order_release);
  // The durable fence survives a restart, but native ReplicaSyncState does
  // not. Keep ordinary writes out through ReplicaRecoveryFenced() until a
  // replacement sync starts; ResetReplicaPartitions alone enables routing
  // into the native rebuild root once its per-partition state exists.
  return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::WriteSystemStateRootOnDeviceLocal(
    std::size_t device_index, const SystemStateRoot& root,
    std::uint8_t target_slot) {
  if (device_index >= devices_.size() || target_slot > 1) {
    co_return absl::InvalidArgumentError("invalid system-state root target");
  }
  DeviceAllocator& allocator = *device_allocators_[device_index];
  if (bycorf::ThisWorker().id_ != allocator.owner_) {
    co_return absl::FailedPreconditionError(
        "system-state root write ran on the wrong worker");
  }
  LAVIK_FAULT_INJECT(
      // Deterministically exercise the multi-device ambiguous-commit branch.
      // Format: <device-index>:<root-generation>, consumed at most once.
      if (const char* configured =
              std::getenv("LAVIK_FAIL_SYSTEM_STATE_ROOT_ONCE");
          configured != nullptr) {
        const std::string_view value(configured);
        const std::size_t separator = value.find(':');
        std::size_t configured_device = 0;
        std::uint64_t configured_generation = 0;
        const auto device = std::from_chars(
            value.data(), value.data() + std::min(separator, value.size()),
            configured_device);
        const char* generation_begin = separator == std::string_view::npos
                                           ? value.data() + value.size()
                                           : value.data() + separator + 1;
        const auto generation =
            std::from_chars(generation_begin, value.data() + value.size(),
                            configured_generation);
        if (separator != std::string_view::npos && device.ec == std::errc{} &&
            device.ptr == value.data() + separator &&
            generation.ec == std::errc{} &&
            generation.ptr == value.data() + value.size() &&
            configured_device == device_index &&
            configured_generation == root.generation_ &&
            !system_state_root_failure_injected_.exchange(
                true, std::memory_order_acq_rel)) {
          co_return absl::UnavailableError(
              "injected system-state root write failure");
        }
      });
  WorkerStore& store = *stores_[allocator.owner_];
  auto acquired = co_await store.buffers_.AcquireReadBuffer();
  if (!acquired.ok()) co_return acquired.status();
  ReadBufferLease lease = std::move(*acquired);
  FixedBuffer buffer = lease.io_buffer();
  buffer.size_ = kDirectIoAlignment;
  std::array<std::byte, sizeof(SystemStateRoot)> payload{};
  EncodeSystemStateRoot(root, payload);
  EncodeMetadataPage(MetadataPageKind::kSystemState, 0, root.generation_,
                     payload,
                     std::span<std::byte, kDirectIoAlignment>(
                         buffer.data_, kDirectIoAlignment));
  const StorageDevice& device = devices_[device_index];
  auto written = co_await WriteStorageBuffer(
      *store.worker_, store.files_[device.file_index_],
      std::span<const std::byte>(buffer.data_, kDirectIoAlignment),
      lease.registered(), buffer,
      MetadataPageSlotOffset(kSystemStateMetadataOffset, 0, target_slot));
  if (!written.ok() || *written != kDirectIoAlignment) {
    co_return written.ok()
        ? absl::InternalError("short system-state root write")
        : written.status();
  }
  co_return co_await bycorf::Fdatasync(*store.worker_,
                                       store.files_[device.file_index_]);
}

Task<absl::Status> StorageEngine::Impl::CommitSystemState(
    DurableSystemState next, std::string_view catalog_dump,
    bool replace_catalog, bool shutdown_metadata) {
  // The caller holds system_state_mutex_ on worker zero.
  if (system_state_failure_.has_value()) co_return *system_state_failure_;
  if (system_state_.generation_ == std::numeric_limits<std::uint64_t>::max()) {
    co_return absl::ResourceExhaustedError(
        "system-state generation is exhausted");
  }
  next.generation_ = system_state_.generation_ + 1;
  WorkerStore& store = *stores_[0];
  ExtentManifest old_catalog = system_state_.catalog_extents_;
  ExtentManifest old_manifest = system_state_.manifest_extents_;
  ExtentManifest new_catalog;
  ExtentManifest new_manifest;

  co_await store.store_state_mutex_.Lock();
  UnlockGuard write_unlock(&store.store_state_mutex_, store.worker_);
  if (replace_catalog) {
    auto written = co_await WriteExtentValueLocked(store, catalog_dump);
    if (!written.ok()) co_return written.status();
    new_catalog = std::move(*written);
    next.catalog_extents_ = new_catalog;
    next.catalog_bytes_ = catalog_dump.size();
    LAVIK_MAYBE_CRASH_AT("function-catalog-body-durable");
  }
  auto manifest_body = EncodeSystemStateManifest(next);
  if (!manifest_body.ok()) {
    if (new_catalog != nullptr) SpawnExtentReclaim(store, new_catalog);
    co_return manifest_body.status();
  }
  auto manifest_written = co_await WriteExtentValueLocked(
      store, *manifest_body, {}, nullptr, {}, shutdown_metadata);
  if (!manifest_written.ok()) {
    if (new_catalog != nullptr) SpawnExtentReclaim(store, new_catalog);
    co_return manifest_written.status();
  }
  new_manifest = std::move(*manifest_written);
  if (new_manifest->size() != 1) {
    SpawnExtentReclaim(store, new_manifest);
    if (new_catalog != nullptr) SpawnExtentReclaim(store, new_catalog);
    co_return absl::InternalError(
        "system-state manifest unexpectedly spans multiple extents");
  }
  next.manifest_extents_ = new_manifest;
  write_unlock.Unlock();

  const SystemStateRoot root{
      .magic_ = kSystemStateRootMagic,
      .version_ = kStorageFormatVersion,
      .root_bytes_ = sizeof(SystemStateRoot),
      .generation_ = next.generation_,
      .manifest_ = new_manifest->front(),
      .manifest_bytes_ = manifest_body->size(),
  };
  const std::uint8_t slot =
      static_cast<std::uint8_t>((next.generation_ - 1) & 1);
  for (std::size_t device_index = 0; device_index < devices_.size();
       ++device_index) {
    const bycorf::WorkerId owner = device_allocators_[device_index]->owner_;
    absl::Status committed;
    if (owner == 0) {
      committed =
          co_await WriteSystemStateRootOnDeviceLocal(device_index, root, slot);
    } else {
      committed = co_await bycorf::SubmitTaskTo(
          owner, [this, device_index, root, slot]() {
            return WriteSystemStateRootOnDeviceLocal(device_index, root, slot);
          });
    }
    if (!committed.ok()) {
      // Some devices may already expose the new root. Stop the writer and let
      // restart select the highest generation common to every device. Fence
      // all request serving as well: the in-memory catalog may now disagree
      // with the generation recovery will select.
      system_state_failure_ =
          absl::UnknownError("system-state commit result is ambiguous: " +
                             std::string(committed.message()));
      LatchRuntimeFailure();
      co_return *system_state_failure_;
    }
    LAVIK_MAYBE_CRASH_AT("system-state-device-root-durable");
  }

  system_state_ = std::move(next);
  if (replace_catalog) {
    LAVIK_MAYBE_CRASH_AT("function-catalog-root-durable");
  }
  if (replace_catalog) recovered_catalog_dump_ = std::string(catalog_dump);
  if (old_manifest != nullptr) SpawnExtentReclaim(store, old_manifest);
  if (replace_catalog && old_catalog != nullptr) {
    SpawnExtentReclaim(store, old_catalog);
  }
  co_return absl::OkStatus();
}

Task<absl::StatusOr<CatalogDurabilityToken>>
StorageEngine::Impl::CommitFunctionCatalog(std::string_view dump) {
  if (dump.empty() || dump.size() > kMaxFunctionCatalogBytes) {
    co_return absl::OutOfRangeError(
        "Function catalog dump must be between 1 byte and 1 GiB");
  }
  if (bycorf::ThisWorker().id_ != 0) {
    std::string owned(dump);
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, owned = std::move(owned)]() {
          return CommitFunctionCatalog(owned);
        });
  }
  co_await system_state_mutex_.Lock();
  UnlockGuard unlock(&system_state_mutex_, bycorf::ThisWorker().self_);
  if (system_state_.catalog_token_.catalog_generation_ ==
      std::numeric_limits<std::uint64_t>::max()) {
    co_return absl::ResourceExhaustedError(
        "Function catalog generation is exhausted");
  }
  DurableSystemState next = system_state_;
  next.catalog_token_ = CatalogDurabilityToken{
      .catalog_generation_ =
          system_state_.catalog_token_.catalog_generation_ + 1,
      .dump_crc64_ = Crc64(AsBytes(dump)),
  };
  if (next.full_sync_session_id_ != 0) next.catalog_ready_ = true;
  absl::Status committed =
      co_await CommitSystemState(std::move(next), dump, true);
  if (!committed.ok()) co_return committed;
  co_return system_state_.catalog_token_;
}

absl::StatusOr<std::optional<RecoveredFunctionCatalog>>
StorageEngine::Impl::RecoverFunctionCatalog() const {
  if (system_state_failure_.has_value()) return *system_state_failure_;
  if (!recovered_catalog_dump_.has_value()) {
    return std::optional<RecoveredFunctionCatalog>{};
  }
  return std::optional<RecoveredFunctionCatalog>(RecoveredFunctionCatalog{
      .dump_ = *recovered_catalog_dump_,
      .token_ = system_state_.catalog_token_,
  });
}

Task<absl::Status> StorageEngine::Impl::MakeDurable(
    const DurabilityFrontier& frontier, std::string_view opaque_accumulator) {
  if (frontier.history_context_.empty() || frontier.flow_cursors_.empty() ||
      std::any_of(frontier.flow_cursors_.begin(), frontier.flow_cursors_.end(),
                  [](std::uint64_t cursor) { return cursor == 0; }) ||
      opaque_accumulator.size() > kMaxPromotionBaseBytes) {
    co_return absl::InvalidArgumentError(
        "promotion durability frontier is incomplete");
  }
  if (bycorf::ThisWorker().id_ != 0) {
    DurabilityFrontier copied = frontier;
    std::string accumulator(opaque_accumulator);
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, copied = std::move(copied),
            accumulator = std::move(accumulator)]() {
          return MakeDurable(copied, accumulator);
        });
  }
  while (active_tx_commits_.load(std::memory_order_acquire) != 0) {
    absl::Status slept = co_await bycorf::SleepFor(
        *bycorf::ThisWorker().self_, std::chrono::milliseconds(1));
    if (!slept.ok()) co_return slept;
  }
  // Keep each suspension in its own statement. GCC 13 can reuse the wrong
  // coroutine-frame slot when both arms of ?: contain co_await.
  for (unsigned worker = 0; worker < worker_count_; ++worker) {
    auto drain = [this, worker]() {
      return DrainReplicaRootWritesLocal(*stores_[worker]);
    };
    absl::Status durable;
    if (worker == 0) {
      durable = co_await drain();
    } else {
      durable = co_await bycorf::SubmitTaskTo(worker, drain);
    }
    if (!durable.ok()) co_return durable;
  }
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::CommitPromotionBase(
    PromotionBase base) {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, base = std::move(base)]() mutable {
          return CommitPromotionBase(std::move(base));
        });
  }
  co_await system_state_mutex_.Lock();
  UnlockGuard unlock(&system_state_mutex_, bycorf::ThisWorker().self_);
  if (system_state_.full_sync_session_id_ != 0 ||
      base.catalog_token_ != system_state_.catalog_token_ ||
      base.population_token_ != system_state_.population_token_) {
    co_return absl::FailedPreconditionError(
        "promotion base no longer matches durable population/catalog");
  }
  DurableSystemState next = system_state_;
  next.promotion_base_ = std::move(base);
  co_return co_await CommitSystemState(std::move(next), {}, false);
}

absl::StatusOr<std::optional<PromotionBase>>
StorageEngine::Impl::RecoverPromotionBase() const {
  if (system_state_failure_.has_value()) return *system_state_failure_;
  return system_state_.promotion_base_;
}

Task<absl::Status> StorageEngine::Impl::CommitPopulationIdentity(
    std::string identity) {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, identity = std::move(identity)]() mutable {
          return CommitPopulationIdentity(std::move(identity));
        });
  }
  co_await system_state_mutex_.Lock();
  UnlockGuard unlock(&system_state_mutex_, bycorf::ThisWorker().self_);
  if (identity.empty() || system_state_.full_sync_session_id_ != 0 ||
      system_state_.population_token_.generation_ == 0 ||
      !system_state_.catalog_ready_ || RuntimeFailureLatched()) {
    co_return absl::FailedPreconditionError(
        "recovery identity requires a complete population");
  }
  DurableSystemState next = system_state_;
  next.population_identity_ = std::move(identity);
  next.clean_shutdown_proof_.clear();
  staged_clean_shutdown_proof_.clear();
  co_return co_await CommitSystemState(std::move(next), {}, false);
}

Task<absl::StatusOr<std::optional<PopulationRecoveryRecord>>>
StorageEngine::Impl::ConsumePopulationRecovery() {
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this] { return ConsumePopulationRecovery(); });
  }
  co_await system_state_mutex_.Lock();
  UnlockGuard unlock(&system_state_mutex_, bycorf::ThisWorker().self_);
  if (system_state_failure_.has_value()) co_return *system_state_failure_;
  if (system_state_.population_identity_.empty()) {
    co_return std::optional<PopulationRecoveryRecord>{};
  }
  PopulationRecoveryRecord record{
      .identity_ = system_state_.population_identity_,
      .clean_proof_ = system_state_.clean_shutdown_proof_,
      .population_token_ = system_state_.population_token_,
      .catalog_token_ = system_state_.catalog_token_,
  };
  if (!record.clean_proof_.empty()) {
    LAVIK_MAYBE_CRASH_AT("recovery_before_proof_consume");
    DurableSystemState next = system_state_;
    next.clean_shutdown_proof_.clear();
    absl::Status consumed =
        co_await CommitSystemState(std::move(next), {}, false);
    if (!consumed.ok()) co_return consumed;
    LAVIK_MAYBE_CRASH_AT("recovery_after_proof_consume");
  }
  co_return std::optional<PopulationRecoveryRecord>(std::move(record));
}

void StorageEngine::Impl::StageCleanShutdownProof(std::string proof) {
  assert(bycorf::ThisWorker().id_ == 0);
  staged_clean_shutdown_proof_ = std::move(proof);
}

Task<absl::Status> StorageEngine::Impl::PublishCleanShutdownProof() {
  if (staged_clean_shutdown_proof_.empty()) co_return absl::OkStatus();
  co_await system_state_mutex_.Lock();
  UnlockGuard unlock(&system_state_mutex_, bycorf::ThisWorker().self_);
  if (RuntimeFailureLatched() ||
      shutdown_flush_failed_.load(std::memory_order_acquire) ||
      active_tx_commits_.load(std::memory_order_acquire) != 0 ||
      system_state_.full_sync_session_id_ != 0 ||
      system_state_.population_identity_.empty() ||
      !system_state_.catalog_ready_) {
    co_return absl::FailedPreconditionError(
        "clean shutdown proof requires fully drained storage");
  }
  LAVIK_MAYBE_CRASH_AT("recovery_before_proof_publish");
  DurableSystemState next = system_state_;
  next.clean_shutdown_proof_ = std::move(staged_clean_shutdown_proof_);
  absl::Status result =
      co_await CommitSystemState(std::move(next), {}, false, true);
  if (result.ok()) {
    LAVIK_MAYBE_CRASH_AT("recovery_after_proof_publish");
    spdlog::info("published clean shutdown recovery proof");
  }
  co_return result;
}

absl::StatusOr<PopulationToken> StorageEngine::Impl::RecoverPopulationToken()
    const {
  if (system_state_failure_.has_value()) return *system_state_failure_;
  if (system_state_.population_token_.generation_ == 0) {
    return absl::FailedPreconditionError(
        "no full-sync population is eligible for promotion");
  }
  return system_state_.population_token_;
}

Task<absl::Status> StorageEngine::Impl::BeginReplicaFullSync(
    std::uint64_t session_id) {
  if (session_id == 0) {
    co_return absl::InvalidArgumentError("invalid full-sync session");
  }
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, session_id]() { return BeginReplicaFullSync(session_id); });
  }
  co_await system_state_mutex_.Lock();
  UnlockGuard unlock(&system_state_mutex_, bycorf::ThisWorker().self_);
  if (system_state_.full_sync_session_id_ == session_id) {
    replica_recovery_fenced_.store(true, std::memory_order_release);
    co_return absl::OkStatus();
  }
  DurableSystemState next = system_state_;
  next.full_sync_session_id_ = session_id;
  next.catalog_ready_ = false;
  next.population_token_ = {};
  next.promotion_base_.reset();
  next.population_identity_.clear();
  next.clean_shutdown_proof_.clear();
  staged_clean_shutdown_proof_.clear();
  absl::Status committed =
      co_await CommitSystemState(std::move(next), {}, false);
  if (!committed.ok()) co_return committed;
  replica_recovery_fenced_.store(true, std::memory_order_release);
  // This fence is shared by native and Redis full sync, while
  // replica_loading_ is not: Redis imports its RDB through ordinary writes,
  // and native sync enables hidden-root routing only after it installs the
  // corresponding per-partition ReplicaSyncState.
  spdlog::info(
      "full-sync session {} durably invalidated system state at "
      "generation {}",
      session_id, system_state_.generation_);
  co_return absl::OkStatus();
}

Task<absl::Status> StorageEngine::Impl::CompleteReplicaFullSync(
    std::uint64_t session_id, PopulationToken population) {
  if (session_id == 0 || population.generation_ == 0) {
    co_return absl::InvalidArgumentError("invalid full-sync completion");
  }
  if (bycorf::ThisWorker().id_ != 0) {
    co_return co_await bycorf::SubmitTaskTo(
        0, [this, session_id, population]() {
          return CompleteReplicaFullSync(session_id, population);
        });
  }
  co_await system_state_mutex_.Lock();
  UnlockGuard unlock(&system_state_mutex_, bycorf::ThisWorker().self_);
  if (system_state_.full_sync_session_id_ != session_id ||
      !system_state_.catalog_ready_ ||
      system_state_.catalog_token_.catalog_generation_ == 0) {
    co_return absl::FailedPreconditionError(
        "full-sync catalog/population is not ready");
  }
  DurableSystemState next = system_state_;
  next.full_sync_session_id_ = 0;
  next.population_token_ = population;
  next.catalog_ready_ = true;
  absl::Status committed =
      co_await CommitSystemState(std::move(next), {}, false);
  if (!committed.ok()) co_return committed;
  replica_recovery_fenced_.store(false, std::memory_order_release);
  replica_loading_.store(false, std::memory_order_release);
  spdlog::info(
      "full-sync session {} durably activated population at system "
      "state generation {}",
      session_id, system_state_.generation_);
  co_return absl::OkStatus();
}

}  // namespace lavik::storage
