#include "keylane/meta/population_manifest_store.h"

#include <algorithm>

#include "keylane/meta/hash.h"
#include "keylane/population_manifest_format.h"

namespace keylane::meta {
namespace {

bool IsCanonical(const std::vector<MetaPopulationManifestEntry>& entries) {
  return std::adjacent_find(entries.begin(), entries.end(),
                            [](const MetaPopulationManifestEntry& lhs,
                               const MetaPopulationManifestEntry& rhs) {
                              return lhs.partition_id_ >= rhs.partition_id_;
                            }) == entries.end();
}

bool EntriesInDomain(const std::vector<MetaPopulationManifestEntry>& entries) {
  return std::all_of(entries.begin(), entries.end(), [](const auto& entry) {
    return entry.partition_id_ < kMetaSlotCount && entry.logical_epoch_ != 0;
  });
}

void WriteEntries(MetaWriter& writer,
                  const std::vector<MetaPopulationManifestEntry>& entries) {
  writer.WriteList(entries, [](MetaWriter& item_writer,
                               const MetaPopulationManifestEntry& entry) {
    item_writer.WriteU32(entry.partition_id_);
    item_writer.WriteU64(entry.logical_epoch_);
  });
}

}  // namespace

MetaHash256 MetaPopulationManifestStore::CanonicalDigest(
    const std::vector<MetaPopulationManifestEntry>& entries) {
  std::vector<PopulationManifestDigestEntry> digest_entries;
  digest_entries.reserve(entries.size());
  for (const MetaPopulationManifestEntry& entry : entries) {
    digest_entries.push_back({entry.partition_id_, entry.logical_epoch_});
  }
  return MetaSha256(EncodePopulationManifestDigestInput(digest_entries));
}

absl::Status MetaPopulationManifestStore::Put(
    const PutPopulationManifest& command) {
  if (command.entries_.size() > kMaxMetaPopulationManifestEntries) {
    return MetaDomainRejectError("population manifest entry cap exceeded");
  }
  if (!IsCanonical(command.entries_)) {
    return MetaDomainRejectError(
        "population manifest entries must be strictly sorted");
  }
  if (!EntriesInDomain(command.entries_)) {
    return MetaDomainRejectError(
        "population manifest contains an out-of-range partition or zero "
        "logical epoch");
  }
  if (CanonicalDigest(command.entries_) != command.manifest_digest_) {
    return MetaDomainRejectError("population manifest digest mismatch");
  }
  if (const auto it = documents_.find(command.manifest_digest_);
      it != documents_.end()) {
    if (it->second.entries_ == command.entries_) return absl::OkStatus();
    return MetaDomainRejectError("population manifest digest collision");
  }
  documents_.emplace(command.manifest_digest_,
                     MetaPopulationManifestDocument{command.manifest_digest_,
                                                    command.entries_});
  return absl::OkStatus();
}

absl::Status MetaPopulationManifestStore::Prune(
    const PrunePopulationManifest& command) {
  documents_.erase(command.manifest_digest_);
  return absl::OkStatus();
}

std::optional<MetaPopulationManifestDocument> MetaPopulationManifestStore::Find(
    const MetaHash256& digest) const {
  const auto it = documents_.find(digest);
  if (it == documents_.end()) return std::nullopt;
  return it->second;
}

bool MetaPopulationManifestStore::Contains(const MetaHash256& digest) const {
  return documents_.contains(digest);
}

std::vector<MetaPopulationManifestDocument>
MetaPopulationManifestStore::Documents() const {
  std::vector<MetaPopulationManifestDocument> result;
  result.reserve(documents_.size());
  for (const auto& [digest, document] : documents_) {
    (void)digest;
    result.push_back(document);
  }
  return result;
}

// Snapshot envelope described on the public API. WriteEntries is also the
// entry layout used by the canonical digest, but that digest prepends its own
// domain separator rather than hashing this store envelope.
void MetaPopulationManifestStore::WriteSnapshot(MetaWriter& writer) const {
  writer.WriteU16(kMetaFormatVersion);
  writer.WriteCount(static_cast<std::uint32_t>(documents_.size()));
  for (const auto& [digest, document] : documents_) {
    WriteFixedArray(writer, digest);
    WriteEntries(writer, document.entries_);
  }
}

std::string MetaPopulationManifestStore::Serialize() const {
  MetaWriter writer;
  WriteSnapshot(writer);
  return writer.TakeBuffer();
}

std::uint64_t MetaPopulationManifestStore::SerializedSize() const {
  MetaWriter counter(false);
  WriteSnapshot(counter);
  return counter.size();
}

absl::StatusOr<MetaPopulationManifestStore>
MetaPopulationManifestStore::Deserialize(std::string_view bytes) {
  if (bytes.size() > kMaxMetaSnapshotBytes) {
    return MetaFailStopError(
        "population manifest store exceeds the snapshot byte cap");
  }
  MetaReader reader(bytes);
  auto version = reader.ReadU16();
  if (!version.ok()) return version.status();
  if (*version != kMetaFormatVersion) {
    return MetaFailStopError("unknown population manifest schema version");
  }
  auto documents = reader.ReadCount(kMaxMetaPopulationManifestsInSnapshot);
  if (!documents.ok()) return documents.status();

  MetaPopulationManifestStore store;
  for (std::uint32_t i = 0; i < *documents; ++i) {
    auto digest = ReadFixedArray<32>(reader);
    if (!digest.ok()) return digest.status();
    auto entries = reader.ReadList<MetaPopulationManifestEntry>(
        kMaxMetaPopulationManifestEntries,
        [](MetaReader& item_reader)
            -> absl::StatusOr<MetaPopulationManifestEntry> {
          auto partition_id = item_reader.ReadU32();
          if (!partition_id.ok()) return partition_id.status();
          auto logical_epoch = item_reader.ReadU64();
          if (!logical_epoch.ok()) return logical_epoch.status();
          return MetaPopulationManifestEntry{*partition_id, *logical_epoch};
        });
    if (!entries.ok()) return entries.status();
    if (!IsCanonical(*entries) || !EntriesInDomain(*entries) ||
        CanonicalDigest(*entries) != *digest ||
        !store.documents_
             .emplace(
                 *digest,
                 MetaPopulationManifestDocument{*digest, std::move(*entries)})
             .second) {
      return MetaFailStopError("invalid population manifest snapshot");
    }
  }
  if (auto status = reader.Finish(); !status.ok()) return status;
  return store;
}

}  // namespace keylane::meta
