#pragma once

#include <functional>
#include <memory>
#include <span>
#include <string_view>

#include "keylane/storage/detail/grouped_collection.h"
#include "keylane/storage/detail/grouped_commit.h"
#include "keylane/storage/detail/grouped_hash.h"
#include "keylane/storage/detail/record_index.h"

namespace keylane::storage {

// An object directory belongs to a physical root version in one population,
// not merely to a user key or a mutation sequence. A relocation changes the
// physical identity; FLUSHDB/reset can reuse both the key and its sequence.
struct GroupedObjectVersion {
  RecordLocation root_;
  std::uint64_t db_epoch_ = 0;
  std::uint64_t replication_epoch_ = 0;
  // Partition/DB-local population generation, not a worker-wide IO guard.
  std::uint64_t index_generation_ = 0;
  // Runtime-only causal dependency, not part of disk identity or Lookup CAS.
  // Recovered committed views have no pending decision.
  std::shared_ptr<GroupedCommitDecision> decision_ = nullptr;

  bool Matches(const GroupedObjectVersion& other) const noexcept;
};

struct HashGroupLocation {
  HashGroupId id_;
  RecordLocation location_;
  std::shared_ptr<const std::vector<ExtentRef>> extents_;
  // Split markers remain live until the incarnation is removed. Dropping a
  // marker while an older parent snapshot remains on disk resurrects it.
  bool retired_ = false;
};

struct GroupedHashPhysicalState;

// Resident metadata only: field names and values are never retained here.
// There is one compact RecordIndex entry per group, not per field. The
// prefix directory selects that entry; the physical owner supplies the
// allocation epoch when materializing its compact location, as for top-level
// RecordIndex entries. The adapter must validate payload identity/checksums
// before creating this view and hold the necessary physical pins during IO.
class GroupedHashObject {
 public:
  using Handle = std::shared_ptr<const GroupedHashObject>;
  using PreparedHandle = std::shared_ptr<GroupedHashObject>;

  // Builds an immutable, unpublished view. This is a recovery/construction
  // operation, not the per-HSET update path: rebuilding every group entry on
  // a one-group mutation would introduce O(group-count) foreground work.
  // Failure publishes nothing. Construction does not write or pin disk data.
  // A supplied shared arena must use external admission and internal retained
  // accounting, matching the worker's ordinary RecordIndex arena.
  static absl::StatusOr<Handle> Create(
      GroupedObjectVersion version, HashGroupDirectory directory,
      std::span<const HashGroupLocation> locations,
      std::shared_ptr<ScanHashMapEntryArena> arena = nullptr);
  static absl::StatusOr<PreparedHandle> PrepareCreate(
      GroupedObjectVersion provisional_version, HashGroupDirectory directory,
      std::span<const HashGroupLocation> locations,
      std::shared_ptr<ScanHashMapEntryArena> arena = nullptr);

  // Constructs an unpublished update by copying only touched bounded index
  // pages and their routing paths. The supplied directory is the validated
  // after-image; unchanged physical groups and manifests remain shared.
  // The provisional root retains the old physical address until FinalizeRoot.
  // Physical allocation ownership is the caller's invariant: new records and
  // extents are freshly allocated, never borrowed from a different group.
  // Relocation may reuse only that same group's extents across versions.
  static absl::StatusOr<PreparedHandle> PrepareUpdate(
      const Handle& expected, GroupedObjectVersion provisional_version,
      HashGroupDirectory directory,
      std::span<const HashGroupLocation> changed_locations);

  // Ordered collections share the same bounded physical RecordIndex pages.
  // For indexed Sorted Sets, locations include BOTH the ordered graph and
  // member-prefix graph; either incomplete graph rejects publication.
  static absl::StatusOr<Handle> CreateOrdered(
      GroupedObjectVersion version, OrderedGroupDirectory directory,
      std::span<const HashGroupLocation> locations,
      std::shared_ptr<ScanHashMapEntryArena> arena = nullptr);
  static absl::StatusOr<PreparedHandle> PrepareCreateOrdered(
      GroupedObjectVersion version, OrderedGroupDirectory directory,
      std::span<const HashGroupLocation> locations,
      std::shared_ptr<ScanHashMapEntryArena> arena = nullptr);
  static absl::StatusOr<PreparedHandle> PrepareUpdateOrdered(
      const Handle& expected, GroupedObjectVersion version,
      OrderedGroupDirectory directory,
      std::span<const HashGroupLocation> changed_locations);

  // TTL-only publication retains the exact value revision, routing directory
  // and physical pages. Only the wrapper/root command sequence, expiration
  // and causal decision change; neither ordered metadata vectors nor values
  // are copied. The command sequence must not move backwards.
  static absl::StatusOr<PreparedHandle> PrepareMetadataUpdate(
      const Handle& expected, GroupedObjectVersion version);

  // Completes an exclusively owned, unpublished builder without allocating.
  // A published const handle must never be cast back into a builder.
  static absl::Status FinalizeRoot(PreparedHandle& prepared,
                                   GroupedObjectVersion exact_version);
  static absl::StatusOr<Handle> RelocateRoot(const Handle& expected,
                                             GroupedObjectVersion replacement);
  static absl::StatusOr<PreparedHandle> PrepareRootRelocation(
      const Handle& expected);
  // Refreshes same-logical-version group relocations that happened while root
  // append suspended. This must not overwrite those newer physical pointers.
  static absl::Status FinalizeRootRelocation(
      PreparedHandle& prepared, const Handle& current,
      GroupedObjectVersion exact_version);
  static absl::StatusOr<Handle> RelocateGroup(
      const Handle& expected, HashGroupId id,
      const RecordLocation& expected_location,
      const RecordLocation& replacement,
      std::shared_ptr<const std::vector<ExtentRef>> extents = nullptr);

  const GroupedObjectVersion& version() const noexcept { return version_; }
  // Pending views remain readable under the owning transaction's key hold.
  // A failed decision is never a readable partial object, even if the writer
  // has fail-stopped before it could restore its predecessor. Retained page
  // readers must check this again after every suspension.
  absl::Status ReadStatus() const;
  // Requires a Hash/Set object or has_member_index(). In the latter case this
  // is the Sorted Set's member-to-score directory, not its ordered pages.
  const HashGroupDirectory& directory() const noexcept {
    return is_ordered() ? *ordered_directory_->member_directory() : *directory_;
  }
  bool has_member_index() const noexcept {
    return is_ordered() && ordered_directory_->member_directory() != nullptr;
  }
  bool is_ordered() const noexcept { return ordered_directory_ != nullptr; }
  const OrderedGroupDirectory& ordered_directory() const noexcept {
    return *ordered_directory_;
  }
  std::uint64_t incarnation() const noexcept {
    return is_ordered() ? ordered_directory_->root().incarnation_
                        : directory_->root().incarnation_;
  }
  std::uint64_t revision() const noexcept {
    return is_ordered() ? ordered_directory_->sequence()
                        : directory_->sequence();
  }
  std::uint64_t command_sequence() const noexcept {
    return version_.root_.mutation_sequence_;
  }
  bool SameLogicalRoot(const GroupedHashObject& other) const noexcept;
  const RecordIndex::Entry* FindGroup(std::string_view field) const;
  const RecordIndex::Entry* FindGroup(HashGroupId id) const;
  const RecordIndex::Entry* FindRecord(HashGroupId id) const;
  // The manifest's retained charge follows this handle even after the object
  // and its side-index entry have been reclaimed.
  std::shared_ptr<const std::vector<ExtentRef>> ExtentsFor(
      HashGroupId id) const;
  // Logical streaming-page count. Sorted Set streams traverse only ordered
  // pages; physical lifecycle code must use ForEachRecord for both graphs.
  std::size_t group_count() const noexcept {
    return is_ordered() ? ordered_directory_->root().group_count_
                        : directory_->root().group_count_;
  }
  std::size_t record_count() const noexcept;
  using RecordVisitor = std::function<void(
      HashGroupId, const RecordIndex::Entry&,
      const std::shared_ptr<const std::vector<ExtentRef>>&, bool)>;
  // Includes active leaves AND retired parent markers in both identity spaces.
  // The callback borrows compact entries and must materialize block epoch/owner
  // before suspension.
  void ForEachRecord(const RecordVisitor& visitor) const;

 private:
  GroupedObjectVersion version_;
  // Directory nodes. Manifest copies carry independent shared charges so
  // their readers can outlive this object without escaping maxmemory.
  std::shared_ptr<const HashGroupDirectory> directory_;
  std::shared_ptr<const OrderedGroupDirectory> ordered_directory_;
  std::shared_ptr<const GroupedHashPhysicalState> physical_;
};

// Sparse second-level USER-KEY map. Only a grouped top-level RecordIndex
// entry warrants a lookup here. This is deliberately ScanHashMap too; neither
// field names nor pointers to replaceable top-level entries are map keys.
// One instance belongs to one (partition, DB), under the owner's store mutex.
// Handles retain metadata across suspension, NOT physical disk-block pins.
class GroupedObjectIndex {
 public:
  using Handle = GroupedHashObject::Handle;

  explicit GroupedObjectIndex(
      std::shared_ptr<ScanHashMapEntryArena> arena = nullptr);
  GroupedObjectIndex(GroupedObjectIndex&&) noexcept = default;
  GroupedObjectIndex& operator=(GroupedObjectIndex&&) noexcept = default;

  class Publication {
   public:
    Publication(Publication&& other) noexcept;
    Publication& operator=(Publication&&) = delete;
    Publication(const Publication&) = delete;
    ~Publication();
    // No allocation. Matching top-level root publication belongs in the same
    // non-suspending section. A failure leaves the reservation cancelable.
    absl::Status Commit(Handle replacement);
    // Revalidates a physical-only group relocation while the durable root
    // remains unchanged. Never permits a new logical root/incarnation.
    absl::Status RefreshExpected(const Handle& current);

   private:
    friend class GroupedObjectIndex;
    Publication(GroupedObjectIndex* index, std::string_view key,
                Handle expected, bool inserted) noexcept;
    GroupedObjectIndex* index_;
    std::string_view key_;
    Handle expected_;
    bool inserted_;
  };

  // Pre-admits side-table capacity before writing the durable root. The token
  // borrows key bytes and must be destroyed on the owner under serialization,
  // before those bytes, this index, or its population are detached/destroyed.
  absl::StatusOr<Publication> PreparePublish(std::string_view key,
                                             const Handle& expected);

  // A non-grouped root bypasses this table. A grouped root with a missing or
  // mismatched view is corruption/stale state, never an empty Hash fallback.
  // Failed transaction views are rejected by default. allow_failed is only
  // for undo/physical lifecycle code that must retire or restore that graph;
  // it never grants permission to return its payload or cardinality.
  absl::StatusOr<Handle> Lookup(std::string_view key,
                                const GroupedObjectVersion& version,
                                bool allow_failed = false) const;
  // Mutation/lifecycle preparation only, under owner serialization. Returns
  // the full-key side entry without asserting a top-level physical version;
  // the caller must validate logical identity/population before using it.
  // Ordinary reads must use the version-checked Lookup instead.
  Handle CurrentForMutation(std::string_view key) const;

  // Compare-and-publish under owner serialization; expected == nullptr means
  // absent. Caller must publish the matching top-level marker in the same
  // non-suspending section, after memory admission and its storage commit
  // protocol. This operation alone provides NO durable transaction semantics.
  // Pointer equality intentionally permits rollback to an older retained view
  // but rejects a stale writer, including erase/recreate with the same key.
  absl::Status Publish(std::string_view key, const Handle& expected,
                       Handle replacement);
  absl::Status Erase(std::string_view key, const Handle& expected);

  // A journaled grouped-to-compact write keeps its already admitted slot so
  // rollback can restore the old view without allocating at maxmemory. Null
  // slots are not logical objects and compact roots never consult them.
  absl::Status ClearKeepingSlot(std::string_view key, const Handle& expected);
  // Releases a settled undo slot only if it is still null; a later grouped
  // publication using that slot must not be removed by an older undo owner.
  void EraseNullSlot(std::string_view key);

  // O(1) logical detachment. The caller retains this population until its
  // physical retirements/pins are settled, then destroys it asynchronously.
  GroupedObjectIndex Detach() noexcept;
  std::size_t size() const noexcept { return objects_.size(); }
  bool empty() const noexcept { return objects_.empty(); }

  template <typename Visitor>
  void ForEach(Visitor&& visitor) {
    objects_.ForEach(
        [&](const auto& entry) { visitor(entry.key(), entry.value_); });
  }

 private:
  explicit GroupedObjectIndex(ScanHashMap<Handle> objects) noexcept
      : objects_(std::move(objects)) {}
  ScanHashMap<Handle> objects_;
};

}  // namespace keylane::storage
