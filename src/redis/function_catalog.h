#pragma once

#include <optional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "celer/runtime/task.h"
#include "keylane/storage/engine.h"
#include "lua_eval.h"

namespace keylane {

class FunctionCatalogOperationGuard {
 public:
  FunctionCatalogOperationGuard(const FunctionCatalogOperationGuard&) = delete;
  FunctionCatalogOperationGuard& operator=(
      const FunctionCatalogOperationGuard&) = delete;
  ~FunctionCatalogOperationGuard();

 private:
  friend celer::Task<std::unique_ptr<FunctionCatalogOperationGuard>>
  AcquireFunctionCatalogOperation();
  FunctionCatalogOperationGuard() = default;
};

// Serializes catalog observers/mutators with FCALL and promotion capture.
// Holding the returned lease is the existing process-wide Function guard.
celer::Task<std::unique_ptr<FunctionCatalogOperationGuard>>
AcquireFunctionCatalogOperation();

// Owns the process-global Redis Function catalog lifecycle. A mutation builds
// a complete hidden registry on every worker, persists its canonical
// FUNCTION DUMP, then exposes it with pointer swaps that cannot fail.
class FunctionCatalog {
 public:
  // Owns the hidden per-worker runtimes created by StageCompleteCatalog.
  // active_ means callers must finish with CommitStagedCatalog or
  // AbortStagedCatalog while retaining the Function operation guard.
  struct StagedCatalog {
    std::vector<LuaFunctionLibrary> libraries_;
    std::string dump_;
    bool active_ = false;
  };

  explicit FunctionCatalog(storage::StorageEngine* storage)
      : storage_(storage) {}

  // Requires the Function operation guard. Dump and replication-event limits
  // are checked before cross-worker compilation. The returned complete catalog
  // is invisible until commit; any worker compile or metadata mismatch aborts
  // every hidden runtime before returning an error.
  celer::Task<absl::StatusOr<StagedCatalog>> StageCompleteCatalog(
      std::vector<LuaFunctionLibrary> target);
  // Publishes only the durable dump/root. Runtime visibility is unchanged on
  // failure, so the staged catalog remains abortable.
  celer::Task<absl::StatusOr<storage::CatalogDurabilityToken>>
  MakeStagedCatalogDurable(const StagedCatalog& staged);
  // Installs an already durable staged catalog with non-failing worker-local
  // runtime swaps, then replaces the process-global metadata.
  celer::Task<absl::Status> CommitStagedCatalog(
      StagedCatalog staged, storage::CatalogDurabilityToken token,
      bool enable_crash_points = true);
  celer::Task<absl::Status> AbortStagedCatalog(StagedCatalog* staged);

  // Restores and validates the selected durable dump before service readiness.
  // A fresh set without one keeps the canonical empty runtime; corruption or
  // cross-worker compilation disagreement fails startup.
  celer::Task<absl::Status> RecoverAtStartup();
  celer::Task<absl::Status> ReplaceFromLibraryCodes(
      const std::vector<std::string>& library_codes);
  celer::Task<absl::Status> ValidateLibraryCodes(
      const std::vector<std::string>& library_codes);

  std::string SnapshotDump() const;
  storage::CatalogDurabilityToken durability_token() const noexcept {
    return durability_token_;
  }

 private:
  static bool SameLibrary(const LuaFunctionLibrary& left,
                          const LuaFunctionLibrary& right) noexcept;
  static std::vector<LuaFunctionLibrary> LibrariesFromCodes(
      const std::vector<std::string>& codes);

  storage::StorageEngine* storage_ = nullptr;
  storage::CatalogDurabilityToken durability_token_{};
};

void InitFunctionCatalog(storage::StorageEngine* storage);
FunctionCatalog& GlobalFunctionCatalog();

}  // namespace keylane
