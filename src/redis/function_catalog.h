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

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "bycorf/runtime/task.h"
#include "lavik/storage/engine.h"
#include "lua_eval.h"

namespace lavik {

class FunctionCatalogOperationGuard {
 public:
  FunctionCatalogOperationGuard(const FunctionCatalogOperationGuard&) = delete;
  FunctionCatalogOperationGuard& operator=(
      const FunctionCatalogOperationGuard&) = delete;
  ~FunctionCatalogOperationGuard();

 private:
  friend bycorf::Task<std::unique_ptr<FunctionCatalogOperationGuard>>
  AcquireFunctionCatalogOperation();
  FunctionCatalogOperationGuard() = default;
};

// Serializes catalog observers/mutators with FCALL and promotion capture.
// Holding the returned lease is the existing process-wide Function guard.
bycorf::Task<std::unique_ptr<FunctionCatalogOperationGuard>>
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
  bycorf::Task<absl::StatusOr<StagedCatalog>> StageCompleteCatalog(
      std::vector<LuaFunctionLibrary> target);
  // Publishes only the durable dump/root. Runtime visibility is unchanged on
  // failure, so the staged catalog remains abortable. Client mutations pass
  // their Group precondition for the first root write; startup and trusted
  // replication replay use their existing lifecycle fences instead.
  bycorf::Task<absl::StatusOr<storage::CatalogDurabilityToken>>
  MakeStagedCatalogDurable(
      const StagedCatalog& staged,
      storage::MutationPrecondition mutation_precondition = {});
  // Installs an already durable staged catalog with non-failing worker-local
  // runtime swaps, then replaces the process-global metadata.
  bycorf::Task<absl::Status> CommitStagedCatalog(
      StagedCatalog staged, storage::CatalogDurabilityToken token,
      bool enable_crash_points = true);
  bycorf::Task<absl::Status> AbortStagedCatalog(StagedCatalog* staged);

  // Restores and validates the selected durable dump before service readiness.
  // A fresh set without one keeps the canonical empty runtime; corruption or
  // cross-worker compilation disagreement fails startup.
  bycorf::Task<absl::Status> RecoverAtStartup();
  bycorf::Task<absl::Status> ReplaceFromLibraryCodes(
      const std::vector<std::string>& library_codes);
  bycorf::Task<absl::Status> ValidateLibraryCodes(
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

}  // namespace lavik
