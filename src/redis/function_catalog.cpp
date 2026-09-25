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

#include "function_catalog.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <memory>

#include "absl/strings/str_cat.h"
#include "bycorf/runtime/cross_core.h"
#include "bycorf/runtime/worker.h"
#include "lavik/fault_injection.h"
#include "lavik/rdb.h"
#include "lavik/replication_command.h"

namespace lavik {
namespace {

std::unique_ptr<FunctionCatalog> g_function_catalog;
std::atomic_flag g_function_catalog_operation = ATOMIC_FLAG_INIT;

}  // namespace

FunctionCatalogOperationGuard::~FunctionCatalogOperationGuard() {
  g_function_catalog_operation.clear(std::memory_order_release);
}

bycorf::Task<std::unique_ptr<FunctionCatalogOperationGuard>>
AcquireFunctionCatalogOperation() {
  while (g_function_catalog_operation.test_and_set(std::memory_order_acquire)) {
    co_await bycorf::Yield(*bycorf::ThisWorker().self_);
  }
  co_return std::unique_ptr<FunctionCatalogOperationGuard>(
      new FunctionCatalogOperationGuard());
}

bool FunctionCatalog::SameLibrary(const LuaFunctionLibrary& left,
                                  const LuaFunctionLibrary& right) noexcept {
  if (left.name_ != right.name_ || left.engine_ != right.engine_ ||
      left.code_ != right.code_ ||
      left.functions_.size() != right.functions_.size()) {
    return false;
  }
  for (std::size_t index = 0; index < left.functions_.size(); ++index) {
    const LuaFunctionInfo& a = left.functions_[index];
    const LuaFunctionInfo& b = right.functions_[index];
    if (a.name_ != b.name_ || a.description_ != b.description_ ||
        a.flags_ != b.flags_) {
      return false;
    }
  }
  return true;
}

std::vector<LuaFunctionLibrary> FunctionCatalog::LibrariesFromCodes(
    const std::vector<std::string>& codes) {
  std::vector<LuaFunctionLibrary> libraries;
  libraries.reserve(codes.size());
  for (const std::string& code : codes) {
    const std::optional<std::string> name =
        LuaFunctionLibraryNameFromCode(code);
    libraries.push_back(LuaFunctionLibrary{
        .name_ = name.value_or(""),
        .engine_ = "LUA",
        .code_ = code,
        .functions_ = {},
    });
  }
  return libraries;
}

bycorf::Task<absl::StatusOr<FunctionCatalog::StagedCatalog>>
FunctionCatalog::StageCompleteCatalog(std::vector<LuaFunctionLibrary> target) {
  if (storage_ == nullptr) {
    co_return absl::FailedPreconditionError(
        "Function catalog storage is not initialized");
  }
  std::sort(
      target.begin(), target.end(),
      [](const LuaFunctionLibrary& left, const LuaFunctionLibrary& right) {
        return left.name_ < right.name_;
      });
  std::vector<std::string> codes;
  codes.reserve(target.size());
  for (const LuaFunctionLibrary& library : target) {
    if (library.name_.empty()) {
      co_return absl::InvalidArgumentError("Missing library metadata");
    }
    codes.push_back(library.code_);
  }

  const std::optional<std::size_t> dump_bytes =
      rdb::FunctionDumpEncodedSize(codes);
  if (!dump_bytes.has_value() ||
      *dump_bytes > storage::kMaxFunctionCatalogBytes) {
    co_return absl::OutOfRangeError(
        "Function catalog dump exceeds the 1 GiB limit");
  }
  std::string dump = rdb::EncodeFunctionDump(codes);
  const std::array<std::string_view, 4> restore_args{"FUNCTION", "RESTORE",
                                                     dump, "FLUSH"};
  auto encoded_restore =
      ReplicationCommandPayloadSource::Create(0, restore_args);
  if (!encoded_restore.ok()) co_return encoded_restore.status();

  std::optional<std::vector<LuaFunctionLibrary>> canonical;
  unsigned staged_workers = 0;
  for (; staged_workers < storage_->worker_count(); ++staged_workers) {
    auto stage = [codes]() {
      return StageCompleteLuaFunctionCatalogLocally(codes);
    };
    absl::StatusOr<std::vector<LuaFunctionLibrary>> result =
        staged_workers == bycorf::ThisWorker().id_
            ? stage()
            : co_await bycorf::SubmitTo(staged_workers, std::move(stage));
    bool same = result.ok();
    if (same && canonical.has_value()) {
      same = result->size() == canonical->size();
      for (std::size_t index = 0; same && index < result->size(); ++index) {
        same = SameLibrary(result->at(index), canonical->at(index));
      }
    }
    if (!result.ok() || !same) {
      const absl::Status failure =
          result.ok() ? absl::InternalError(
                            "workers registered different Function metadata")
                      : result.status();
      const unsigned abort_count =
          result.ok() ? staged_workers + 1 : staged_workers;
      for (unsigned worker = 0; worker < abort_count; ++worker) {
        auto abort = [] {
          AbortStagedLuaFunctionCatalogLocally();
          return true;
        };
        if (worker == bycorf::ThisWorker().id_) {
          abort();
        } else {
          (void)co_await bycorf::SubmitTo(worker, std::move(abort));
        }
      }
      co_return failure;
    }
    if (!canonical.has_value()) canonical = std::move(*result);
  }
  if (!canonical.has_value()) {
    co_return absl::FailedPreconditionError("Lua runtime has no workers");
  }
  co_return StagedCatalog{
      .libraries_ = std::move(*canonical),
      .dump_ = std::move(dump),
      .active_ = true,
  };
}

bycorf::Task<absl::StatusOr<storage::CatalogDurabilityToken>>
FunctionCatalog::MakeStagedCatalogDurable(
    const StagedCatalog& staged,
    storage::MutationPrecondition mutation_precondition) {
  if (!staged.active_) {
    co_return absl::FailedPreconditionError(
        "Function catalog staging is inactive");
  }
  co_return co_await storage_->CommitFunctionCatalog(
      staged.dump_, std::move(mutation_precondition));
}

bycorf::Task<absl::Status> FunctionCatalog::CommitStagedCatalog(
    StagedCatalog staged, storage::CatalogDurabilityToken token,
    bool enable_crash_points) {
  if (!staged.active_ || token.catalog_generation_ == 0) {
    co_return absl::FailedPreconditionError(
        "Function catalog commit has no durable staging token");
  }
  if (enable_crash_points) {
    LAVIK_MAYBE_CRASH_AT("function-catalog-before-runtime-swap");
  }
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    auto commit = [] {
      CommitStagedLuaFunctionCatalogLocally();
      return true;
    };
    if (worker == bycorf::ThisWorker().id_) {
      commit();
    } else {
      (void)co_await bycorf::SubmitTo(worker, std::move(commit));
    }
  }
  ReplaceStoredLuaFunctionCatalog(std::move(staged.libraries_));
  durability_token_ = token;
  if (enable_crash_points) {
    LAVIK_MAYBE_CRASH_AT("function-catalog-after-runtime-swap");
  }
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> FunctionCatalog::AbortStagedCatalog(
    StagedCatalog* staged) {
  if (staged == nullptr || !staged->active_) co_return absl::OkStatus();
  for (unsigned worker = 0; worker < storage_->worker_count(); ++worker) {
    auto abort = [] {
      AbortStagedLuaFunctionCatalogLocally();
      return true;
    };
    if (worker == bycorf::ThisWorker().id_) {
      abort();
    } else {
      (void)co_await bycorf::SubmitTo(worker, std::move(abort));
    }
  }
  staged->active_ = false;
  co_return absl::OkStatus();
}

bycorf::Task<absl::Status> FunctionCatalog::RecoverAtStartup() {
  auto recovered = storage_->RecoverFunctionCatalog();
  if (!recovered.ok()) co_return recovered.status();
  if (!recovered->has_value()) {
    // A fresh device set already starts with empty worker runtimes and global
    // metadata. Do not consume storage merely to persist that absence; the
    // first real Function mutation enters the normal durable COW path.
    co_return absl::OkStatus();
  }
  auto decoded = rdb::DecodeFunctionDump((**recovered).dump_);
  if (!decoded.ok()) {
    co_return absl::InternalError(
        "durable Function catalog is not a valid FUNCTION DUMP");
  }
  auto staged = co_await StageCompleteCatalog(LibrariesFromCodes(*decoded));
  if (!staged.ok()) co_return staged.status();
  co_return co_await CommitStagedCatalog(std::move(*staged),
                                         (**recovered).token_, false);
}

bycorf::Task<absl::Status> FunctionCatalog::ReplaceFromLibraryCodes(
    const std::vector<std::string>& library_codes) {
  auto staged =
      co_await StageCompleteCatalog(LibrariesFromCodes(library_codes));
  if (!staged.ok()) co_return staged.status();
  auto token = co_await MakeStagedCatalogDurable(*staged);
  if (!token.ok()) {
    co_await AbortStagedCatalog(&*staged);
    co_return token.status();
  }
  co_return co_await CommitStagedCatalog(std::move(*staged), *token);
}

bycorf::Task<absl::Status> FunctionCatalog::ValidateLibraryCodes(
    const std::vector<std::string>& library_codes) {
  auto staged =
      co_await StageCompleteCatalog(LibrariesFromCodes(library_codes));
  if (!staged.ok()) co_return staged.status();
  co_await AbortStagedCatalog(&*staged);
  co_return absl::OkStatus();
}

std::string FunctionCatalog::SnapshotDump() const {
  std::vector<std::string> codes;
  for (const LuaFunctionLibrary& library : SnapshotLuaFunctionLibraries()) {
    codes.push_back(library.code_);
  }
  return rdb::EncodeFunctionDump(codes);
}

void InitFunctionCatalog(storage::StorageEngine* storage) {
  g_function_catalog = std::make_unique<FunctionCatalog>(storage);
}

FunctionCatalog& GlobalFunctionCatalog() {
  assert(g_function_catalog != nullptr);
  return *g_function_catalog;
}

}  // namespace lavik
