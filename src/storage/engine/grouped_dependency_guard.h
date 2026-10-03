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

#include "lavik/storage/engine.h"

namespace lavik::storage {

// A failed EXEC/Lua command must not leave dependency edges for a graph it
// never published: a later failure of that unused predecessor would otherwise
// poison independent successful commands in the enclosing transaction. Shared
// links make the checkpoint independent of the number of earlier commands.
class GroupedDependencyGuard {
 public:
  GroupedDependencyGuard(TxShardWrites& writes, bool restore)
      : writes_(writes), restore_(restore) {
    if (restore_) {
      previous_ = writes_.grouped_predecessor_;
      others_ = writes_.grouped_dependencies_;
    }
  }
  ~GroupedDependencyGuard() {
    if (!restore_) return;
    writes_.grouped_predecessor_ = std::move(previous_);
    writes_.grouped_dependencies_ = std::move(others_);
  }
  // Root publication transfers these causal edges to the transaction receipt.
  void Keep() noexcept { restore_ = false; }

 private:
  TxShardWrites& writes_;
  bool restore_;
  std::shared_ptr<GroupedCommitDecision> previous_;
  std::shared_ptr<GroupedCommitDependency> others_;
};

}  // namespace lavik::storage
