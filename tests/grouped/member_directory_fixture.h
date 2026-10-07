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

#include "lavik/storage/detail/grouped/collection.h"

namespace lavik::storage::grouped_test {

// Directory-only tests need both routing graphs but no member payloads. Use
// one unsplit prefix with matching counts; physical and command tests build
// their own locations and encoded member pages.
inline GroupedHashRoot MemberRoot(const OrderedCollectionRoot& root) {
  return {.incarnation_ = root.incarnation_,
          .field_count_ = root.item_count_,
          .group_count_ = 1,
          .revision_ = root.revision_};
}

inline RecoveredGroupedRecord MemberRecord(const OrderedCollectionRoot& root) {
  const auto& members = *root.member_index_;
  return {.incarnation_ = members.incarnation_,
          .id_ = {0, 0},
          .sequence_ = members.revision_,
          .lsn_ = members.revision_,
          .field_count_ = members.field_count_,
          .record_token_ = 1};
}

inline HashGroupDirectory MemberDirectory(const OrderedCollectionRoot& root) {
  const auto member = MemberRecord(root);
  return HashGroupDirectory::Recover(*root.member_index_, root.revision_,
                                     std::span(&member, 1), {})
      .value();
}

}  // namespace lavik::storage::grouped_test
