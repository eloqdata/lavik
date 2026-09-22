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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "lavik/memory.h"
#include "lavik/storage/format.h"

namespace lavik::storage {

struct CollectionField {
  std::string field_;
  std::string value_;
};

struct CollectionScoredMember {
  std::string member_;
  double score_ = 0;
};

// One independently decoded collection page, not a complete logical value.
// Exactly the container selected by value_type_ may contain entries. Hash/Set
// routing can produce empty pages: consumers must advance next_cursor_ even
// when no entries are returned. List and Sorted Set pages preserve logical
// order. Individual strings retain Redis' 512 MiB limit; the collection's
// aggregate size is not represented by, or limited to, a single string.
// Stream elements_ contain ordered internal logical records (including metadata
// and PEL), so size() counts records rather than messages. Portable projection
// removes their routing envelopes and uses the independent LXS1 message count.
struct CollectionPage {
  // First member deliberately dies last: payload buffers must be freed
  // before their retained-memory allowance is returned. This makes pages
  // move-only while preserving aggregate initialization and borrowed encoding.
  RetainedMemoryCharge retained_charge_{};
  ValueType value_type_ = ValueType::kNone;
  std::vector<CollectionField> fields_{};
  std::vector<std::string> elements_{};
  std::vector<CollectionScoredMember> scored_members_{};
  std::uint64_t next_cursor_ = 0;
  bool done_ = false;

  std::size_t size() const noexcept {
    if (value_type_ == ValueType::kHash) return fields_.size();
    if (value_type_ == ValueType::kSortedSet) return scored_members_.size();
    return elements_.size();
  }

  // Conservative owned-capacity accounting (including inline string space,
  // excluding the DTO itself, which is part of its caller's frame/state).
  // Source adapters reserve an envelope/count-derived upper bound BEFORE
  // loading, then adopt this amount; post-hoc admission is not sufficient.
  std::size_t RetainedBytes() const noexcept {
    constexpr auto maximum = std::numeric_limits<std::size_t>::max();
    std::size_t bytes = 0;
    auto add = [&](std::size_t count, std::size_t width = 1) {
      if (count > (maximum - bytes) / width)
        bytes = maximum;
      else
        bytes += count * width;
    };
    add(fields_.capacity(), sizeof(CollectionField));
    add(elements_.capacity(), sizeof(std::string));
    add(scored_members_.capacity(), sizeof(CollectionScoredMember));
    for (const auto& field : fields_) {
      add(field.field_.capacity());
      add(1);
      add(field.value_.capacity());
      add(1);
    }
    for (const auto& element : elements_) {
      add(element.capacity());
      add(1);
    }
    for (const auto& member : scored_members_) {
      add(member.member_.capacity());
      add(1);
    }
    return bytes;
  }
};

}  // namespace lavik::storage
