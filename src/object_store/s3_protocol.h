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

#include "absl/time/time.h"
#include "lavik/object_store/s3_client.h"

namespace lavik::object_store::internal {

// Starts with the raw object key in target_ and an optional if-match header.
// Replaces the target with the exact signed path and canonical query.
absl::Status SignRequest(net::HttpRequest& request, std::string_view query,
                         std::string_view authority, std::string_view region,
                         std::string_view bucket,
                         const S3Credentials& credentials, absl::Time now);

// A malformed or incomplete page cannot signal the end of a listing.
absl::StatusOr<S3ListPage> ParseListPage(std::string_view body,
                                         std::string_view previous_token,
                                         unsigned max_keys);

}  // namespace lavik::object_store::internal
