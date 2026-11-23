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

#include <string>
#include <string_view>
#include <vector>

#include "lavik/net/http_client.h"

namespace lavik::object_store {

struct S3Credentials {
  std::string access_key_;
  std::string secret_key_;
  std::string session_token_;
};

struct S3ListPage {
  std::vector<std::string> keys_;
  // Opaque. Empty marks the final page.
  std::string next_token_;
};

// A path-style S3 client for one bucket and region. Supply credentials and
// a resolved endpoint before starting requests. The client must outlive all
// outstanding tasks. Server rejections are returned as status errors.
class S3Client {
 public:
  S3Client(net::HttpEndpoint endpoint, std::string region, std::string bucket,
           S3Credentials credentials, net::HttpRequestOptions options = {});

  // Returns the opaque, quoted ETag. Pass it verbatim to replace a matching
  // object. A stale tag returns FailedPrecondition. The caller keeps the
  // binary payload immutable until the task completes.
  [[nodiscard]] bycorf::Task<absl::StatusOr<std::string>> Put(
      std::string key, std::string_view data, std::string if_match = {}) const;

  // Returns the object's bytes, or NotFound for a missing key.
  [[nodiscard]] bycorf::Task<absl::StatusOr<std::string>> Get(
      std::string key) const;

  // Issues one DeleteObject request for this key.
  [[nodiscard]] bycorf::Task<absl::Status> Delete(std::string key) const;

  // Reads one ListObjectsV2 page. Pass next_token_ back with the same prefix.
  [[nodiscard]] bycorf::Task<absl::StatusOr<S3ListPage>> List(
      std::string prefix, std::string token = {},
      unsigned max_keys = 1000) const;

 private:
  bycorf::Task<absl::StatusOr<net::HttpResponse>> Send(
      net::HttpRequest request, std::string query = {}) const;

  std::string authority_;
  net::HttpClient http_;
  std::string region_;
  std::string bucket_;
  S3Credentials credentials_;
  net::HttpRequestOptions options_;
};

}  // namespace lavik::object_store
