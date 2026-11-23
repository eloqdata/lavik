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

#include "s3_protocol.h"

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"

namespace lavik::object_store::internal {
namespace {

TEST(S3Protocol, SignsWireTargetAndTemporaryCredentials) {
  // Reference signature from botocore S3SigV4Auth for these fixture bytes.
  std::string bytes("new\0bytes", 9);
  net::HttpRequest request{.method_ = net::HttpMethod::kPut,
                           .target_ = "blocks//./x %+",
                           .headers_ = {{"if-match", "\"old\""}},
                           .body_ = bytes};
  auto status =
      SignRequest(request, "", "example.test:9443", "us-east-1", "bucket",
                  {"example-id", "example-secret", "example-token"},
                  absl::FromCivil(absl::CivilSecond(2026, 1, 2, 3, 4, 5),
                                  absl::UTCTimeZone()));
  ASSERT_TRUE(status.ok()) << status;
  EXPECT_EQ(request.target_, "/bucket/blocks//./x%20%25%2B");
  ASSERT_EQ(request.headers_.back().name_, "Authorization");
  EXPECT_EQ(request.headers_.back().value_,
            "AWS4-HMAC-SHA256 "
            "Credential=example-id/20260102/us-east-1/s3/aws4_request, "
            "SignedHeaders=host;if-match;x-amz-content-sha256;x-amz-date;x-amz-"
            "security-token, "
            "Signature="
            "9180c795021998cc3f7c0f80dee31665b93dbdea630c46ba8c9dbd512fc16eed");
}

auto ListResponse(std::string_view fields) {
  return absl::StrCat("<ListBucketResult><EncodingType>url</EncodingType>",
                      fields, "</ListBucketResult>");
}

TEST(S3Protocol, DecodesKeysAndPreservesOpaqueToken) {
  auto page = ParseListPage(
      ListResponse("<KeyCount>1</KeyCount><IsTruncated>true</"
                   "IsTruncated><Contents><Key>a%00+%25</"
                   "Key></Contents>"
                   "<NextContinuationToken>&amp;%2B/+</NextContinuationToken>"),
      "", 1);
  ASSERT_TRUE(page.ok()) << page.status();
  ASSERT_EQ(page->keys_.size(), 1);
  EXPECT_EQ(page->keys_[0], std::string("a\0+%", 4));
  EXPECT_EQ(page->next_token_, "&%2B/+");
}

TEST(S3Protocol, VerifiesKeyCountBeforeCompletingTheListing) {
  const std::string final = "<IsTruncated>false</IsTruncated>";
  auto empty =
      ParseListPage(ListResponse(final + "<KeyCount>0</KeyCount>"), "", 2);
  ASSERT_TRUE(empty.ok()) << empty.status();
  EXPECT_TRUE(empty->keys_.empty());
  EXPECT_TRUE(empty->next_token_.empty());

  for (const char* count :
       {"<KeyCount>2</KeyCount>", "<KeyCount>1x</KeyCount>", ""}) {
    auto page = ParseListPage(
        ListResponse(final + count + "<Contents><Key>a</Key></Contents>"), "",
        2);
    EXPECT_TRUE(absl::IsDataLoss(page.status())) << count;
  }
}

TEST(S3Protocol, RejectsMalformedPagesWithoutReportingCompletion) {
  for (const char* fields : {
           "<IsTruncated>false",
           "<IsTruncated>true</IsTruncated>",
           "<Contents><Key>key</Key></Contents>",
           "<IsTruncated>false</IsTruncated><Contents><Key>%3x</Key></"
           "Contents>",
           "<IsTruncated>true</IsTruncated><NextContinuationToken>repeat</"
           "NextContinuationToken>",
       }) {
    auto page = ParseListPage(ListResponse(fields), "repeat", 1);
    EXPECT_TRUE(absl::IsDataLoss(page.status())) << fields;
  }
}
}  // namespace
}  // namespace lavik::object_store::internal
