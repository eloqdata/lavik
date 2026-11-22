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

#include "lavik/object_store/s3_client.h"

#include <libxml/parser.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <charconv>
#include <climits>
#include <memory>
#include <utility>

#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "s3_protocol.h"

namespace lavik::object_store {
namespace {
using net::HttpMethod;
using net::HttpRequest;
using net::HttpResponse;

constexpr char kHex[] = "0123456789ABCDEF";

std::string Encode(std::string_view value, bool keep_slashes = false) {
  std::string encoded;
  for (unsigned char c : value) {
    if (absl::ascii_isalnum(c) || c == '-' || c == '_' || c == '.' ||
        c == '~' || (keep_slashes && c == '/')) {
      encoded += static_cast<char>(c);
    } else {
      encoded += '%';
      encoded += kHex[c >> 4];
      encoded += kHex[c & 15];
    }
  }
  return encoded;
}

// ListObjectsV2 encoding-type=url applies to keys, not continuation tokens.
// '+' is a literal byte in URI encoding.
absl::StatusOr<std::string> DecodeKey(std::string_view encoded) {
  std::string key;
  for (size_t i = 0; i < encoded.size(); ++i) {
    if (encoded[i] != '%') {
      key += encoded[i];
      continue;
    }
    if (encoded.size() - i < 3)
      return absl::DataLossError("truncated URL-encoded S3 key");
    const char* digits = encoded.data() + i + 1;
    unsigned char byte;
    auto [end, ec] = std::from_chars(digits, digits + 2, byte, 16);
    if (ec != std::errc{} || end != digits + 2)
      return absl::DataLossError("invalid URL-encoded S3 key");
    key += static_cast<char>(byte);
    i += 2;
  }
  return key;
}

absl::StatusOr<std::string> Sha256Hex(std::string_view bytes) {
  unsigned char digest[32];
  if (!EVP_Digest(bytes.data(), bytes.size(), digest, nullptr, EVP_sha256(),
                  nullptr))
    return absl::InternalError("SHA-256 failed");
  return absl::BytesToHexString(
      std::string_view(reinterpret_cast<const char*>(digest), sizeof(digest)));
}

absl::StatusOr<std::string> Signature(std::string_view secret,
                                      std::string_view date,
                                      std::string_view region,
                                      std::string_view string_to_sign) {
  std::string key = absl::StrCat("AWS4", secret);
  for (std::string_view input :
       {date, region, std::string_view("s3"), std::string_view("aws4_request"),
        string_to_sign}) {
    unsigned char digest[32];
    if (key.size() > INT_MAX ||
        !HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
              reinterpret_cast<const unsigned char*>(input.data()),
              input.size(), digest, nullptr))
      return absl::InternalError("SigV4 HMAC-SHA256 failed");
    key.assign(reinterpret_cast<const char*>(digest), sizeof(digest));
  }
  return absl::BytesToHexString(key);
}

absl::Status CheckResponse(unsigned status, unsigned expected) {
  if (status == expected) return absl::OkStatus();
  const auto message = absl::StrCat("S3 returned HTTP ", status);
  switch (status) {
    case 400:
      return absl::InvalidArgumentError(message);
    case 401:
    case 403:
      return absl::PermissionDeniedError(message);
    case 404:
      return absl::NotFoundError(message);
    case 409:
      return absl::AbortedError(message);
    case 412:
      return absl::FailedPreconditionError(message);
    case 429:
      return absl::ResourceExhaustedError(message);
    default:
      return status >= 500 ? absl::UnavailableError(message)
                           : absl::UnknownError(message);
  }
}

bool IsElement(xmlNodePtr node, const char* name) {
  return node && node->type == XML_ELEMENT_NODE &&
         xmlStrEqual(node->name, BAD_CAST name);
}

absl::StatusOr<std::string> Field(xmlNodePtr parent, const char* name,
                                  bool required = true) {
  std::string value;
  bool seen = false;
  for (auto* child = parent->children; child; child = child->next) {
    if (!IsElement(child, name)) continue;
    if (seen)
      return absl::DataLossError(absl::StrCat("duplicate S3 field ", name));
    seen = true;
    for (auto* part = child->children; part; part = part->next) {
      if (part->type == XML_COMMENT_NODE) continue;
      if (part->type != XML_TEXT_NODE && part->type != XML_CDATA_SECTION_NODE)
        return absl::DataLossError(absl::StrCat("invalid S3 field ", name));
      value += reinterpret_cast<const char*>(part->content);
    }
  }
  if (required && (!seen || value.empty()))
    return absl::DataLossError(absl::StrCat("missing S3 field ", name));
  return value;
}

}  // namespace

absl::StatusOr<S3ListPage> internal::ParseListPage(std::string_view body,
                                                   std::string_view previous,
                                                   unsigned max_keys) {
  if (body.size() > INT_MAX)
    return absl::ResourceExhaustedError("S3 XML too large");
  // Parse only in-memory UTF-8. Leave DTD loading and entity substitution off.
  std::unique_ptr<xmlDoc, decltype(&xmlFreeDoc)> doc(
      xmlReadMemory(body.data(), static_cast<int>(body.size()), nullptr,
                    "UTF-8",
                    XML_PARSE_NONET | XML_PARSE_NOERROR | XML_PARSE_NOWARNING),
      xmlFreeDoc);
  auto* root = doc ? xmlDocGetRootElement(doc.get()) : nullptr;
  if (!IsElement(root, "ListBucketResult") || doc->intSubset || doc->extSubset)
    return absl::DataLossError("invalid S3 ListBucketResult");
  auto encoding = Field(root, "EncodingType");
  if (!encoding.ok()) return encoding.status();
  if (*encoding != "url")
    return absl::DataLossError("unexpected S3 key encoding");
  auto truncated = Field(root, "IsTruncated");
  if (!truncated.ok()) return truncated.status();
  if (*truncated != "true" && *truncated != "false")
    return absl::DataLossError("invalid S3 IsTruncated");
  auto next = Field(root, "NextContinuationToken", *truncated == "true");
  if (!next.ok()) return next.status();
  if (!next->empty() && (*truncated == "false" || *next == previous))
    return absl::DataLossError("inconsistent S3 continuation token");
  S3ListPage page{.next_token_ = *std::move(next)};
  for (auto* item = root->children; item; item = item->next) {
    if (!IsElement(item, "Contents")) continue;
    auto field = Field(item, "Key");
    if (!field.ok()) return field.status();
    auto key = DecodeKey(*field);
    if (!key.ok()) return key.status();
    if (page.keys_.size() == max_keys)
      return absl::DataLossError("oversized S3 list page");
    page.keys_.push_back(*std::move(key));
  }
  return page;
}

S3Client::S3Client(net::HttpEndpoint endpoint, std::string region,
                   std::string bucket, S3Credentials credentials,
                   net::HttpRequestOptions options)
    : authority_(endpoint.Authority()),
      http_(std::move(endpoint)),
      region_(std::move(region)),
      bucket_(std::move(bucket)),
      credentials_(std::move(credentials)),
      options_(options) {}

absl::Status internal::SignRequest(HttpRequest& request, std::string_view query,
                                   std::string_view authority,
                                   std::string_view region,
                                   std::string_view bucket,
                                   const S3Credentials& credentials,
                                   absl::Time now) {
  if (region.empty() || bucket.empty() || bucket.contains('/') ||
      credentials.access_key_.empty() || credentials.secret_key_.empty())
    return absl::InvalidArgumentError("missing or invalid S3 configuration");
  if (query.empty() && request.target_.empty())
    return absl::InvalidArgumentError("empty S3 object key");
  // Canonical URI and query must reach the server byte-for-byte. In particular
  // an S3 key can contain repeated slashes or dot segments.
  std::string path = absl::StrCat("/", Encode(bucket));
  if (query.empty()) absl::StrAppend(&path, "/", Encode(request.target_, true));
  auto hash = Sha256Hex(request.body_);
  if (!hash.ok()) return hash.status();
  const auto timestamp =
      absl::FormatTime("%Y%m%dT%H%M%SZ", now, absl::UTCTimeZone());
  const std::string_view date(timestamp.data(), 8);
  const auto scope = absl::StrCat(date, "/", region, "/s3/aws4_request");
  // host is emitted by HttpClient. The remaining fields are in lexical
  // order: optional if-match, then the required and optional x-amz fields.
  request.headers_.push_back({"x-amz-content-sha256", *hash});
  request.headers_.push_back({"x-amz-date", timestamp});
  if (!credentials.session_token_.empty())
    request.headers_.push_back(
        {"x-amz-security-token", credentials.session_token_});
  std::string headers = absl::StrCat("host:", authority, "\n");
  std::string names = "host";
  for (const auto& [name, value] : request.headers_) {
    std::string canonical = value;
    absl::RemoveExtraAsciiWhitespace(&canonical);
    absl::StrAppend(&headers, name, ":", canonical, "\n");
    absl::StrAppend(&names, ";", name);
  }
  auto canonical_hash =
      Sha256Hex(absl::StrCat(net::MethodName(request.method_), "\n", path, "\n",
                             query, "\n", headers, "\n", names, "\n", *hash));
  if (!canonical_hash.ok()) return canonical_hash.status();
  auto signature = Signature(credentials.secret_key_, date, region,
                             absl::StrCat("AWS4-HMAC-SHA256\n", timestamp, "\n",
                                          scope, "\n", *canonical_hash));
  if (!signature.ok()) return signature.status();
  request.headers_.push_back(
      {"Authorization",
       absl::StrCat("AWS4-HMAC-SHA256 Credential=", credentials.access_key_,
                    "/", scope, ", SignedHeaders=", names,
                    ", Signature=", *signature)});
  request.target_ = std::move(path);
  if (!query.empty()) absl::StrAppend(&request.target_, "?", query);
  return absl::OkStatus();
}

bycorf::Task<absl::StatusOr<HttpResponse>> S3Client::Send(
    HttpRequest request, std::string query) const {
  auto status = internal::SignRequest(request, query, authority_, region_,
                                      bucket_, credentials_, absl::Now());
  if (!status.ok()) co_return status;
  const unsigned expected = request.method_ == HttpMethod::kDelete ? 204 : 200;
  auto response = co_await http_.Send(std::move(request), options_);
  if (!response.ok()) co_return response.status();
  status = CheckResponse(response->status_, expected);
  if (!status.ok()) co_return status;
  co_return std::move(response);
}

bycorf::Task<absl::StatusOr<std::string>> S3Client::Put(
    std::string key, std::string_view data, std::string if_match) const {
  HttpRequest request{
      .method_ = HttpMethod::kPut, .target_ = std::move(key), .body_ = data};
  if (!if_match.empty())
    request.headers_.push_back({"if-match", std::move(if_match)});
  auto response = co_await Send(std::move(request));
  if (!response.ok()) co_return response.status();
  auto tag = response->Header("ETag");
  if (!tag || tag->empty())
    co_return absl::DataLossError("S3 PUT omitted ETag");
  co_return std::string(*tag);
}

bycorf::Task<absl::StatusOr<std::string>> S3Client::Get(std::string key) const {
  HttpRequest request{.method_ = HttpMethod::kGet, .target_ = std::move(key)};
  auto response = co_await Send(std::move(request));
  if (!response.ok()) co_return response.status();
  co_return std::move(response->body_);
}

bycorf::Task<absl::Status> S3Client::Delete(std::string key) const {
  HttpRequest request{.method_ = HttpMethod::kDelete,
                      .target_ = std::move(key)};
  auto response = co_await Send(std::move(request));
  co_return response.status();
}

bycorf::Task<absl::StatusOr<S3ListPage>> S3Client::List(
    std::string prefix, std::string token, unsigned max_keys) const {
  if (!max_keys || max_keys > 1000)
    co_return absl::InvalidArgumentError("max_keys must be 1..1000");
  std::string query;
  if (!token.empty())
    absl::StrAppend(&query, "continuation-token=", Encode(token), "&");
  absl::StrAppend(&query, "encoding-type=url&list-type=2&max-keys=", max_keys,
                  "&prefix=", Encode(prefix));
  HttpRequest request;
  auto response = co_await Send(std::move(request), std::move(query));
  if (!response.ok()) co_return response.status();
  co_return internal::ParseListPage(response->body_, token, max_keys);
}
}  // namespace lavik::object_store
