<!--
Copyright (C) 2026 EloqData Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# S3 object storage client

`lavik::s3_client` provides bucket-scoped PUT, GET, DELETE, and
ListObjectsV2 over the [outbound HTTP client](13-outbound-http.md).
The caller configures a region, path-style endpoint, bucket and credentials.
Each request uses AWS Signature Version 4 with the SHA-256 of the payload,
the HTTP authority and the encoded request target. Temporary credentials
include their session token in the signed fields.

PUT returns the service's opaque ETag. A caller can supply that value in
`If-Match` when overwriting the object. A rejected precondition is returned
to the caller. A deadline or transport failure after sending a write leaves
its outcome unknown. The application owns reconciliation and retry policy.

Each prefix listing returns one page of decoded keys and an opaque
continuation token. A caller advances through the pages with the same prefix
until the token is empty. Object bytes, keys and tokens are independent of
Lavik's local device layout.

The [public API](../../include/lavik/object_store/s3_client.h) exposes these
operations independently of the storage engine.
