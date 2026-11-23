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

# Outbound HTTP client

`lavik::http_client` provides HTTP/1.1 exchanges over Bycorf streams. The
caller supplies a resolved numeric address and the HTTP host. For HTTPS the
caller prepares the client TLS context off-worker. The handshake verifies
the certificate against the configured host, or the dial address if the host
is omitted.

Each `HttpClient::Send` opens one connection on the calling worker and owns
it through handshake, request write, response read and close. Concurrent
calls have independent connections and timers. The caller keeps the client
and any borrowed upload bytes alive until the exchange completes. After
sending the headers, an exchange with a body reads the response concurrently
with the upload. An early final response retires any pending send before the
caller regains ownership of its upload buffer.

One deadline covers the entire exchange. The worker checks it before sending
and closes the stream if it expires during I/O. A timeout after a write can
leave the remote outcome unknown. The caller decides whether to retry.

The response retains the final status and ordered header fields, including
duplicates. The client incrementally reads fixed-length, chunked and
close-delimited bodies into bounded memory. HTTP error statuses are returned
with their responses. A caller that only consumes error statuses can finish
after the final error headers and close the connection. Transport, framing,
deadline and size failures return
an error status from the exchange.

The public API is in [http_client.h](../../include/lavik/net/http_client.h)
and the exchange lifecycle is in [http_client.cpp](../../src/net/http_client.cpp).
