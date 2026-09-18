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

# TLS and password authentication

Lavik can expose plaintext and TLS Redis endpoints at the same time. TLS is
implemented in Celer over the existing io_uring transport with an OpenSSL BIO
pair; plaintext connections continue to use multishot receive when available,
while TLS connections use per-connection receive buffers.

## Server configuration

```text
bind 127.0.0.1 ::1 redis.example.internal
port 6379
tls-port 6380
tls-cert-file /etc/lavik/server.crt
tls-key-file /etc/lavik/server.key
tls-ca-cert-file /etc/lavik/ca.crt
tls-auth-clients no
requirepass client-secret
```

`bind` accepts any number of IPv4 addresses, IPv6 addresses, or hostnames.
Every hostname is resolved to all of its IPv4 and IPv6 results during startup.
Use `bind *` to listen on all local IPv4 and IPv6 interfaces. TLS access is not
restricted by the address form used by the client; the certificate still needs
the appropriate DNS or IP subject alternative names for normal client-side
verification.

`port 0` disables plaintext Redis. `tls-port 0` disables TLS. At least one must
remain enabled, and enabled TLS requires a certificate and matching private
key.

`tls-auth-clients` has Redis-compatible values:

- `no` does not request a client certificate.
- `optional` verifies a client certificate when one is supplied.
- `yes` requires a client certificate signed by `tls-ca-cert-file`.

`requirepass` enables the single Redis `default` user. Clients may use either
`AUTH <password>` or `AUTH default <password>`. Other commands return `NOAUTH`
until authentication succeeds. This implementation intentionally does not add
the Redis ACL command/file model; put `requirepass` in the main configuration
file when the password must survive a restart.

## TLS replication

A replica can authenticate every control and data-flow connection and protect
all of them with TLS:

```text
replicaof redis-primary.example.internal 6380
tls-replication yes
tls-ca-cert-file /etc/lavik/ca.crt
masteruser default
masterauth source-secret
```

The upstream certificate is verified against the exact `replicaof` hostname or
IP address. A hostname is also sent as TLS SNI. If `tls-cert-file` and
`tls-key-file` are configured on the replica, that identity is also presented
to an upstream which requires client certificates.

Metrics endpoints remain plaintext.
