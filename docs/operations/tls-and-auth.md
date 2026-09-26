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

Lavik can expose plaintext and TLS Redis endpoints at the same time. This
guide covers kernel TCP serving and outgoing replication authentication.
Bycorf provides the TLS transport.

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

Both `optional` and `yes` require `tls-ca-cert-file`. Configure
`tls-cert-file` and `tls-key-file` together, including when they are used only
as an outgoing client identity. Certificate authentication does not replace
Redis password authentication.

`requirepass` enables the single Redis `default` user. Clients may use either
`AUTH <password>` or `AUTH default <password>`. Other commands return `NOAUTH`
until authentication succeeds. This implementation intentionally does not add
the Redis ACL command/file model; put `requirepass` in the main configuration
file when the password must survive a restart.

## Following Redis over TLS

In non-Meta mode, `replicaof` follows an external Redis or Redis Cluster
source using PSYNC. It does not establish native Lavik replication. Configure
the source's TLS port, trusted CA, and Redis credentials:

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

`tls-replication yes` requires `tls-ca-cert-file`; only the `default`
replication user is supported.

## Following a standalone Lavik source

On a Lavik node without Meta management, use `LAVIK.REPLICAOF <host> <port>`
to follow another standalone Lavik node through the native `LVPSYNC`/`LVFLOW`
protocol. The source must be a writable primary; a Lavik replica rejects
another replica's attempt to attach. `LAVIK.REPLICAOF NO ONE` detaches and
promotes through the same durability boundary as `REPLICAOF NO ONE`. Standard
`REPLICAOF` continues to select Redis PSYNC, including when given a Lavik
endpoint, and does not fall back to native replication.

Set `masterauth` (and `masteruser default`) when the source requires a Redis
password. For TLS, use the source's TLS port with `tls-replication yes` and a
trusted `tls-ca-cert-file`; the same outgoing replication credentials and TLS
identity apply to the native control and flow connections. The native command
is runtime-only; startup `replicaof` remains a Redis PSYNC setting.
`CONFIG REWRITE` rejects a native upstream rather than saving it as a Redis
`replicaof` directive.

## Meta-managed TLS

Meta-managed nodes establish native Lavik replication through Follow Owner,
not `replicaof`. When `tls-replication yes` is enabled, native control and all
data-flow connections use TLS. Data also reuses that client identity for its
Meta control session, requiring the CA, certificate and private key together.
Meta membership and Data-node identity must be provisioned through the Meta
control plane; a Redis password alone does not authorize those sessions.

For deployment instructions, see the repository's
[Meta control-plane guide](https://github.com/eloqdata/lavik/blob/main/docs/operations/meta-control-plane.md).

Metrics endpoints remain plaintext.
