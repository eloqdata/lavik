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
file when the password must survive a restart. An authenticated client can run
administrative commands, including `REPLICAOF` and `LAVIK.REPLICAOF`; restrict
client access to trusted operators when exposing these commands.

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
identity apply to the native control and flow connections. When `masterauth`
is set without `tls-replication yes`, the native `AUTH` exchange sends it over
plaintext, as with Redis `REPLICAOF`. Enable TLS when credentials or replicated
data cross an untrusted network. The native command is runtime-only; startup
`replicaof` remains a Redis PSYNC setting.
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

## Meta Sentinel discovery

Configure Sentinel independently from Data and Admin. `--sentinel-requirepass`
is the discovery password; Data uses `--requirepass`, with `--masterauth` on
all potential replicas. Only the `default` username is supported. Successful
mTLS still requires AUTH (or HELLO AUTH), including after reconnect.

Each Meta can use `--sentinel-addr 127.0.0.1:26379`,
`--sentinel-tls-addr 127.0.0.1:26380`, or both. TLS additionally requires
`--sentinel-tls-cert meta.pem --sentinel-tls-key meta.key` and, when client
certificates are verified, `--sentinel-tls-ca ca.pem`.
`--sentinel-tls-auth-clients no|optional|yes` defaults to `yes`, like Redis
Sentinel. This does not change Data's existing default. Keep Admin mTLS enabled
with its independent `--ctl-tls-ca/cert/key` settings; application certificates
must contain no `lavik://operator/...` identity. Certificates are loaded at
startup, and rotation requires restart.

Set `--sentinel-data-transport tls` on every Meta to announce registered Data
TLS endpoints; the default is `plaintext`. This setting does not depend on
whether discovery arrived on a TLS connection. Missing TLS registration
withdraws the Primary or omits that replica without plaintext fallback and
without restricting Meta election. Add the TLS endpoint through the existing
Data identity update lifecycle. Each Meta still registers just one
`sentinel_endpoint`, changed only by member replacement. All advertised peers
must accept the same client transport and credentials.

For DNS names, set `--sentinel-resolve-hostnames yes`. Add
`--sentinel-announce-hostnames yes` to retain names in replies and events;
otherwise clients receive a locally resolved IP (certificates then need that
IP SAN). Failed DNS does not block workers; an earlier successful result may
remain cached. No DNS update convergence deadline is guaranteed. Register
`tls://data.example:6380` and `tls://sentinel.example:26380` in the existing
manifest/identity fields. Names and tags survive snapshots and restarts.
Raft, Data-control and Admin continue requiring numeric addresses. Advertised
endpoints retain the existing 256-byte limit, including scheme and port.

A TCP proxy can expose ports different from the bind ports: register its public
TLS addresses in the same manifest fields. Native replication uses those same
Data routes. End-to-end certificate SANs must match the advertised IP/name.
Do not split internal and external address views or assume a TLS query selects
a Data TLS port. Keep the local publication options identical on all Meta nodes.

A Python application can use separate credentials while retaining ordinary
Sentinel connection pools (set `protocol` to 2 or 3 for Data):

```python
from redis.sentinel import Sentinel

tls = dict(ssl=True, ssl_ca_certs="ca.pem", ssl_certfile="application.pem",
           ssl_keyfile="application.key", ssl_check_hostname=True)
sentinel = Sentinel([("sentinel.example", 26380)],
                    sentinel_kwargs=dict(password="sentinel-secret", **tls),
                    socket_timeout=2)
primary = sentinel.master_for("single-discovery", password="data-secret",
                              protocol=3, **tls)
replica = sentinel.slave_for("single-discovery", password="data-secret",
                            protocol=3, **tls)
primary.set("example", "value")
print(primary.get("example"))
# Replica reads are asynchronous; wait for the expected value when needed.
print(replica.get("example"))
```

For Go, load the CA and application certificate into one TLS configuration and
pass it to the standard failover client. Leave `ServerName` empty so each dial
verifies its actual destination. The same configuration serves both connections:

```go
pair, err := tls.LoadX509KeyPair("application.pem", "application.key")
if err != nil { panic(err) }
pem, err := os.ReadFile("ca.pem")
if err != nil { panic(err) }
roots := x509.NewCertPool()
if !roots.AppendCertsFromPEM(pem) { panic("invalid CA") }
client := redis.NewFailoverClient(&redis.FailoverOptions{
    MasterName: "single-discovery",
    SentinelAddrs: []string{"sentinel.example:26380"},
    SentinelPassword: "sentinel-secret", Password: "data-secret", Protocol: 3,
    TLSConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{pair},
                           MinVersion: tls.VersionTLS12},
})
defer client.Close()
if err := client.Set(context.Background(), "example", "value", 0).Err(); err != nil {
    panic(err)
}
```

The Go snippet uses `context`, `crypto/tls`, `crypto/x509`, `os` and
`github.com/redis/go-redis/v9`. For a fully plaintext deployment, omit both
clients' TLS settings and register TCP ports; independent passwords still apply.
For mapped TLS deployment, replace the seeds and registered Data ports with
proxy-facing ports without changing client dialing or certificate verification.

The executable acceptance examples generate certificates, enable Admin mTLS,
use an application certificate without operator authority, and exercise actual
replica reads. Given a configured build with its pinned client dependencies:

```bash
# Set TMPDIR to a local scratch directory; all fixture data is created below it.
# BUILD is the configured CMake build directory.
export TMPDIR=/path/to/scratch/lavik-sentinel-tests
mkdir -p "$TMPDIR"
ctest --test-dir "$BUILD" --output-on-failure -R '^meta_integration.sentinel_tls_'
```

| Reference/client | Pinned version | Acceptance modes |
|---|---|---|
| Redis Sentinel behavior | 7.2.14 | Local publication policy, no fallback, mTLS plus AUTH |
| redis-py | 8.1.0 | Data RESP2/3, direct and mapped plaintext/TLS |
| go-redis | 9.22.0 | Data RESP2/3 and explicit Sentinel RESP2/3, same modes |

Go uses one TLSConfig for Sentinel and Data, leaves ServerName empty to verify
the actual destination, and wraps `redis.NewDialer` only to count connections.
Python supplies independent Sentinel/Data credentials and TLS settings.
Upgrade all participating Meta and Data processes before enabling hostname or
TLS Sentinel registration. New binaries read old untagged numeric registrations;
after enabling new registrations, mixed old binaries or rollback are unsupported.
