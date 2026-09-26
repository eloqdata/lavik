<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->

# Lavik 0.1.0-beta.1 Docker images

| Image | Contents | Platforms |
|---|---|---|
| `eloqdata/lavik:0.1.0-beta.1` | Standalone Lavik server and `redis-cli` | Linux AMD64, ARM64 |
| `eloqdata/lavik:0.1.0-beta.1-cluster` | Lavik Data, Meta, `lavik-ctl`, `redis-cli`, and Compose bootstrap helper | Linux AMD64, ARM64 |

These images package the checksummed **published `v0.1.0-beta.1` minimal
binaries**, revision `3955b98d43b312324aa8d52775df52cfb111c0d0`. They use kernel
TCP and io_uring. They do not include SPDK/DPDK or Lavik Admin. This release
uses the NuRaft Meta implementation and its release-specific CLI flags; do not
mix this Compose setup with the newer source-built Admin quick start or reuse
its volumes. All files accompanying the original release, including its
license notices, are retained under `/usr/share/doc/lavik`.

Install Docker Engine with Compose v2, or Docker Desktop on macOS. The Linux
host/VM must support io_uring; see the [kernel requirements](../../docs/operations/building-and-packaging.md#kernel-requirements).
The examples allow io_uring using `seccomp=unconfined`. They run as UID/GID
`10001:10001` and do not need privileged containers. Reserve at least 4 GiB
for the Docker VM and several GiB of free disk space for the cluster example.

## Single node

```bash
docker pull eloqdata/lavik:0.1.0-beta.1
docker run -d --name lavik \
  --security-opt seccomp=unconfined \
  --stop-timeout 60 \
  -p 127.0.0.1:6379:6379 \
  -v lavik-data:/data \
  eloqdata/lavik:0.1.0-beta.1

docker exec lavik redis-cli PING
docker exec lavik redis-cli SET hello lavik
docker exec lavik redis-cli GET hello
```

The first PING may need a short startup wait. A client installed on the host
can also use `redis-cli -h 127.0.0.1 -p 6379`. On macOS, use the published
localhost port; Docker bridge addresses are inside the Docker VM.

The entrypoint preallocates a 1 GiB `/data/lavik.data` on first start and uses
two workers. Set `-e LAVIK_DATA_SIZE=4G` and `-e LAVIK_THREADS=4` before the
image name to change those defaults. Storage and file logs persist in the
named volume. Existing files are never resized or truncated, even when
`LAVIK_DATA_SIZE` changes. Do not attach one data volume to concurrent servers.

Restart with `docker restart -t 60 lavik`. To replace the container, stop it
with `docker stop -t 60 lavik`, remove the stopped container, and run the same
command with the same volume. Removing a container does not remove its named
volume. Removing the volume deletes the database.

Additional server flags follow the image name, for example `--threads 4` or
`--requirepass ...`. Explicit long options override corresponding container
defaults. To use a Redis-style configuration file, mount it and run
`... eloqdata/lavik:0.1.0-beta.1 lavik /etc/lavik/lavik.conf`; that configuration
owns all settings, including the listener and preallocated storage. Supplying
`--data-file` also delegates storage provisioning to the operator. Bind mounts
must already be writable by UID 10001; named volumes are initialized with the
correct ownership. `LAVIK_DATA_FILE` changes the autoallocated path and should
point inside a persistent mount.

## Primary and follower with Compose

From a checkout of this repository:

```bash
cd deploy/docker
docker compose pull
docker compose up -d
docker compose logs -f bootstrap
```

Wait for `Cluster is READY (one primary, one follower).` and a successful
bootstrap exit. There are **two Data nodes and three Meta voters**. All 16,384
slots initially belong to one primary; the follower receives native Lavik
replication. Meta owns the roles, and they can change after failover. The
Compose service names describe their initial roles.

```bash
docker compose exec meta-1 lavik-ctl cluster-status \
  --socket /data/meta/meta-admin.sock --allow-plaintext-admin --json
docker compose exec primary redis-cli -c SET hello lavik
docker compose exec primary redis-cli -c GET hello

# Read from the follower on a single connection, after replication catches up.
printf 'READONLY\nGET hello\n' | docker compose exec -T follower redis-cli --raw
```

Host ports are `127.0.0.1:6379` for the initial primary and
`127.0.0.1:6380` for the initial follower. If those ports are occupied, set
`LAVIK_PRIMARY_PORT=16779 LAVIK_FOLLOWER_PORT=16780 docker compose up -d`.
The bridge subnet `172.30.92.0/24` must be unused. To change it, update both
`compose.yaml` and `cluster.toml` consistently **before the first startup**.
Deploy only one copy of this fixed-subnet example on a Docker daemon at a time.

The cluster advertises Docker-internal addresses, so cluster-aware clients
that follow `MOVED` redirects must run on the Compose network. The
`docker compose exec ... redis-cli -c` commands above do this. Host clients
can issue direct commands through the published ports, but cannot follow
those internal redirects on macOS. A production deployment needs addresses
reachable by all clients and nodes. This local example binds published ports
to loopback, leaves Meta ports unpublished, and uses plaintext inside its
Docker network. Configure TLS/authentication before exposing it remotely.

Controlled failover through the bundled release client:

```bash
docker compose exec meta-1 lavik-ctl failover group-1 \
  --socket /data/meta/meta-admin.sock --allow-plaintext-admin \
  --failover-timeout-ms 60000
docker compose exec meta-1 lavik-ctl cluster-status \
  --socket /data/meta/meta-admin.sock --allow-plaintext-admin --json
docker compose exec primary redis-cli -c GET hello
```

Each Data/Meta service has a separate named volume. `docker compose down`
stops/removes containers and the network while preserving those volumes;
`docker compose up -d` recovers them. Meta uses its persisted configuration
on restart. Bootstrap creates the cluster only when Meta reports
`uninitialized`, then waits for readiness. It refuses non-pristine/failed
state and never automatically replays an uncertain create within a run.
An existing cluster recovers its durable topology without reapplying initial
role assignments from the manifest. Automatic failover can change the owner
during recovery; use `cluster-status` to discover the current primary. Inspect
that status and logs after a bootstrap failure. Per-container health checks
indicate process availability;
`cluster-status` is the cluster readiness check.

For an intentional **complete reset**, `docker compose down --volumes`
deletes all five database volumes. Do not use it to restart or upgrade.

## Build locally

Run from the repository root. One multi-stage Dockerfile exposes both targets;
the default target is `single`. No source compilation or submodule checkout
is required: the build downloads the official release and verifies the
architecture-specific SHA-256 in `release.sha256`, VERSION, and REVISION.

```bash
docker buildx build --target single --load \
  -t eloqdata/lavik:0.1.0-beta.1 deploy/docker
docker buildx build --target cluster --load \
  -t eloqdata/lavik:0.1.0-beta.1-cluster deploy/docker
```

For both platforms, add `--platform linux/amd64,linux/arm64` to each command.
Loading a multi-platform image requires Docker's containerd image store
(enabled by default in current Docker Desktop). The narrow build context
contains only packaging inputs. The Ubuntu base digest and Lavik artifacts
are pinned; Ubuntu runtime packages are resolved when the image is built,
so later uncached builds may have different image digests.

## Verify and publish

The smoke test needs Python 3.8+, Docker, Compose, and the built images. It
uses random host ports and private project/volume names, verifies standalone
recovery, follower replication, controlled failover, and full cluster recovery,
then removes only its own containers and volumes. The fixed example subnet
must be free. Run natively for each target platform when possible:

```bash
python3 deploy/docker/smoke-test.py
# Select AMD64 explicitly on a native AMD64 Docker host:
python3 deploy/docker/smoke-test.py --platform linux/amd64
```

On an ARM Mac, AMD64 emulation may start the binaries but reject
`io_uring_setup` with `ENOSYS` (not implemented). Run the ARM64 images natively
there; validate the AMD64 storage engine on a native AMD64 Linux host.

After building **both platforms** and testing, publish the loaded multi-platform
images using a Docker Hub account with write access to `eloqdata`:

```bash
docker login
docker push eloqdata/lavik:0.1.0-beta.1
docker push eloqdata/lavik:0.1.0-beta.1-cluster
docker buildx imagetools inspect eloqdata/lavik:0.1.0-beta.1
docker buildx imagetools inspect eloqdata/lavik:0.1.0-beta.1-cluster
```

Check that both `linux/amd64` and `linux/arm64` appear. Preserve published
release tags; use a new version for changed contents. No `latest` tag is
assigned to this prerelease.
