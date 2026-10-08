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

# Local demo from Lavik Admin

Extract a Lavik release and run its launcher:

```sh
./lavik-admin
```

On Linux the archive includes Node.js; on macOS install Node.js 24.15+.
From a source checkout the equivalent launcher is `./admin/lavik-admin`.
A source checkout is not needed when using a release.

Open the printed URL (normally http://localhost:4173), sign in with the printed
token, and choose **Create cluster → Try a demo cluster**.

## What the button does

1. Checks the local Docker Engine and Compose v2.
2. Prepares a small Ubuntu runtime image with OS tools, without compiling Lavik.
3. Installs the Linux executables included in the tarball when they match Docker’s
   architecture. From a checkout or a different-architecture package, it downloads
   the matching nightly minimal tarball and checks the published SHA-256 instead.
   Runtime compatibility is checked in Docker; later runs retain the same bytes.
4. Starts **three Meta voters and three Data nodes**: one primary and two replicas.
5. Creates `demo-cluster` through its release's client and connects it to the
   running Admin. Click **Open demo dashboard** when ready.

The button does not start an Admin container. All setup assets ship in the release;
there is no manifest to paste, command to copy, source tree to build, or npm install.
The progress page shows the current step, a bounded setup log, and actionable
errors. You can return through **Demo setup progress** on the cluster card.
Closing the browser leaves setup running in Admin.

## Requirements and boundaries

Start Docker Desktop on macOS, or Docker Engine with Compose v2 on Linux, before
clicking the button. The Docker engine must run Linux containers with kernel 6.1+
and io_uring enabled. Allow at least 3 GiB for the data files plus runtime/release
storage. The Admin process needs permission to run the Docker CLI. Run the
launcher on the Docker host; do not mount a Docker socket into an Admin container.
Remote Docker contexts are rejected.

Each Admin workspace owns a randomly named Compose project, six nodes, a release
volume and a Docker-allocated private network. It does not reuse the legacy
quick-start network or ports. All nodes run on one machine; this is a learning
environment without host-failure redundancy. Meta/Data use plaintext on that
isolated network, and node containers enable `seccomp=unconfined` for io_uring.

Admin runs the Linux client and bounded RESP requests through `docker exec`.
The dashboard and **Send command** work from a Mac without routing into Docker's
private IPs. The demo does not publish Data or Meta ports on the host. Use Admin's
console to try `SET greeting "hello from Lavik"`, then `GET greeting`.

## Retry, restart, and removal

Setup stores its job and Docker ownership in the private Admin workspace. Runtime
assets are copied under `demos/OWNER` there so moving the extracted release does
not break Docker bind mounts. Keep that workspace and the Docker volumes together.
Stopping Admin leaves the node containers running. Restart the same launcher with
the same `LAVIK_ADMIN_DATA` workspace to manage them again.

After a download or Docker error, fix the reported prerequisite and click **Retry
demo setup**. Later runs retain the original release and stored data. A completed
demo can be restarted by choosing **Try a demo cluster** again. Once initialization
has been submitted, retries only observe Meta; they never replay uncertain Genesis.
If Meta state was lost after successful initialization, restore it or explicitly
tear down the demo before creating a new one.

**Remove cluster → Permanently tear down deployment** deletes this demo's node
containers, Meta/Data volumes, release volume and private network. Type the cluster
name and confirm data deletion. This Admin and other Docker projects stay running.
The Docker runtime image may remain cached. **Remove from Admin only** retains the
containers/data and archives ownership; it does not reconnect or adopt them
implicitly into a new demo.

## Download connection failures

TLS handshake failures and connection resets are retried up to three times.
Staging and checksum verification prevent failed transfers from publishing a
release. For persistent `curl: (35) ... SSL_ERROR_SYSCALL`, check Docker Desktop's
proxy/VPN settings and HTTPS access to `github.com` and
`release-assets.githubusercontent.com`, then retry from the browser. TLS and
checksum verification remain enabled.

## Legacy all-in-Docker quick start

The checkout's `./admin/quickstart/setup.sh` remains available for the older flow
that starts seven containers, including its own Admin. It is independent of the
browser-created demo and is not used by the button. Its default Admin port is
4173; set `LAVIK_QUICKSTART_PORT=4183` if needed. It retains its original fixed
network `172.29.91.0/24` and Data ports 16379–16381.

For that legacy project only, run `./admin/quickstart/remove.sh` on the Docker host
and type its project name to delete its containers and volumes, including its
Admin catalog and token. Use the same `COMPOSE_PROJECT_NAME` if you selected one.
The browser-created demo uses a separate project and is removed from Admin.
