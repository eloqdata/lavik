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

# Lavik Admin

Lavik Admin provides a shared fleet workspace for a browser and `lavik-ctl`.
The familiar dashboard, topology, command console, and activity
views use Lavik's own Meta and Redis interfaces. There are no production npm
dependencies; Node.js 24.15 or newer is required.

The workflow follows [Valkey Admin](https://valkey-admin.valkey.io/): connect
a cluster, see its health and topology, send commands, and inspect
slow activity. Lavik-specific controls add initialization, replica sizing,
controlled primary switching, and shared operation progress.

See the [operator guide](../docs/operations/lavik-admin.md) for installation,
connection profiles, cluster creation, replica resizing, recovery, and Docker
verification. The [architecture](../docs/architecture/11-admin.md) explains
ownership and the shared database boundary.

Start the Admin already included in an extracted release:

```sh
./lavik-admin
```

From this checkout, use `./admin/lavik-admin`. Open the printed URL and choose
**Create cluster → Try a demo cluster**. With Docker Desktop or Docker Engine
and Compose v2 running on the same computer, the button uses the packaged Linux binaries (or downloads a verified matching
release), starts three Meta and three Data containers, creates
`demo-cluster`, and connects it to this Admin. Progress, errors and retry appear
in the browser. No source checkout, build tools, pasted manifest or second Admin
container is needed. Linux releases bundle Node; macOS needs Node.js 24.15+.
See the [local demo guide](quickstart/README.md) for prerequisites and recovery.

Production setup reviews SSH host preparation, node placement, optional SPDK
preparation, and monitoring. Existing connections are tested before saving.

From a source checkout with initialized submodules:

```sh
docker build -f admin/Dockerfile.toolchain -t lavik-admin-toolchain:local .
docker compose -f admin/compose.yaml up -d --build
docker compose -f admin/compose.yaml exec admin cat /data/lavik-admin/token
```

Open <http://localhost:4173> and enter that token. Connect the numeric IP and
Admin port of an existing Meta deployment. Every advertised Meta and Data
address must be reachable from the Admin container.

The same catalog is available through the bundled CLI:

```sh
docker compose -f admin/compose.yaml exec admin \
  lavik-ctl --socket /data/lavik-admin/admin.sock fleet-add production 10.0.0.11:7200
docker compose -f admin/compose.yaml exec admin \
  lavik-ctl --socket /data/lavik-admin/admin.sock fleet-list
```

Replica addition/removal changes redundancy within existing primary groups.
Primary-group expansion/shrink and online key redistribution are not
implemented. Hot-key tracking, `COMMANDLOG`, and per-key memory analysis also
require server capabilities beyond the current Lavik interfaces.

### Remove a cluster

Use **Remove cluster** on the fleet card or cluster navigation. **Remove from
Admin only** retains running services/data and archives completed history.
**Permanently tear down deployment** deletes owned file-storage deployments and
monitoring volumes after typed confirmation; interrupted teardown is resumable
from Operations. Machines and shared SSH access remain. Imported clusters and
SPDK deployments require manual teardown. Resolve active operations first.
The browser-created Docker demo supports permanent teardown in the same dialog;
its six nodes and volumes are removed while this Admin keeps running.
