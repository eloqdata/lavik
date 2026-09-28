<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->

# Lavik Admin 101: create a cluster from your Mac

This walkthrough starts with three empty Ubuntu Docker containers, prepares
passwordless SSH, and uses **Create cluster** to install and initialize Lavik.
Admin runs on your Mac. The containers initially run only SSH, not Lavik.
You do not need to compile Lavik or initialize Git submodules.

Allow roughly 15 minutes, plus the first image/release downloads. This is a
local lab using development-process supervision; it is not a production
systemd or host-failure test.

## 1. Understand hosts versus nodes

A **host** is a machine/container you can SSH into. A **node** is a Lavik Data
or Meta process. One host can run both kinds of node.

Your requested cluster has **six processes**: three Meta voters, one primary,
and two followers. This walkthrough places them on **three hosts**:

| Docker host | SSH from the Mac | Cluster IP | Meta process | Data process |
|---|---|---|---|---|
| `lavik101-host-1` | `127.0.0.1:2211` | `172.30.95.11` | `meta-1` | `data-1`: initial primary |
| `lavik101-host-2` | `127.0.0.1:2212` | `172.30.95.12` | `meta-2` | `data-2`: follower |
| `lavik101-host-3` | `127.0.0.1:2213` | `172.30.95.13` | `meta-3` | `data-3`: follower |

The wizard first prepares SSH access, then shows a proposed mapping in **Node
placement**. Each row has a host selector. Match hosts by SSH port or cluster
IP: the UI’s host numbers reflect your selection order, not container names.
Meta leadership is elected; `meta-1` does not mean “permanent Meta leader.”
The Data primary can also change after a failover.

All three containers run inside one Docker Desktop VM. This demonstrates
placement and management, not physical-machine redundancy.

## 2. Check your Mac and choose an isolated lab directory

Install/start Docker Desktop, and have Node.js **24.15 or newer** and OpenSSH
available. Run these commands from the Lavik checkout that contains the Admin
SSH setup wizard:

```sh
docker version
node --version
ssh -V
```

If you need a checkout, clone `https://github.com/eloqdata/lavik.git` and enter
its directory first. This guide uses the source launcher
`./admin/lavik-admin`; it does not depend on an older downloaded release
already containing Admin. Future bundled releases use `./lavik-admin`.

Give Docker Desktop sufficient capacity for three Data processes and Meta.
For this lab, 4 CPUs, 8 GiB RAM, and at least 6 GiB free Docker disk space are
reasonable starting settings. We reduce each Data file to 1 GiB below.

Keep the same terminal for steps 2–4 so it retains these variables:

```sh
export LAVIK_LAB="$PWD/.lavik-admin/101"
mkdir -p "$PWD/.lavik-admin"
(umask 077; mkdir "$LAVIK_LAB")
export LAVIK_ADMIN_DATA="$LAVIK_LAB/workspace"
export LAVIK_ADMIN_PORT=4175
```

If `101` already exists, stop and use a new lab directory or follow cleanup
at the end. Do not overwrite a previous lab's SSH key or workspace. The
`.lavik-admin/` directory is ignored by Git. Port **4175** keeps this exercise
separate from an existing Admin on 4173 or 4174.

## 3. Start three empty SSH hosts with Docker

Create a disposable lab password file. This password is only for the local
containers; you will enter it once in Admin to prepare all three hosts:

```sh
(umask 077; printf '%s' 'lavik-local-lab' > "$LAVIK_LAB/password")
docker build -f admin/test/ssh/Dockerfile -t lavik-admin-ssh-test:local .
docker network create --subnet 172.30.95.0/24 lavik101
```

Start the three hosts:

```sh
for i in 1 2 3; do
  docker run -d --name "lavik101-host-$i" \
    --security-opt seccomp=unconfined \
    --network lavik101 --ip "172.30.95.$((10 + i))" \
    -p "127.0.0.1:$((2210 + i)):22" \
    -p "127.0.0.1:$((17378 + i)):6379" \
    --mount "type=bind,src=$LAVIK_LAB/password,dst=/fixture-password,readonly" \
    lavik-admin-ssh-test:local
done
```

`seccomp=unconfined` is for these disposable lab containers so Linux io_uring
is available. Admin itself does not need it. The image creates SSH user
**lavik** with password **lavik-local-lab**. There are no authorized SSH keys
yet; Admin installs its own key in the browser workflow below.

The `-p` options publish SSH and one Data port per container **on Mac loopback
only**. Each container's first Data node listens on **6379**; the Mac ports
are **17379**, **17380**, and **17381**. Admin increments the Data port only
when placing additional Data nodes on the same cluster IP. The fixed container
port therefore works even if you place the primary on a different host.
Use the updated Admin and inspect the ports at review before deploying.
If Admin is already running, restart it to load the updated planner.
Existing clusters retain their original ports; see the existing-lab note below.
Meta's three kinds of port stay inside the Docker network. If Docker
reports a subnet overlap or occupied port, resolve it before continuing and
update the corresponding addresses/ports in the form; do not reuse a different
application's containers or network.

## 4. Start Lavik Admin on the Mac

```sh
./admin/lavik-admin
```

Leave that terminal running. Open **http://localhost:4175**, and sign in with
the token printed at startup. This workspace should have no clusters yet.
Do not use the older Admin service at a different port for this exercise.

## 5. Prepare all three hosts in the browser

1. Choose **Create cluster** (or **Create your first cluster**).
   The first page is **Prepare hosts**. No node assignment is needed yet.
2. Paste this into **Host list**:

   ```text
   127.0.0.1:2211 172.30.95.11
   127.0.0.1:2212 172.30.95.12
   127.0.0.1:2213 172.30.95.13
   ```

3. Set **SSH user** to `lavik`, keep **Connect using = Password**, and enter
   **SSH password = lavik-local-lab**.
4. Click **Prepare hosts**. Each host should report **Admin SSH access verified**.
   Admin creates a private workspace key, installs its public key on each host,
   saves the first-seen SSH host key, and checks a fresh key-only login.
   The password field is cleared and the password is not saved by Admin.
5. All three prepared hosts are selected. Click **Continue to node placement**.
   Admin rechecks access before opening the next page.

You do not run `ssh-keygen`, `ssh-copy-id`, or manually create known-hosts
entries. A changed host key is rejected. For real hosts that already use SSH
keys, choose **Private key / SSH agent**, enter the key's path on the computer
running Admin, and optionally its passphrase. Different credentials can be
prepared in separate batches. Prepared hosts remain available for other
clusters and follower additions.

## 6. Configure the cluster and map nodes to hosts

1. Set **Cluster name** to `mac101` and **Lavik release** to `nightly`.
   The release field links to the
   [official releases page](https://github.com/eloqdata/lavik/releases).
2. The three **Prepared hosts** show their verified SSH endpoints. Check the
   cluster IPs are `172.30.95.11`, `.12`, and `.13`, respectively. These were
   filled from the second column of the host list. **Back to host preparation**
   lets you change the selection.
3. Keep **Primary groups = 1** and **Followers per primary = 2**. Directly
   below Cluster size, set **Service lifecycle** to **Development processes
   (no automatic restart)**. This selector is outside the advanced settings.
4. Expand **Tune advanced settings** and use:

   | Setting | Lab value |
   |---|---|
   | Meta voters | `3` |
   | Workers per Data node | `1` |
   | Storage GiB per Data node | `1` |
   | Storage directory | `.local/share/lavik/clusters` |
   | Data / Raft / Admin / Control base ports | `6379` / `7100` / `7200` / `7300` |
   | Client mode | `Redis Cluster` |

   These containers do not run systemd; do not use `loginctl` in this lab.

5. In **Node placement**, select the endpoints below to follow this walkthrough
   exactly. Check the address in each selector rather than its “Host N” label:

   | Node | Role | Selected SSH endpoint / cluster IP |
   |---|---|---|
   | `meta-1` | Meta voter | `127.0.0.1:2211` / `172.30.95.11` |
   | `meta-2` | Meta voter | `127.0.0.1:2212` / `172.30.95.12` |
   | `meta-3` | Meta voter | `127.0.0.1:2213` / `172.30.95.13` |
   | `data-1` | group-1 · Primary | `127.0.0.1:2211` / `172.30.95.11` |
   | `data-2` | group-1 · Follower | `127.0.0.1:2212` / `172.30.95.12` |
   | `data-3` | group-1 · Follower | `127.0.0.1:2213` / `172.30.95.13` |

   You can choose a different placement. With one Data node per host, all
   three still use 6379. If you move the primary, use its host's published Mac
   port in step 8. **Spread evenly across hosts** uses the current host-list
   order; it does not guarantee the container-name mapping above. Changing
   cluster size or selected hosts resets placement. Editing a reviewed
   configuration invalidates its review, so run the checks again.

The **SSH hostname** is `127.0.0.1` because SSH travels through a Mac-published
port. The **Cluster IP** is `172.30.95.x` because processes communicate inside
Docker. Using `127.0.0.1` as every Cluster IP would point each container at
itself and prevent the cluster from forming.

## 7. Check, review, and deploy

Click **Check hosts & review**. This checks SSH, Linux/libc/io_uring, writable
storage, free disk, and available ports. All three hosts should say **Ready**.
The development-process warning is expected for this lab.

For the explicit placement in step 6, verify the reviewed ports:

| Host | Meta ports (Raft, Admin, Data control) | Data port | Published Data port on Mac |
|---|---|---|---|
| `172.30.95.11` | `7100`, `7200`, `7300` | `6379` | `17379` |
| `172.30.95.12` | `7101`, `7201`, `7301` | `6379` | `17380` |
| `172.30.95.13` | `7102`, `7202`, `7302` | `6379` | `17381` |

Type **mac101** in the confirmation field and click **Deploy cluster**.
Admin downloads the checked release, installs it on the hosts, allocates fresh
files, starts Meta/Data, and submits cluster creation through `lavik-ctl`.
You do not paste a manifest or separately click Initialize cluster.

**Operations** shows progress. After the deployment completes, open
**Dashboard** and **Topology**: expect one group, one primary, two followers,
three Meta members, and a ready cluster. If an operation is uncertain, inspect
its detail and logs rather than creating another cluster over those files.

## 8. Verify data and the shared CLI

In **Send command**, execute:

```text
SET hello "from Lavik Admin"
GET hello
```

Confirm the write when prompted. The GET result should be `from Lavik Admin`.
In **Key browser**, search for `hello`. The dashboard should show Data metrics.

From another terminal on the Mac, query through a client inside the Docker
network, using `-c` so redirects still work after a primary change:

```sh
docker exec lavik101-host-1 redis-cli -c -h 172.30.95.11 -p 6379 GET hello
```

If `redis-cli` is installed on the Mac, the primary in the explicit placement
above is published at port 17379:

```sh
redis-cli -h 127.0.0.1 -p 17379 GET hello
```

Check **Topology** for the current primary before connecting. With the updated
container mappings, use this table:

| Primary endpoint inside Docker | Address for redis-cli on the Mac |
|---|---|
| `172.30.95.11:6379` | `127.0.0.1:17379` |
| `172.30.95.12:6379` | `127.0.0.1:17380` |
| `172.30.95.13:6379` | `127.0.0.1:17381` |

Do not connect from the Mac to `172.30.95.11` directly: that address is inside
Docker Desktop's VM. A Mac client using `-c` also cannot follow redirects to
those private addresses. Use the `docker exec ... redis-cli -c` command or
Admin's SSH-backed data tools for reliable access after failover.

From the checkout in that second terminal, use the same workspace:

```sh
export LAVIK_ADMIN_DATA="$PWD/.lavik-admin/101/workspace"
./admin/lavik-admin ctl fleet-hosts
./admin/lavik-admin ctl fleet-list
./admin/lavik-admin ctl fleet-status mac101
./admin/lavik-admin ctl fleet-operations mac101
```

The UI and CLI should show the same cluster and deployment operation.

### Existing labs created with the old port mappings

Older Admin plans assigned Data ports 6379, 6380, and 6381 by node number
across the whole cluster. Updating Admin does not change those running nodes
or Docker's published ports. Inspect **Topology** and `docker port CONTAINER`
before selecting a Mac endpoint. A host-list reorder may have placed the
6379 primary in a container publishing only 6381.

Keep an existing cluster running and use an SSH tunnel when its primary's
port is not published. For example, for primary `172.30.95.13:6379`:

```sh
# Use the running Admin's workspace; this guide sets LAVIK_ADMIN_DATA in step 2.
ssh-add "$LAVIK_ADMIN_DATA/ssh/identity/id_ed25519"
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:17390:172.30.95.13:6379 \
  -p 2213 lavik@127.0.0.1
```

Keep that terminal open; in another terminal run:

```sh
redis-cli -h 127.0.0.1 -p 17390
```

The tunnel targets that primary and must be updated after a primary change.
A container restart does not change its `-p` mappings. The corrected Docker
loop is for a fresh lab; these containers store cluster files in their writable
layers, so removing/recreating them would remove those files.

## 9. Try adding/removing a follower

For an easy first expansion, reuse host 3 with another Data port:

1. Open **Topology → Add replica → Deploy on a host** for `group-1`.
2. In **Available hosts**, select `lavik@127.0.0.1:2213` and click
   **Continue to node placement**. No password or key path is needed again.
   Keep its cluster IP `172.30.95.13`.
3. Use **Data port = 6390**; this avoids the existing `6379` process on host 3.
4. Check hosts, confirm `mac101`, and deploy. The follower inherits the
   original nightly binaries and 1 GiB storage setting.
5. Wait for the operation to complete and the new follower to show current
   population and health. Four Data nodes now belong to the group.
6. Choose **Remove** on the new follower to return to one primary/two followers.
   Removal changes membership; its process and files remain until you stop or
   clean up the lab.

The additional port does not need publishing to the Mac: other nodes and
Admin's SSH data tools can reach it inside Docker.

## If you want six separate hosts

Six hosts are optional. Prepare six SSH hosts using the same procedure and
paste all six into the host list. In **Node placement**, select:

| Node | Host |
|---|---|
| `meta-1`, `meta-2`, `meta-3` | Hosts 1, 2, 3 respectively |
| `data-1` (primary), `data-2`, `data-3` (followers) | Hosts 4, 5, 6 respectively |

For Docker, extend the `docker run` loop to `1 2 3 4 5 6`.
The additional SSH ports are `2214`–`2216`, and cluster IPs end in `.14`–`.16`.
Hosts 4–6 each have one Data node, so each uses 6379. The same Docker loop
publishes them at Mac ports `17382`–`17384`. For primary on host 4, use
`redis-cli -h 127.0.0.1 -p 17382` from the Mac, or a Docker-internal client:

```sh
docker exec lavik101-host-4 redis-cli -c -h 172.30.95.14 -p 6379 GET hello
```

## Troubleshooting and stopping

| Symptom | Check |
|---|---|
| `GitHub release lookup failed (403)` from an older Admin process | Restart Admin with the updated code. Discovery now reads `github.com/eloqdata/lavik/releases` and its checksum assets, without the REST API quota. Refreshing the browser alone does not restart the backend. |
| GitHub page/asset still returns 403 | Check that the Mac running Admin can open the public releases page and download its assets. A proxy/VPN policy can separately block these. |
| Browser cannot reach Admin | Keep the launcher running and check `curl --noproxy '*' http://127.0.0.1:4175/`. This Admin runs on the Mac, not in Docker. |
| Mac redis-cli cannot reach the primary | Match the live Topology endpoint to `docker port CONTAINER`. For an older lab with a mismatched mapping, use the SSH tunnel above. |
| SSH host-key verification fails | A recreated container has a new SSH identity. For a fresh lab, use a fresh workspace. For an intentional host replacement, verify its new fingerprint out of band before removing only that host’s entry from `workspace/ssh/known_hosts` and preparing it again. |
| Permission denied over SSH | Match user `lavik`, password `lavik-local-lab`, the password-file mount, and SSH port. Retry **Prepare hosts**; nothing is marked prepared until key-only access succeeds. |
| systemd/lingering check fails | Select **Development processes** for these containers. |
| io_uring check fails | Check Docker Desktop's Linux kernel and the container's `seccomp=unconfined` option. |
| Address/port unavailable | Use the private Cluster IP belonging to that container and an unused node port. |
| A deployment directory already exists | Resume/inspect its original operation. For a fresh experiment, use fresh containers and a fresh Admin workspace. |

Press Ctrl-C in the Admin terminal to stop Admin; this leaves cluster
processes running. Relaunch with the same `LAVIK_ADMIN_DATA` to return to your
workspace. In this lab, stopping/restarting a host container does **not**
automatically restart its Lavik processes; production uses systemd services.

To delete this disposable lab, stop Admin, then remove only these containers
and this network. Removing containers deletes their cluster data:

```sh
docker rm -f lavik101-host-1 lavik101-host-2 lavik101-host-3
docker network rm lavik101
```

If you created hosts 4–6, remove those too before removing the network.
Retain `.lavik-admin/101` for inspection, or delete that specific lab directory
when you no longer need its SSH keys, catalog, or release cache. Do not reuse
its catalog against recreated empty containers.
