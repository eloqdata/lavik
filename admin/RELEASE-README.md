<!-- Copyright (C) 2026 EloqData Inc. SPDX-License-Identifier: Apache-2.0 -->

# Start with Lavik Admin

This archive includes Lavik Admin, `lavik`, `lavik-meta`, and `lavik-ctl`.
On Linux, the Admin launcher uses the included Node.js runtime. On macOS,
install Node.js 24.15 or newer; the same launcher runs Admin locally and
installs Linux binaries on your hosts over SSH. No npm installation is needed.

From the extracted directory:

```sh
./lavik-admin
```

Open http://localhost:4173 and sign in with the access token printed in your
terminal. Keep this terminal running. The private workspace defaults to
`~/.local/share/lavik-admin`; set `LAVIK_ADMIN_DATA` to use a different directory.
Stopping Admin does not stop your clusters.

## Prepare hosts

Use existing AMD64 or ARM64 Linux hosts with Python 3, glibc 2.39 or newer
(Ubuntu 24.04), Linux 6.1 or newer, and io_uring enabled. Admin needs outbound
HTTPS to GitHub and node hosts need SSH access from the Admin computer.
Cluster IPs and the ports shown at review must be reachable between hosts on
a trusted private network. Provisioning uses kernel networking, with io_uring file storage or SPDK.

In **Create cluster → Set up machines → Prepare hosts**, paste the host list, set the SSH user,
and provide a login password or a private-key path on the Admin computer
(optionally its passphrase). Admin installs its own public key and verifies a
fresh passwordless login. Use separate batches for different credentials.
The initial password/passphrase is not saved. First connections trust and
save the host's SSH key; changed keys stop the connection. Prepared hosts
remain available for additional clusters and followers. **Admin SSH access
verified** refers to Admin's workspace key. To log in from your terminal with
that key, use `ssh -i /PATH/TO/ADMIN_WORKSPACE/ssh/identity/id_ed25519 USER@HOST`
(and `-p PORT` if needed). A plain `ssh USER@HOST` uses your usual SSH identities.

For the default systemd user services, run this once on each host as its SSH
user, then reconnect:

```sh
sudo loginctl enable-linger "$USER"
```

Admin does not enable passwordless sudo or change the host's SSH policy.
Use an existing key when the host does not allow password login.

## Create and manage

1. **Prepare hosts:** enter the initial login, click **Prepare hosts**, select
   hosts with verified access, and **Continue to node placement**.
2. **Node placement:** name the cluster and choose `nightly` or a GitHub release.
   Confirm private cluster IPs, then map each Meta voter/primary/follower to a
   prepared host. Defaults give one primary, two followers, and three voters.
3. Use three separate hosts for host-level redundancy. Advanced settings
   expose workers, storage size, ports, and client mode. **Service lifecycle**
   is visible in Node placement; use **Development processes** for Docker labs.
4. **Check hosts & review**, resolve any prerequisites, inspect ports, type the
   cluster name, and deploy. **Operations** tracks progress to readiness.
5. Use **Dashboard**, **Topology**, and **Send command**.
   Repeat setup to create additional independent clusters.

In **Topology**, **Add replica → Deploy on a host** installs the cluster's
pinned release and waits for the new follower to synchronize. You can also
attach an already-running node. **Remove** unassigns a follower and retains
its process and files. Stop its service separately when no longer needed.
Older releases without safe membership APIs support setup and monitoring;
Admin disables resizing and controlled failover for them.

Admin installs under `~/.local/share/lavik/clusters/CLUSTER` on each host by
default. It refuses existing unowned directories and never overwrites a data
file. Service names follow `lavik-CLUSTER-NODE.service`; inspect them with
`systemctl --user status` and `journalctl --user -u SERVICE`. The
**Development processes** lifecycle option is for lab containers and does not restart
nodes after crashes or reboot.

## Share the workspace with the CLI

Leave Admin running and open another terminal in this directory:

```sh
./lavik-admin ctl fleet-hosts
./lavik-admin ctl fleet-list
./lavik-admin ctl fleet-status CLUSTER
./lavik-admin ctl fleet-operations CLUSTER
```

On Linux, `lavik-ctl --socket ~/.local/share/lavik-admin/admin.sock fleet-list`
uses the same catalog and operation records. The launcher helper also works
on macOS, where the Linux `lavik-ctl` executable itself cannot run.

An interrupted operation may be **Uncertain**. Inspect its details before
using **Retry original request**; Admin preserves its identity and never
blindly repeats cluster initialization. Back up the entire private Admin
workspace while Admin is stopped, including its `ssh/` identity/trust files
and retained release cache, plus
each cluster's separate Meta and Data files.

For connection profiles, automation, recovery, and Docker verification, see
[the operations guide](https://github.com/eloqdata/lavik/blob/main/docs/operations/lavik-admin.md).


## Storage, monitoring, and existing clusters

Production node placement offers file storage or automatic SPDK preparation of
explicitly selected dedicated NVMe controllers. SPDK requires root SSH, IOMMU
isolation, fresh single-namespace media, and one Data node per physical host.
Host checks report unsupported configurations before deployment; the review
requires explicit controller confirmation. Follow the repository’s
[SPDK setup instructions](https://github.com/eloqdata/lavik/blob/main/docs/operations/lavik-admin.md#spdk-on-dedicated-data-hosts)
for prerequisites and resource ownership.

Select prepared monitoring hosts to provision Prometheus and Grafana with the
Lavik dashboard. They need Docker Engine, Compose v2, and image registry access
or cached images. Grafana defaults to the private IP at port 3000; its generated
password is in `monitoring/grafana-password` under the remote deployment
folder. Prometheus defaults to localhost:9090. Monitoring ports are configurable.

For a cluster created with `lavik-ctl`, choose **Connect existing cluster**,
enter Meta Admin addresses, and click **Test connection**. Review the discovered
state, then **Connect cluster**. This flow leaves cluster state and storage intact.
