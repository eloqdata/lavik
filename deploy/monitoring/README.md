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

# Lavik monitoring stack

This directory runs Prometheus and Grafana on any Docker host that can reach
the Lavik metrics endpoints. Data and Meta use separate jobs and dashboards.
Lavik itself does not run in this Compose project.

## Prerequisites

- Docker Engine with Docker Compose v2 (`docker compose version`).
- TCP connectivity from the monitoring host to every Lavik metrics port.
- Lavik started with a nonzero metrics port, for example:

  ```sh
  ./lavik --bind 10.0.0.11 --port 7379 --metrics-port 9100 \
    --data-file test2.bin
  ```

Before deploying, run this on the monitoring host and confirm that it returns
Prometheus text metrics:

```sh
curl http://10.0.0.11:9100/metrics
```

## Deploy

Copy the example configuration and edit it:

```sh
cd deploy/monitoring
cp .env.example .env
${EDITOR:-vi} .env
```

At minimum, set a reachable target. The default Grafana login is
`admin`/`admin`; replace the password for production deployments:

```dotenv
LAVIK_TARGETS=10.0.0.11:9100
LAVIK_META_TARGETS=10.0.0.21:9200,10.0.0.22:9200,10.0.0.23:9200
LAVIK_CLUSTER=production-a
GRAFANA_ADMIN_PASSWORD=admin
```

Validate and start the stack:

```sh
docker compose config
docker compose up -d
docker compose ps
```

Open the following URL in a browser, replacing the host and port if needed:

```text
http://MONITORING_HOST:3000/d/lavik-overview/lavik-overview
```

Log in with `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` from `.env`
(`admin`/`admin` by default). The Prometheus datasource and the
`Lavik Overview` dashboard are provisioned automatically.

Prometheus is bound to `127.0.0.1:9090` on the monitoring host by default. Its
target status is available locally at:

```text
http://127.0.0.1:9090/targets
```

## Add or remove Lavik nodes

Put every endpoint in the comma-separated `LAVIK_TARGETS` value:

```dotenv
LAVIK_TARGETS=10.0.0.11:9100,10.0.0.12:9100,10.0.0.13:9100
```

Apply the change:

```sh
docker compose up -d --force-recreate target-config
```

Prometheus file discovery notices the new target file within 30 seconds; it
does not need to restart. The Grafana dashboard has a multi-select
`Lavik instance` variable. It shows all nodes by default and can filter to
one or more nodes.

Use an address reachable from inside the Prometheus container. When Lavik is
on the same Linux Docker host, `host.docker.internal:9100` is supported by this
Compose file. Do not use `127.0.0.1:9100`, because that address would refer to
the Prometheus container itself.

## Operations

```sh
# Follow logs.
docker compose logs -f prometheus grafana

# Restart after a configuration change.
docker compose up -d

# Stop containers while retaining all stored data.
docker compose down
```

Prometheus and Grafana data are stored in named volumes. `docker compose down
-v` permanently deletes the collected metrics, Grafana database, and generated
target file.

The metrics endpoint has no authentication or TLS. Restrict every Lavik
metrics port so only monitoring hosts can connect. Grafana listens on all host
interfaces by default; protect its host port with a firewall and a strong admin
password. Set `GRAFANA_BIND_ADDRESS` to a specific private address when
appropriate.

## Alerts and target identity

Prometheus loads the plain YAML files in `prometheus/rules/`. They are maintained
defaults that operators can edit: expressions, thresholds, `for`, `keep_firing_for`,
severity and annotations. [The alert runbook](../../docs/operations/alerts.md)
explains every signal, threshold, recovery condition and coverage limitation.

Set target metadata before deployment:

```dotenv
LAVIK_CLUSTER=orders
LAVIK_MODE=meta-managed
GRAFANA_PUBLIC_URL=https://grafana.example.com
```

`LAVIK_MODE` accepts `standalone` and `meta-managed`; managed Single and Cluster
both use the latter. Legacy `LAVIK_TARGETS` defaults to standalone. Data-side
Meta-session rules require the managed label; standalone control gauges are not
faults. The independent Meta server rules are scoped by job `lavik-meta`.
Admin supplies managed labels and the deployed Grafana URL automatically.

For mixed clusters or modes, copy `prometheus/targets.example.json`, replace its
addresses and set `LAVIK_TARGETS_FILE` to the host file path. Each target group
must supply `cluster` and `lavik_mode` labels, and each endpoint must appear only
once. `LAVIK_TARGETS` is ignored in this mode. Recreate `target-config` after an
edit. Prometheus validates the discovery file; inspect Targets for any load error.

`up` is scrape reachability, `lavik_server_ready` is storage startup/runtime
health, and neither proves serving authority or complete FULL population
readiness. Target Status displays missing readiness as UNKNOWN.

The **Alerts** section sits immediately below the overview cards. Its compact
title bar shows Critical/Warning firing counts and a Pending count as badges to
the right of the heading, using the built-in Grafana Canvas panel. A
sortable/filterable current-alert table and an **Alert Activity** state timeline
follow below. The table includes
scope (worker/device/signal), active duration, and links to the relevant runbook
and metric panel. **Worker Memory** contains memory-admission rejections;
**Replication Pressure** contains only replication queues and backlog pressure.
Additional overview metrics remain available in their own collapsed row.

Counts and the table evaluate at the selected time range's end; the timeline
covers the whole range. They follow the instance filter, while `LavikNoTargets`
remains visible without an instance. Empty results mean no active samples, not
proof of health. See [dashboard semantics](../../docs/operations/alerts.md#dashboard)
for duration, missing-data and notification-state details.

## Choose notification delivery

Default startup evaluates rules without contacting an Alertmanager or requiring
notification credentials. Firing alerts remain visible in Prometheus and Grafana.
It does **not** deliver production notifications until a receiver is configured.

For a local Alertmanager, set this in `.env`:

```dotenv
COMPOSE_PROFILES=alertmanager
ALERTMANAGER_TARGETS=alertmanager:9093
```

Then run `docker compose up -d`. Its web endpoint defaults to the monitoring
host's loopback port 9093. Notification state and silences persist in the
`alertmanager-data` volume. The supplied configuration deliberately has empty
receivers. Copy `alertmanager/alertmanager.yml` to your own file, merge the
receivers from `alertmanager/receivers.example.yml`, and point
`ALERTMANAGER_CONFIG` at that file. Mount the referenced URL/credential files
with a local Compose override; do not commit them or place them in labels.
Templates in `alertmanager/templates/` can be used by text-capable integrations;
generic webhooks receive Alertmanager's standard JSON with alert annotations.

For an existing external Alertmanager, leave the local profile disabled and set
`ALERTMANAGER_TARGETS` to its reachable endpoints, separated by commas. Recreate
`target-config` after changing endpoints. Configure matching routes/inhibition in
that external service. For HTTPS/authentication, edit the native Prometheus
`alerting.alertmanagers` settings and mount credential files read-only. Simple
endpoint discovery defaults to HTTP and never accepts credentials in addresses.

The supplied routes distinguish warning and critical, group related alerts,
inhibit dependent alerts while a target is down, and suppress warning only for
the same critical condition and worker/device. Webhook examples send resolved
notifications. A resolved event means a condition ended, not proof of health;
check current telemetry. Use expiring Alertmanager silences for maintenance.

## Validate and reload user changes

Validate using the tools from the same pinned images as deployment:

```sh
docker compose run --rm --no-deps --entrypoint promtool prometheus \
  check rules /etc/prometheus/rules/recording.yml /etc/prometheus/rules/alerts.yml \
  /etc/prometheus/rules/meta-recording.yml /etc/prometheus/rules/meta-alerts.yml
docker compose run --rm --no-deps --entrypoint promtool prometheus \
  check config /etc/prometheus/prometheus.yml
docker compose run --rm --no-deps --entrypoint amtool alertmanager \
  check-config /etc/alertmanager/alertmanager.yml
```

After successful validation, use `docker compose kill -s SIGHUP prometheus` to
reload rules. Reload Alertmanager with `docker compose kill -s SIGHUP alertmanager`.
Neither requires restarting or recompiling Lavik. If replacing a directly
bind-mounted configuration file by atomic rename, first recreate the affected
container so its mount points to the new inode. Inspect configuration reload
success and rule evaluation errors in the service logs before relying on changes.

Changes to `.env` require `docker compose up -d` and target changes also require
`docker compose up -d --force-recreate target-config`. Keep your rule overrides
and receiver files backed up outside release archives. On upgrade, compare and
merge the shipped defaults with those files, validate, then reload. Admin retains
its existing ownership and identical-config retry checks: it refuses to overwrite
changed deployed assets. An existing Admin deployment requires an explicit
operator-reviewed configuration update before retries with a newer asset set.
Admin preserves custom notification/image settings in `.env` when updating targets.

## Automated local notification drill

From the repository root, install Python's `PyYAML` in your development environment,
then install the pinned, checksum-verified Prometheus/Alertmanager tools:

```sh
scripts/install_monitoring_tools.sh /tmp/lavik-monitoring-tools
python3 scripts/check_monitoring.py --tools /tmp/lavik-monitoring-tools
python3 -m unittest discover -s tests -p test_monitoring_config.py
python3 tests/monitoring/e2e.py --tools /tmp/lavik-monitoring-tools \
  --output /tmp/lavik-monitoring-drill --lavik-bin ./build_debug/lavik \
  --meta-bin ./build_debug/lavik-meta
```

Use a new output directory for each drill. The script starts isolated loopback
services and a recording HTTP receiver, drives real rule evaluation and validates
firing, warning/critical routing, deduplication, inhibition and resolved delivery.
It also pauses/resumes a temporary Data process and a voter in a real three-Meta
cluster to exercise scrape failure, reduced voter redundancy and recovery.
Only test timing is shortened; production rule expressions are reused and their
full timing is covered by promtool tests. It needs no production receiver secrets.
Omitting a binary flag skips that real-process portion; controllable-exporter
checks alone are not complete real-process acceptance. Logs, notification JSON and a result file
remain in the output directory for inspection; all started processes are stopped.

### Native replication and FULL recovery

The Overview's **Replication and FULL Recovery** row keeps backlog/publisher and
FULL queue pressure alongside native progress, pending work, state and phase
history, terminal durations, retries, failures and population readiness. Missing
or inapplicable cursors remain gaps. Rates use process counters; no panel computes
lag by subtracting source and target LSNs. Use the existing Admin INFO command for
peer/session/history detail and the most recent in-process FULL result.

Default editable rules warn after 5 minutes of connected pending work without
progress, become critical after 8 minutes, warn on at least 3 FULL failures in
15 minutes, and flag required population recovery with no progress for 15
minutes. Long FULL duration alone is not an alarm. FULL busy, authority waiting,
normal cancellation and disconnected flows are not confirmed replication stalls.
The existing TargetDown inhibition and receiver routes apply; severity inhibition
matches replication side, flow and step. See the [native replication runbook](../../docs/operations/alerts.md#native-replication-stalled).

The monitoring drill also verifies native stall, repeated FULL failure and
population recovery notifications from firing through resolved. Rule tests cover
idle, slow progress, missing snapshots, disconnects and counter resets. Deploy
the matching exporter with these rules: an older exporter lacks the observation
availability signal and triggers `LavikMetricsMissing` after five minutes.

## Meta / Raft dashboard

Start each Meta with `--metrics-port 9200 --metrics-bind <private-IP>` and add
its reachable address to `LAVIK_META_TARGETS`. An empty value supports a
Data-only deployment. `LAVIK_CLUSTER` is a stable identifier shared by both
jobs; use letters, digits, dots, dashes or underscores. After changing targets,
run `docker compose up -d --force-recreate target-config` to regenerate both
file-discovery lists (each file is replaced atomically). Verify `lavik-meta` as well as `lavik` under
Prometheus `/targets`.

Meta alerts use the same Alertmanager routing and are shipped in
`prometheus/rules/meta-*.yml`; see the [Meta alert policy](../../docs/operations/meta-monitoring.md#alert-policy).
An empty Meta target list is supported and does not raise an absent-target alert.

Open `/d/lavik-meta/lavik-meta` for **Lavik Meta / Raft**. It follows the existing
palette, stat cards and section layout, but contains only the Meta job. The
Data overview's Data → Meta row reports client-side control sessions. Select
one cluster: the six summary cards always cover that whole cluster and keep a
fixed size as nodes are added. Use **Node details** to filter the sortable,
paginated node table and the charts, or click a node in the table to focus it.
Summary values that require complete source coverage show **Unknown** when a
target or required source is unavailable. Down targets remain in the table
with unknown runtime fields; expired peer-applied values are absent.

Start with proposal outcomes, latency, in-flight work and replication/apply
lag. Storage/snapshot, transport, Data-control and exporter diagnostics are
collapsed initially. Raw term, leader hints, log indices and vote-response
counts remain available through Prometheus for specific investigations; they
are not dashboard health indicators. Consult the
[metric contracts](../../docs/operations/metrics.md#meta--raft-metrics) and
[fault runbook](../../docs/operations/meta-monitoring.md) before interpreting
leader hints, transient replication gaps or on-demand lane connections.
