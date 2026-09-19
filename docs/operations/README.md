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

# Operations

This directory contains procedural guidance for building, packaging,
provisioning, observing, and tuning Lavik. Read the runbook for the affected
operational branch before changing its scripts, configuration, prerequisites,
or safety boundaries.

## Runbooks

| Runbook | Use it when |
|---|---|
| [Quick startup tuning](quick-start-tuning.md) | Generate a CPU plan for the current host and start Lavik |
| [Cluster deployment](cluster-deployment.md) | Launch the local Meta-managed cluster example |
| [Building and packaging](building-and-packaging.md) | Building locally, producing release artifacts, or changing package contents |
| [SPDK storage](spdk-storage.md) | Run release packages on dedicated NVMe with kernel TCP, VFIO, and hugepages |
| [Multi-device storage](multi-device-storage.md) | Provisioning storage paths, expanding a storage set, or diagnosing membership and capacity constraints |
| [Tomb Raider scheduling](tomb-raider.md) | Configuring or operating tombstone cleanup schedules |
| [Active expiration tuning](active-expiration.md) | Inspecting or changing TTL scan and index-maintenance pacing |
| [Prometheus metrics](metrics.md) | Integrating metrics, interpreting exported values, or changing monitoring behavior |
| [Meta control plane](meta-control-plane.md) | Creating the first multi-Group cluster, running and diagnosing controlled failover, starting plaintext or mTLS Meta clusters, changing membership, replacing binaries, handling snapshot/WAL incidents, and exporting audit or operation archives |
| [Network IRQ affinity tuning](irq-affinity-tuning.md) | Measuring or changing host IRQ placement for latency tuning |
| [Monitoring stack](../../deploy/monitoring/README.md) | Running the repository's Prometheus and Grafana deployment |

## Maintenance

Keep commands, prerequisites, paths, expected results, and rollback or safety
notes synchronized with the scripts and configuration they describe. Current
source and executable behavior take precedence when a runbook disagrees.
