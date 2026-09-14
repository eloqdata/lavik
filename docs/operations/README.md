# Operations

This directory contains procedural guidance for building, packaging,
provisioning, observing, and tuning Keylane. Read the runbook for the affected
operational branch before changing its scripts, configuration, prerequisites,
or safety boundaries.

## Runbooks

| Runbook | Use it when |
|---|---|
| [Building and packaging](building-and-packaging.md) | Building locally, producing release artifacts, or changing package contents |
| [Multi-device storage](multi-device-storage.md) | Provisioning storage paths, expanding a storage set, or diagnosing membership and capacity constraints |
| [Tomb Raider scheduling](tomb-raider.md) | Configuring or operating tombstone cleanup schedules |
| [Active expiration tuning](active-expiration.md) | Inspecting or changing TTL scan and index-maintenance pacing |
| [Prometheus metrics](metrics.md) | Integrating metrics, interpreting exported values, or changing monitoring behavior |
| [Meta control plane](meta-control-plane.md) | Creating the first multi-Group cluster, starting plaintext or mTLS Meta clusters, changing membership, replacing binaries, handling snapshot/WAL incidents, and exporting audit or operation archives |
| [Network IRQ affinity tuning](irq-affinity-tuning.md) | Measuring or changing host IRQ placement for latency tuning |
| [Monitoring stack](../../deploy/monitoring/README.md) | Running the repository's Prometheus and Grafana deployment |

## Maintenance

Keep commands, prerequisites, paths, expected results, and rollback or safety
notes synchronized with the scripts and configuration they describe. Current
source and executable behavior take precedence when a runbook disagrees.
