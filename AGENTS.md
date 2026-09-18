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

<!-- BEGIN bootstrap-project: engineering-standards -->
## Engineering standards

- Explain non-obvious intent, invariants, ownership, failure behavior, compatibility constraints, and performance or safety tradeoffs near the affected code. Do not restate syntax or names.
- Add or update documentation comments for public APIs when the language supports them.
- Correct stale nearby comments while changing behavior.
- Update the relevant architecture document in the same change when module boundaries, core flows, durable formats, lifecycle, or external integrations change.
- When a change introduces, removes, splits, or merges a durable module, update the architecture taxonomy, the relevant focused documents, and the architecture index (`docs/architecture/README.md` by default, or its existing equivalent) in the same change.
- Keep the overview focused on system context, high-level flows, cross-cutting invariants, and navigation. A repository with at most one durable module may keep readable architecture detail in its overview. When multiple durable modules emerge or detail needs independent navigation, use focused documents and update the architecture index (`docs/architecture/README.md` by default, or its existing equivalent).
- Treat `docs/design-docs/`, `docs/plans/`, and `docs/superpowers/` as historical reference material. Use source code, tests, and `docs/architecture/` to determine current behavior.
- Keep `docs/architecture/` synchronized with current module boundaries, flows, formats, lifecycles, and integrations. Design history is updated only when a task explicitly asks to revise that history.
- Keep one-off source audits and task-specific investigations in task, issue, or pull-request context rather than repository documentation.
- Before unfamiliar work, read `docs/README.md` and the relevant architecture documents. Consult design documents only when historical rationale or prior alternatives are relevant.
- When code and documentation disagree, treat code as authoritative and repair the documentation in the same change.
- Avoid introducing process-wide or cross-worker mutexes that can block worker threads, especially on request paths. Prefer worker-local ownership, immutable snapshots, message passing, or narrowly scoped atomics where they preserve the required correctness and lifetime guarantees. When a shared blocking mutex is necessary, document why the alternatives are unsuitable and, if workers can contend on it, measure its effect on throughput and tail latency under relevant concurrency.
<!-- END bootstrap-project: engineering-standards -->

## Repository documentation

- Operations: before changing build or packaging, metrics and monitoring,
  storage provisioning or maintenance, or IRQ tuning, read the
  [operations index](docs/operations/README.md) and the relevant linked guide.

## Architecture documentation

- Architecture updates are claim-driven. For an implementation change,
  identify the current architectural claim or core model that the change makes
  false or materially incomplete; leave architecture unchanged when there is
  none. Dedicated documentation work may correct inaccuracies, fill a known
  core-design gap, or consolidate existing sediment.
- Keep architecture as a compact, present-tense model of Lavik's core module
  boundaries, control and data flows, ownership and lifecycles, durable or wire
  formats, external integrations, and system-level correctness, safety, and
  compatibility invariants.
- Preserve stable design rationale and tradeoffs needed to understand that
  model. Put change-specific motivation and before/after explanation in the
  pull request or commit, and keep local algorithms, representation details,
  and performance mechanics near the affected code.
- Revise and consolidate existing prose so the result stands on its own without
  knowledge of the change that produced it. Use the authoring standard and
  examples in [the architecture index](docs/architecture/README.md).

## Agent skills

### Issue tracker

Issues and specs are tracked in this repository's GitHub Issues. See `docs/agents/issue-tracker.md`.

### Triage labels

Triage uses the five canonical label names unchanged. See `docs/agents/triage-labels.md`.

### Domain docs

This repository uses a single-context domain documentation layout. See `docs/agents/domain.md`.
