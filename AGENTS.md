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
<!-- END bootstrap-project: engineering-standards -->

## Repository documentation

- Operations: before changing build or packaging, metrics and monitoring,
  storage provisioning or maintenance, or IRQ tuning, read the
  [operations index](docs/operations/README.md) and the relevant linked guide.
