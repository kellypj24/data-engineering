# Delivery Component Template

> **This is a specification, not a skeleton.** It states what a tool of this
> role must provide. Read it as a requirements checklist, then create the
> files. `delivery/file-export/` is the model of what "finished" looks like.

## What This Role Does

Delivery moves modeled data **out** of the warehouse to the people and systems
that consume it outside the company: files to recipients, extracts to
partners, feeds to downstream platforms. It is the outbound mirror of EL.

## What a New Delivery Tool Must Provide

- **Declarative config.** Each delivery is defined as data (one file per
  delivery), validated before anything runs, so a delivery is reviewable as a
  config change rather than bespoke code.
- **An orchestrator-free core.** Loader, query builder, and executor import no
  orchestrator. Orchestrator adapters (Dagster, Airflow, Prefect) wrap the
  core and live in their own modules or extras.
- **Non-writing modes.** A dry run that prints what would run and writes
  nothing, and a read-only mode that reports counts.
- **A run log.** One record per attempt, success or failure, which also stores
  incremental watermarks.
- **Fail-closed isolation.** When one source serves several recipients, prove
  before writing that a recipient's result holds only its own rows.
- **No partial delivery.** A failure midway leaves nothing half-written at the
  destination.
- **Generated schedules** that follow the toolkit's schedule invariants:
  default stopped, UTC, unique names.
- **Tests that need no network** and run against duckdb.
