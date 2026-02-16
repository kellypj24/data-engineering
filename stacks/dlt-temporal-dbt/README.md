# dlt + Temporal + dbt Stack

A Temporal-based alternative for pipelines that need durable execution and advanced workflow control.

## When to Use This Stack

Choose this when:

- You have **long-running pipelines** that may take hours and need reliable resumption on failure.
- You need **complex retry logic** — per-step retries, exponential backoff, or custom retry policies.
- Your pipelines involve **multi-step workflows** with branching, fan-out/fan-in, or human-in-the-loop approval gates.
- You want **visibility into workflow state** at a granular level beyond what a DAG scheduler provides.

## Architecture Overview

```
Sources ──> dlt (EL, Python) ──> Warehouse ──> dbt (Transform) ──> Marts
                  ^                                  ^
                  |                                  |
              Temporal (Orchestration) ───────────────┘
```

- **dlt** handles extract and load as Python code, invoked within Temporal activities.
- **Temporal** orchestrates the full pipeline as durable workflows. Each step (extract, load, transform) is an activity with its own retry policy and timeout.
- **dbt** transforms raw data, triggered as a Temporal activity after upstream loads complete.

## Setup

1. Install and run Temporal server (see `../../orchestration/temporal/`)
2. Define dlt pipelines under `../../extract_load/dlt/`
3. Define Temporal workflows and activities under `../../orchestration/temporal/`
4. Configure dbt models under `../../transformation/dbt/`
5. Start Temporal workers to execute workflows

## Related Documentation

- [dlt EL](../../extract_load/dlt/README.md)
- [Temporal orchestration](../../orchestration/temporal/README.md)
- [dbt transformation](../../transformation/dbt/README.md)
