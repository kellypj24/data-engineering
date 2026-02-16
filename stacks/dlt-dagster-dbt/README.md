# dlt + Dagster + dbt Stack

A lightweight alternative to the Airbyte-based stack, using dlt (data load tool) for extract and load.

## When to Use This Stack

Choose this over `airbyte-dagster-dbt` when:

- You want a **simpler setup** with no separate Airbyte instance to manage.
- You prefer a **code-first approach** to EL — dlt pipelines are pure Python.
- Your sources are custom APIs or databases where writing a short Python pipeline is easier than configuring a connector in a UI.
- You want everything to run **in a single process** without additional infrastructure.

## Architecture Overview

```
Sources ──> dlt (EL, Python) ──> Warehouse ──> dbt (Transform) ──> Marts
                  ^                                  ^
                  |                                  |
               Dagster (Orchestration) ──────────────┘
```

- **dlt** is embedded directly into Dagster assets as Python code. No separate service to run.
- **Dagster** orchestrates dlt pipelines and dbt runs, providing scheduling and monitoring.
- **dbt** transforms raw data into clean models, same as in the primary stack.

## Setup

1. Install Python dependencies: `pip install dlt dagster dagster-dbt dbt-core`
2. Define dlt pipelines under `../../extract_load/dlt/`
3. Wire them into Dagster assets under `../../orchestration/dagster/`
4. Configure dbt models under `../../transformation/dbt/`
5. Use the Dagster docker-compose or run `dagster dev` locally

## Related Documentation

- [dlt EL](../../extract_load/dlt/README.md)
- [Dagster orchestration](../../orchestration/dagster/README.md)
- [dbt transformation](../../transformation/dbt/README.md)
