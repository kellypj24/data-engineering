# Dagster Orchestration Boilerplate

A composable data engineering toolkit built on [Dagster](https://dagster.io).
This project provides a production-ready scaffolding for orchestrating
Airbyte syncs, dbt transformations, S3 sensors, and Snowflake resources --
all wired together through Dagster's software-defined asset framework.

## Prerequisites

- Python 3.11+
- pip (or uv)
- A running Airbyte instance (if using Airbyte assets)
- A dbt project (if using dbt assets)
- AWS credentials configured (if using the S3 sensor)

## Quick Start

```bash
# From this directory:
pip install -e ".[dev]"

# Launch the Dagster webserver + daemon locally:
dagster dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

## Project Structure

```
.
├── dagster.yaml            # Instance config (storage backends)
├── workspace.yaml          # Code location definition
├── pyproject.toml          # Python packaging and Dagster module config
├── Dockerfile              # Production container image
├── README.md
└── src/
    ├── __init__.py          # Definitions entry point
    ├── assets/
    │   ├── __init__.py      # Re-exports all asset groups
    │   ├── airbyte.py       # Airbyte connection assets
    │   └── dbt.py           # dbt model assets
    ├── sensors/
    │   ├── __init__.py
    │   └── s3_sensor.py     # S3 file-arrival sensor
    ├── schedules/
    │   ├── __init__.py
    │   └── daily.py         # Daily materialisation schedule
    ├── jobs/
    │   ├── __init__.py
    │   └── landing.py       # Job launched by the S3 sensor
    ├── resources/
    │   ├── __init__.py
    │   └── connections.py   # Airbyte, dbt, Snowflake resources
    ├── checks/
    │   ├── __init__.py
    │   └── freshness.py     # Asset freshness checks
    ├── telemetry/
    │   ├── run_events.sql   # Run-event table DDL
    │   ├── sensors.py       # Run-status sensors that append run events
    │   └── store.py         # `telemetry` resource: where events are written
    └── utils/
        └── invariants.py    # check_definitions(): rules every definition must follow
```

## How to Add New Components

### Assets

1. Create a new file in `src/assets/` (e.g. `src/assets/my_source.py`).
2. Define your assets using `@asset` or a framework-specific helper.
3. Import and append them to `all_assets` in `src/assets/__init__.py`.

### Sensors

1. Create a new file in `src/sensors/`.
2. Decorate your function with `@sensor`, with `job=` if it yields `RunRequest`s.
   Leave it STOPPED by default. A sensor that only observes (no job) must be
   named in `observing_sensors` in `tests/test_invariants.py`.
3. Import and append to `all_sensors` in `src/sensors/__init__.py`.

### Schedules

1. Create a new file in `src/schedules/`.
2. Use `ScheduleDefinition` or the `@schedule` decorator, with
   `execution_timezone="UTC"` and the default STOPPED status; turn it on in the UI.
3. Import and append to `all_schedules` in `src/schedules/__init__.py`.

`tests/test_invariants.py` checks every schedule, sensor, job, and dbt asset
definition against these rules (see `src/utils/invariants.py`). A downstream
project runs the same checks on its own code location:

```python
from src.utils.invariants import check_definitions
assert check_definitions(defs, observing_sensors={"my_watch_sensor"}) == []
```

### Resources

1. Add a new resource instance to the `RESOURCES` dict in
   `src/resources/connections.py`.
2. Reference the resource by its dict key in your asset or op function
   signatures.

### Asset Checks

1. Create a new file in `src/checks/`.
2. Use the `@asset_check` decorator targeting the relevant asset.
3. Import and append to `all_checks` in `src/checks/__init__.py`.

## Run telemetry

Three run-status sensors (`telemetry_run_started`, `_success`, `_failure`)
append one row per run event to `orchestrator_run_events`
(`src/telemetry/run_events.sql`). The dbt project summarises it in
`mart_orchestrator_run_summary`: duration, failure rates, and test failures over
time, which the Dagster UI does not report.

- They are sensors, not op hooks, so a run whose process dies is still recorded:
  Dagster marks it FAILURE and the failure sensor fires. Completion writers also
  write a missing STARTED row.
- Telemetry errors are logged and swallowed; they never fail a run.
- The `telemetry` resource writes to `telemetry.duckdb` by default. On a
  warehouse, subclass `RunEventStore` and override `_connect`.
- They only observe, so they default to RUNNING and are declared in
  `OBSERVING_SENSORS` for the invariant suite.

## Key Concepts

| Concept      | Description |
|------------- |------------ |
| **Assets**   | Software-defined data artifacts. Each asset declares what it produces and what it depends on. |
| **Resources**| Shared services (databases, APIs) injected into assets/ops at runtime. Configured once, used everywhere. |
| **Sensors**  | Event-driven triggers that poll external systems and launch runs when conditions are met. |
| **Schedules**| Time-based triggers that materialise assets on a cron cadence. |
| **Checks**   | Assertions about asset quality or freshness, surfaced in the Dagster UI. |

## Environment Variables

The following environment variables are expected by the resources defined in
`src/resources/connections.py`:

| Variable              | Used by   |
|---------------------- |---------- |
| `AIRBYTE_USERNAME`    | Airbyte   |
| `AIRBYTE_PASSWORD`    | Airbyte   |
| `SNOWFLAKE_ACCOUNT`   | Snowflake |
| `SNOWFLAKE_USER`      | Snowflake |
| `SNOWFLAKE_PASSWORD`  | Snowflake |
| `SNOWFLAKE_DATABASE`  | Snowflake |
| `SNOWFLAKE_SCHEMA`    | Snowflake |
| `SNOWFLAKE_WAREHOUSE` | Snowflake |

## Further Reading

- [Dagster Docs](https://docs.dagster.io)
- [Software-Defined Assets](https://docs.dagster.io/concepts/assets/software-defined-assets)
- [dagster-dbt Integration](https://docs.dagster.io/integrations/dbt)
- [dagster-airbyte Integration](https://docs.dagster.io/integrations/airbyte)
- [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors)
- [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules)
- [Asset Checks](https://docs.dagster.io/concepts/assets/asset-checks)
