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
    ├── resources/
    │   ├── __init__.py
    │   └── connections.py   # Airbyte, dbt, Snowflake resources
    └── checks/
        ├── __init__.py
        └── freshness.py     # Asset freshness checks
```

## How to Add New Components

### Assets

1. Create a new file in `src/assets/` (e.g. `src/assets/my_source.py`).
2. Define your assets using `@asset` or a framework-specific helper.
3. Import and append them to `all_assets` in `src/assets/__init__.py`.

### Sensors

1. Create a new file in `src/sensors/`.
2. Decorate your function with `@sensor`.
3. Import and append to `all_sensors` in `src/sensors/__init__.py`.

### Schedules

1. Create a new file in `src/schedules/`.
2. Use `ScheduleDefinition` or the `@schedule` decorator.
3. Import and append to `all_schedules` in `src/schedules/__init__.py`.

### Resources

1. Add a new resource instance to the `RESOURCES` dict in
   `src/resources/connections.py`.
2. Reference the resource by its dict key in your asset or op function
   signatures.

### Asset Checks

1. Create a new file in `src/checks/`.
2. Use the `@asset_check` decorator targeting the relevant asset.
3. Import and append to `all_checks` in `src/checks/__init__.py`.

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
