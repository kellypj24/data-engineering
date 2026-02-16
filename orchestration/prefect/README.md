# Prefect Orchestration

Data pipeline flows built with [Prefect](https://docs.prefect.io/). This implementation mirrors the Dagster reference implementation, providing equivalent functionality for Airbyte sync, dbt build, S3 file arrival sensing, daily scheduling, and freshness checks.

## Overview

| Flow | Description | Dagster Equivalent |
|---|---|---|
| `airbyte_sync` | Triggers Airbyte connection sync via HTTP API, polls for completion | Airbyte sync asset |
| `dbt_transform` | Runs `dbt deps` + `dbt build` (models + tests) | dbt build asset |
| `full_pipeline` | Composes airbyte_sync, dbt_transform, and freshness check as subflows | Full asset job |
| `s3_file_sensor` | Polls S3 for new files using boto3 with cursor-based tracking | S3 file arrival sensor |
| `example_flow` | Original dbt pipeline (dbt run + dbt test) | N/A |

## Project Structure

```
orchestration/prefect/
├── Dockerfile
├── pyproject.toml
├── README.md
├── flows/
│   ├── __init__.py
│   ├── example_flow.py          # Original dbt run + test flow
│   ├── airbyte_sync.py          # Airbyte HTTP trigger + polling
│   ├── dbt_transform.py         # dbt deps + dbt build
│   ├── full_pipeline.py         # Composed ELT pipeline + freshness check
│   └── sensors/
│       ├── __init__.py
│       └── s3_sensor.py         # S3 file arrival polling
├── blocks/
│   ├── __init__.py
│   └── connections.py           # Block definitions (Airbyte, Snowflake, dbt)
└── schedules/
    ├── __init__.py
    └── daily.py                 # Cron schedule definitions
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AIRBYTE_SERVER_URL` | `http://localhost:8006/api/public/v1` | Airbyte API base URL |
| `AIRBYTE_CONNECTION_ID` | `your-airbyte-connection-uuid` | Airbyte connection UUID to sync |
| `DBT_PROJECT_DIR` | `./dbt_project` | Path to dbt project directory |
| `DBT_TARGET` | `dev` | dbt target/profile name |
| `S3_BUCKET` | `my-data-lake-landing` | S3 bucket for file arrival sensor |
| `S3_PREFIX` | `inbound/` | S3 prefix to monitor |
| `FRESHNESS_THRESHOLD_HOURS` | `25` | Max hours for pipeline freshness SLA |
| `SNOWFLAKE_ACCOUNT` | (empty) | Snowflake account identifier |
| `SNOWFLAKE_USER` | (empty) | Snowflake username |
| `SNOWFLAKE_PASSWORD` | (empty) | Snowflake password |
| `SNOWFLAKE_DATABASE` | (empty) | Snowflake database name |
| `SNOWFLAKE_SCHEMA` | (empty) | Snowflake schema name |
| `SNOWFLAKE_WAREHOUSE` | (empty) | Snowflake warehouse name |
| `SNOWFLAKE_ROLE` | (empty) | Snowflake role name |

## Setup

```bash
# Install dependencies
pip install -e ".[dev]"

# Start a local Prefect server (optional, for UI)
prefect server start
# UI available at http://localhost:4200
```

## Running Flows

```bash
# Run individual flows directly
python flows/example_flow.py
python flows/airbyte_sync.py
python flows/dbt_transform.py
python flows/full_pipeline.py

# Run the S3 sensor
python flows/sensors/s3_sensor.py
```

## Blocks (Connection Configuration)

Blocks store reusable configuration for external services. Register them once, then load in any flow.

```bash
# Register blocks (set env vars first)
python -m blocks.connections
```

Load blocks in flows:

```python
from blocks.connections import AirbyteConnection, SnowflakeConnection, DbtConfig

airbyte = AirbyteConnection.load("production")
snowflake = SnowflakeConnection.load("production")
dbt = DbtConfig.load("production")
```

## Deploying with Schedules

```bash
# Deploy the full pipeline with a daily 06:00 UTC schedule
prefect deploy flows/full_pipeline.py:full_pipeline \
  --name "daily-full-pipeline" \
  --cron "0 6 * * *"

# Or use the schedule constant programmatically
python -c "
from flows.full_pipeline import full_pipeline
from schedules.daily import DAILY_SCHEDULE
full_pipeline.serve(name='daily-full-pipeline', cron=DAILY_SCHEDULE)
"
```

## Docker

```bash
# Build the image
docker build -t prefect-orchestration .

# Run the Prefect server
docker run -p 4200:4200 prefect-orchestration

# Run a specific flow
docker run prefect-orchestration python flows/full_pipeline.py
```

## Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=flows --cov=blocks --cov=schedules --cov-report=term-missing
```

## Comparison to Dagster Patterns

| Dagster Concept | Prefect Equivalent |
|---|---|
| Asset | Flow (with return values for lineage) |
| Op | Task |
| Resource | Block |
| Sensor | Polling flow (scheduled or long-running) |
| Schedule | Cron deployment or `flow.serve(cron=...)` |
| Freshness policy | Freshness check task within a flow |
| Asset job | Composed flow calling subflows |

## Flow Patterns

- **`@task`** -- Decorate functions to get retries, caching, logging, and observability for free.
- **`@flow`** -- Compose tasks into a flow. Flows can call other flows (subflows) for composability.
- **Retries** -- Set `retries` and `retry_delay_seconds` on tasks for automatic retry logic.
- **Caching** -- Use `cache_key_fn` and `cache_expiration` on tasks to avoid redundant computation.
- **Concurrency** -- Use `task.submit()` with a task runner (e.g., `ConcurrentTaskRunner`) for parallel execution.
- **Parameters** -- Flow arguments become parameters visible in the Prefect UI.

## Links

- [Prefect documentation](https://docs.prefect.io/)
- [Prefect tutorials](https://docs.prefect.io/latest/tutorial/)
- [Prefect integrations](https://docs.prefect.io/latest/integrations/)
- [Prefect GitHub](https://github.com/PrefectHQ/prefect)
