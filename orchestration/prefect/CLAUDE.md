# Prefect Orchestration

## Role
Flow-based orchestration. Mirrors Dagster reference features: Airbyte sync, dbt build, S3 sensor, daily schedule, freshness checks.

## Key Files

- `flows/example_flow.py` — Original dbt pipeline (dbt_run + dbt_test tasks)
- `flows/airbyte_sync.py` — Flow triggering Airbyte via HTTP API, polling for completion
- `flows/dbt_transform.py` — Enhanced dbt flow with deps + build
- `flows/full_pipeline.py` — Composing airbyte_sync → dbt_build → freshness_check as subflows
- `flows/sensors/s3_sensor.py` — Polling flow using boto3 (mirrors Dagster S3 sensor)
- `blocks/connections.py` — Prefect Block definitions for Airbyte, Snowflake, dbt config
- `schedules/daily.py` — CronSchedule at "0 6 * * *"
- `Dockerfile` — python:3.11-slim, install from pyproject.toml

## Testing

```bash
just prefect::test     # or: cd orchestration/prefect && uv run pytest
```

- Tests in `tests/` use `prefect_test_harness()` (session-scoped fixture)
- Flows tested with mocked HTTP/subprocess
- Tasks testable via `.fn()` for direct invocation

## Environment Variables

AIRBYTE_SERVER_URL, AIRBYTE_CONNECTION_ID, S3_BUCKET, S3_PREFIX, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, DBT_PROJECT_DIR

## Patterns

- `@flow` for orchestration, `@task` for units of work
- Blocks for connection configuration (loaded via `Block.load("block-type/block-name")`)
- Subflows for composition (flow calling other flows)
- Retries and retry_delay_seconds on tasks
