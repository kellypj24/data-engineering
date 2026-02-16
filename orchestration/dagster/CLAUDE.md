# Dagster Orchestration

## Role
Reference orchestrator implementation. All other orchestrators (Airflow, Prefect) should match this feature set.

## Key Files

- `src/__init__.py` — Entry point, wires all components into `Definitions`
- `src/assets/airbyte.py` — Airbyte connection assets via `build_airbyte_assets()`
- `src/assets/dbt.py` — dbt project wrapped as Dagster assets via `@dbt_assets`
- `src/sensors/s3_sensor.py` — S3 file arrival polling sensor (boto3, cursor-based)
- `src/schedules/daily.py` — Daily 06:00 UTC `ScheduleDefinition`
- `src/checks/freshness.py` — Asset freshness check (25h threshold)
- `src/resources/connections.py` — `RESOURCES` dict: airbyte, dbt, snowflake
- `src/utils/alerts.py` — Slack failure hook factory
- `src/utils/factories.py` — `build_source_assets()` factory pattern
- `dagster.yaml` — Instance config (SQLite storage)
- `workspace.yaml` — Code location config

## Testing

```bash
just dagster::test     # or: cd orchestration/dagster && uv run pytest
```

- Tests in `tests/` use `unittest.mock` — no live services
- `conftest.py` provides mock fixtures for airbyte, dbt, s3
- dbt assets require mocking `Path.exists()` or providing a minimal manifest fixture
- Use `build_asset_context()`, `build_sensor_context()` from dagster for test contexts

## Environment Variables

AIRBYTE_USERNAME, AIRBYTE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE

## Patterns

- Assets export via `all_assets` list in `src/assets/__init__.py`
- Same pattern for sensors (`all_sensors`), schedules (`all_schedules`), checks (`all_checks`)
- Resources are a flat dict passed to `Definitions(resources=RESOURCES)`
- dbt manifest loaded at import time — must exist or tests must mock it
