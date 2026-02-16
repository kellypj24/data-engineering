# Airflow Orchestration

## Role
Task-based DAG orchestration. Mirrors Dagster reference features: Airbyte sync, dbt build, S3 sensor, daily schedule, freshness checks.

## Key Files

- `dags/airbyte_sync.py` — DAG triggering Airbyte sync via AirbyteTriggerSyncOperator
- `dags/dbt_transform.py` — DAG running `dbt build` via BashOperator
- `dags/full_pipeline.py` — ELT DAG: airbyte >> dbt_run >> dbt_test >> freshness_check (TaskGroup)
- `dags/sensors/s3_file_sensor.py` — DAG with S3KeySensor for file arrival detection
- `docker-compose.yml` — Full Airflow 2.8 stack (webserver, scheduler, postgres, init)
- `Dockerfile` — Custom image based on apache/airflow:2.8-python3.11
- `pyproject.toml` — Dependencies: apache-airflow, providers (airbyte, amazon, snowflake), dbt-core

## Testing

```bash
just airflow::test     # or: cd orchestration/airflow && uv run pytest
```

- Tests in `tests/` — DAG validation (no import errors, no cycles, correct dependencies)
- `conftest.py` sets `AIRFLOW__CORE__UNIT_TEST_MODE=True` and uses SQLite backend
- DagBag fixture loads all DAGs for validation
- Task tests mock external APIs (Airbyte, subprocess)

## Environment Variables

AIRFLOW_CONN_AIRBYTE_DEFAULT, AIRFLOW_CONN_AWS_DEFAULT, AIRFLOW_CONN_SNOWFLAKE_DEFAULT, AIRBYTE_CONNECTION_ID, S3_BUCKET, S3_PREFIX, DBT_PROJECT_DIR

## Patterns

- DAGs use `@dag` decorator style with `dag_id`, `schedule`, `default_args`
- Connections configured via Airflow connection URIs or environment variables
- TaskGroups for visual grouping in the UI
- All DAGs have `catchup=False` and `tags` for filtering
