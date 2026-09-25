# Dagster Orchestration

## Role
Reference orchestrator implementation. All other orchestrators (Airflow, Prefect) should match this feature set.

## Key Files

- `src/__init__.py` — Entry point, wires all components into `Definitions`
- `src/assets/airbyte.py` — Airbyte connection assets via `build_airbyte_assets()`
- `src/assets/dbt.py` — dbt project wrapped as Dagster assets via `@dbt_assets`
- `src/sensors/s3_sensor.py` — S3 file arrival polling sensor (boto3, cursor-based); launches `process_landing_file_job`
- `src/jobs/chain.py`, `src/sensors/chain.py` — Worked chain: `daily_asset_schedule` → `extract_job` (Airbyte); `transform_after_extract` run-status sensor → `transform_job` (dbt). Only the head has a cron
- `src/jobs/dbt_docs.py` + `src/schedules/dbt_docs.py` — `publish_dbt_docs_job`: `dbt docs generate`, stamps `_build.json` (git SHA, build time, dbt version, deployment), uploads to `versions/<sha>/` and `latest/`. prod → live, anything else → test; live from non-prod raises `LivePublishRefused` unless `allow_live_from_non_prod`
- `src/jobs/landing.py` — `process_landing_file_job`, one run per landed S3 object
- `src/schedules/daily.py` — Daily 06:00 UTC `ScheduleDefinition` for `extract_job`, the head of the chain
- `src/checks/freshness.py` — Asset freshness check (25h threshold)
- `src/resources/connections.py` — `RESOURCES` dict: airbyte, dbt, snowflake, telemetry
- `src/utils/notifier.py` — One `Notification` model rendered to Slack Block Kit by severity (success = one line; warning/failure expand). Severity→channel routing only in `channel_for`. `make_slack_on_failure_hook` builds on it. Renders are snapshot-tested (`tests/snapshots/`; `UPDATE_SNAPSHOTS=1` to rewrite)
- `src/utils/factories.py` — `build_source_assets()` factory pattern
- `src/telemetry/` — Run-event telemetry. `run_events.sql` is the table DDL; `sensors.py` has run-status sensors appending STARTED/SUCCESS/FAILURE rows (completion writers backfill a missing STARTED); `store.py` is the `telemetry` resource (duckdb by default). Every telemetry error is logged, never raised. dbt summarises the table in `mart_orchestrator_run_summary`
- `src/utils/deployment.py` — Deployment → target database (`prod`/`stage`/else non-prod, exact match) and the `toolkit/deployment` / `toolkit/target_database` run tags
- `src/utils/invariants.py` — `check_definitions(defs)`: invariants over every schedule, sensor, job, and `@dbt_assets`. Run by `tests/test_invariants.py`; importable by downstream projects
- `dagster.yaml` — Instance config. Storage is intentionally unconfigured so it
  defaults to SQLite under `$DAGSTER_HOME`. Never use `base_dir: ~/...` — `~` is
  not expanded here and a literal `~` directory ends up in the repo.
- `workspace.yaml` — Code location config

## Testing

```bash
just dagster::test     # runs `dbt parse` in transformation/dbt, then pytest
```

- Tests in `tests/` use `unittest.mock` — no live services
- `conftest.py` provides mock fixtures for airbyte, dbt, s3
- `tests/test_dbt_assets.py` loads the real dbt project as assets. With
  `DAGSTER_REQUIRE_DBT_MANIFEST=1` (set by `just dagster::test` and CI) a missing
  manifest fails; bare `uv run pytest` without one skips those tests
- Use `build_asset_context()`, `build_sensor_context()` from dagster for test contexts

## Environment Variables

AIRBYTE_USERNAME, AIRBYTE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, and `DAGSTER_CLOUD_DEPLOYMENT_NAME` or `DEPLOYMENT`.

The Snowflake database is **not** an env var: `src/utils/deployment.py` maps the deployment to it, and anything unrecognised maps to non-prod. Never route it back to `EnvVar`

## Patterns

- Assets export via `all_assets` list in `src/assets/__init__.py`
- Same pattern for sensors (`all_sensors`), schedules (`all_schedules`), jobs (`all_jobs`), checks (`all_checks`)
- `tests/test_invariants.py` enforces, for every definition: schedules and job-launching sensors default to STOPPED; schedules set `execution_timezone="UTC"`; a sensor without a job target is listed in `observing_sensors`; no duplicate names; jobs tagged `toolkit/manual_only` are never scheduled; a chained job has no schedule and one upstream sensor whose monitored jobs exist; `@dbt_assets` partition the manifest and run `dbt build`; a job selecting a dbt node selects its parent seeds
- Resources are a flat dict passed to `Definitions(resources=RESOURCES)`
- dbt manifest loaded at import time; `@dbt_assets` rejects two dbt resources
  with one asset key (e.g. a seed in schema `raw` and source `raw.<table>`)
