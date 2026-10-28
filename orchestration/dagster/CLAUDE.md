# Dagster Orchestration

## Role
Reference orchestrator implementation. All other orchestrators (Airflow, Prefect) should match this feature set.

## Key Files

- `src/__init__.py` — Entry point, wires all components into `Definitions`
- `src/assets/airbyte.py` — Airbyte connection assets via `build_airbyte_assets()`
- `src/assets/dbt.py` — dbt project wrapped as Dagster assets via `@dbt_assets`
- `src/sensors/s3_sensor.py` — S3 file arrival polling sensor (boto3, cursor-based); launches `process_landing_file_job`
- `src/sensors/sqs_sensor.py` — `build_sqs_sensor(name, queue_url, job, run_config_fn)`: one RunRequest per message, `run_key` = message id; receipt handles go in the cursor and are deleted **the next tick**, so a crash before the cursor saves redelivers without a duplicate run. Empty `queue_url` raises at load. `sqs_landing_sensor` reads S3 event notifications into `process_landing_file_job`; an unparseable message stays on the queue (DLQ via redrive). Visibility timeout must exceed two intervals. Tested with `moto`
- `src/jobs/chain.py`, `src/sensors/chain.py` — Worked chain: `daily_asset_schedule` → `extract_job` (Airbyte); `transform_after_extract` run-status sensor → `transform_job` (dbt). Only the head has a cron
- `src/jobs/dbt_docs.py` + `src/schedules/dbt_docs.py` — `publish_dbt_docs_job`: `dbt docs generate`, stamps `_build.json` (git SHA, build time, dbt version, deployment), uploads to `versions/<sha>/` and `latest/`. prod → live, anything else → test; live from non-prod raises `LivePublishRefused` unless `allow_live_from_non_prod`
- `src/jobs/environment_refresh.py` — `refresh_stage_environment_job` + weekly STOPPED schedule: runs dbt `refresh_environment` for stage (Snowflake zero-copy clone of prod)
- `src/jobs/boundary.py` — Manual-only (`toolkit/manual_only`, never scheduled): `identity_probe_job` logs CURRENT_USER/ROLE/SECONDARY_ROLES/DATABASE/WAREHOUSE; `write_boundary_job` passes only if creating a table in a forbidden database (default: the prod database; required config in the prod deployment) is refused (errno 3001/2003), fails if secondary roles are active, and drops + fails if the table is created
- `src/jobs/landing.py` — `process_landing_file_job`, one run per landed S3 object
- `src/schedules/daily.py` — Daily 06:00 UTC `ScheduleDefinition` for `extract_job`, the head of the chain
- `src/schedules/__init__.py` — `BASE_SCHEDULES`, per-deployment cron overrides (`DEPLOYMENT_CRONS`, applied by `schedules_for_deployment` in `src/utils/deployment.py`), and `NOT_OVERRIDDEN`. Every daily schedule is in one of the two; `tests/test_schedules.py` enforces it
- `src/checks/freshness.py` — Asset freshness check (25h threshold)
- `src/resources/connections.py` — `RESOURCES` dict: airbyte, dbt, snowflake (`RoutedSnowflakeResource`), telemetry, backup
- `src/resources/routed_snowflake.py` — `RoutedSnowflakeResource`: database and warehouse resolved on every `get_connection()`, configured values ignored; default connector only
- `src/resources/routed_dbt.py` — `routed_dbt_cli(context, dbt, args)`: every dbt call goes through it; sets `SNOWFLAKE_DATABASE`/`SNOWFLAKE_WAREHOUSE` for the dbt process on every target: non-prod mirrors prod, no per-target exceptions
- `src/utils/notifier.py` — One `Notification` model rendered to Slack Block Kit by severity (success = one line; warning/failure expand). Severity→channel routing only in `channel_for`. `make_slack_on_failure_hook` builds on it. Renders are snapshot-tested (`tests/snapshots/`; `UPDATE_SNAPSHOTS=1` to rewrite)
- `src/utils/factories.py` — `build_source_assets()` factory pattern; `build_backup_asset(table, deps)`: Parquet copy to `<root>/<prefix>/<table>/snapshot_ts=<ts>/run_id=<run_id>/`, prefix from `backup_prefix()` in `deployment.py` (resolved in the asset body; unknown → `non_prod`), provenance in materialization metadata. Example: `src/assets/backups.py` (`raw_orders_backup`, after the `airbyte/raw_orders` sync). The copy is the `backup` resource (in `RESOURCES`): `src/resources/backup.py` (`BackupStorage`, duckdb `COPY ... (FORMAT parquet)`; an `s3://` root loads `httpfs` with a temporary secret from `s3_secret`, AWS credential chain by default) or `src/resources/snowflake_backup.py` (**Snowflake extra**, `COPY INTO @stage` via `RoutedSnowflakeResource`). Asset functions need real annotations: no `from __future__ import annotations` in modules that define them
- `src/telemetry/` — Run-event telemetry. `run_events.sql` is the table DDL; `sensors.py` has run-status sensors appending STARTED/SUCCESS/FAILURE rows (completion writers backfill a missing STARTED); `store.py` is the `telemetry` resource (duckdb by default). Every telemetry error is logged, never raised. dbt summarises the table in `mart_orchestrator_run_summary`
- `src/utils/deployment.py` — Deployment → target database and warehouse size key (`prod`/`stage`/else non-prod, exact match), `resolve_routing()`, and the display-only `toolkit/deployment` / `toolkit/target_database` run tags
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
- `conftest.py` provides mock fixtures for airbyte, dbt, s3; `tests/test_sqs_sensor.py` uses a `moto` queue and `tests/test_backup.py` a `moto` S3 server (duckdb `httpfs`, downloaded on first use) instead
- Dagster runs dbt with **this** venv's dbt, so it carries `dbt-duckdb` (the profile's default target) as well as `dbt-snowflake`; `test_dbt_runs_from_the_dagster_environment` fails without it
- `tests/test_dbt_assets.py` loads the real dbt project as assets. With
  `DAGSTER_REQUIRE_DBT_MANIFEST=1` (set by `just dagster::test` and CI) a missing
  manifest fails; bare `uv run pytest` without one skips those tests
- Use `build_asset_context()`, `build_sensor_context()` from dagster for test contexts

## Environment Variables

AIRBYTE_USERNAME, AIRBYTE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_SCHEMA, and `DAGSTER_CLOUD_DEPLOYMENT_NAME` or `DEPLOYMENT`.

The Snowflake database and warehouse are **not** env vars: `src/utils/deployment.py` maps the deployment to them, and anything unrecognised maps to non-prod. Never route them back to `EnvVar`.

**Resolve routing in the op/asset body, never at import.** A code server can import without the deployment variable that the run worker has; anything resolved at import then sends prod runs to non-prod with no error (this happened in production). Use `RoutedSnowflakeResource` and `routed_dbt_cli`; never call `dbt.cli` directly or build a resource from `target_database(...)` at module level. Run tags from `deployment_tags` are display-only

## Patterns

- Assets export via `all_assets` list in `src/assets/__init__.py`
- Same pattern for sensors (`all_sensors`), schedules (`all_schedules`), jobs (`all_jobs`), checks (`all_checks`)
- `tests/test_invariants.py` enforces, for every definition: schedules and job-launching sensors default to STOPPED; schedules set `execution_timezone="UTC"`; a sensor without a job target is listed in `observing_sensors`; no duplicate names; jobs tagged `toolkit/manual_only` are never scheduled; a chained job has no schedule and one upstream sensor whose monitored jobs exist; `@dbt_assets` partition the manifest and run `dbt build`; a job selecting a dbt node selects its parent seeds; with `restricted={tag: allowed_jobs}`, only allowed jobs' resolved selections reach an asset carrying `tag`
- Resources are a flat dict passed to `Definitions(resources=RESOURCES)`
- dbt manifest loaded at import time; `@dbt_assets` rejects two dbt resources
  with one asset key (e.g. a seed in schema `raw` and source `raw.<table>`)
