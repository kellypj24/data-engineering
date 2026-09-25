# dbt Transformation

## Role
SQL-based transformation framework. Models raw data into staging, intermediate, and mart layers.

## Key Files

- `pyproject.toml` — Dependencies: dbt-core, dbt-snowflake, dbt-duckdb; dev: sqlfluff + dbt templater, yamllint, pre-commit, pytest. Extras: `postgres`, `bigquery`
- `dbt_project.yml` — Project config: name=data_warehouse, models materialization by layer
- `profiles.yml` — One profile, four outputs (duckdb, snowflake, postgres, bigquery), selected by `DBT_TARGET`. **Defaults to duckdb** so everything runs without credentials
- `packages.yml` / `package-lock.yml` — dbt_utils, dbt_expectations (metaplane), audit_helper, codegen, dbt_date (godatadriven). The lock file is committed
- `.sqlfluff` — SQL linting: Snowflake dialect, uppercase keywords, trailing commas forbidden
- `.pre-commit-config.yaml` — sqlfluff, yamllint, dbt-checkpoint hooks
- `macros/overrides/generate_schema_name.sql` — Non-prod/prod schema routing
- `macros/utils/limit_data_in_dev.sql` — Non-prod data filtering (recent N days), as a composable predicate
- `macros/utils/safe_divide.sql` — Null/zero-safe division
- `macros/utils/mint_surrogate_key.sql` — Versioned, collision-free UUID-shaped surrogate keys (`mint_surrogate_key`, `surrogate_key_version`). Use instead of `dbt_utils.generate_surrogate_key`
- `macros/utils/backfill_surrogate_keys.sql` — `run-operation` that fills or upgrades key columns in place (no source reads, no `--full-refresh`). Dry run by default; dependent keys in a second UPDATE; version column written last
- `tests/python/` — pytest suite that runs dbt in-process (`dbtRunner`) on a throwaway copy of the project and duckdb file (`conftest.py`): run-operations, seeds, the durable fact, and a whole-project `dbt build`
- `tests/macros/` — Singular tests over literal rows that pin macro behaviour; no sources, so they run on duckdb in CI
- `macros/staging/audit_columns.sql` — _loaded_at (EL timestamp or fallback), _dbt_updated_at columns
- `macros/staging/clean_strings.sql` — TRIM + LOWER + NULLIF
- `seeds/example_raw/` — Fixtures standing in for `raw.orders` / `raw.customers` / `raw.order_daily_totals`, so the example project builds on duckdb with no EL tool. `+schema: example_raw`, **enabled on duckdb only**; `_sources.yml` resolves the `raw` source to wherever they land. Delete this folder in a real project
- `seeds/` — CSV + one `.yml` per seed. Project-wide `+full_refresh: true` (drop and recreate every run, so a new CSV column never needs a manual `--full-refresh`) and seed-level `+persist_docs`. Every seed needs a description, `meta.owner`, and at least one test — enforced by `tests/python/test_seeds.py`
- `macros/validation/` — Tiered-severity validation framework: `validate_data_source`, config/notification routing via vars, `no_validation_failures` generic test. See its README
- `models/validation/` — Rule-set models (`val_*`, one per rule set), incremental `validation_log` (purged per `retention_days`), `validation_summary`
- `models/staging/` — 1:1 with source tables (views)
- `models/intermediate/` — Business logic joins (views)
- `models/marts/` — Consumer-facing tables (tables)
- `models/marts/fct_daily_order_revenue.sql` — Worked example of a **durable fact** (history outlives its source): `full_refresh=false`, bounded restatement window, control total. See `docs/patterns/durable-facts.md`
- `tests/generic/ties_to_control_total.sql` — Ties a column's per-period sum to an independent control; fails when zero periods are compared

## Commands

```bash
uv sync --dev          # install dbt + sqlfluff (first time)
uv run dbt deps        # install packages.yml dependencies (first time)

just dbt::run          # dbt run
just dbt::test         # pytest, then `dbt seed` + `dbt build` on the example fixtures (DBT_ENV=prod)
just dbt::lint         # sqlfluff lint
just dbt::fix          # sqlfluff fix
just dbt::docs         # dbt docs generate + serve
```

All of the above run against duckdb by default — no warehouse credentials
needed. Set `DBT_TARGET=snowflake` (plus the `SNOWFLAKE_*` env vars) to point at
the real warehouse. `postgres` and `bigquery` need their extra installed first,
e.g. `uv sync --extra postgres`.

## Patterns

- `require-dbt-version: ">=1.8.0"` for unit test support
- Macros organized: `overrides/` (built-in overrides), `utils/` (helpers), `staging/` (staging-specific), `validation/` (validation framework)
- **Environment is `DBT_ENV`, not the target.** A target picks a *warehouse*
  (`duckdb`, `snowflake`, …); the `dbt_env` var in `dbt_project.yml` picks an
  *environment* (`dev` default, `prod`). Macros branch on `var('dbt_env')`.
  Branching on `target.name` conflates the two and the branch never fires —
  that bug shipped once and made both macros below dead code.
- Schema routing: non-prod prefixes with the target's schema (`main_staging`),
  prod uses the custom schema directly (`staging`)
- `limit_data_in_dev` is a **complete predicate** — `WHERE {{ limit_data_in_dev('created_at') }}`.
  It returns `TRUE` in prod, so it needs no `WHERE 1 = 1` anchor and composes with `AND`
- All env vars for connections — never hardcode credentials in profiles.yml
