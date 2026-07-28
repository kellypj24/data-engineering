# dbt Transformation

## Role
SQL-based transformation framework. Models raw data into staging, intermediate, and mart layers.

## Key Files

- `pyproject.toml` — Dependencies: dbt-core, dbt-snowflake, dbt-duckdb; dev: sqlfluff + dbt templater, yamllint, pre-commit. Extras: `postgres`, `bigquery`
- `dbt_project.yml` — Project config: name=data_warehouse, models materialization by layer
- `profiles.yml` — One profile, four outputs (duckdb, snowflake, postgres, bigquery), selected by `DBT_TARGET`. **Defaults to duckdb** so everything runs without credentials
- `packages.yml` / `package-lock.yml` — dbt_utils, dbt_expectations (metaplane), audit_helper, codegen, dbt_date (godatadriven). The lock file is committed
- `.sqlfluff` — SQL linting: Snowflake dialect, uppercase keywords, trailing commas forbidden
- `.pre-commit-config.yaml` — sqlfluff, yamllint, dbt-checkpoint hooks
- `macros/overrides/generate_schema_name.sql` — Dev/prod schema routing
- `macros/utils/limit_data_in_dev.sql` — Dev data filtering (recent N days)
- `macros/utils/safe_divide.sql` — Null/zero-safe division
- `macros/staging/audit_columns.sql` — _loaded_at (EL timestamp or fallback), _dbt_updated_at columns
- `macros/staging/clean_strings.sql` — TRIM + LOWER + NULLIF
- `models/staging/` — 1:1 with source tables (views)
- `models/intermediate/` — Business logic joins (views)
- `models/marts/` — Consumer-facing tables (tables)

## Commands

```bash
uv sync --dev          # install dbt + sqlfluff (first time)
uv run dbt deps        # install packages.yml dependencies (first time)

just dbt::run          # dbt run
just dbt::test         # dbt test (includes unit tests, requires >=1.8)
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
- Macros organized: `overrides/` (built-in overrides), `utils/` (helpers), `staging/` (staging-specific)
- Schema routing: dev prefixes with target name, prod uses custom schema directly
- `limit_data_in_dev` appended to staging models for faster dev runs
- All env vars for connections — never hardcode credentials in profiles.yml
