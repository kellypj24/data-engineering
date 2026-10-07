# 0004. `dbt_env` var, independent of `target.name`

- Status: Accepted
- Date: 2026-10-07

## Context
Macros need dev-versus-prod behaviour: dev schemas, limiting data in dev.
Branching on `target.name` conflates two things: which warehouse (duckdb,
snowflake) and which environment (dev, prod). The branches silently never fired,
because no target was named `prod`, and that bug shipped.

## Decision
The environment is the `dbt_env` var, defaulting to `env_var('DBT_ENV', 'dev')`.
Macros branch on `var('dbt_env')`. The target picks the warehouse only.

## Consequences
Any warehouse can run as dev or prod. Property YAML sees `--vars` but not the
`env_var()` default, so YAML that branches on environment must also check
`env_var('DBT_ENV')` (see `models/staging/_sources.yml`).
