# 0003. duckdb as the credential-free default dbt target

- Status: Accepted
- Date: 2026-10-07

## Context
The dbt project targets Snowflake, Postgres, and BigQuery, but parsing,
linting (sqlfluff's dbt templater compiles the project), tests, and CI all need
a warehouse connection. Requiring credentials for any of that would leave CI
weaker than local work, or require secrets in a public repository.

## Decision
`profiles.yml` defaults `DBT_TARGET` to `duckdb`. Fixture seeds in
`seeds/example_raw/` stand in for the raw sources on duckdb only, so the whole
example project builds and every data and unit test runs with no credentials.
Other warehouses are selected explicitly.

## Consequences
CI builds the full project on every dbt change with no secrets. SQL must stay
portable, or dispatch per adapter, and Snowflake-only features are isolated
(`macros/snowflake/`, `models/cost/`) and tested on duckdb fixtures or literal
inputs. duckdb passing does not prove Snowflake behaviour; those pieces state
what was not verified live.
