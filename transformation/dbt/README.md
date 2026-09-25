# data_warehouse -- dbt Project

A warehouse-agnostic dbt project template designed as a composable data engineering toolkit. This project supports Snowflake, DuckDB, PostgreSQL, and BigQuery out of the box.

## Layered Architecture

The project follows a three-layer modeling pattern:

```
sources (raw data)
    |
    v
staging (stg_)        -- 1:1 with source tables, light renaming and casting
    |
    v
intermediate (int_)   -- business logic joins, aggregations, spine tables
    |
    v
marts (fct_, dim_)    -- final fact and dimension tables for consumption
```

**Staging** models are materialized as **views**. They clean, rename, and cast columns from raw source tables. Each staging model maps to exactly one source table.

**Intermediate** models are materialized as **views**. They contain reusable business logic -- joins across staging models, filters, aggregations, and spine generation.

**Marts** models are materialized as **tables**. They are the final, consumer-facing datasets organized by business domain (e.g., `marts/finance/`, `marts/marketing/`).

## Configuring Your Warehouse

This project ships with profile templates for four warehouses in `profiles.yml`. Each profile reads connection details from environment variables so that no secrets are stored in code.

### Snowflake

```bash
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=transformer
export SNOWFLAKE_PASSWORD=secret
export SNOWFLAKE_ROLE=TRANSFORMER
export SNOWFLAKE_DATABASE=ANALYTICS
export SNOWFLAKE_WAREHOUSE=TRANSFORMING
export SNOWFLAKE_SCHEMA=PUBLIC
```

### DuckDB

```bash
export DUCKDB_PATH=dev.duckdb   # optional, defaults to dev.duckdb
```

### PostgreSQL

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=dbt
export POSTGRES_PASSWORD=secret
export POSTGRES_DBNAME=analytics
export POSTGRES_SCHEMA=public
```

### BigQuery

```bash
export BIGQUERY_PROJECT=my-gcp-project
export BIGQUERY_DATASET=analytics
export BIGQUERY_LOCATION=US
```

Then select the target with `DBT_TARGET` (default `duckdb`):

```bash
DBT_TARGET=snowflake dbt build
```

## Target vs. environment

A **target** says which *warehouse* (`duckdb`, `snowflake`, `postgres`,
`bigquery`). The **environment** is the `dbt_env` var (`DBT_ENV`, default
`dev`), which picks *dev or prod behaviour*. Macros branch on
`var('dbt_env')`, never on `target.name`:

- `generate_schema_name`: dev prefixes custom schemas with the target schema
  (`main_staging`); prod uses them as-is (`staging`).
- `limit_data_in_dev`: dev reads only the last few days; prod reads everything.

In property YAML, `var('dbt_env')` sees `--vars` overrides but **not**
`DBT_ENV`. YAML that branches on environment must check
`env_var('DBT_ENV', 'dev')` as well (see `models/staging/_sources.yml`).

## Adding a New Source

1. Create a source YAML file in `models/staging/`:

```yaml
# models/staging/my_source/_my_source__sources.yml
sources:
  - name: my_source
    database: "{{ env_var('RAW_DATABASE', 'RAW') }}"
    schema: my_source
    tables:
      - name: users
      - name: orders
```

2. Create a staging model for each source table:

```sql
-- models/staging/my_source/stg_my_source__users.sql
WITH source AS (
    SELECT * FROM {{ source('my_source', 'users') }}
),

renamed AS (
    SELECT
        id AS user_id,
        created_at,
        updated_at
    FROM source
)

SELECT * FROM renamed
```

3. Add a `.yml` with the same name as the model, for column docs and tests.
   **One `.yml` per model**, not a shared `_models.yml`:

```yaml
# models/staging/my_source/stg_my_source__users.yml
models:
  - name: stg_my_source__users
    columns:
      - name: user_id
        tests:
          - unique
          - not_null
```

## Naming Conventions

| Layer        | Prefix  | Example                        |
|--------------|---------|--------------------------------|
| Staging      | `stg_`  | `stg_billing__payments`        |
| Intermediate | `int_`  | `int_payments__pivoted`        |
| Fact         | `fct_`  | `fct_orders`                   |
| Dimension    | `dim_`  | `dim_customers`                |

Source YAML files use the pattern `_<source>__sources.yml`. Every model and
seed has its own `.yml` with the same name.

SQL files end **without** a semicolon. dbt wraps every model in
`create ... as (...)`, so a terminator is a syntax error on every adapter.

## What this project ships

| Piece | What it's for |
|---|---|
| `macros/utils/mint_surrogate_key` | Versioned, collision-free UUID-shaped keys. NULL is distinct from `''`, and there are no delimiter collisions. Use it instead of `dbt_utils.generate_surrogate_key` |
| `macros/utils/backfill_surrogate_keys` | `run-operation` that fills or upgrades key columns in place. No source reads, no `--full-refresh`. Dry run by default |
| `macros/utils/limit_data_in_dev`, `safe_divide` | Dev-only recency predicate; null- and zero-safe division |
| `macros/staging/audit_columns`, `clean_strings` | `_loaded_at` / `_dbt_updated_at`; trim + lower + nullif |
| `macros/validation/` | Tiered-severity validation framework with a failure log. See [its README](macros/validation/README.md) |
| `tests/generic/ties_to_control_total` | Ties a per-period sum to an independent control; fails when zero periods are compared |
| `tests/generic/timestamp_is_utc` | Fails non-UTC offsets: ISO-8601 strings on every adapter, and `TIMESTAMP_TZ` on Snowflake. Types that store no offset (duckdb/Postgres `timestamptz`, BigQuery `TIMESTAMP`, naive timestamps) get a **logged no-op**, not a silent pass |
| `models/marts/fct_daily_order_revenue` | Worked example of a **durable fact** (see [docs/patterns/durable-facts.md](../../docs/patterns/durable-facts.md)) |
| Seed contract | Seeds are dropped and recreated on every run (`+full_refresh: true`). Each needs a description, `meta.owner`, and a test |

## Example fixtures and testing

`seeds/example_raw/` stands in for the raw sources on duckdb, so the whole
project builds with no EL tool and no credentials. The seeds are enabled on
duckdb only. **Delete the folder in a real project.**

```bash
just dbt::test    # pytest, then `dbt build` of the seeds, then of everything else
```

- Seeds (and their tests) build first, then everything else with
  `--exclude resource_type:seed`. Models and source tests read the fixtures
  through `source()`, which dbt does not order after seeds, and a seed rebuilt
  in the same run is dropped and recreated (`+full_refresh`) while they read
  it. One `dbt build` races.
- The build runs with `DBT_ENV=prod`. The fixtures have fixed dates, which the
  dev-only `limit_data_in_dev` window would filter out.
- `tests/python/` drives dbt in-process (`dbtRunner`) on a throwaway copy of
  the project. It covers run-operations, seed behaviour, the durable fact, the
  validation framework, and a whole-project build.
- `tests/macros/` holds singular tests over literal rows that pin macro
  behaviour.

CI runs the same build on every dbt change.

## Installed Packages

| Package            | Purpose                                                |
|--------------------|--------------------------------------------------------|
| `dbt_utils`        | Cross-database macros (pivots, date spines, etc.)      |
| `dbt_expectations` | Great Expectations-style data quality tests (metaplane fork) |
| `audit_helper`     | Compare relations during refactors                     |
| `codegen`          | Generate source and model YAML                         |
| `dbt_date`         | Date helpers (required by `dbt_expectations`)          |

Install packages after cloning:

```bash
dbt deps
```

## Commands Reference

```bash
dbt deps                  # Install packages
dbt seed                  # Load CSV seeds into the warehouse
dbt run                   # Run all models
dbt run -s staging        # Run only staging models
dbt test                  # Run all tests
dbt build                 # Run + test in DAG order
dbt docs generate         # Generate documentation site
dbt docs serve            # Serve docs locally
dbt compile               # Compile SQL without executing
dbt debug                 # Validate connection and config
dbt clean                 # Remove target/ and dbt_packages/
```

## Further Reading

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/best-practices)
- [dbt-utils](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)
- [dbt-expectations](https://hub.getdbt.com/metaplane/dbt_expectations/latest/)
- [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
