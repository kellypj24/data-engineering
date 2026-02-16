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

Then point dbt at the correct profile target:

```bash
dbt run --profile data_warehouse --target snowflake
```

Or set the `DBT_TARGET` environment variable.

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

3. Add a schema YAML alongside the model for column docs and tests:

```yaml
# models/staging/my_source/_my_source__models.yml
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
| Staging      | `stg_`  | `stg_stripe__payments`         |
| Intermediate | `int_`  | `int_payments__pivoted`        |
| Fact         | `fct_`  | `fct_orders`                   |
| Dimension    | `dim_`  | `dim_customers`                |

Source YAML files use the pattern `_<source>__sources.yml`.
Model YAML files use the pattern `_<source>__models.yml`.

## Installed Packages

| Package            | Purpose                                                |
|--------------------|--------------------------------------------------------|
| `dbt_utils`        | Cross-database macros (surrogate keys, pivots, etc.)   |
| `dbt_expectations` | Great Expectations-style data quality tests            |

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
- [dbt-expectations](https://hub.getdbt.com/calogica/dbt_expectations/latest/)
- [How we structure our dbt projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
