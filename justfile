set dotenv-load

mod dagster 'orchestration/dagster'
mod airflow 'orchestration/airflow'
mod prefect 'orchestration/prefect'
mod temporal 'orchestration/temporal'
mod dbt 'transformation/dbt'
mod airbyte 'extract_load/airbyte'
mod dlt 'extract_load/dlt'

# List available commands
default:
    @just --list

# Run all tests
test: dagster::test airflow::test prefect::test temporal::test dlt::test

# Lint all code
lint: dagster::lint airflow::lint prefect::lint temporal::lint dbt::lint dlt::lint

# Format all code -- rewrites files. Use `fmt-check` to verify without changing.
fmt: dagster::fmt airflow::fmt prefect::fmt temporal::fmt dlt::fmt

# Check formatting without rewriting. This is the one to run before pushing.
fmt-check: dagster::fmt-check airflow::fmt-check prefect::fmt-check temporal::fmt-check dlt::fmt-check
