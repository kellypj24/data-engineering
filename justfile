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
lint: dagster::lint airflow::lint prefect::lint dbt::lint dlt::lint

# Format all code
fmt: dagster::fmt airflow::fmt prefect::fmt dlt::fmt
