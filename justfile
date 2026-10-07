set dotenv-load

mod dagster 'orchestration/dagster'
mod airflow 'orchestration/airflow'
mod prefect 'orchestration/prefect'
mod temporal 'orchestration/temporal'
mod dbt 'transformation/dbt'
mod airbyte 'extract_load/airbyte'
mod dlt 'extract_load/dlt'
mod file-export 'delivery/file-export'
mod sensitive-scan 'tooling/sensitive-scan'

# List available commands
default:
    @just --list

# Run all tests
test: dagster::test airflow::test prefect::test temporal::test dlt::test dbt::test file-export::test sensitive-scan::test

# Lint all code
lint: dagster::lint airflow::lint prefect::lint temporal::lint dbt::lint dlt::lint file-export::lint sensitive-scan::lint

# Format all code -- rewrites files. Use `fmt-check` to verify without changing.
fmt: dagster::fmt airflow::fmt prefect::fmt temporal::fmt dlt::fmt file-export::fmt sensitive-scan::fmt

# Check every tool is wired into the justfile, ci.yml, dependabot, and README (CI runs this too)
check-wiring:
    uv run --no-project --with pyyaml python .github/scripts/check_wiring.py

# Check formatting without rewriting. This is the one to run before pushing.
fmt-check: dagster::fmt-check airflow::fmt-check prefect::fmt-check temporal::fmt-check dlt::fmt-check file-export::fmt-check sensitive-scan::fmt-check
