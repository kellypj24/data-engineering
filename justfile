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
mod dagster-status 'tooling/dagster-status'
mod claude-review 'tooling/claude-review'
mod release-diff 'tooling/release-diff'

# List available commands
default:
    @just --list

# Everything CI checks, from the root: hooks, wiring, lint, format check, tests. Run before pushing.
check: pins hooks check-wiring lint fmt-check test

# Every tool pinned to a Python version; Dockerfiles and CI agree with it (CI runs this too)
pins:
    uv run --no-project python .github/scripts/check_python_pins.py .

# Every pre-commit hook over every file, as CI's "Pre-commit hooks" job runs them
hooks:
    uvx pre-commit run --all-files --show-diff-on-failure

# Run all tests
test: dagster::test airflow::test prefect::test temporal::test dlt::test dbt::test file-export::test sensitive-scan::test claude-review::test release-diff::test dagster-status::test

# Lint all code
lint: dagster::lint airflow::lint prefect::lint temporal::lint dbt::lint dlt::lint file-export::lint sensitive-scan::lint claude-review::lint release-diff::lint dagster-status::lint

# Format all code -- rewrites files. Use `fmt-check` to verify without changing.
fmt: dagster::fmt airflow::fmt prefect::fmt temporal::fmt dlt::fmt file-export::fmt sensitive-scan::fmt claude-review::fmt release-diff::fmt dagster-status::fmt

# Check every tool is wired into the justfile, ci.yml, dependabot, and README (CI runs this too)
check-wiring:
    uv run --no-project --with pyyaml python .github/scripts/check_wiring.py

# Check formatting without rewriting (`check` runs this).
fmt-check: dagster::fmt-check airflow::fmt-check prefect::fmt-check temporal::fmt-check dlt::fmt-check file-export::fmt-check sensitive-scan::fmt-check claude-review::fmt-check release-diff::fmt-check dagster-status::fmt-check
