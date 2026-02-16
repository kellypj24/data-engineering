# Data Engineering Composable Toolkit

## Project Structure

This is a composable collection of data engineering tools organized by role:
- `extract_load/` — EL tools (Airbyte, dlt)
- `orchestration/` — Orchestrators (Dagster, Airflow, Prefect, Temporal)
- `transformation/` — Transform tools (dbt)
- `stacks/` — Pre-assembled combinations (airbyte-dagster-dbt, dlt-dagster-dbt, dlt-temporal-dbt)
- `infrastructure/` — Shared Docker/Terraform
- `docs/` — Architecture patterns, tool comparison matrices
- `archive/` — Previous experiments (reference only, not maintained)

## Conventions

- **Python**: 3.11+, dependencies managed via `pyproject.toml` (no requirements.txt)
- **Package manager**: `uv` for all Python operations (`uv run`, `uv pip install`)
- **Task runner**: `just` (Justfile at root, per-tool `mod.just` files)
- **Linting**: `ruff` for Python, `sqlfluff` for SQL (Snowflake dialect)
- **Testing**: `pytest` for all Python tools, `terraform test` for Terraform
- **Each tool is independent**: own pyproject.toml, own Dockerfile, own README, own tests

## Key Patterns

- **Dagster** is the reference implementation — fully built with assets, sensors, schedules, resources, checks
- **Airflow** and **Prefect** mirror Dagster's feature set: Airbyte sync, dbt build, S3 sensor, daily schedule, freshness checks
- **dbt macros** follow naming: `overrides/` for built-in overrides, `utils/` for helpers, `staging/` for staging-specific
- **Environment variables** are used for all secrets — never hardcoded
- Resources use `EnvVar()` (Dagster), `Variable.get()` (Airflow), or `Block.load()` (Prefect)

## Common Commands

```bash
just test              # Run all test suites
just lint              # Lint all code
just fmt               # Format all code
just dagster::test     # Run Dagster tests only
just airflow::test     # Run Airflow tests only
just dbt::lint         # Lint dbt SQL
```

## CI/CD

GitHub Actions with cross-paradigm impact detection:
- dbt changes trigger dagster + airflow + prefect tests
- airbyte changes trigger orchestrator tests
- Tool-specific changes trigger only that tool's tests

## Adding a New Tool

1. Copy `<role>/_template/` to `<role>/new-tool/`
2. Add pyproject.toml, README.md, Dockerfile, CLAUDE.md
3. Add `mod.just` for task runner integration
4. Add tests in `tests/` directory
5. Update root README table and Justfile module imports
