# Data Engineering Composable Toolkit

## Project Structure

This is a composable collection of data engineering tools organized by role:
- `extract_load/` — EL tools (Airbyte, dlt)
- `orchestration/` — Orchestrators (Dagster, Airflow, Prefect, Temporal)
- `transformation/` — Transform tools (dbt)
- `stacks/` — Pre-assembled combinations (airbyte-dagster-dbt, dlt-dagster-dbt, dlt-temporal-dbt)
- `infrastructure/` — Shared Docker/Terraform
- `docs/` — Architecture patterns, tool comparison matrices, `patterns/` write-ups, and the two roadmaps
- `archive/` — Previous experiments (reference only, not maintained)

## Conventions

- **Python**: 3.11+, dependencies managed via `pyproject.toml` (no requirements.txt)
- **Package manager**: `uv` for all Python operations (`uv run`, `uv pip install`)
- **Task runner**: `just` (Justfile at root, per-tool `mod.just` files)
- **Linting**: `ruff` for Python, `sqlfluff` for SQL (Snowflake dialect)
- **Testing**: `pytest` for all Python tools, `terraform test` for Terraform. dbt: pytest drives dbt in-process, plus `dbt seed` + `dbt build` on `seeds/example_raw/` fixtures — see `transformation/dbt/CLAUDE.md`
- **Each tool is independent**: own pyproject.toml, own Dockerfile, own README, own tests

## Key Patterns

- **Dagster** is the reference implementation — fully built with assets, sensors, schedules, resources, checks
- **Airflow** and **Prefect** mirror Dagster's feature set: Airbyte sync, dbt build, S3 sensor, daily schedule, freshness checks
- **dbt macros** follow naming: `overrides/` for built-in overrides, `utils/` for helpers, `staging/` for staging-specific, `validation/` for the validation framework
- **dbt environment is `DBT_ENV`**, not the target: the target picks a warehouse, `dbt_env` picks dev/prod behaviour
- **Environment variables** are used for all secrets — never hardcoded
- Resources use `EnvVar()` (Dagster), `Variable.get()` (Airflow), or `Block.load()` (Prefect)

## Common Commands

```bash
just test              # Run all test suites
just lint              # Lint all code
just fmt               # Format all code (rewrites files)
just fmt-check         # Verify formatting without rewriting -- run this before pushing
just dagster::test     # Run Dagster tests only
just airflow::test     # Run Airflow tests only
just dbt::test         # pytest + dbt seed/build on the example fixtures
just dbt::lint         # Lint dbt SQL
```

## CI/CD

GitHub Actions with cross-paradigm impact detection:
- dbt changes trigger dagster + airflow + prefect tests
- airbyte changes trigger orchestrator tests
- Tool-specific changes trigger only that tool's tests
- The dbt job builds the full example project (models, data tests, unit test) on duckdb

## Adding a New Tool

Use the `add-tool` skill in `.claude/skills/` — it does all of this and verifies it.

`<role>/_template/` is a **specification README** stating what a tool of that
role must provide. It is not a code skeleton; there is nothing to copy.

1. Read `<role>/_template/README.md` for the role's requirements
2. Create `<role>/<tool-name>/` with pyproject.toml, uv.lock, README.md,
   CLAUDE.md, `mod.just`, and `tests/` — model it on `extract_load/dlt/`
3. Wire into the root `justfile`: the `mod` import and the aggregate recipes
4. Wire into `.github/workflows/ci.yml`: paths-filter entry, `test-<tool>` job,
   `lint` matrix row
5. Wire into `.github/dependabot.yml` with `package-ecosystem: uv` (not `pip` —
   `pip` leaves `uv.lock` untouched and the `lockfiles` job will fail)
6. Update the root README tool table
7. Verify: `just --list`, `just <tool>::test`, `uv lock --check`

## Claude Skills

`.claude/skills/` holds two families of skills.

**Toolkit skills** — for working **on** this repo:

- `add-tool` — add a new tool under a role and wire it into the justfile, CI,
  dependabot, and the README table
- `add-stack` — compose existing tools into a new `stacks/<name>/`
- `verify-tool` — run the local gauntlet CI does not cover (CI never goes
  through `just`)

**Stack skills** — for working **with** a stack after it has been copied into a
downstream project. These infer the tool root and never assume this repo's
layout:

- `dbt-model` — scaffold a staging/intermediate/mart model and its paired
  `.yml`, honouring the project's macros and sqlfluff rules
- `dbt-source` — declare a raw table in `_sources.yml` with tests, and generate
  the staging model in front of it
- `dagster-asset` — define an asset, walk the registration chain into
  `Definitions`, and add the resource keys it needs
- `dlt-pipeline` — scaffold a dlt source/resource/pipeline with an offline
  mocked test
- `run-stack` — bring a stack up locally and prove each hop, in the order that
  avoids the silent dbt-manifest failure

`.claude/skills/CLAUDE.md` states the rules for writing them — chiefly that
skills are *procedure* and `CLAUDE.md` files are *convention*, and that a skill
references conventions rather than restating them.

## Roadmaps

Two Claude Code–executable backlogs, same format:

- `docs/ci-cd-hardening.md` — CI/CD, supply chain, repo hygiene, and agent
  hooks: how **this repo** is checked.
- `docs/toolkit-expansion.md` — features the toolkit **ships**: dbt macros,
  Dagster patterns, Snowflake environment management, developer tools, and
  further stack skills.

If asked to harden CI, add agent skills/hooks, extend the toolkit, or "make
improvements," start with the relevant file: pick the highest-priority unchecked
task from its index, implement it, verify against its Acceptance criteria, and
tick the box. One task per PR.

Each task is self-contained — everything needed to do the work is in its file.
Toolkit content must stay company-agnostic: no company, client, business-domain,
or vendor-dataset names; use neutral examples (`orders`, `customers`).
