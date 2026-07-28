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

`.claude/skills/` holds repo-local skills for working **on** the toolkit:

- `add-tool` — add a new tool under a role and wire it into the justfile, CI,
  dependabot, and the README table
- `add-stack` — compose existing tools into a new `stacks/<name>/`
- `verify-tool` — run the local gauntlet CI does not cover (CI never goes
  through `just`)

`.claude/skills/CLAUDE.md` states the rules for writing them — chiefly that
skills are *procedure* and `CLAUDE.md` files are *convention*, and that a skill
references conventions rather than restating them.

## CI/CD & Tooling Roadmap

`docs/ci-cd-hardening.md` is a Claude Code–executable backlog of CI/CD,
infrastructure, and Claude-skills improvements for this repo. If asked to harden
CI, add agent skills/hooks, or "make improvements," start there: pick the
highest-priority unchecked task from its index, implement it, verify against its
Acceptance criteria, and tick the box. One task per PR.

Each task is self-contained — everything needed to do the work is in that file.
