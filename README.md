# Data Engineering Toolkit

A composable collection of data engineering tools organized by role. Designed to be mixed, matched, and cloned to kickstart new projects.

Each tool lives in its own directory with independent configuration, documentation, and examples. Pick a pre-built stack or assemble your own from individual components.

---

## Architecture

The toolkit is organized around the roles that make up a modern data pipeline:

- **EL (Extract & Load)** — Moves data from sources (APIs, databases, files) to destinations (warehouses, lakes). Handles connection management, schema detection, incremental loading, and error recovery.

- **Orchestration** — Schedules, triggers, and monitors pipelines. Manages dependencies between steps, retries on failure, and provides observability into pipeline health.

- **Transformation** — Models and transforms data inside the warehouse. Applies business logic, builds dimensional models, and ensures data quality through testing.

- **Delivery** — Moves modeled data out of the warehouse to recipients: files, extracts, feeds. Config-driven, fail-closed on tenant isolation, logged.

Data flows left to right: **Sources -> EL -> Warehouse (raw) -> Transformation -> Warehouse (modeled) -> Delivery / Consumers**. Orchestration wraps the entire process.

---

## Available Tools

| Role | Tool | Description | Status |
|------|------|-------------|--------|
| EL | [Airbyte](extract_load/airbyte/) | Managed connectors via abctl + Terraform | Ready |
| EL | [dlt](extract_load/dlt/) | Code-first Python EL pipelines | Ready |
| Orchestration | [Dagster](orchestration/dagster/) | Asset-based orchestration | Ready |
| Orchestration | [Temporal](orchestration/temporal/) | Durable workflow execution | Ready |
| Orchestration | [Airflow](orchestration/airflow/) | Task-based DAG orchestration | Ready |
| Orchestration | [Prefect](orchestration/prefect/) | Flow-based orchestration | Ready |
| Delivery | [file-export](delivery/file-export/) | Config-driven file exports: generated SQL, watermarks, fail-closed tenant isolation, generated schedules | Ready |
| Transformation | [dbt](transformation/dbt/) | SQL transformation, plus a macro library: surrogate keys, in-place backfills, a validation framework, durable facts | Ready |

**Ready** = fully configured with working examples, tests, and documentation.

---

## Available Stacks

Pre-assembled combinations of tools that work together out of the box:

| Stack | Components | Use Case |
|-------|-----------|----------|
| [airbyte-dagster-dbt](stacks/airbyte-dagster-dbt/) | Airbyte + Dagster + dbt | Production-ready, managed connectors |
| [dlt-dagster-dbt](stacks/dlt-dagster-dbt/) | dlt + Dagster + dbt | Lightweight, code-first |
| [dlt-temporal-dbt](stacks/dlt-temporal-dbt/) | dlt + Temporal + dbt | Long-running workflows |

Each stack includes its own README with setup instructions, environment configuration, and a working example pipeline.

---

## Quick Start

```bash
# Clone the repo
git clone <repo-url> && cd data-engineering

# Pick a stack from stacks/
ls stacks/

# Follow the stack's README for setup
cat stacks/dlt-dagster-dbt/README.md
```

Or to use an individual tool, navigate to its directory and follow the tool-level README.

---

## Project Structure

```
data-engineering/
├── extract_load/           # Extract & Load tools
│   ├── airbyte/
│   ├── dlt/
│   └── _template/
├── orchestration/          # Orchestration tools
│   ├── dagster/
│   ├── temporal/
│   ├── airflow/
│   ├── prefect/
│   └── _template/
├── transformation/         # Transformation tools
│   ├── dbt/
│   └── _template/
├── delivery/               # Outbound delivery tools
│   ├── file-export/
│   └── _template/
├── infrastructure/         # Shared infra (Docker, Terraform, etc.)
├── stacks/                 # Pre-assembled tool combinations
│   ├── airbyte-dagster-dbt/
│   ├── dlt-dagster-dbt/
│   └── dlt-temporal-dbt/
├── docs/                   # Architecture docs, comparisons, roadmaps
│   ├── architecture-patterns.md
│   ├── tool-comparison.md
│   ├── patterns/           # Reusable patterns (e.g. durable facts)
│   ├── ci-cd-hardening.md  # Roadmap: how this repo is checked
│   └── toolkit-expansion.md  # Roadmap: features the toolkit ships
├── .claude/skills/         # Claude Code skills (toolkit + stack)
└── archive/                # Previous experiments for reference
```

---

## Task Runner

The project uses [just](https://github.com/casey/just) as a command runner with [uv](https://github.com/astral-sh/uv) for Python dependency management.

```bash
# List all available commands
just --list

# Run all tests across all tools
just test

# Run tests for a specific tool
just dagster::test
just airflow::test
just prefect::test
just temporal::test
just dlt::test
just dbt::test
just file-export::test

# Lint all code
just lint

# Check formatting without rewriting (run this before pushing)
just fmt-check

# Format all code (rewrites files)
just fmt
```

Each tool has its own `mod.just` file with tool-specific commands. Run `just --list` to see everything available.

---

## Testing

Every tool has its own test suite using its native test framework. All tests use mocked external services — no live connections required.

| Tool | Framework | Command |
|------|-----------|---------|
| Dagster | pytest + dagster test utilities | `just dagster::test` |
| Airflow | pytest + DagBag validation | `just airflow::test` |
| Prefect | pytest + prefect_test_harness | `just prefect::test` |
| Temporal | pytest-asyncio + WorkflowEnvironment | `just temporal::test` |
| dlt | pytest + DuckDB | `just dlt::test` |
| Airbyte | terraform test (mock provider) | `just airbyte::test` |
| dbt | pytest (dbt in-process) + `dbt seed` / `dbt build` on example fixtures | `just dbt::test` |
| file-export | pytest + DuckDB (validates every config in `configs/`) | `just file-export::test` |

---

## CI/CD

GitHub Actions workflows with intelligent change detection:

- **`ci.yml`** — Detects which tools changed and runs only the relevant tests. Includes cross-paradigm impact detection: dbt or Airbyte changes also trigger orchestrator tests (since Dagster, Airflow, and Prefect all wrap dbt and Airbyte). Lint runs `ruff check` and `ruff format --check`; the dbt job builds the whole example project on its fixtures; a `lockfiles` job enforces `uv lock --check` for every tool; `exposures-drift` fails when the dbt exposures generated from file-export configs are stale; `wiring` fails when a tool is missing from any shared surface. A fan-in `CI Success` job is the single required check, enforced by a ruleset no one can bypass.
- **`terraform-validate.yml`** — Runs `terraform fmt`, `validate`, and `test` on Airbyte Terraform changes.
- **`dependabot.yml`** — Weekly updates for Python (`uv` ecosystem, so `uv.lock` moves with `pyproject.toml`; the dbt tool is `lockfile-only`, since its floors track `require-dbt-version`), Terraform, and GitHub Actions.

---

## Adding a New Tool

Each role directory contains a `_template/` subdirectory. It holds a **README that specifies what a tool of that role must provide** — it is a requirements checklist, not a code skeleton, so there is nothing to `cp -r`. Read it, then create the tool's files.

1. Read `<role>/_template/README.md` for what that role requires
2. Create `<role>/<tool-name>/` with `pyproject.toml`, `uv.lock`, `README.md`, `CLAUDE.md`, `mod.just`, and `tests/`. Model it on `extract_load/dlt/`, the smallest complete tool
3. Wire it into all five shared surfaces, or it will be invisible to part of the system:
   - root `justfile` — the `mod` import plus the aggregate `test` / `lint` / `fmt` / `fmt-check` recipes
   - `.github/workflows/ci.yml` — a `paths-filter` entry, a `test-<tool>` job, a `lint` matrix row, and the `lockfiles` job's list
   - `.github/dependabot.yml` — a `package-ecosystem: uv` entry (never `pip`; it skips `uv.lock`)
   - this README's tool table
   - root `CLAUDE.md`, only if the tool introduces a new convention
4. Verify: `just --list`, `just check-wiring`, `just <tool>::test`, `just <tool>::lint`, and `uv lock --check`
5. If the tool participates in a new stack, create a stack directory under `stacks/`

Working with Claude Code? The `add-tool` skill in `.claude/skills/` does all of the above, and `verify-tool` runs the checks.

---

## Docs

- [Architecture Patterns](docs/architecture-patterns.md) — ELT, orchestration models, idempotency, and how the pieces fit together
- [Tool Comparison](docs/tool-comparison.md) — Decision matrices for choosing between tools in each role
- [Durable facts](docs/patterns/durable-facts.md) — Incremental tables whose history outlives their source
- [Validation framework](transformation/dbt/macros/validation/README.md) — Tiered-severity rules, notification routing, failure log
- Roadmaps: [CI/CD hardening](docs/ci-cd-hardening.md) (how this repo is checked) and [toolkit expansion](docs/toolkit-expansion.md) (features to add)

---

## Archive

The `archive/` directory contains previous experiments, proof-of-concept work, and deprecated configurations. These are kept for reference but are not actively maintained. Check there before building something from scratch -- there may be prior art worth borrowing from.
