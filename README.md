# Data Engineering Toolkit

A composable collection of data engineering tools organized by role. Designed to be mixed, matched, and cloned to kickstart new projects.

Each tool lives in its own directory with independent configuration, documentation, and examples. Pick a pre-built stack or assemble your own from individual components.

---

## Architecture

The toolkit is organized around three roles that make up a modern data pipeline:

- **EL (Extract & Load)** — Moves data from sources (APIs, databases, files) to destinations (warehouses, lakes). Handles connection management, schema detection, incremental loading, and error recovery.

- **Orchestration** — Schedules, triggers, and monitors pipelines. Manages dependencies between steps, retries on failure, and provides observability into pipeline health.

- **Transformation** — Models and transforms data inside the warehouse. Applies business logic, builds dimensional models, and ensures data quality through testing.

Data flows left to right: **Sources -> EL -> Warehouse (raw) -> Transformation -> Warehouse (modeled) -> Consumers**. Orchestration wraps the entire process.

---

## Available Tools

| Role | Tool | Description | Status |
|------|------|-------------|--------|
| EL | [Airbyte](extract_load/airbyte/) | Managed connectors via abctl + Terraform | Ready |
| EL | [dlt](extract_load/dlt/) | Code-first Python EL pipelines | Ready |
| Orchestration | [Dagster](orchestration/dagster/) | Asset-based orchestration | Ready |
| Orchestration | [Temporal](orchestration/temporal/) | Durable workflow execution | Ready |
| Orchestration | [Airflow](orchestration/airflow/) | Task-based DAG orchestration | Scaffold |
| Orchestration | [Prefect](orchestration/prefect/) | Flow-based orchestration | Scaffold |
| Transformation | [dbt](transformation/dbt/) | SQL-based transformation framework | Ready |

**Ready** = fully configured with working examples and documentation.
**Scaffold** = directory structure and basic config in place, not yet production-tested.

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
├── infrastructure/         # Shared infra (Docker, Terraform, etc.)
├── stacks/                 # Pre-assembled tool combinations
│   ├── airbyte-dagster-dbt/
│   ├── dlt-dagster-dbt/
│   └── dlt-temporal-dbt/
├── docs/                   # Architecture docs and comparisons
│   ├── architecture-patterns.md
│   └── tool-comparison.md
└── archive/                # Previous experiments for reference
```

---

## Adding a New Tool

Each role directory contains a `_template/` subdirectory with the standard structure for that role. To add a new tool:

1. Copy the template: `cp -r extract_load/_template extract_load/new-tool`
2. Follow the template's README to fill in tool-specific configuration
3. Add the tool to the table in this README
4. If the tool participates in a new stack, create a stack directory under `stacks/`

---

## Docs

- [Architecture Patterns](docs/architecture-patterns.md) — ELT, orchestration models, idempotency, and how the pieces fit together
- [Tool Comparison](docs/tool-comparison.md) — Decision matrices for choosing between tools in each role

---

## Archive

The `archive/` directory contains previous experiments, proof-of-concept work, and deprecated configurations. These are kept for reference but are not actively maintained. Check there before building something from scratch -- there may be prior art worth borrowing from.
