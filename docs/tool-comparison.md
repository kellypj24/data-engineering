# Tool Comparison

Decision matrices for choosing between tools in each role. These reflect the opinions of this toolkit -- your mileage may vary depending on team size, existing infrastructure, and specific requirements.

---

## EL Tools

| Feature | Airbyte | dlt | Fivetran |
|---------|---------|-----|----------|
| Deployment | Self-hosted (abctl) / Cloud | Python library | SaaS only |
| Connector model | Pre-built + custom | Code-first | Pre-built |
| Config approach | UI + Terraform | Python code | UI |
| Connector count | 300+ | Growing (100+) | 300+ |
| Custom connectors | CDK (Python/Java) | Python decorators | Partner SDK |
| Schema handling | Auto-detected, configurable | Auto-detected, programmable | Auto-detected |
| Incremental sync | Built-in | Built-in | Built-in |
| Cost model | Free (self-hosted) or per-row (Cloud) | Free (open source) | Per-row (expensive) |
| Best for | Managed connectors, standard sources | Custom sources, rapid prototyping | Enterprise, hands-off |

### When to pick what

**Airbyte** when your sources are well-known (Salesforce, Stripe, Postgres, etc.) and you want pre-built connectors that handle edge cases. The Terraform provider makes it infrastructure-as-code friendly. Self-hosting with abctl keeps costs at zero.

**dlt** when you need to ingest from custom APIs, internal services, or anything without a pre-built connector. A dlt pipeline is just Python -- you can write one in an afternoon and it handles schema inference, incremental loading, and state management for you. Also a strong choice when you want everything in version control with no UI dependency.

**Fivetran** when budget is not a concern and you want zero operational overhead. Good for teams without dedicated data engineers who just need data flowing.

---

## Orchestration Tools

| Feature | Dagster | Airflow | Prefect | Temporal |
|---------|---------|---------|---------|----------|
| Model | Asset-based | Task-based DAGs | Flow-based | Workflow-based |
| Core abstraction | Software-defined assets | Operators + DAGs | Tasks + flows | Activities + workflows |
| Scheduling | Built-in (cron + sensors) | Built-in (cron + sensors) | Built-in (cron + triggers) | External (cron) or internal timers |
| Data awareness | Native (knows about tables, files) | None (knows about task success/failure) | Minimal | None |
| Testing | First-class (unit test assets) | Possible but awkward | Good | Good (replay-based) |
| UI | Excellent | Functional | Clean | Functional |
| Deployment | Dagster Cloud or self-hosted | Self-hosted or managed (Astronomer, MWAA) | Prefect Cloud or self-hosted | Self-hosted or Temporal Cloud |
| Python version | 3.8+ | 3.8+ | 3.9+ | 3.8+ |
| Strengths | Data awareness, testing, lineage | Mature ecosystem, widespread adoption | Developer experience, simple API | Durability, long-running workflows, retries |
| Weaknesses | Smaller ecosystem | Complex config, painful upgrades | Less battle-tested at scale | High learning curve, overkill for simple jobs |
| Learning curve | Medium | Medium-High | Low | High |
| Best for | Data platforms | Legacy/enterprise | Simple pipelines | Complex, long-running workflows |

### When to pick what

**Dagster** is the default choice for this toolkit. Its asset-based model aligns naturally with data pipelines: you think in terms of tables and datasets, not tasks and operators. The built-in integration with dbt is particularly strong -- dbt models become Dagster assets automatically.

**Airflow** when you are joining an organization that already runs Airflow, or when you need a specific operator from its massive ecosystem. Do not start a new project on Airflow unless you have a compelling reason.

**Prefect** when you want the fastest path from zero to a working pipeline. The API is clean and Pythonic, and Prefect Cloud handles infrastructure. Good for small teams and simple workflows. Less proven for complex, multi-step data platforms.

**Temporal** when your workflows are long-running (hours or days), involve external service calls that may fail, or require complex retry and compensation logic. Temporal guarantees workflow completion even through infrastructure failures. Overkill for a simple "extract, load, transform" pipeline, but ideal for workflows like "call API, wait for webhook, process result, call another API, handle timeout."

---

## Transformation Tools

| Feature | dbt | SQLMesh | Custom Python |
|---------|-----|---------|---------------|
| Language | SQL + Jinja | SQL + Python | Python |
| Model definition | `.sql` files with `{{ config() }}` | `.sql` or `.py` files | Scripts / notebooks |
| Testing | Built-in (`schema.yml` tests) | Built-in | Custom (pytest, etc.) |
| Documentation | Auto-generated from YAML | Auto-generated | Manual |
| Incremental models | Supported (merge, append, etc.) | Supported (with plan/apply) | Manual |
| Environment management | Targets (dev/staging/prod) | Virtual environments | Manual |
| Change management | Full refresh or incremental | Plan/apply with impact analysis | Manual |
| CI/CD | dbt Cloud or CLI in any CI | CLI in any CI | CLI in any CI |
| Community & ecosystem | Massive (packages, blog posts, Slack) | Growing | N/A |
| Best for | SQL-heavy teams, standard warehouse transforms | Large-scale changes with safety guarantees | Non-SQL transforms, ML pipelines |

### When to pick what

**dbt** is the industry standard for warehouse transformation. If your transforms are SQL-expressible (and most are), use dbt. The ecosystem is enormous: dbt packages for common patterns, extensive documentation, active community. The Jinja templating is ugly but powerful. Every data engineer should know dbt.

**SQLMesh** when you need safer deployments for large-scale transformations. Its plan/apply workflow shows you exactly what will change before you run it, similar to Terraform. Worth evaluating if you have hundreds of models and deployment failures are costly.

**Custom Python** when your transformations genuinely cannot be expressed in SQL -- ML feature engineering, complex parsing, image processing, etc. In practice, this is rarer than people think. Try SQL first. If you find yourself writing Python that essentially constructs SQL strings, just use dbt.

---

## Stack Recommendations

For most teams starting fresh:

| Team Size | Recommended Stack | Why |
|-----------|------------------|-----|
| Solo / Small | dlt + Dagster + dbt | Minimal infrastructure, everything in Python/SQL, fast to set up |
| Medium | Airbyte + Dagster + dbt | Pre-built connectors save time, Dagster provides observability |
| Large / Enterprise | Airbyte + Dagster + dbt (or Airflow if migrating) | Proven at scale, strong governance features |
| Complex workflows | dlt + Temporal + dbt | When pipelines involve long-running processes or external service coordination |

These are starting points. The whole purpose of this toolkit is that the pieces are interchangeable -- start with one stack and swap components as your needs evolve.
