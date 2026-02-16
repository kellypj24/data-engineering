# Prefect Orchestration

Data pipeline flows built with [Prefect](https://docs.prefect.io/).

## Setup

```bash
# Install dependencies
pip install -e ".[dev]"

# Start a local Prefect server (optional, for UI)
prefect server start
# UI available at http://localhost:4200

# Run a flow directly
python flows/example_flow.py

# Or deploy and schedule via Prefect
prefect deploy flows/example_flow.py:dbt_pipeline \
  --name "dbt-pipeline-daily" \
  --cron "0 6 * * *"
```

## Flow Patterns

- **`@task`** — Decorate functions to get retries, caching, logging, and observability for free.
- **`@flow`** — Compose tasks into a flow. Flows can call other flows (subflows) for composability.
- **Retries** — Set `retries` and `retry_delay_seconds` on tasks for automatic retry logic.
- **Caching** — Use `cache_key_fn` and `cache_expiration` on tasks to avoid redundant computation.
- **Concurrency** — Use `task.submit()` with a task runner (e.g., `ConcurrentTaskRunner`) for parallel execution.
- **Parameters** — Flow arguments become parameters visible in the Prefect UI.

## Comparison to Other Orchestrators

| Consideration | Prefect | Airflow | Temporal |
|---|---|---|---|
| Core model | Python-native flows and tasks | DAGs of operators | Durable workflow execution |
| Deployment | Lightweight; flows are just Python scripts | Requires Airflow server + scheduler | Requires Temporal server + worker |
| UI | Clean dashboard with flow run tracking | Mature DAG-centric UI | Workflow execution history |
| Scheduling | Cron, interval, event-driven, or ad-hoc | Cron-based with sensors | Timer-based or signal-driven |
| Best for | Python-first teams, rapid iteration, dbt orchestration | Large-scale scheduling, mature ecosystems | Long-running durable workflows |

**Rule of thumb:** Use Prefect when you want the lightest-weight orchestrator that stays close to native Python. Use Airflow for mature, large-scale DAG scheduling. Use Temporal for durable, long-running workflows.

## Links

- [Prefect documentation](https://docs.prefect.io/)
- [Prefect tutorials](https://docs.prefect.io/latest/tutorial/)
- [Prefect integrations](https://docs.prefect.io/latest/integrations/)
- [Prefect GitHub](https://github.com/PrefectHQ/prefect)
