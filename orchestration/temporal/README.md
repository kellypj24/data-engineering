# Temporal Orchestration

Data pipeline workflows built with [Temporal](https://docs.temporal.io/).

## When to Use Temporal vs Dagster

| Consideration | Temporal | Dagster |
|---|---|---|
| Core model | Durable workflow execution (code-as-workflow) | Asset-based orchestration (declarative data lineage) |
| Long-running processes | Built for it; workflows survive restarts, can run for days/months | Designed for batch jobs; long-running ops need workarounds |
| Retries and failure handling | Fine-grained per-activity retry policies, saga patterns | Per-op retry policies, but no built-in saga support |
| Observability | Temporal UI shows workflow execution history step-by-step | Dagster UI shows asset lineage and materialization history |
| Best for | Multi-step durable workflows, microservice orchestration, human-in-the-loop | Analytics pipelines, dbt orchestration, asset-centric data platforms |

**Rule of thumb:** Use Temporal when you need durable execution guarantees for long-running or complex workflows with fine-grained failure handling. Use Dagster when your pipeline is centered around data assets and you want declarative lineage and materialization tracking.

## Workflow Patterns

- **Sequential activities** — Execute extract, transform, load in order using `workflow.execute_activity`.
- **Parallel activities** — Use `asyncio.gather` to run independent activities concurrently.
- **Saga pattern** — Define compensating activities to roll back on failure.
- **Child workflows** — Break large pipelines into composable sub-workflows.
- **Signals and queries** — Allow external input into running workflows.

## Local Setup

```bash
# Start Temporal server, UI, and Postgres
docker compose up -d

# Verify Temporal server is running
# UI available at http://localhost:8080
# gRPC API at localhost:7233

# Install Python dependencies
pip install -e ".[dev]"

# Start the worker
python workers/worker.py

# Trigger a workflow (from another terminal)
python -c "
import asyncio
from temporalio.client import Client

async def main():
    client = await Client.connect('localhost:7233')
    result = await client.execute_workflow(
        'DataPipelineWorkflow',
        'my-source',
        id='pipeline-run-1',
        task_queue='data-pipeline-queue',
    )
    print(f'Result: {result}')

asyncio.run(main())
"
```

## Links

- [Temporal documentation](https://docs.temporal.io/)
- [Temporal Python SDK](https://github.com/temporalio/sdk-python)
- [Temporal UI](https://github.com/temporalio/ui)
- [Workflow patterns](https://docs.temporal.io/develop/python/core-application)
