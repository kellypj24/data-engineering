# Temporal Orchestration

## Role
Durable workflow execution for long-running, fault-tolerant data pipelines.

## Key Files

- `workflows/example_workflow.py` — `DataPipelineWorkflow`: extract → transform → load
- `activities/example_activity.py` — `extract_data`, `transform_data`, `load_data` activities
- `workers/worker.py` — Worker connecting to Temporal server, registers workflows + activities
- `docker-compose.yml` — Temporal server + PostgreSQL + UI
- `pyproject.toml` — Dependencies: temporalio>=1.6.0, dev: pytest-asyncio, temporalio[testing]

## Testing

```bash
just temporal::test    # or: cd orchestration/temporal && uv run pytest
```

- Tests use `WorkflowEnvironment.start_time_skipping()` for workflow tests
- `ActivityEnvironment` for isolated activity testing
- All tests are async — use `pytest-asyncio`
- Mock activities when testing workflows, test activities in isolation

## Patterns

- `@workflow.defn` + `@workflow.run` for workflow classes
- `@activity.defn` for activity functions (all async)
- `workflow.unsafe.imports_passed_through()` for importing activities into workflows
- RetryPolicy on `execute_activity()` calls
- Task queue: `data-pipeline-queue`
