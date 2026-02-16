# Orchestration Component Template

## What This Role Does

The orchestration component is the **control plane** of the data pipeline. It is responsible for:

- Defining and scheduling jobs, assets, or workflows.
- Triggering upstream EL syncs and downstream transformations in the correct order.
- Monitoring pipeline health, tracking run status, and alerting on failures.
- Providing a UI or CLI for operators to inspect and manage pipelines.

## What a New Orchestrator Must Provide

When adding a new orchestration tool to the toolkit, create a directory under `orchestration/<tool-name>/` and include:

### Job or Asset Definitions

- A clear structure for defining what runs and in what order.
- Support for dependencies between steps (e.g., dbt runs after EL completes).
- Parameterization — ability to pass configuration to runs.

### Scheduling

- Cron-based or event-based scheduling.
- Support for manual triggering and backfills.

### Monitoring and Observability

- Run history and status tracking.
- Logging and error reporting.
- Integration with alerting systems (email, Slack, PagerDuty).

### Trigger Mechanisms

- How the orchestrator triggers EL tools (API calls, CLI commands, SDK).
- How the orchestrator triggers transformation tools.
- How sensors or listeners detect new data or external events.

## Conventions

- Place all tool-specific code and config under `orchestration/<tool-name>/`.
- Include a `README.md` with setup instructions, prerequisites, and usage examples.
- Include a `Dockerfile` for containerized execution.
- Define a `workspace.yaml` or equivalent entrypoint for the orchestrator to discover pipelines.
- Use environment variables for all connection details and secrets.
