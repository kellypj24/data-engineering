# Transformation Component Template

## What This Role Does

The transformation component takes **raw data** that has been loaded into the warehouse and transforms it into **clean, tested, documented models** ready for analytics and downstream consumption. It handles:

- Defining transformation logic as modular, version-controlled models.
- Testing data quality with assertions and contracts.
- Generating documentation for data consumers.
- Managing model dependencies and execution order.

## What a New Transformation Tool Must Provide

When adding a new transformation tool to the toolkit, create a directory under `transformation/<tool-name>/` and include:

### Model Definitions

- A clear structure for organizing transformation logic (e.g., staging, intermediate, marts).
- Support for modular, reusable components.
- Materialization strategies (views, tables, incremental).

### Testing

- Data quality tests (not null, unique, accepted values, referential integrity).
- Custom test definitions for business logic validation.
- A way to run tests as part of the pipeline and fail on violations.

### Documentation Generation

- Auto-generated documentation from model definitions and metadata.
- Column-level descriptions and lineage tracking.
- A browsable documentation site or catalog integration.

### Integration Points

- How the orchestrator triggers transformations (CLI commands, Python SDK).
- How to check transformation status and handle failures.
- What artifacts or metadata are produced for downstream tools.

## Conventions

- Place all tool-specific code and config under `transformation/<tool-name>/`.
- Include a `README.md` with setup instructions, prerequisites, and usage examples.
- Follow a consistent naming convention for models (e.g., `stg_`, `int_`, `fct_`, `dim_`).
- Use environment variables or profiles for warehouse connection details.
- Never commit warehouse credentials to the repository.
