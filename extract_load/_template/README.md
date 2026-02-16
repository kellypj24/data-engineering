# EL (Extract & Load) Component Template

## What This Role Does

The EL component is responsible for **extracting** data from source systems and **loading** it into a destination (typically a data warehouse or data lake). It handles:

- Connecting to source APIs, databases, files, and SaaS platforms.
- Extracting data incrementally or via full refresh.
- Loading raw data into the destination, preserving schema.
- Managing credentials and connection configuration securely.

## What a New EL Tool Must Provide

When adding a new EL tool to the toolkit, create a directory under `extract_load/<tool-name>/` and include:

### Connection Configuration

- A clear way to define sources and destinations (config files, Python code, or UI).
- Support for common source types (databases, REST APIs, file systems).
- Documentation on how to add a new source or destination.

### Credential Management

- Instructions for storing credentials securely (environment variables, secrets manager references).
- Never commit credentials to the repository. Use `.env` files (gitignored) or external secret stores.

### Source and Destination Definitions

- A catalog or registry of available connectors, or instructions for writing custom ones.
- Schema detection and mapping between source and destination.

### Integration Points

- How the orchestrator triggers syncs (CLI commands, Python SDK, API calls).
- How to check sync status and handle failures.
- What metadata or events are emitted for downstream consumption.

## Conventions

- Place all tool-specific code and config under `extract_load/<tool-name>/`.
- Include a `README.md` with setup instructions, prerequisites, and usage examples.
- Include a `Dockerfile` if the tool requires its own runtime environment.
- Use environment variables for all connection strings and secrets.
