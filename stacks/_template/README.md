# Stack Template

> **This is a specification, not a skeleton.** It states what a stack of this
> repo must provide. There are no files here to `cp -r` — read it as a
> requirements checklist, then create the files. `stacks/dlt-dagster-dbt/` is the best
> model of what "finished" looks like.


How to compose a new stack from the toolkit's components.

## Overview

A stack combines one tool from each of the three roles:

| Role              | What It Does                          | Available Tools       |
|-------------------|---------------------------------------|-----------------------|
| **EL**            | Extract data from sources, load to warehouse | Airbyte, dlt    |
| **Orchestration** | Schedule, trigger, and monitor pipelines     | Dagster, Temporal |
| **Transformation**| Transform raw data into clean models         | dbt              |

## Steps to Create a New Stack

### 1. Pick One Tool from Each Role

Choose based on your requirements. See each component's README for trade-offs and capabilities.

### 2. Create a New Directory

```bash
mkdir stacks/<el-tool>-<orchestrator>-<transform-tool>
```

Follow the naming convention: `<el>-<orchestration>-<transformation>`, all lowercase, separated by hyphens.

### 3. Provide a docker-compose.yml

Create a `docker-compose.yml` for local development that wires together the services needed by your stack. Reference Dockerfiles and configs from the component directories using relative paths (e.g., `../../orchestration/<tool>/Dockerfile`).

### 4. Document the Architecture and Setup

Write a `README.md` for your stack covering:

- **When to use** this combination and what trade-offs it makes.
- **Architecture overview** with a diagram showing data flow.
- **Prerequisites** — tools, accounts, and versions needed.
- **Step-by-step setup** — from zero to a running local pipeline.
- **How the pieces connect** — which component triggers which and how.
- **Local development workflow** — common commands for day-to-day use.

### 5. Reference Component READMEs

Link to each component's own README for detailed configuration:

- `../../extract_load/<tool>/README.md`
- `../../orchestration/<tool>/README.md`
- `../../transformation/<tool>/README.md`
