# Airbyte + Dagster + dbt Stack

This is the primary data engineering stack. It combines Airbyte for extract and load, Dagster for orchestration, and dbt for transformation.

## Architecture Overview

```
Sources ──> Airbyte (EL) ──> Warehouse ──> dbt (Transform) ──> Marts
                 ^                              ^
                 |                              |
              Dagster (Orchestration) ──────────┘
```

- **Airbyte** handles extracting data from sources and loading it into the warehouse. It provides a connector catalog, schema detection, and incremental sync.
- **Dagster** orchestrates the full pipeline. It triggers Airbyte syncs, waits for completion, then kicks off dbt runs. It provides scheduling, monitoring, and a web UI.
- **dbt** transforms raw data in the warehouse into clean, tested, documented models.

## Prerequisites

- Docker and Docker Compose
- `abctl` (Airbyte CLI) — see [Airbyte docs](https://docs.airbyte.com/)
- Python 3.11+
- A target warehouse (e.g., Snowflake, BigQuery, Postgres)

## Step-by-Step Setup

### 1. Install and Start Airbyte

Airbyte runs separately from the Docker Compose stack, managed by `abctl`.

```bash
# Install abctl
curl -LsfS https://get.airbyte.com | bash -

# Start Airbyte locally (default port 8000)
abctl local install
```

Open http://localhost:8000 to access the Airbyte UI.

### 2. Configure Airbyte Sources and Destinations

In the Airbyte UI:

1. Add your **source** connections (e.g., Postgres, APIs, SaaS tools).
2. Add your **destination** (e.g., Snowflake, BigQuery, or a local Postgres).
3. Create **connections** linking sources to the destination with your desired sync frequency and namespace settings.

Note the connection IDs — Dagster will use these to trigger syncs programmatically.

### 3. Start the Dagster Stack

```bash
# From this directory
docker compose up -d
```

This starts:

| Service            | Port | Description                |
|--------------------|------|----------------------------|
| dagster-webserver  | 3000 | Dagster UI                 |
| dagster-daemon     | —    | Dagster scheduler/sensors  |
| postgres           | 5432 | Dagster metadata store     |

Open http://localhost:3000 to access the Dagster UI.

### 4. Configure the dbt Project

The dbt project lives under `../../transformation/dbt/`. Configure your warehouse connection in a `profiles.yml` and verify:

```bash
cd ../../transformation/dbt
dbt debug
dbt run
```

See the [dbt component README](../../transformation/dbt/README.md) for details.

## How the Pieces Connect

Dagster acts as the control plane:

1. **Dagster triggers Airbyte syncs** using the `dagster-airbyte` integration. Each Airbyte connection is represented as a Dagster asset.
2. **After syncs complete**, Dagster runs dbt models using the `dagster-dbt` integration. dbt models are also represented as Dagster assets with dependency tracking.
3. **Scheduling and sensors** in Dagster control when pipelines run — on a cron schedule, on new data arrival, or on demand.

## Local Development Workflow

```bash
# Start the stack
docker compose up -d

# Watch logs
docker compose logs -f

# Restart after code changes
docker compose restart dagster-webserver dagster-daemon

# Tear down
docker compose down
```

## Related Documentation

- [Dagster orchestration](../../orchestration/dagster/README.md)
- [dbt transformation](../../transformation/dbt/README.md)
- [Airbyte EL](../../extract_load/airbyte/README.md)
