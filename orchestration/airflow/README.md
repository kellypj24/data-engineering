# Airflow Orchestration

DAG-based pipeline orchestration with [Apache Airflow](https://airflow.apache.org/docs/). Mirrors the Dagster reference implementation with equivalent Airbyte sync, dbt build, S3 sensor, daily schedule, and freshness check capabilities.

## DAGs

| DAG ID | Schedule | Description | Tags |
|---|---|---|---|
| `airbyte_sync` | `0 6 * * *` (daily 06:00 UTC) | Triggers an Airbyte connection sync | `extract`, `airbyte` |
| `dbt_transform` | `0 6 * * *` (daily 06:00 UTC) | Runs `dbt build` (models + tests) | `transform`, `dbt` |
| `full_pipeline` | `0 6 * * *` (daily 06:00 UTC) | Full ELT: Airbyte sync >> dbt run >> dbt test >> freshness check | `elt`, `pipeline` |
| `s3_file_sensor` | None (externally triggered) | Polls S3 for new files, triggers downstream processing | `sensor`, `s3` |

### full_pipeline

The main orchestration DAG composes the complete ELT workflow:

1. **Airbyte sync** -- Extract & load via `AirbyteTriggerSyncOperator`
2. **dbt run** -- Transform models (inside a `TaskGroup`)
3. **dbt test** -- Validate models (inside the same `TaskGroup`)
4. **Freshness check** -- Verify the pipeline completed within the SLA threshold (25 hours by default)

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `AIRBYTE_CONNECTION_ID` | `your-airbyte-connection-uuid` | Airbyte connection UUID to sync |
| `DBT_PROJECT_DIR` | `/opt/dbt` | Path to the dbt project directory |
| `DBT_TARGET` | `prod` | dbt target profile to use |
| `S3_BUCKET` | `my-data-lake-landing` | S3 bucket for file arrival sensor |
| `S3_PREFIX` | `inbound/` | S3 key prefix to watch for new files |
| `FRESHNESS_THRESHOLD_HOURS` | `25` | Maximum allowed pipeline duration (hours) |

### Airflow Connections

Configure these via environment variables or the Airflow UI:

| Connection ID | Variable | Purpose |
|---|---|---|
| `airbyte_default` | `AIRFLOW_CONN_AIRBYTE_DEFAULT` | Airbyte API endpoint |
| `aws_default` | `AIRFLOW_CONN_AWS_DEFAULT` | AWS credentials for S3 sensor |
| `snowflake_default` | `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` | Snowflake warehouse connection |

## Project Structure

```
orchestration/airflow/
├── dags/
│   ├── __init__.py
│   ├── airbyte_sync.py          # Airbyte sync DAG
│   ├── dbt_transform.py         # dbt build DAG
│   ├── full_pipeline.py         # Full ELT pipeline DAG
│   └── sensors/
│       ├── __init__.py
│       └── s3_file_sensor.py    # S3 file arrival sensor DAG
├── plugins/                     # Custom Airflow plugins
├── docker-compose.yml           # Full Airflow stack (webserver, scheduler, postgres)
├── Dockerfile                   # Custom image based on apache/airflow:2.8-python3.11
├── pyproject.toml               # Python dependencies
└── README.md
```

## Setup

### Docker Compose (quickstart)

```bash
# Start Airflow (Postgres, webserver, scheduler)
docker compose up -d

# Wait for initialization to complete, then open the UI
# http://localhost:8080 (admin / admin)
```

### Custom Docker Image

```bash
# Build the custom image with all providers and dbt
docker build -t airflow-custom .

# Use the custom image in docker-compose.yml by setting:
#   image: airflow-custom
```

### Local Development

```bash
# Install dependencies with uv
uv pip install ".[dev]"

# Run tests
uv run pytest

# Lint
uv run ruff check dags/
```

## Testing

```bash
just airflow::test     # or: cd orchestration/airflow && uv run pytest
```

Tests validate:
- DAG parsing (no import errors)
- DAG structure (no cycles, correct task dependencies)
- Task configuration (correct operator parameters)

## DAG Patterns

- **TaskFlow API** -- Use `@task` decorators for Python-native DAGs with implicit XCom passing.
- **Traditional operators** -- Use `BashOperator`, `PythonOperator`, and provider operators for explicit task definitions.
- **Dynamic task mapping** -- Use `.expand()` to fan out tasks over a list of inputs at runtime.
- **Task groups** -- Use `TaskGroup` to visually and logically group related tasks.
- **Sensors** -- Use `S3KeySensor`, `ExternalTaskSensor`, etc. to wait for external conditions.

## Comparison to Dagster Reference

| Feature | Dagster | Airflow Equivalent |
|---|---|---|
| Airbyte sync | `build_airbyte_assets` | `AirbyteTriggerSyncOperator` in `airbyte_sync` DAG |
| dbt build | `@dbt_assets` | `BashOperator("dbt build")` in `dbt_transform` DAG |
| S3 sensor | boto3 polling with cursor | `S3KeySensor` in `s3_file_sensor` DAG |
| Daily schedule | `ScheduleDefinition(cron="0 6 * * *")` | `schedule="0 6 * * *"` on each DAG |
| Freshness check | `FreshnessPolicy(maximum_lag_minutes=1500)` | `@task` with elapsed-time check in `full_pipeline` |
| Resources | `EnvVar()` resource config | `os.environ.get()` + Airflow connections |
| Asset lineage | First-class asset graph | TaskGroups for visual grouping (no native lineage) |

## Links

- [Airflow documentation](https://airflow.apache.org/docs/)
- [TaskFlow API tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Provider packages](https://airflow.apache.org/docs/apache-airflow-providers/)
- [Docker Compose quickstart](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
