# Airflow Orchestration

DAG-based pipeline orchestration with [Apache Airflow](https://airflow.apache.org/docs/).

## Setup

```bash
# Start Airflow (Postgres, webserver, scheduler)
docker compose up -d

# Wait for initialization to complete, then open the UI
# http://localhost:8080 (admin / admin)

# Place DAGs in the ./dags directory — they are mounted into the container
```

## DAG Patterns

- **TaskFlow API** — Use `@task` decorators for Python-native DAGs with implicit XCom passing.
- **Traditional operators** — Use `BashOperator`, `PythonOperator`, and provider operators for explicit task definitions.
- **Dynamic task mapping** — Use `.expand()` to fan out tasks over a list of inputs at runtime.
- **Task groups** — Use `TaskGroup` to visually and logically group related tasks.
- **Sensors** — Use `ExternalTaskSensor`, `S3KeySensor`, etc. to wait for external conditions.

## Comparison to Asset-Based Orchestration (Dagster)

| Consideration | Airflow | Dagster |
|---|---|---|
| Core model | DAG of tasks (imperative scheduling) | Graph of assets (declarative materialization) |
| Scheduling | Time-based cron schedules, sensors | Event-driven, cron, or asset-dependency triggers |
| Data lineage | Not built in; requires external tooling | First-class asset lineage and observability |
| Ecosystem | Massive provider ecosystem (1000+ operators) | Smaller but growing integration library |
| Maturity | Industry standard since 2015, very large community | Newer, opinionated toward modern data stack |
| Best for | Complex scheduling logic, large existing DAG codebases | Asset-centric pipelines, dbt-heavy workflows |

**Rule of thumb:** Use Airflow when you need mature scheduling with a vast operator ecosystem. Use Dagster when your pipeline thinking is asset-first and you want built-in lineage tracking.

## Links

- [Airflow documentation](https://airflow.apache.org/docs/)
- [TaskFlow API tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Provider packages](https://airflow.apache.org/docs/apache-airflow-providers/)
- [Docker Compose quickstart](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
