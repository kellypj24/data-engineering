"""Full ELT pipeline DAG.

Orchestrates the complete data pipeline:
  1. Airbyte sync (extract & load)
  2. dbt run (transform)
  3. dbt test (validate)
  4. Freshness check (SLA verification)

Uses TaskGroups for visual grouping in the Airflow UI.
Mirrors the Dagster full pipeline pattern.
"""

import os
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.task_group import TaskGroup

AIRBYTE_CONNECTION_ID = os.environ.get("AIRBYTE_CONNECTION_ID", "your-airbyte-connection-uuid")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")
FRESHNESS_THRESHOLD_HOURS = int(os.environ.get("FRESHNESS_THRESHOLD_HOURS", "25"))


@dag(
    dag_id="full_pipeline",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["elt", "pipeline"],
    doc_md=__doc__,
)
def full_pipeline():
    """Orchestrate the full ELT pipeline."""

    # Step 1: Extract & Load
    sync = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync",
        airbyte_conn_id="airbyte_default",
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=False,
        timeout=3600,
        wait_seconds=30,
    )

    # Step 2 & 3: Transform (grouped)
    with TaskGroup("dbt") as dbt_group:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --target {DBT_TARGET}",
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --target {DBT_TARGET}",
        )
        dbt_run >> dbt_test

    # Step 4: Freshness check
    @task(task_id="freshness_check")
    def freshness_check(**context):
        """Verify that the pipeline completed within the SLA threshold."""
        dag_run = context["dag_run"]
        start_time = dag_run.start_date
        now = datetime.now(timezone.utc)
        elapsed = now - start_time

        threshold = timedelta(hours=FRESHNESS_THRESHOLD_HOURS)
        if elapsed > threshold:
            raise ValueError(
                f"Pipeline exceeded freshness threshold: {elapsed} > {threshold}"
            )
        return {"elapsed_hours": round(elapsed.total_seconds() / 3600, 2)}

    check = freshness_check()

    sync >> dbt_group >> check


full_pipeline()
