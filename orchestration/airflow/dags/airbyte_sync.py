"""Airbyte sync DAG.

Triggers an Airbyte connection sync on a daily schedule.
Mirrors the Dagster airbyte asset pattern.
"""

import os

import pendulum
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

AIRBYTE_CONNECTION_ID = os.environ.get(
    "AIRBYTE_CONNECTION_ID", "your-airbyte-connection-uuid"
)


@dag(
    dag_id="airbyte_sync",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["extract", "airbyte"],
    doc_md=__doc__,
)
def airbyte_sync():
    """Trigger an Airbyte connection sync."""
    AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id="airbyte_default",
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=False,
        timeout=3600,
        wait_seconds=30,
    )


airbyte_sync()
