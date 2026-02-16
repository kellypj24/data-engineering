"""dbt transformation DAG.

Runs `dbt build` (models + tests) against the configured dbt project.
Mirrors the Dagster dbt asset pattern.
"""

import os

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/dbt")
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")


@dag(
    dag_id="dbt_transform",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["transform", "dbt"],
    doc_md=__doc__,
)
def dbt_transform():
    """Run dbt build: compile and test all models."""
    BashOperator(
        task_id="dbt_build",
        bash_command=f"dbt build --project-dir {DBT_PROJECT_DIR} --target {DBT_TARGET}",
    )


dbt_transform()
