import os
from datetime import timedelta
import pendulum
from airflow import DAG
from operators.dbt import Dbt
from airflow.operators.python_operator import PythonOperator


def check_environment():
    import sys
    import subprocess

    print(f"Python version: {sys.version}")
    print(f"Python executable: {sys.executable}")
    print("Installed packages:")
    subprocess.run([sys.executable, "-m", "pip", "list"])
    print("Environment variables:")
    import os

    for key, value in os.environ.items():
        print(f"{key}={value}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    os.path.basename(__file__).replace(".py", ""),
    default_args=default_args,
    description="DAG for dbt processes",
    schedule_interval="30 7 * * *",  # Runs at 7:30 AM ET
    start_date=pendulum.datetime(2024, 1, 1, tz="America/New_York"),
    catchup=False,
) as dag:

    check_env = PythonOperator(
        task_id="check_environment", python_callable=check_environment, dag=dag
    )

    dbt_deps = Dbt(task_id="dbt_deps", command="deps", dag=dag)

    dbt_run = Dbt(task_id="dbt_run", command="run", dag=dag)

    check_env >> dbt_deps >> dbt_run
