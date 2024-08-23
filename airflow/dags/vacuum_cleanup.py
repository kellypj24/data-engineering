from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import time
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from operators.postgres_connection import connect_to_db  # Updated import path

# Create a logger
logger = LoggingMixin().log

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "vacuum_full_analyze",
    default_args=default_args,
    description="A DAG to perform VACUUM FULL ANALYZE on PostgreSQL tables",
    schedule_interval="0 5 * * 6",  # Run at 5 AM UTC every Saturday (midnight ET)
    max_active_runs=1,
    catchup=False,
)


def get_vacuum_commands(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 'VACUUM FULL ANALYZE ' || quote_ident(schemaname) || '.' || quote_ident(relname) || ';' AS vacuum_command,
            schemaname,
            relname,
            n_dead_tup,
            n_live_tup,
            last_vacuum,
            last_autovacuum
            FROM pg_stat_user_tables
            WHERE schemaname IN ('raw', 'airbyte_internal', 'staging', 'intermediate', 'marts')
            AND n_dead_tup > 0
            ORDER BY n_dead_tup DESC;
        """
        )
        return cur.fetchall()


def is_vacuum_running(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM pg_stat_activity
            WHERE state != 'idle'
            AND query ILIKE '%VACUUM%'
            AND query NOT ILIKE '%pg_stat_activity%';
        """
        )
        return cur.fetchone()[0] > 0


def run_vacuum_commands(conn, commands):
    total_commands = len(commands)
    for index, command in enumerate(commands, start=1):
        (
            vacuum_sql,
            schema,
            table,
            dead_tuples,
            live_tuples,
            last_vacuum,
            last_autovacuum,
        ) = command

        logger.info(f"Processing table {index}/{total_commands}: {schema}.{table}")
        logger.info(f"Dead tuples: {dead_tuples}, Live tuples: {live_tuples}")
        logger.info(f"Last vacuum: {last_vacuum}, Last autovacuum: {last_autovacuum}")

        while is_vacuum_running(conn):
            logger.info("A VACUUM operation is currently running. Waiting...")
            time.sleep(60)  # Wait for 1 minute before checking again

        logger.info(f"Executing: {vacuum_sql}")
        start_time = time.time()
        with conn.cursor() as cur:
            cur.execute(vacuum_sql)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Completed: {vacuum_sql}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(
            f"Progress: {index}/{total_commands} ({index/total_commands*100:.2f}% complete)"
        )
        logger.info("--------------------")


def vacuum_task():
    logger.info("Starting VACUUM FULL ANALYZE process")
    conn = connect_to_db()  # Use the imported connection function
    try:
        vacuum_commands = get_vacuum_commands(conn)
        total_tables = len(vacuum_commands)
        logger.info(f"Found {total_tables} tables to vacuum")
        run_vacuum_commands(conn, vacuum_commands)
        logger.info("VACUUM FULL ANALYZE process completed successfully")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise AirflowException(f"VACUUM FULL ANALYZE process failed: {str(e)}")
    finally:
        conn.close()


vacuum_operator = PythonOperator(
    task_id="vacuum_full_analyze",
    python_callable=vacuum_task,
    dag=dag,
)

vacuum_operator
