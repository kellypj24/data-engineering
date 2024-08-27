import json
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

# Create a logger
logger = LoggingMixin().log


def get_secret(secret_name, region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_string = get_secret_value_response["SecretString"]
        # Parse the JSON string
        secret_dict = json.loads(secret_string)
        # Return the value, not the entire dictionary
        return secret_dict[secret_name.split("/")[-1]]
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise AirflowException(f"Failed to retrieve secret {secret_name}: {str(e)}")


def get_postgres_secrets():
    return {
        "dbname": get_secret("airflow/variables/PG_DATABASE"),
        "username": get_secret("airflow/variables/PG_USER"),
        "password": get_secret("airflow/variables/PG_PASSWORD"),
        "host": get_secret("airflow/variables/PG_HOST"),
        "port": int(get_secret("airflow/variables/PG_PORT")),
    }


def diagnostic_check(conn_params):
    logger.info("Performing diagnostic checks on retrieved secrets:")
    for key, value in conn_params.items():
        if key != "password":
            logger.info(
                f"{key}: {'[NOT EMPTY]' if value else '[EMPTY]'} (type: {type(value)}, length: {len(str(value))})"
            )
        else:
            logger.info(
                f"{key}: {'[NOT EMPTY]' if value else '[EMPTY]'} (type: {type(value)}, length: {len(str(value)) if value else 0})"
            )

    logger.info(f"Port value: {conn_params['port']}")


def test_postgres_connection():
    conn_params = get_postgres_secrets()
    diagnostic_check(conn_params)

    logger.info("Attempting to connect to PostgreSQL...")
    try:
        with psycopg2.connect(
            dbname=conn_params["dbname"],
            user=conn_params["username"],
            password=conn_params["password"],
            host=conn_params["host"],
            port=conn_params["port"],
        ) as conn:
            with conn.cursor() as cur:
                logger.info(
                    "Successfully connected to PostgreSQL. Executing test query..."
                )
                cur.execute("SELECT 1")
                result = cur.fetchone()
                logger.info(f"Query result: {result}")
                if result[0] == 1:
                    logger.info("PostgreSQL connection test successful!")
                else:
                    raise AirflowException("Unexpected query result")
    except psycopg2.OperationalError as e:
        logger.error(f"OperationalError connecting to PostgreSQL: {str(e)}")
        raise AirflowException(f"PostgreSQL connection test failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error connecting to PostgreSQL: {str(e)}")
        raise AirflowException(f"PostgreSQL connection test failed: {str(e)}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "postgres_connection_test",
    default_args=default_args,
    description="A DAG to test PostgreSQL connection",
    schedule_interval=None,
    catchup=False,
)

test_connection_task = PythonOperator(
    task_id="test_postgres_connection",
    python_callable=test_postgres_connection,
    dag=dag,
)

test_connection_task
