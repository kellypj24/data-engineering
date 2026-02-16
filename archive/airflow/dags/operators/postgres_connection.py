import json
import boto3
import psycopg2
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log


def get_secret(secret_name, region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_string = get_secret_value_response["SecretString"]
        secret_dict = json.loads(secret_string)
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


def connect_to_db():
    conn_params = get_postgres_secrets()
    try:
        conn = psycopg2.connect(
            dbname=conn_params["dbname"],
            user=conn_params["username"],
            password=conn_params["password"],
            host=conn_params["host"],
            port=conn_params["port"],
        )
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise AirflowException(f"Failed to connect to PostgreSQL: {str(e)}")
