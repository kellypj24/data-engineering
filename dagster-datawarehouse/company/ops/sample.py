import psycopg2
from dagster import get_dagster_logger, op


@op
def hello_redshift():
    """Example of a Dagster op that retrieves data from Redshift."""
    with psycopg2.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM dim_patient")
            row = cursor.fetchone()
            result = row[0] if row else None
    get_dagster_logger().info(f"Counted {result} patients")
