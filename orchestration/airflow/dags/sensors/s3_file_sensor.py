"""S3 file arrival sensor DAG.

Polls an S3 bucket/prefix for new files and triggers downstream processing
when a file is detected. Mirrors the Dagster S3 sensor pattern.
"""

import os

import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

S3_BUCKET = os.environ.get("S3_BUCKET", "my-data-lake-landing")
S3_PREFIX = os.environ.get("S3_PREFIX", "inbound/")


@dag(
    dag_id="s3_file_sensor",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["sensor", "s3"],
    doc_md=__doc__,
)
def s3_file_sensor():
    """Wait for a file in S3 and process it."""

    wait_for_file = S3KeySensor(
        task_id="wait_for_s3_file",
        aws_conn_id="aws_default",
        bucket_name=S3_BUCKET,
        bucket_key=f"{S3_PREFIX}*",
        wildcard_match=True,
        poke_interval=60,
        timeout=3600,
        mode="poke",
    )

    @task(task_id="process_file")
    def process_file():
        """Process the detected S3 file.

        In production, this would trigger dbt, move the file to a
        processed prefix, or kick off downstream pipelines.
        """
        return {"status": "processed", "bucket": S3_BUCKET, "prefix": S3_PREFIX}

    wait_for_file >> process_file()


s3_file_sensor()
