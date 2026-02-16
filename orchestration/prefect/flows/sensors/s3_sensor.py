"""S3 file arrival sensor flow.

Polls an S3 bucket/prefix for new files. When a new file is detected,
triggers downstream processing. Mirrors the Dagster S3 sensor pattern.

Deploy as a long-running flow with a schedule or use as a subflow.
"""

import os
import time

import boto3
from prefect import flow, get_run_logger, task

S3_BUCKET = os.environ.get("S3_BUCKET", "my-data-lake-landing")
S3_PREFIX = os.environ.get("S3_PREFIX", "inbound/")


@task(name="check-s3-for-new-files")
def check_s3_for_new_files(
    bucket: str,
    prefix: str,
    last_key: str = "",
) -> list[str]:
    """Check S3 for files newer than last_key."""
    logger = get_run_logger()

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        StartAfter=last_key,
    )

    contents = response.get("Contents", [])
    new_keys = [
        obj["Key"]
        for obj in sorted(contents, key=lambda o: o["Key"])
        if obj["Key"] > last_key
    ]

    logger.info(f"Found {len(new_keys)} new file(s) in s3://{bucket}/{prefix}")
    return new_keys


@task(name="process-s3-file")
def process_s3_file(bucket: str, key: str) -> dict:
    """Process a detected S3 file.

    In production, this would trigger dbt, move the file to a
    processed prefix, or kick off downstream pipelines.
    """
    logger = get_run_logger()
    logger.info(f"Processing s3://{bucket}/{key}")
    return {"status": "processed", "bucket": bucket, "key": key}


@flow(name="s3-file-sensor", log_prints=True)
def s3_file_sensor(
    bucket: str = S3_BUCKET,
    prefix: str = S3_PREFIX,
    poll_interval: int = 60,
    max_polls: int = 60,
) -> list[dict]:
    """Poll S3 for new files and process each one."""
    last_key = ""
    all_results = []

    for _ in range(max_polls):
        new_keys = check_s3_for_new_files(
            bucket=bucket, prefix=prefix, last_key=last_key,
        )

        if new_keys:
            for key in new_keys:
                result = process_s3_file(bucket=bucket, key=key)
                all_results.append(result)
            last_key = new_keys[-1]
            print(f"Processed {len(new_keys)} file(s), cursor at: {last_key}")
            break

        time.sleep(poll_interval)

    return all_results


if __name__ == "__main__":
    s3_file_sensor()
