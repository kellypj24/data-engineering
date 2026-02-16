"""S3 file-arrival sensor.

Polls an S3 bucket/prefix for new objects and triggers a run whenever a
previously-unseen key appears.

Customisation
-------------
* Set ``S3_BUCKET`` and ``S3_PREFIX`` to your landing-zone location.
* Change ``target_asset_key`` to the asset that should be materialised when
  a new file lands.
* For production workloads consider using SQS-backed S3 event notifications
  instead of polling.
"""

import boto3
from dagster import (
    RunRequest,
    SensorEvaluationContext,
    sensor,
)

# ---- Configuration ---------------------------------------------------------
S3_BUCKET = "my-data-lake-landing"
S3_PREFIX = "inbound/"

# ---- Sensor definition ------------------------------------------------------

@sensor(
    name="s3_file_arrival_sensor",
    minimum_interval_seconds=60,
    description="Polls S3 for new files and triggers a run when one appears.",
)
def s3_file_arrival_sensor(context: SensorEvaluationContext):
    """Yield a RunRequest for each new S3 object discovered since the last tick."""

    s3 = boto3.client("s3")

    # Use the cursor to track the last-seen key so we only react to new files.
    last_key = context.cursor or ""

    response = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX,
        StartAfter=last_key,
    )

    contents = response.get("Contents", [])
    if not contents:
        return

    new_last_key = last_key
    for obj in sorted(contents, key=lambda o: o["Key"]):
        key = obj["Key"]
        if key <= last_key:
            continue

        context.log.info(f"New S3 object detected: s3://{S3_BUCKET}/{key}")
        yield RunRequest(
            run_key=key,
            run_config={
                "ops": {
                    "process_landing_file": {
                        "config": {
                            "s3_bucket": S3_BUCKET,
                            "s3_key": key,
                        }
                    }
                }
            },
        )
        new_last_key = key

    context.update_cursor(new_last_key)
