"""SQS sensor with redelivery-safe acknowledgement.

Polls a queue and yields one RunRequest per message. The default wiring reads
S3 event notifications, so it launches the same job as ``s3_sensor.py``
without listing the bucket.

Acknowledgement
---------------
* ``run_key`` is the SQS message id, so a redelivered message produces no
  second run.
* A message is deleted **one tick after** its RunRequest: the cursor holds the
  receipt handles of messages pending deletion, and the next tick deletes
  them. A crash between yielding the run and saving the cursor loses nothing
  (the message redelivers once its visibility timeout lapses) and duplicates
  nothing (the run_key was already used).
* A message whose body cannot be turned into run config is left on the queue;
  give the queue a redrive policy so it ends in a dead-letter queue.

The queue's visibility timeout must exceed two sensor intervals, or a message
redelivers before its deletion tick. A redelivery is harmless (same run_key),
only noisy.

Customisation
-------------
* Set ``SQS_QUEUE_URL`` to your queue. An empty URL raises when the code
  location loads.
* For another message format, call ``build_sqs_sensor`` with your own
  ``run_config_fn`` (message body -> run config).
"""

from __future__ import annotations

import json
from collections.abc import Callable

import boto3
from dagster import (
    DefaultSensorStatus,
    JobDefinition,
    RunRequest,
    SensorDefinition,
    SensorEvaluationContext,
    sensor,
)

from src.jobs.landing import process_landing_file_job

# ---- Configuration ---------------------------------------------------------
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789012/landing-events"
MAX_MESSAGES = 10  # SQS's per-receive maximum


def s3_event_run_config(body: str) -> dict:
    """Run config for ``process_landing_file_job`` from one S3 event
    notification. Raises unless the message carries exactly one record."""
    (record,) = json.loads(body)["Records"]
    return {
        "ops": {
            "process_landing_file": {
                "config": {
                    "s3_bucket": record["s3"]["bucket"]["name"],
                    "s3_key": record["s3"]["object"]["key"],
                }
            }
        }
    }


def build_sqs_sensor(
    *,
    name: str,
    queue_url: str,
    job: JobDefinition,
    run_config_fn: Callable[[str], dict],
    minimum_interval_seconds: int = 30,
) -> SensorDefinition:
    if not queue_url:
        raise ValueError(f"sensor {name}: queue_url is empty")

    @sensor(
        name=name,
        job=job,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=DefaultSensorStatus.STOPPED,
        description=f"One run per message on {queue_url}; deletes a message the tick after its run.",
    )
    def _sqs_sensor(context: SensorEvaluationContext):
        sqs = boto3.client("sqs")

        # 1. Acknowledge last tick's messages: their RunRequests are queued.
        pending: dict[str, str] = json.loads(context.cursor) if context.cursor else {}
        items = list(pending.items())
        for start in range(0, len(items), MAX_MESSAGES):
            batch = items[start : start + MAX_MESSAGES]
            response = sqs.delete_message_batch(
                QueueUrl=queue_url,
                Entries=[
                    {"Id": str(i), "ReceiptHandle": handle}
                    for i, (_, handle) in enumerate(batch)
                ],
            )
            for failed in response.get("Failed", []):
                # A stale handle (the message was received again); it is in
                # this tick's batch with a fresh one.
                context.log.warning(
                    f"delete failed for {batch[int(failed['Id'])][0]}: {failed.get('Message')}"
                )

        # 2. One RunRequest per new message; delete them on the next tick.
        messages = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=MAX_MESSAGES
        ).get("Messages", [])
        next_pending: dict[str, str] = {}
        for message in messages:
            try:
                run_config = run_config_fn(message["Body"])
            except Exception as exc:  # noqa: BLE001 -- leave it for the DLQ
                context.log.error(
                    f"message {message['MessageId']}: no run config ({exc}); left on the queue"
                )
                continue
            yield RunRequest(run_key=message["MessageId"], run_config=run_config)
            next_pending[message["MessageId"]] = message["ReceiptHandle"]

        context.update_cursor(json.dumps(next_pending))

    return _sqs_sensor


# ---- Sensor definition ------------------------------------------------------
sqs_landing_sensor = build_sqs_sensor(
    name="sqs_landing_sensor",
    queue_url=SQS_QUEUE_URL,
    job=process_landing_file_job,
    run_config_fn=s3_event_run_config,
)
