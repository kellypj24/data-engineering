"""Airbyte sync flow.

Triggers an Airbyte connection sync via the Airbyte API and polls
for completion. Mirrors the Dagster airbyte asset pattern.
"""

import os
import time

import requests
from prefect import flow, get_run_logger, task

AIRBYTE_SERVER_URL = os.environ.get("AIRBYTE_SERVER_URL", "http://localhost:8006/api/public/v1")
AIRBYTE_CONNECTION_ID = os.environ.get("AIRBYTE_CONNECTION_ID", "your-airbyte-connection-uuid")


@task(name="trigger-airbyte-sync", retries=2, retry_delay_seconds=30)
def trigger_sync(server_url: str, connection_id: str) -> str:
    """Trigger an Airbyte connection sync and return the job ID."""
    logger = get_run_logger()
    logger.info(f"Triggering Airbyte sync for connection {connection_id}")

    response = requests.post(
        f"{server_url}/jobs",
        json={"connectionId": connection_id, "jobType": "sync"},
        timeout=30,
    )
    response.raise_for_status()
    job_id = response.json()["jobId"]
    logger.info(f"Sync job started: {job_id}")
    return str(job_id)


@task(name="poll-airbyte-sync", retries=1, retry_delay_seconds=60)
def poll_sync(server_url: str, job_id: str, poll_interval: int = 30, timeout: int = 3600) -> str:
    """Poll the Airbyte API until the sync job completes."""
    logger = get_run_logger()
    elapsed = 0

    while elapsed < timeout:
        response = requests.get(
            f"{server_url}/jobs/{job_id}",
            timeout=30,
        )
        response.raise_for_status()
        status = response.json()["status"]

        if status == "succeeded":
            logger.info(f"Sync job {job_id} completed successfully")
            return status
        elif status in ("failed", "cancelled"):
            raise RuntimeError(f"Sync job {job_id} {status}")

        logger.info(f"Sync job {job_id} status: {status}, waiting {poll_interval}s...")
        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Sync job {job_id} timed out after {timeout}s")


@flow(name="airbyte-sync", log_prints=True)
def airbyte_sync(
    server_url: str = AIRBYTE_SERVER_URL,
    connection_id: str = AIRBYTE_CONNECTION_ID,
) -> str:
    """Trigger and monitor an Airbyte connection sync."""
    job_id = trigger_sync(server_url=server_url, connection_id=connection_id)
    status = poll_sync(server_url=server_url, job_id=job_id)
    print(f"Airbyte sync completed with status: {status}")
    return status


if __name__ == "__main__":
    airbyte_sync()
