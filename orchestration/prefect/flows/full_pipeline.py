"""Full ELT pipeline flow.

Orchestrates the complete data pipeline:
  1. Airbyte sync (extract & load)
  2. dbt transform (deps + build)
  3. Freshness check (SLA verification)

Mirrors the Dagster full pipeline pattern using Prefect subflows.
"""

import os
from datetime import datetime, timedelta, timezone

from prefect import flow, get_run_logger, task

from flows.airbyte_sync import airbyte_sync
from flows.dbt_transform import dbt_transform

FRESHNESS_THRESHOLD_HOURS = int(os.environ.get("FRESHNESS_THRESHOLD_HOURS", "25"))


@task(name="freshness-check")
def freshness_check(
    pipeline_start: datetime, threshold_hours: int = FRESHNESS_THRESHOLD_HOURS
) -> dict:
    """Verify that the pipeline completed within the SLA threshold."""
    logger = get_run_logger()
    now = datetime.now(timezone.utc)
    elapsed = now - pipeline_start
    threshold = timedelta(hours=threshold_hours)

    passed = elapsed <= threshold
    result = {
        "passed": passed,
        "elapsed_hours": round(elapsed.total_seconds() / 3600, 2),
        "threshold_hours": threshold_hours,
    }

    if not passed:
        logger.warning(f"Freshness check FAILED: {elapsed} > {threshold}")
        raise ValueError(
            f"Pipeline exceeded freshness threshold: {elapsed} > {threshold}"
        )

    logger.info(f"Freshness check passed: {elapsed} < {threshold}")
    return result


@flow(name="full-pipeline", log_prints=True)
def full_pipeline() -> dict:
    """Orchestrate the full ELT pipeline."""
    pipeline_start = datetime.now(timezone.utc)

    # Step 1: Extract & Load
    airbyte_sync()

    # Step 2: Transform
    dbt_transform()

    # Step 3: Freshness check
    result = freshness_check(pipeline_start=pipeline_start)
    print(f"Full pipeline completed: {result}")
    return result


if __name__ == "__main__":
    full_pipeline()
