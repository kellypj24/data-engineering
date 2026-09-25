"""Chain link: launch ``transform_job`` when ``extract_job`` succeeds.

See src/jobs/chain.py for why jobs are chained rather than given staggered
crons. The run key is the upstream run id, so one extract success launches at
most one transform run, even if the sensor re-evaluates.
"""

from dagster import (
    DagsterRunStatus,
    RunRequest,
    RunStatusSensorContext,
    run_status_sensor,
)

from src.jobs.chain import extract_job, transform_job


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="transform_after_extract",
    monitored_jobs=[extract_job],
    request_job=transform_job,
    description="Launches transform_job when extract_job succeeds.",
)
def transform_after_extract(context: RunStatusSensorContext):
    return RunRequest(run_key=context.dagster_run.run_id)
