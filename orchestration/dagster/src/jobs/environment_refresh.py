"""Weekly refresh of the shared stage environment from production.

Runs the dbt macro ``refresh_environment`` (transformation/dbt/macros/
snowflake/clone_database.sql): a zero-copy clone of the production database
over the stage database, with the stage database's own grants re-applied. The
macro refuses to target production. Snowflake only; the schedule is STOPPED
until you turn it on.
"""

import json

from dagster import (
    DefaultScheduleStatus,
    OpExecutionContext,
    ScheduleDefinition,
    job,
    op,
)
from dagster_dbt import DbtCliResource

from src.utils.deployment import current_deployment, deployment_tags

REFRESHED_ENVIRONMENT = "stage"


def refresh_args(env: str) -> list[str]:
    return [
        "run-operation",
        "refresh_environment",
        "--args",
        json.dumps({"env": env, "dry_run": False}),
    ]


@op
def refresh_stage_environment(context: OpExecutionContext, dbt: DbtCliResource):
    dbt.cli(refresh_args(REFRESHED_ENVIRONMENT)).wait()
    context.log.info(f"refreshed {REFRESHED_ENVIRONMENT} from prod")


@job(
    description="Zero-copy clone of prod over stage (dbt refresh_environment).",
    tags=deployment_tags(current_deployment()),
)
def refresh_stage_environment_job():
    refresh_stage_environment()


refresh_stage_environment_schedule = ScheduleDefinition(
    name="refresh_stage_environment_schedule",
    cron_schedule="0 3 * * 0",  # Sundays 03:00 UTC
    execution_timezone="UTC",
    job=refresh_stage_environment_job,
    default_status=DefaultScheduleStatus.STOPPED,
    tags=deployment_tags(current_deployment()),
)
