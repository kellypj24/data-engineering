"""Daily dbt docs publish at 07:00 UTC.

A cron of its own, not a link in the extract -> transform chain: the docs
describe the dbt project at the deployed commit, which does not depend on
today's data having loaded.
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from src.jobs.dbt_docs import publish_dbt_docs_job
from src.utils.deployment import current_deployment, deployment_tags

dbt_docs_schedule = ScheduleDefinition(
    name="dbt_docs_schedule",
    cron_schedule="0 7 * * *",
    execution_timezone="UTC",
    job=publish_dbt_docs_job,
    default_status=DefaultScheduleStatus.STOPPED,
    tags=deployment_tags(current_deployment()),
    description="Publishes the versioned dbt docs site daily at 07:00 UTC.",
)
