"""Daily materialisation schedule.

Starts the extract -> transform chain once per day at 06:00 UTC. It targets
only the head, ``extract_job``; ``transform_job`` follows when extract succeeds
(src/sensors/chain.py), so no second cron guesses how long extract takes.

Customisation
-------------
* Change ``cron_schedule`` to any valid cron expression.
* Point ``job`` at the head of your chain. Never schedule a chained job.
* Keep ``default_status=STOPPED`` and turn the schedule on in the UI:
  ``tests/test_invariants.py`` fails on a schedule that starts itself.
"""

from dagster import DefaultScheduleStatus, ScheduleDefinition

from src.jobs.chain import extract_job
from src.utils.deployment import current_deployment, deployment_tags

# ---- Schedule definition ----------------------------------------------------
daily_asset_schedule = ScheduleDefinition(
    name="daily_asset_schedule",
    # Run every day at 06:00 UTC.
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    job=extract_job,
    default_status=DefaultScheduleStatus.STOPPED,
    # Shows each run's deployment and target database in the UI.
    tags=deployment_tags(current_deployment()),
    description="Starts the extract -> transform chain daily at 06:00 UTC.",
)
