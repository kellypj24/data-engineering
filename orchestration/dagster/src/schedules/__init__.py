"""Schedule definitions — re-exported from submodules.

Per-deployment times (src/utils/deployment.py, ``schedules_for_deployment``):
every daily schedule is either in ``DEPLOYMENT_CRONS`` or in
``NOT_OVERRIDDEN``, so a new one cannot silently keep prod's time in stage
(tests/test_schedules.py).
"""

from src.jobs.environment_refresh import refresh_stage_environment_schedule
from src.schedules.daily import daily_asset_schedule
from src.schedules.dbt_docs import dbt_docs_schedule
from src.utils.deployment import current_deployment, schedules_for_deployment

BASE_SCHEDULES = [
    daily_asset_schedule,
    dbt_docs_schedule,
    refresh_stage_environment_schedule,
]

# Deployment -> schedule name -> cron, replacing the base (prod) time there.
DEPLOYMENT_CRONS = {
    # Stage runs after prod's 06:00 extract has loaded the shared sources.
    "stage": {"daily_asset_schedule": "0 9 * * *"},
}

# Daily schedules that deliberately keep the same time in every deployment.
NOT_OVERRIDDEN = {
    # Docs describe the deployed commit, not today's data.
    "dbt_docs_schedule",
}

all_schedules = schedules_for_deployment(
    current_deployment(), BASE_SCHEDULES, DEPLOYMENT_CRONS
)
