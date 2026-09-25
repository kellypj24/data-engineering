"""Schedule definitions — re-exported from submodules."""

from src.schedules.daily import daily_asset_schedule
from src.schedules.dbt_docs import dbt_docs_schedule

all_schedules = [
    daily_asset_schedule,
    dbt_docs_schedule,
]
