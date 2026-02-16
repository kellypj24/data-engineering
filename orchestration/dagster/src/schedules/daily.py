"""Daily materialisation schedule.

Targets a selection of assets to be materialised once per day at 06:00 UTC.

Customisation
-------------
* Change ``cron_schedule`` to any valid cron expression.
* Update ``AssetSelection`` to target specific assets, groups, or tags.
* Use ``default_status=DefaultScheduleStatus.RUNNING`` to enable the
  schedule automatically on first load.
"""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
)

# ---- Schedule definition ----------------------------------------------------
daily_asset_schedule = ScheduleDefinition(
    name="daily_asset_schedule",
    # Run every day at 06:00 UTC.
    cron_schedule="0 6 * * *",
    # Materialise all assets in the "default" group.  Swap this for a more
    # targeted selection (e.g. AssetSelection.keys("my_asset")) as needed.
    target=AssetSelection.all(),
    default_status=DefaultScheduleStatus.STOPPED,
    description="Materialises all assets daily at 06:00 UTC.",
)
