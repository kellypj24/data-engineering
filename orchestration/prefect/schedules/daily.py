"""Schedule definitions for Prefect deployments.

Apply these schedules when creating Prefect deployments:

    full_pipeline.serve(
        name="daily-full-pipeline",
        cron=DAILY_SCHEDULE,
    )
"""

# Daily at 06:00 UTC — mirrors Dagster's daily_asset_schedule
DAILY_SCHEDULE = "0 6 * * *"
