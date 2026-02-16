"""Tests for Dagster schedule definitions."""

from dagster import DefaultScheduleStatus

from src.schedules.daily import daily_asset_schedule


class TestDailySchedule:
    """Tests for the daily asset schedule."""

    def test_cron_schedule(self):
        """Schedule should run daily at 06:00 UTC."""
        assert daily_asset_schedule.cron_schedule == "0 6 * * *"

    def test_default_status_is_stopped(self):
        """Schedule should be STOPPED by default (opt-in activation)."""
        assert daily_asset_schedule.default_status == DefaultScheduleStatus.STOPPED

    def test_schedule_has_name(self):
        """Schedule should have a descriptive name."""
        assert daily_asset_schedule.name == "daily_asset_schedule"
