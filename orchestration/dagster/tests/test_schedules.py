"""Tests for Dagster schedule definitions."""

import pytest
from dagster import DefaultScheduleStatus, ScheduleDefinition

from src.jobs.chain import extract_job
from src.schedules import BASE_SCHEDULES, DEPLOYMENT_CRONS, NOT_OVERRIDDEN
from src.schedules.daily import daily_asset_schedule
from src.schedules.dbt_docs import dbt_docs_schedule
from src.utils.deployment import schedules_for_deployment


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


OVERRIDES = {"stage": {"daily_asset_schedule": "0 9 * * *"}}


class TestSchedulesForDeployment:
    """Per-deployment cron overrides (E46)."""

    def test_unknown_or_missing_deployment_returns_base(self):
        for deployment in (None, "", "Stage", "qa"):
            assert (
                schedules_for_deployment(deployment, BASE_SCHEDULES, OVERRIDES)
                == BASE_SCHEDULES
            )

    def test_named_deployment_gets_the_override_and_keeps_every_other_field(self):
        stage = {
            s.name: s
            for s in schedules_for_deployment("stage", BASE_SCHEDULES, OVERRIDES)
        }
        moved = stage["daily_asset_schedule"]
        assert moved.cron_schedule == "0 9 * * *"
        for field in (
            "name",
            "job_name",
            "execution_timezone",
            "default_status",
            "tags",
            "description",
        ):
            assert getattr(moved, field) == getattr(daily_asset_schedule, field), field
        assert stage["dbt_docs_schedule"] is dbt_docs_schedule

    def test_override_for_a_nonexistent_schedule_raises(self):
        with pytest.raises(ValueError, match="renamed_schedule"):
            schedules_for_deployment(
                None, BASE_SCHEDULES, {"stage": {"renamed_schedule": "0 9 * * *"}}
            )


def _is_daily(cron: str) -> bool:
    minute, hour, *rest = cron.split()
    return minute.isdigit() and hour.isdigit() and rest == ["*", "*", "*"]


def daily_schedules_without_a_decision(
    schedules, overrides, not_overridden
) -> list[str]:
    decided = set(not_overridden).union(*overrides.values())
    return sorted(
        s.name
        for s in schedules
        if _is_daily(s.cron_schedule) and s.name not in decided
    )


def test_every_daily_schedule_is_overridden_or_listed():
    assert (
        daily_schedules_without_a_decision(
            BASE_SCHEDULES, DEPLOYMENT_CRONS, NOT_OVERRIDDEN
        )
        == []
    )


def test_a_new_daily_schedule_in_neither_list_fails_coverage():
    new = ScheduleDefinition(
        name="new_daily_schedule",
        cron_schedule="30 5 * * *",
        execution_timezone="UTC",
        job=extract_job,
    )
    assert daily_schedules_without_a_decision(
        [*BASE_SCHEDULES, new], DEPLOYMENT_CRONS, NOT_OVERRIDDEN
    ) == ["new_daily_schedule"]


def test_weekly_schedule_is_not_daily():
    assert not _is_daily("0 3 * * 0")
