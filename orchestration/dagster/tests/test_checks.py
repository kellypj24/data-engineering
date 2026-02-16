"""Tests for Dagster asset check definitions."""

import datetime
from unittest.mock import MagicMock, patch

from dagster import AssetKey, build_asset_context, DagsterInstance

from src.checks.freshness import freshness_check, FRESHNESS_THRESHOLD


class TestFreshnessCheck:
    """Tests for the asset freshness check."""

    def test_freshness_threshold_is_25_hours(self):
        """Default threshold should be 25 hours."""
        assert FRESHNESS_THRESHOLD == datetime.timedelta(hours=25)

    def test_passes_when_recently_materialized(self):
        """Check should pass when asset was materialized within threshold."""
        recent_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=1
        )

        mock_event = MagicMock()
        mock_event.timestamp = recent_time.timestamp()

        instance = DagsterInstance.ephemeral()
        context = build_asset_context(instance=instance)

        with patch.object(
            instance,
            "get_latest_materialization_events",
            return_value={AssetKey(["airbyte", "raw_orders"]): mock_event},
        ):
            result = freshness_check(context)
        assert result.passed is True

    def test_fails_when_stale(self):
        """Check should fail when asset exceeds threshold."""
        stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=30
        )

        mock_event = MagicMock()
        mock_event.timestamp = stale_time.timestamp()

        instance = DagsterInstance.ephemeral()
        context = build_asset_context(instance=instance)

        with patch.object(
            instance,
            "get_latest_materialization_events",
            return_value={AssetKey(["airbyte", "raw_orders"]): mock_event},
        ):
            result = freshness_check(context)
        assert result.passed is False

    def test_fails_when_never_materialized(self):
        """Check should fail when asset has never been materialized."""
        instance = DagsterInstance.ephemeral()
        context = build_asset_context(instance=instance)

        with patch.object(
            instance,
            "get_latest_materialization_events",
            return_value={AssetKey(["airbyte", "raw_orders"]): None},
        ):
            result = freshness_check(context)
        assert result.passed is False
        assert "never" in result.metadata["reason"].text.lower()
