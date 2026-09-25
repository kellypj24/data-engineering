"""Tests for Dagster resource definitions."""

from src.resources.connections import RESOURCES


class TestResources:
    """Tests for the shared resource definitions."""

    def test_resources_dict_has_expected_keys(self):
        """RESOURCES should contain exactly airbyte, dbt, snowflake, telemetry."""
        assert set(RESOURCES) == {"airbyte", "dbt", "snowflake", "telemetry"}
