"""Tests for Dagster resource definitions."""

from src.resources.connections import RESOURCES


class TestResources:
    """Tests for the shared resource definitions."""

    def test_resources_dict_has_expected_keys(self):
        """RESOURCES should contain airbyte, dbt, and snowflake."""
        assert "airbyte" in RESOURCES
        assert "dbt" in RESOURCES
        assert "snowflake" in RESOURCES

    def test_resources_count(self):
        """Should have exactly 3 resources."""
        assert len(RESOURCES) == 3
