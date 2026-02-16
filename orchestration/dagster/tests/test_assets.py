"""Tests for Dagster asset definitions."""

from src.assets.airbyte import airbyte_assets, AIRBYTE_CONNECTION_ID, DESTINATION_TABLES


class TestAirbyteAssets:
    """Tests for Airbyte asset definitions."""

    def test_airbyte_assets_exist(self):
        """Verify airbyte_assets is a non-empty list."""
        assert isinstance(airbyte_assets, list)
        assert len(airbyte_assets) > 0

    def test_airbyte_assets_match_destination_tables(self):
        """Each destination table should produce an asset."""
        assert len(airbyte_assets) == len(DESTINATION_TABLES)

    def test_connection_id_is_placeholder(self):
        """Connection ID should be set (placeholder for now)."""
        assert AIRBYTE_CONNECTION_ID is not None
        assert isinstance(AIRBYTE_CONNECTION_ID, str)
