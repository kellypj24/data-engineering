"""Tests for Dagster asset definitions."""

from src.assets.airbyte import airbyte_assets, AIRBYTE_CONNECTION_ID, DESTINATION_TABLES


class TestAirbyteAssets:
    """Tests for Airbyte asset definitions."""

    def test_airbyte_assets_exist(self):
        """Verify airbyte_assets is a non-empty list."""
        assert isinstance(airbyte_assets, list)
        assert len(airbyte_assets) > 0

    def test_airbyte_assets_cover_destination_tables(self):
        """Asset keys should include all destination tables."""
        all_keys = set()
        for asset_def in airbyte_assets:
            for key in asset_def.keys:
                all_keys.add(key.path[-1])
        for table in DESTINATION_TABLES:
            assert table in all_keys, f"Table '{table}' not found in asset keys"

    def test_connection_id_is_placeholder(self):
        """Connection ID should be set (placeholder for now)."""
        assert AIRBYTE_CONNECTION_ID is not None
        assert isinstance(AIRBYTE_CONNECTION_ID, str)
