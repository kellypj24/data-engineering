"""Shared fixtures for Dagster tests."""

import json
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_airbyte_resource():
    """Mock AirbyteResource that doesn't require a real Airbyte server."""
    resource = MagicMock()
    resource.sync_and_poll.return_value = MagicMock(
        output=MagicMock(
            job=MagicMock(id="test-job-123"),
            attempts=[MagicMock(status="succeeded")],
        )
    )
    return resource


@pytest.fixture
def mock_dbt_resource():
    """Mock DbtCliResource that doesn't require a real dbt project."""
    resource = MagicMock()
    cli_output = MagicMock()
    cli_output.stream.return_value = iter([])
    resource.cli.return_value = cli_output
    return resource


@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client with configurable responses."""
    client = MagicMock()
    client.list_objects_v2.return_value = {"Contents": []}
    return client


@pytest.fixture
def minimal_dbt_manifest(tmp_path):
    """Create a minimal dbt manifest.json for testing."""
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json"
        },
        "nodes": {},
        "sources": {},
        "docs": {},
        "macros": {},
        "exposures": {},
        "selectors": {},
        "disabled": [],
        "child_map": {},
        "parent_map": {},
    }
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest))
    return manifest_path
