"""Tests for Temporal activity definitions.

Tests each activity in isolation using ActivityEnvironment.
"""

import pytest
from temporalio.testing import ActivityEnvironment

from activities.example_activity import extract_data, load_data, transform_data


@pytest.fixture
def activity_env():
    """Create an ActivityEnvironment for testing."""
    return ActivityEnvironment()


@pytest.mark.asyncio
async def test_extract_data_returns_records(activity_env):
    """extract_data should return a list of records with the source name."""
    result = await activity_env.run(extract_data, "test-source")

    assert isinstance(result, list)
    assert len(result) == 3
    assert all(r["source"] == "test-source" for r in result)


@pytest.mark.asyncio
async def test_transform_data_uppercases_values(activity_env):
    """transform_data should uppercase the 'value' field."""
    raw_data = [
        {"id": 1, "value": "hello", "source": "test"},
        {"id": 2, "value": "world", "source": "test"},
    ]

    result = await activity_env.run(transform_data, raw_data)

    assert len(result) == 2
    assert result[0]["value"] == "HELLO"
    assert result[1]["value"] == "WORLD"
    assert all(r["transformed"] is True for r in result)


@pytest.mark.asyncio
async def test_load_data_returns_summary(activity_env):
    """load_data should return a summary string with record count."""
    data = [{"id": 1}, {"id": 2}, {"id": 3}]

    result = await activity_env.run(load_data, data)

    assert "3 records" in result
    assert "successfully" in result.lower()
