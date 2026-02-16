"""Shared fixtures for Prefect tests.

Uses prefect_test_harness() at session scope to avoid per-test
SQLite overhead.  Patches get_run_logger so that tasks invoked via
``.fn()`` (outside a real flow run context) get a standard logger
instead of raising MissingContextError.
"""

import logging
from unittest.mock import patch

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def prefect_test_fixture():
    """Set up the Prefect test harness for all tests."""
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True)
def _mock_get_run_logger():
    """Provide a stdlib logger when tasks are called via .fn() outside a flow."""
    logger = logging.getLogger("prefect.test")
    with (
        patch("flows.airbyte_sync.get_run_logger", return_value=logger),
        patch("flows.dbt_transform.get_run_logger", return_value=logger),
        patch("flows.full_pipeline.get_run_logger", return_value=logger),
        patch("flows.sensors.s3_sensor.get_run_logger", return_value=logger),
    ):
        yield
