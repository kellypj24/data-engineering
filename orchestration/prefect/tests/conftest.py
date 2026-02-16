"""Shared fixtures for Prefect tests.

Uses prefect_test_harness() at session scope to avoid per-test
SQLite overhead.
"""

import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def prefect_test_fixture():
    """Set up the Prefect test harness for all tests."""
    with prefect_test_harness():
        yield
