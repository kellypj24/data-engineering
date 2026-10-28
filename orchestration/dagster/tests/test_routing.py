"""Routing is resolved when the work runs (E37): a code server imported
without the deployment variable must not pin runs to non-prod."""

import importlib
import os
from contextlib import contextmanager
from unittest.mock import patch

import dagster as dg
import pytest

from src.jobs.boundary import identity_probe_job
from src.resources import connections
from src.resources.routed_dbt import routed_dbt_cli
from src.resources.routed_snowflake import RoutedSnowflakeResource

CONNECTED: list[dict] = []


class FakeCursor:
    def __init__(self, kwargs):
        self.kwargs = kwargs

    def execute(self, sql):
        pass

    def fetchone(self):
        return ("SVC", "ROLE", None, self.kwargs["database"], self.kwargs["warehouse"])


class FakeConnection:
    def __init__(self, kwargs):
        self.kwargs = kwargs

    def cursor(self):
        return FakeCursor(self.kwargs)

    def commit(self):
        pass

    def close(self):
        pass


def fake_connect(**kwargs):
    CONNECTED.append(kwargs)
    return FakeConnection(kwargs)


@pytest.fixture
def env(monkeypatch):
    for key in ("DAGSTER_CLOUD_DEPLOYMENT_NAME", "DEPLOYMENT", "DBT_TARGET"):
        monkeypatch.delenv(key, raising=False)
    for key in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"):
        monkeypatch.setenv(key, "x")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    CONNECTED.clear()
    return monkeypatch


def test_import_without_deployment_then_prod_run_connects_to_prod(env):
    importlib.reload(connections)  # the code server loads with no deployment
    env.setenv("DEPLOYMENT", "prod")  # the run worker has it
    with patch("snowflake.connector.connect", side_effect=fake_connect):
        result = identity_probe_job.execute_in_process(
            resources={"snowflake": connections.RESOURCES["snowflake"]}
        )
    assert result.success
    assert CONNECTED[0]["database"] == "ANALYTICS_PROD"
    assert CONNECTED[0]["warehouse"] == "TRANSFORMING"


def test_unset_deployment_at_run_time_still_fails_safe(env):
    with (
        patch("snowflake.connector.connect", side_effect=fake_connect),
        connections.RESOURCES["snowflake"].get_connection(),
    ):
        pass
    assert CONNECTED[0]["database"] == "ANALYTICS_DEV"


def test_warehouse_size_key_is_routed(env):
    env.setenv("DEPLOYMENT", "prod")
    resource = RoutedSnowflakeResource(
        account="a", user="u", password="p", warehouse_size="large"
    )
    with (
        patch("snowflake.connector.connect", side_effect=fake_connect),
        resource.get_connection(),
    ):
        pass
    assert CONNECTED[0]["warehouse"] == "TRANSFORMING_LARGE"


def test_configured_database_is_ignored(env):
    resource = RoutedSnowflakeResource(
        account="a", user="u", password="p", database="ANALYTICS_PROD"
    )
    with (
        patch("snowflake.connector.connect", side_effect=fake_connect),
        resource.get_connection(),
    ):
        pass
    assert CONNECTED[0]["database"] == "ANALYTICS_DEV"


def test_other_connectors_are_refused(env):
    resource = RoutedSnowflakeResource(
        account="a", user="u", password="p", connector="sqlalchemy"
    )
    with (
        pytest.raises(dg.Failure, match="default connector only"),
        resource.get_connection(),
    ):
        pass


class FakeDbt:
    def __init__(self):
        self.seen: dict = {}

    def cli(self, args, **kwargs):
        self.seen = {
            key: os.environ.get(key)
            for key in ("SNOWFLAKE_DATABASE", "SNOWFLAKE_WAREHOUSE")
        }
        return "invocation"


@contextmanager
def log_context():
    yield type("Context", (), {"log": dg.get_dagster_logger()})()


def test_dbt_on_the_deployed_target_writes_where_the_run_resolves(env):
    env.setenv("DBT_TARGET", "snowflake_prod")
    env.setenv("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")  # stale value in the env
    env.setenv("DEPLOYMENT", "prod")
    dbt = FakeDbt()
    with log_context() as context:
        assert routed_dbt_cli(context, dbt, ["build"]) == "invocation"
    assert dbt.seen == {
        "SNOWFLAKE_DATABASE": "ANALYTICS_PROD",
        "SNOWFLAKE_WAREHOUSE": "TRANSFORMING",
    }
    assert os.environ["SNOWFLAKE_DATABASE"] == "ANALYTICS_DEV"  # restored
    assert "SNOWFLAKE_WAREHOUSE" not in os.environ


@pytest.mark.parametrize("target", ["snowflake", "duckdb", None])
def test_every_target_is_routed(env, target):
    """Non-prod mirrors prod: the personal `snowflake` dev target run through
    Dagster also writes where the deployment routes, not to a personal clone."""
    if target:
        env.setenv("DBT_TARGET", target)
    dbt = FakeDbt()
    with log_context() as context:
        routed_dbt_cli(context, dbt, ["build"])
    assert dbt.seen == {
        "SNOWFLAKE_DATABASE": "ANALYTICS_DEV",
        "SNOWFLAKE_WAREHOUSE": "TRANSFORMING",
    }
