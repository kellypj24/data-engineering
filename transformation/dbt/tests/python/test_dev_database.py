"""Personal development databases (E31): the clone's name, the dev target's
identity-derived defaults, and a production target with no defaults."""

import json

import pytest
from dbt.cli.main import dbtRunner

SNOWFLAKE_ENV = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_AUTHENTICATOR",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
)


def invoke(project, *args) -> tuple[bool, list[str]]:
    messages: list[str] = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        [*args, "--project-dir", str(project.path)]
    )
    return result.success, messages


@pytest.fixture
def clean_env(monkeypatch):
    for name in SNOWFLAKE_ENV:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_dry_run_prints_source_and_derived_target(project):
    ok, messages = invoke(
        project, "run-operation", "refresh_dev_database", "--args",
        json.dumps({"username": "jdoe"}),
    )
    assert ok
    log = "\n".join(messages)
    assert "DRY RUN" in log
    assert "ANALYTICS_PROD -> ANALYTICS_PROD_JDOE" in log
    assert "CREATE OR REPLACE DATABASE ANALYTICS_PROD_JDOE CLONE ANALYTICS_PROD" in log


def test_username_is_required_off_snowflake(project):
    ok, _ = invoke(project, "run-operation", "refresh_dev_database")
    assert not ok


def test_dev_target_defaults_to_the_personal_clone(project, clean_env):
    clean_env.setenv("DBT_TARGET", "snowflake")
    clean_env.setenv("SNOWFLAKE_ACCOUNT", "example-account")
    clean_env.setenv("SNOWFLAKE_USER", "jdoe")
    (project.path / "macros" / "test_log_target.sql").write_text(
        "{% macro test_log_target() %}"
        "{{ log(target.database ~ '|' ~ target.schema, info=true) }}"
        "{% endmacro %}"
    )
    ok, messages = invoke(project, "run-operation", "test_log_target")
    assert ok
    assert messages == ["ANALYTICS_PROD_JDOE|JDOE_DEV"]


@pytest.mark.parametrize("missing", ["SNOWFLAKE_DATABASE", "SNOWFLAKE_ROLE", "SNOWFLAKE_PRIVATE_KEY_PATH"])
def test_prod_target_fails_when_any_variable_is_unset(project, clean_env, missing):
    clean_env.setenv("DBT_TARGET", "snowflake_prod")
    for name in SNOWFLAKE_ENV:
        if name not in (missing, "SNOWFLAKE_AUTHENTICATOR"):
            clean_env.setenv(name, "set")
    ok, _ = invoke(project, "parse")
    assert not ok


def test_prod_target_resolves_when_everything_is_set(project, clean_env):
    clean_env.setenv("DBT_TARGET", "snowflake_prod")
    for name in SNOWFLAKE_ENV:
        if name != "SNOWFLAKE_AUTHENTICATOR":
            clean_env.setenv(name, "set")
    ok, _ = invoke(project, "parse")
    assert ok
