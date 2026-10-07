"""clone_database / refresh_environment (E10). Snowflake extra: on duckdb only
the dry run and the refusals execute; the grant re-application is checked by
building the statements from literal grants."""

import json

from dbt.cli.main import dbtRunner


def run_operation(project, macro, args) -> tuple[bool, list[str]]:
    messages: list[str] = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        [
            "run-operation",
            macro,
            "--project-dir",
            str(project.path),
            "--args",
            json.dumps(args),
        ]
    )
    return result.success, messages


def test_dry_run_logs_the_clone(project):
    ok, messages = run_operation(project, "refresh_environment", {"env": "stage"})
    assert ok
    log = "\n".join(messages)
    assert "DRY RUN" in log
    assert "CREATE OR REPLACE DATABASE ANALYTICS_STAGE CLONE ANALYTICS_PROD" in log


def test_refuses_prod_by_env_and_by_name(project):
    assert not run_operation(project, "refresh_environment", {"env": "prod"})[0]
    ok, _ = run_operation(
        project,
        "clone_database",
        {"source": "ANALYTICS_STAGE", "target": "analytics_prod"},
    )
    assert not ok


def test_refuses_an_env_mapped_to_the_prod_database(project):
    config = project.path / "dbt_project.yml"
    config.write_text(
        config.read_text().replace("stage: ANALYTICS_STAGE", "stage: ANALYTICS_PROD")
    )
    assert not run_operation(project, "refresh_environment", {"env": "stage"})[0]


def test_refuses_unknown_env_and_same_source_and_target(project):
    assert not run_operation(project, "refresh_environment", {"env": "qa"})[0]
    ok, _ = run_operation(
        project, "clone_database", {"source": "X", "target": "x", "dry_run": True}
    )
    assert not ok


def test_executing_needs_snowflake(project):
    ok, _ = run_operation(
        project, "refresh_environment", {"env": "stage", "dry_run": False}
    )
    assert not ok


def test_grants_are_reapplied_with_ownership_last(project):
    (project.path / "macros" / "test_log_clone_statements.sql").write_text(
        """
{% macro test_log_clone_statements() %}
    {% set grants = [
        {'privilege': 'OWNERSHIP', 'grantee_type': 'ROLE', 'grantee': 'ANALYTICS_STAGE_OWNER'},
        {'privilege': 'USAGE', 'grantee_type': 'ROLE', 'grantee': 'ANALYTICS_STAGE_READ'},
    ] %}
    {% for statement in clone_database_statements('ANALYTICS_PROD', 'ANALYTICS_STAGE', grants) %}
        {{ log(statement, info=true) }}
    {% endfor %}
{% endmacro %}
"""
    )
    ok, messages = run_operation(project, "test_log_clone_statements", {})
    assert ok
    assert messages == [
        "CREATE OR REPLACE DATABASE ANALYTICS_STAGE CLONE ANALYTICS_PROD",
        "GRANT USAGE ON DATABASE ANALYTICS_STAGE TO ROLE ANALYTICS_STAGE_READ",
        "GRANT OWNERSHIP ON DATABASE ANALYTICS_STAGE TO ROLE ANALYTICS_STAGE_OWNER COPY CURRENT GRANTS",
    ]
