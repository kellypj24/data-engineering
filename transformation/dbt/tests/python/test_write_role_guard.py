"""Pre-run write-role guard (E44). CURRENT_ROLE() needs Snowflake, so the rule
(write_role_check) is driven with literal inputs; on duckdb the hook itself
must run and do nothing."""

import json

import pytest
from dbt.cli.main import dbtRunner

SHARED = ["ANALYTICS_PROD", "ANALYTICS_STAGE", "ANALYTICS_DEV"]

CHECK_MACRO = """
{% macro test_write_role_check(database, role, command, allowlist) %}
    {% set check = write_role_check(database, role, command, var('environment_databases').values() | list, allowlist) %}
    {{ log('VERDICT ' ~ check.verdict, info=true) }}
    {{ log('REASON ' ~ check.reason, info=true) }}
{% endmacro %}
"""


def check(project, database, role, command, allowlist=None) -> tuple[str, str]:
    (project.path / "macros" / "test_write_role_check.sql").write_text(CHECK_MACRO)
    messages: list[str] = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    args = {"database": database, "role": role, "command": command, "allowlist": allowlist or {}}
    result = dbtRunner(callbacks=[collect]).invoke(
        ["run-operation", "test_write_role_check", "--project-dir", str(project.path), "--args", json.dumps(args)]
    )
    assert result.success
    verdict = next(m.removeprefix("VERDICT ") for m in messages if m.startswith("VERDICT "))
    reason = next(m.removeprefix("REASON ") for m in messages if m.startswith("REASON "))
    return verdict, reason


@pytest.mark.parametrize("command", ["run", "build", "seed", "snapshot"])
def test_personal_role_against_shared_database_fails(project, command):
    verdict, reason = check(project, "analytics_stage", "dev_alice", command)
    assert verdict == "fail"
    assert "ANALYTICS_STAGE_OWNER" in reason


def test_owner_role_passes_case_insensitively(project):
    assert check(project, "ANALYTICS_STAGE", "analytics_stage_owner", "build")[0] == "pass"


def test_another_environments_owner_fails(project):
    assert check(project, "ANALYTICS_STAGE", "ANALYTICS_PROD_OWNER", "run")[0] == "fail"


def test_allowlist_per_database_and_wildcard(project):
    allowlist = {"analytics_stage": ["stage_breakglass"], "*": ["sysadmin"]}
    assert check(project, "ANALYTICS_STAGE", "STAGE_BREAKGLASS", "run", allowlist)[0] == "pass"
    assert check(project, "ANALYTICS_PROD", "SYSADMIN", "run", allowlist)[0] == "pass"
    assert check(project, "ANALYTICS_PROD", "STAGE_BREAKGLASS", "run", allowlist)[0] == "fail"


def test_personal_database_is_skipped(project):
    assert check(project, "ANALYTICS_PROD_ALICE", "DEV_ALICE", "run")[0] == "skip"


@pytest.mark.parametrize("command", ["compile", "parse", "docs", "ls", "test", "run-operation"])
def test_non_writing_commands_are_skipped(project, command):
    assert check(project, "ANALYTICS_PROD", "DEV_ALICE", command)[0] == "skip"


def test_hook_is_a_no_op_on_duckdb(project):
    assert project.dbt("seed", "--select", "example_raw")
