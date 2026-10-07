"""Ownership drift (E12). Ownership is Snowflake-only, so the shared rule
(ownership_drift) and the repair (ownership_repair_statements) are driven with
literal catalog rows, including the full cycle: drift after a clone, repair,
no drift."""

import json

from dbt.cli.main import dbtRunner

CLONED_STAGE = [
    {"object_type": "SCHEMA", "name": '"ANALYTICS_STAGE"."MARTS"', "owner": "ANALYTICS_PROD_OWNER"},
    {"object_type": "TABLE", "name": '"ANALYTICS_STAGE"."MARTS"."FCT_ORDERS"', "owner": "ANALYTICS_PROD_OWNER"},
    {"object_type": "VIEW", "name": '"ANALYTICS_STAGE"."STAGING"."STG_ORDERS"', "owner": "analytics_stage_owner"},
]

CYCLE_MACRO = """
{% macro test_ownership_cycle(objects, can_create) %}
    {% set owner = 'ANALYTICS_STAGE_OWNER' %}
    {% set drift = ownership_drift(objects, can_create, owner) %}
    {% for statement in ownership_repair_statements(drift, 'ANALYTICS_STAGE', owner) %}
        {{ log('STATEMENT ' ~ statement, info=true) }}
    {% endfor %}
    {# Apply the repair to the literal catalog, then audit again. #}
    {% set repaired = [] %}
    {% for o in objects %}
        {% do repaired.append({'object_type': o.object_type, 'name': o.name, 'owner': owner}) %}
    {% endfor %}
    {{ log('DRIFT_BEFORE ' ~ drift | length, info=true) }}
    {{ log('DRIFT_AFTER ' ~ ownership_drift(repaired, true, owner) | length, info=true) }}
{% endmacro %}
"""


def run_operation(project, macro, args) -> tuple[bool, list[str]]:
    messages: list[str] = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        ["run-operation", macro, "--project-dir", str(project.path), "--args", json.dumps(args)]
    )
    return result.success, messages


def cycle(project, objects, can_create):
    (project.path / "macros" / "test_ownership_cycle.sql").write_text(CYCLE_MACRO)
    ok, messages = run_operation(
        project, "test_ownership_cycle", {"objects": objects, "can_create": can_create}
    )
    assert ok
    return messages


def test_clone_drifts_then_repair_clears_it(project):
    messages = cycle(project, CLONED_STAGE, can_create=False)
    assert "DRIFT_BEFORE 3" in messages  # two prod-owned objects + CREATE SCHEMA
    assert "DRIFT_AFTER 0" in messages
    assert [m for m in messages if m.startswith("STATEMENT ")] == [
        "STATEMENT GRANT CREATE SCHEMA ON DATABASE ANALYTICS_STAGE TO ROLE ANALYTICS_STAGE_OWNER",
        'STATEMENT GRANT OWNERSHIP ON SCHEMA "ANALYTICS_STAGE"."MARTS" TO ROLE ANALYTICS_STAGE_OWNER COPY CURRENT GRANTS',
        'STATEMENT GRANT OWNERSHIP ON TABLE "ANALYTICS_STAGE"."MARTS"."FCT_ORDERS" TO ROLE ANALYTICS_STAGE_OWNER COPY CURRENT GRANTS',
    ]


def test_owner_match_is_case_insensitive_and_clean_catalog_has_no_drift(project):
    clean = [dict(o, owner="analytics_stage_owner") for o in CLONED_STAGE]
    messages = cycle(project, clean, can_create=True)
    assert "DRIFT_BEFORE 0" in messages
    assert not [m for m in messages if m.startswith("STATEMENT ")]


def test_normalize_defaults_to_dry_run_and_never_executes_off_snowflake(project):
    ok, messages = run_operation(project, "normalize_object_ownership", {"database": "analytics_stage"})
    assert ok
    assert any("DRY RUN" in m and "ANALYTICS_STAGE_OWNER" in m for m in messages)
    assert not run_operation(
        project, "normalize_object_ownership", {"database": "analytics_stage", "dry_run": False}
    )[0]


def test_audit_needs_snowflake(project):
    assert not run_operation(project, "audit_object_ownership", {"database": "analytics_stage"})[0]


def test_refresh_dry_run_includes_ownership_normalization(project):
    ok, messages = run_operation(project, "refresh_environment", {"env": "stage"})
    assert ok
    assert any(m.startswith("normalize_object_ownership: DRY RUN") for m in messages)
