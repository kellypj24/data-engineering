"""Acceptance tests for the validation framework (E4): results and routing from
the example rule set (val_orders), behaviour changed purely through vars, and
validation_log accumulating across runs and purging beyond retention."""

import json

import duckdb
import pytest

TARGETS = "+validation_summary"

# record_id -> (failed_rules, max_failed_severity, validation_result)
ORDERS = [
    (1, "active", 10.0),  # passes every rule
    (2, "active", -5.0),  # HIGH: amount_non_negative
    (3, "bogus", 10.0),  # MEDIUM: status_known
    (4, "active", 5000.0),  # LOW: amount_under_review_limit
    (None, "active", 10.0),  # CRITICAL: order_id_present
]
EXPECTED = {
    "1": ("", None, "PASS"),
    "2": ("amount_non_negative", "HIGH", "FAIL"),
    "3": ("status_known", "MEDIUM", "FAIL"),
    "4": ("amount_under_review_limit", "LOW", "WARN"),
    None: ("order_id_present", "CRITICAL", "FAIL"),
}


def sql(project, statement, params=None):
    with duckdb.connect(str(project.db_path)) as con:
        return con.execute(statement, params or []).fetchall()


def run(project, **overrides):
    variables = {"dbt_env": "prod", **overrides}
    return project.dbt("run", "--select", TARGETS, "--vars", json.dumps(variables))


@pytest.fixture
def warehouse(project):
    # source('raw', ...) resolves to example_raw on duckdb in prod.
    sql(project, "CREATE SCHEMA example_raw")
    sql(
        project,
        "CREATE TABLE example_raw.orders "
        "(id INTEGER, status VARCHAR, amount DOUBLE, created_at TIMESTAMP)",
    )
    for order_id, status, amount in ORDERS:
        sql(
            project,
            "INSERT INTO example_raw.orders VALUES (?, ?, ?, TIMESTAMP '2026-01-01')",
            [order_id, status, amount],
        )
    assert project.dbt(
        "seed", "--select", "order_status_codes", "--vars", "{dbt_env: prod}"
    )
    return project


def results(project):
    rows = sql(
        project,
        "SELECT record_id, failed_rules, max_failed_severity, validation_result, "
        "should_notify, notification_channel FROM validation.val_orders",
    )
    return {r[0]: r[1:] for r in rows}


def test_results_severities_and_default_routing(warehouse):
    assert run(warehouse)
    got = results(warehouse)
    assert {k: v[:3] for k, v in got.items()} == EXPECTED

    notify = {k: v[3] for k, v in got.items()}
    assert notify == {"1": False, "2": True, "3": False, "4": False, None: True}

    channel = {k: v[4] for k, v in got.items()}
    assert channel == {
        "1": None,
        "2": "#data-alerts",
        "3": "#data-quality",
        "4": "#data-quality",
        None: "#data-alerts",
    }


def test_high_priority_escalates_medium_failures(warehouse):
    assert run(warehouse, high_priority_validations=["orders"])
    assert results(warehouse)["3"][3] is True


def test_channel_override_applies_to_failures_only(warehouse):
    assert run(
        warehouse, validation_configs={"orders": {"notification_channel": "#orders"}}
    )
    channels = {k: v[4] for k, v in results(warehouse).items()}
    assert channels["1"] is None
    assert {channels[k] for k in ("2", "3", "4", None)} == {"#orders"}


def test_notifications_can_be_disabled(warehouse):
    assert run(
        warehouse, validation_configs={"orders": {"notification_enabled": False}}
    )
    assert not any(v[3] for v in results(warehouse).values())


def test_disabled_validation_produces_no_rows(warehouse):
    assert run(warehouse, validation_configs={"orders": {"enabled": False}})
    assert results(warehouse) == {}


def test_unknown_config_key_is_rejected(warehouse):
    assert not run(warehouse, validation_configs={"orders": {"retention": 5}})


def test_log_accumulates_across_runs_and_purges_beyond_retention(warehouse):
    log = "validation.validation_log"
    assert run(warehouse)
    assert sql(warehouse, f"SELECT COUNT(*) FROM {log}") == [(4,)]
    assert run(warehouse)
    assert sql(warehouse, f"SELECT COUNT(*) FROM {log}") == [(8,)]

    # Age two copies of one failure: past the 90-day default, and within it.
    for key, days in (("aged-out", 100), ("recent", 10)):
        sql(
            warehouse,
            f"INSERT INTO {log} (validation_key, validation_name, source_table, "
            "record_id, validated_at, validation_result) VALUES (?, 'orders', "
            f"'stg_example', '2', CURRENT_TIMESTAMP - INTERVAL {days} DAY, 'FAIL')",
            [key],
        )
    assert run(warehouse)
    keys = {k for (k,) in sql(warehouse, f"SELECT validation_key FROM {log}")}
    assert "aged-out" not in keys
    assert "recent" in keys

    # Tightening retention is a config change, not a code change.
    assert run(warehouse, validation_configs={"orders": {"retention_days": 5}})
    keys = {k for (k,) in sql(warehouse, f"SELECT validation_key FROM {log}")}
    assert "recent" not in keys
    assert len(keys) == 16  # four runs x four failures, all from just now
