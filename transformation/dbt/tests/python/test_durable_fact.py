"""Acceptance tests for the durable-fact pattern (E29): fct_daily_order_revenue
refuses --full-refresh, restates only its recent window, and ties to a control
total that cannot pass on an empty comparison."""

import duckdb
import pytest

MODEL = "fct_daily_order_revenue"
PROD = ["--vars", "{dbt_env: prod}"]
ORDERS = [
    (1, "active", 10.0, "2026-01-01 09:00"),
    (2, "active", 15.0, "2026-01-01 17:00"),
    (3, "pending", 20.0, "2026-01-02 09:00"),
    (4, "active", 25.0, "2026-01-03 09:00"),
    (5, "active", 30.0, "2026-01-04 09:00"),
    (6, "active", 35.0, "2026-01-05 09:00"),
]
# Complete days only: the control total never includes the open day (01-05).
CONTROL = [
    ("2026-01-01", 25.0),
    ("2026-01-02", 20.0),
    ("2026-01-03", 25.0),
    ("2026-01-04", 30.0),
]


# Where source('raw', ...) resolves on duckdb in prod: the example_raw schema
# (see models/staging/_sources.yml). These tests write it directly instead of
# seeding it, so each can shape the source data.


def sql(project, statement, params=None):
    with duckdb.connect(str(project.db_path)) as con:
        return con.execute(statement, params or []).fetchall()


@pytest.fixture
def warehouse(project):
    sql(project, "CREATE SCHEMA example_raw")
    sql(
        project,
        "CREATE TABLE example_raw.orders (id INTEGER, status VARCHAR, amount DOUBLE, created_at TIMESTAMP, customer_id INTEGER)",
    )
    sql(
        project,
        "CREATE TABLE example_raw.order_daily_totals (order_date DATE, revenue DOUBLE)",
    )
    for row in ORDERS:
        sql(
            project,
            "INSERT INTO example_raw.orders (id, status, amount, created_at) VALUES (?, ?, ?, ?)",
            list(row),
        )
    for row in CONTROL:
        sql(
            project,
            "INSERT INTO example_raw.order_daily_totals VALUES (?, ?)",
            list(row),
        )
    assert project.dbt("build", "--select", f"+{MODEL}", *PROD)
    return project


def fact(project):
    return sql(
        project,
        f"SELECT CAST(order_date AS VARCHAR), order_count, revenue FROM marts.{MODEL} ORDER BY 1",
    )


def test_full_refresh_keeps_history_the_source_no_longer_has(warehouse):
    before = fact(warehouse)
    assert len(before) == 5

    # The source ages out its oldest three days.
    sql(warehouse, "DELETE FROM example_raw.orders WHERE created_at < '2026-01-04'")
    assert warehouse.dbt("build", "--full-refresh", "--select", f"+{MODEL}", *PROD)

    assert fact(warehouse) == before


def test_only_the_restatement_window_is_reprocessed(warehouse):
    # A late correction on an old day and on a recent day, plus a new day.
    sql(warehouse, "UPDATE example_raw.orders SET amount = 99 WHERE id IN (1, 6)")
    sql(
        warehouse,
        "INSERT INTO example_raw.orders (id, status, amount, created_at) VALUES (7, 'active', 40, '2026-01-06 09:00')",
    )
    assert warehouse.dbt("run", "--select", f"+{MODEL}", *PROD)

    rows = {day: revenue for day, _, revenue in fact(warehouse)}
    assert rows["2026-01-01"] == 25.0, "day outside the window must stay final"
    assert rows["2026-01-05"] == 99.0, "day inside the window is restated"
    assert rows["2026-01-06"] == 40.0, "new day is appended"


@pytest.mark.parametrize(
    ("control_rows", "passes"),
    [
        pytest.param(CONTROL, True, id="matching"),
        pytest.param([], False, id="empty-control-fails"),
        pytest.param([("2026-01-02", 21.0)], False, id="mismatch"),
        pytest.param([("2025-12-31", 0.0)], False, id="day-missing-from-fact"),
    ],
)
def test_control_total(warehouse, control_rows, passes):
    sql(warehouse, "DELETE FROM example_raw.order_daily_totals")
    for row in control_rows:
        sql(
            warehouse,
            "INSERT INTO example_raw.order_daily_totals VALUES (?, ?)",
            list(row),
        )
    ok = warehouse.dbt(
        "test", "--select", f"{MODEL},test_name:ties_to_control_total", *PROD
    )
    assert ok is passes
