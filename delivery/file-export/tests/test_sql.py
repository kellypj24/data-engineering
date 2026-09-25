"""SQL builder: literal rendering, filter operators, window semantics."""

import datetime as dt

import pytest

from file_export.config import ExportConfig, Filter, Output
from file_export.dialects import DuckDBDialect, SnowflakeDialect
from file_export.sql import (
    ResolvedWindow,
    build_select,
    last_sunday,
    literal,
    render_filter,
)
from tests.conftest import shared_config


@pytest.mark.parametrize(
    ("value", "rendered"),
    [
        (True, "TRUE"),
        (False, "FALSE"),
        (3, "3"),
        (1.5, "1.5"),
        ("it's", "'it''s'"),
        (None, "NULL"),
    ],
)
def test_literal(value, rendered):
    assert literal(value) == rendered


@pytest.mark.parametrize(
    ("spec", "sql"),
    [
        ({"column": "s", "in": ["a", True]}, "s IN ('a', TRUE)"),
        ({"column": "s", "not_in": [1]}, "(s NOT IN (1) OR s IS NULL)"),
        ({"column": "s", "ilike": "%x%"}, "s ILIKE '%x%'"),
        ({"column": "s", "not_null": True}, "s IS NOT NULL"),
    ],
)
def test_filter_operators(spec, sql):
    assert render_filter(Filter.model_validate(spec)) == sql


def config(**overrides):
    return ExportConfig.model_validate(shared_config(**overrides))


def test_exclusive_start_inclusive_end():
    c = config()
    sql = build_select(c, c.recipients[0], ResolvedWindow("2026-01-01", "2026-01-31"))
    assert "updated_at > '2026-01-01'" in sql
    assert "updated_at <= '2026-01-31'" in sql


def test_inclusive_start_inclusive_end():
    c = config(
        window={
            "column": "updated_at",
            "start_inclusive_end_inclusive": {"start": "2026-01-01", "end": "today"},
        }
    )
    sql = build_select(c, c.recipients[0], ResolvedWindow("2026-01-01", "2026-01-31"))
    assert "updated_at >= '2026-01-01'" in sql


def test_unbounded_start_has_no_lower_bound():
    c = config()
    sql = build_select(c, c.recipients[0], ResolvedWindow(None, "2026-01-31"))
    assert ">" not in sql.replace("<=", "")


def test_recipient_without_filters_is_selected_by_tenant_keys():
    c = config()
    assert "customer_id IN (101)" in build_select(
        c, c.recipients[0], ResolvedWindow(None, None)
    )


def test_recipient_filters_replace_the_default_tenant_filter():
    c = config(
        recipients=[
            {
                "name": "n",
                "tenant_keys": [101],
                "filters": [{"column": "region", "in": ["north"]}],
            }
        ]
    )
    sql = build_select(c, c.recipients[0], ResolvedWindow(None, None))
    assert "region IN ('north')" in sql
    assert "customer_id IN" not in sql


def test_computed_columns_get_window_literals_and_aliases():
    c = config(
        columns=[{"expr": "{range_start}", "alias": "a"}, {"name": "x", "alias": "y"}]
    )
    sql = build_select(c, c.recipients[0], ResolvedWindow(None, "2026-01-31"))
    assert "NULL AS a" in sql
    assert "x AS y" in sql


@pytest.mark.parametrize(
    ("today", "expected"),
    [
        (dt.date(2026, 3, 4), dt.date(2026, 3, 1)),
        (dt.date(2026, 3, 1), dt.date(2026, 2, 22)),
        (dt.date(2026, 3, 2), dt.date(2026, 3, 1)),
    ],
)
def test_last_sunday_is_strictly_before_today(today, expected):
    assert last_sunday(today) == expected


def test_duckdb_unload():
    sql = DuckDBDialect().unload(
        "SELECT 1", "/tmp/x.csv", Output(path="x", delimiter="|")
    )
    assert (
        sql
        == "COPY (SELECT 1) TO '/tmp/x.csv' (FORMAT csv, HEADER true, DELIMITER '|')"
    )


def test_snowflake_unload_statement():
    sql = SnowflakeDialect(stage="@exports").unload(
        "SELECT 1", "north/x.csv", Output(path="x")
    )
    assert sql.startswith("COPY INTO @exports/north/x.csv\nFROM (SELECT 1)")
    assert "SINGLE = TRUE" in sql
    assert "HEADER = TRUE" in sql
