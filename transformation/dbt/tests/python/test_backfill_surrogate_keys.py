"""Acceptance tests for the backfill_surrogate_keys run-operation (E2).

Drives dbt in-process against a throwaway duckdb file, so no warehouse or
credentials are needed. Requires `dbt deps` to have run.
"""

import hashlib
import json
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

PROJECT_DIR = Path(__file__).resolve().parents[2]

ROWS = [
    (1, 1, "SPRING"),
    (1, 2, None),
    (2, 1, "SPRING"),
    (3, 1, "N/A"),
]


def mint(values: list[str | None], null_as: str | None = None) -> str:
    """Reference implementation of mint_surrogate_key's version-1 encoding."""
    payload = "v1|"
    for value in values:
        if value is None:
            value = null_as
        payload += "~" if value is None else f"{len(value)}:{value}"
    digest = hashlib.md5(payload.encode()).hexdigest()
    return "-".join(
        (digest[:8], digest[8:12], digest[12:16], digest[16:20], digest[20:])
    )


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    db_path = tmp_path / "backfill.duckdb"
    monkeypatch.setenv("DBT_PROFILES_DIR", str(PROJECT_DIR))
    monkeypatch.setenv("DBT_TARGET", "duckdb")
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DBT_TARGET_PATH", str(tmp_path / "target"))
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path / "logs"))
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            CREATE TABLE main.order_lines (
                order_id INTEGER,
                line_number INTEGER,
                coupon_code VARCHAR,
                order_line_key VARCHAR,
                coupon_key VARCHAR,
                order_coupon_key VARCHAR,
                key_hash_version INTEGER
            )
            """
        )
        con.executemany(
            "INSERT INTO main.order_lines (order_id, line_number, coupon_code) VALUES (?, ?, ?)",
            ROWS,
        )
    return db_path


def backfill(**overrides) -> tuple[bool, list[str]]:
    args = {
        "relation": "main.order_lines",
        "key_expressions": {
            "order_line_key": ["order_id", "line_number"],
            "coupon_key": {"fields": ["coupon_code"], "null_as": "N/A"},
        },
        "dependent_keys": {"order_coupon_key": ["order_line_key", "coupon_key"]},
        "version_column": "key_hash_version",
    }
    args.update(overrides)
    messages: list[str] = []

    def collect(event):
        if event.info.name == "JinjaLogInfo":
            messages.append(event.info.msg)

    result = dbtRunner(callbacks=[collect]).invoke(
        [
            "run-operation",
            "backfill_surrogate_keys",
            "--project-dir",
            str(PROJECT_DIR),
            "--args",
            json.dumps(args),
        ]
    )
    return result.success, messages


def table(db_path: Path) -> list[tuple]:
    with duckdb.connect(str(db_path)) as con:
        return con.execute(
            "SELECT order_id, line_number, coupon_code, order_line_key, coupon_key, "
            "order_coupon_key, key_hash_version FROM main.order_lines ORDER BY 1, 2"
        ).fetchall()


def test_dry_run_is_the_default_and_writes_nothing(warehouse):
    before = table(warehouse)
    ok, messages = backfill()
    assert ok
    assert table(warehouse) == before
    log = "\n".join(messages)
    assert "statement 1 of 2 [dry run]" in log
    assert "statement 2 of 2 [dry run]" in log
    assert "4 row(s) in scope" in log


def test_real_run_fills_keys_then_rerun_is_a_no_op(warehouse):
    ok, _ = backfill(dry_run=False)
    assert ok

    rows = table(warehouse)
    for order_id, line_number, coupon, line_key, coupon_key, dependent, version in rows:
        assert line_key == mint([str(order_id), str(line_number)])
        assert coupon_key == mint([coupon], null_as="N/A")
        # Hashed from the keys the first statement wrote, not the NULLs they replaced.
        assert dependent == mint([line_key, coupon_key])
        assert version == 1

    assert len({row[5] for row in rows}) == len(rows), (
        "dependent key must differ row-to-row"
    )
    # null_as maps the NULL coupon onto the literal's key.
    assert rows[1][4] == rows[3][4]

    ok, messages = backfill(dry_run=False)
    assert ok
    assert "0 row(s) in scope" in "\n".join(messages)
    assert table(warehouse) == rows


def test_version_is_written_by_the_last_statement_only(warehouse):
    _, messages = backfill()
    statements = [m for m in messages if "[dry run]:" in m]
    assert "key_hash_version = 1" not in statements[0]
    assert "key_hash_version = 1" in statements[1]


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"version_column": None, "dependent_keys": None}, id="unscoped"),
        pytest.param(
            {"version_column": None, "scope_predicate": "order_id > 1"},
            id="dependent-without-version",
        ),
        pytest.param(
            {"scope_predicate": "order_line_key IS NULL"},
            id="predicate-reads-written-column",
        ),
        pytest.param({"relation": "main.no_such_table"}, id="missing-relation"),
    ],
)
def test_refuses_unsafe_calls(warehouse, overrides):
    before = table(warehouse)
    ok, _ = backfill(dry_run=False, **overrides)
    assert not ok
    assert table(warehouse) == before
