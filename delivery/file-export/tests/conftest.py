"""Shared fixtures: a throwaway duckdb warehouse and a controllable clock."""

import datetime as dt
from pathlib import Path

import duckdb
import pytest
import yaml

from file_export.config import load_config
from file_export.engine import ExportEngine

CONFIGS_DIR = Path(__file__).resolve().parents[1] / "configs"


class Clock:
    """Deterministic clock; each call returns the current value, and tests
    move it forward with `advance`."""

    def __init__(self, start: dt.datetime):
        self.now = start

    def __call__(self) -> dt.datetime:
        return self.now

    def advance(self, **delta) -> None:
        self.now += dt.timedelta(**delta)


@pytest.fixture
def clock():
    return Clock(dt.datetime(2026, 3, 4, 9, 0, tzinfo=dt.UTC))  # a Wednesday


@pytest.fixture
def warehouse(tmp_path):
    connection = duckdb.connect(str(tmp_path / "warehouse.duckdb"))
    connection.execute("CREATE SCHEMA marts")
    connection.execute(
        """
        CREATE TABLE marts.order_lines (
            order_id INTEGER, line_number INTEGER, customer_id INTEGER,
            region VARCHAR, status VARCHAR, note VARCHAR, amount DOUBLE,
            is_priority BOOLEAN, updated_at TIMESTAMP
        )
        """
    )
    yield connection
    connection.close()


def add_lines(connection, *rows):
    """rows: (order_id, customer_id, updated_at[, region])"""
    for row in rows:
        order_id, customer_id, updated_at, *rest = row
        region = rest[0] if rest else "north"
        connection.execute(
            "INSERT INTO marts.order_lines VALUES (?, 1, ?, ?, 'active', NULL, 10.0, FALSE, ?)",
            [order_id, customer_id, region, updated_at],
        )


@pytest.fixture
def engine(warehouse, tmp_path, clock):
    return ExportEngine(warehouse, tmp_path / "out", clock=clock)


@pytest.fixture
def write_config(tmp_path):
    def _write(data: dict):
        path = tmp_path / "configs" / f"{data['name']}.yml"
        path.parent.mkdir(exist_ok=True)
        path.write_text(yaml.safe_dump(data))
        return load_config(path)

    return _write


def shared_config(**overrides) -> dict:
    config = {
        "name": "lines",
        "kind": "shared",
        "owner": {"name": "data-platform"},
        "source": "marts.order_lines",
        "tenant_column": "customer_id",
        "columns": [{"name": "order_id"}, {"name": "customer_id"}],
        "window": {
            "column": "updated_at",
            "start_exclusive_end_inclusive": {
                "start": "last_run_end",
                "end": "max_value",
            },
        },
        "outputs": [{"path": "{recipient}/lines.csv"}],
        "recipients": [
            {"name": "north", "tenant_keys": [101]},
            {"name": "south", "tenant_keys": [201]},
        ],
    }
    config.update(overrides)
    return config
