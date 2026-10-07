"""The run log is read by dbt (fct_delivery_reconciliation) through the
seeds/example_raw/export_run_log.csv fixture on duckdb. Its columns must match
the table this package creates."""

import csv
from pathlib import Path

import duckdb

from file_export.runlog import RunLog

DBT_FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "transformation/dbt/seeds/example_raw/export_run_log.csv"
)


def test_run_log_columns_match_the_dbt_fixture():
    connection = duckdb.connect()
    RunLog(connection).ensure()
    columns = [
        row[0]
        for row in connection.execute("DESCRIBE file_export.export_run_log").fetchall()
    ]
    with DBT_FIXTURE.open() as f:
        assert next(csv.reader(f)) == columns
