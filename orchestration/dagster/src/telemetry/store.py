"""Where run events are written: the table in ``run_events.sql``.

``RunEventStore`` writes to a duckdb file, which is enough to run the example
and its tests. On a warehouse, subclass it and override ``_connect`` to return
a DB-API connection (e.g. ``snowflake.connector.connect(...)``); the SQL is
plain INSERT/SELECT with positional parameters.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import astuple, dataclass, fields
from pathlib import Path

from dagster import ConfigurableResource

DDL = (Path(__file__).parent / "run_events.sql").read_text()
TABLE = "orchestrator_run_events"


@dataclass(frozen=True)
class RunEvent:
    run_id: str
    job_name: str
    status: str  # STARTED | SUCCESS | FAILURE
    trigger_source: str
    trigger_name: str | None
    dbt_command: str | None
    warehouse: str | None
    tests_total: int | None
    tests_failed: int | None
    error: str | None
    event_at: dt.datetime  # UTC, naive in the table


COLUMNS = [f.name for f in fields(RunEvent)]


class RunEventStore(ConfigurableResource):
    """Appends run events. ``warehouse`` is recorded on every row."""

    path: str = "telemetry.duckdb"
    warehouse: str | None = None

    def _connect(self):
        import duckdb

        connection = duckdb.connect(self.path)
        connection.execute(DDL)
        return connection

    def record(self, event: RunEvent) -> None:
        placeholders = ", ".join("?" for _ in COLUMNS)
        connection = self._connect()
        try:
            connection.execute(
                f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES ({placeholders})",
                list(astuple(event)),
            )
        finally:
            connection.close()

    def has_started(self, run_id: str) -> bool:
        connection = self._connect()
        try:
            (count,) = connection.execute(
                f"SELECT COUNT(*) FROM {TABLE} WHERE run_id = ? AND status = 'STARTED'",
                [run_id],
            ).fetchone()
        finally:
            connection.close()
        return count > 0
