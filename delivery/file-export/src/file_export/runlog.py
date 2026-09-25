"""The run log: one row per execute attempt, success or failure.

It is also the watermark store. `last_run_end` resolves to the window_end of
the latest *successful* run for (export, recipient). Window bounds are stored
as text so the log works for date and timestamp windows alike.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

DDL = """
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    run_id VARCHAR NOT NULL,
    export_name VARCHAR NOT NULL,
    recipient VARCHAR NOT NULL,
    status VARCHAR NOT NULL,          -- success | failure
    window_start VARCHAR,
    window_end VARCHAR,
    row_count BIGINT,
    files VARCHAR,                    -- newline-separated paths written
    error VARCHAR,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL
);
"""


@dataclass(frozen=True)
class RunLogEntry:
    run_id: str
    export_name: str
    recipient: str
    status: str
    window_start: str | None
    window_end: str | None
    row_count: int | None
    files: list[str]
    error: str | None
    started_at: dt.datetime
    finished_at: dt.datetime


class RunLog:
    def __init__(
        self, connection, schema: str = "file_export", table: str = "export_run_log"
    ):
        self.connection = connection
        self.relation = f"{schema}.{table}"
        self._ddl = DDL.format(schema=schema, table=table)

    def ensure(self) -> None:
        self.connection.execute(self._ddl)

    def exists(self) -> bool:
        schema, table = self.relation.split(".")
        (count,) = self.connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
        return count > 0

    def last_successful_end(self, export_name: str, recipient: str) -> str | None:
        if not self.exists():
            return None
        row = self.connection.execute(
            f"SELECT window_end FROM {self.relation} "
            "WHERE export_name = ? AND recipient = ? AND status = 'success' "
            "ORDER BY finished_at DESC LIMIT 1",
            [export_name, recipient],
        ).fetchone()
        return row[0] if row else None

    def record(self, entry: RunLogEntry) -> None:
        self.ensure()
        self.connection.execute(
            f"INSERT INTO {self.relation} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                entry.run_id,
                entry.export_name,
                entry.recipient,
                entry.status,
                entry.window_start,
                entry.window_end,
                entry.row_count,
                "\n".join(entry.files),
                entry.error,
                entry.started_at.replace(tzinfo=None),
                entry.finished_at.replace(tzinfo=None),
            ],
        )

    def entries(self, export_name: str, recipient: str) -> list[tuple]:
        if not self.exists():
            return []
        return self.connection.execute(
            f"SELECT status, window_start, window_end, row_count, error FROM {self.relation} "
            "WHERE export_name = ? AND recipient = ? ORDER BY started_at",
            [export_name, recipient],
        ).fetchall()
