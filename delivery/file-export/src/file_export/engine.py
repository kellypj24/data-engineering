"""Run an export for one recipient in one of three modes.

  dry-run      resolve the window, print the SQL; writes nothing, logs nothing
  select-only  run the query and report the row count; writes nothing, logs nothing
  execute      write every output, then record the run in the run log

Execute, in order:
  1. Resolve both window bounds to literals *before* querying, so the rows
     exported and the watermark recorded come from the same snapshot. A stale
     upstream then causes lag (rows arrive next run), never loss.
  2. Materialise the recipient's rows once, in a temp table.
  3. Shared exports: count rows outside the recipient's tenant keys, and NULL
     tenants. Anything above zero raises -- no file is written, and none of
     the recipient's outputs are attempted.
  4. Write each output to `<path>.partial`; rename them all only after every
     one succeeded. A failure midway leaves no partial delivery.
  5. Log the attempt -- success or failure -- to the run log.

Watermark edge cases, encoded here:
  - A 0-row run is a success and advances the watermark, so idle days do not
    stall it.
  - With no successful run logged, `last_run_end` is unbounded: the first run
    exports all history. Cutting over from a legacy job means inserting one
    success row carrying the legacy job's last window end.
  - `max_value` is MAX(column) over the whole source, ignoring row filters
    (config.warnings() flags the risky combination).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import shutil
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from file_export.config import ExportConfig, Kind, Output, Recipient
from file_export.dialects import DuckDBDialect
from file_export.runlog import RunLog, RunLogEntry
from file_export.sql import (
    ResolvedWindow,
    build_rows_sql,
    build_select,
    last_sunday,
    tenant_violation_count_sql,
)

ROWS_TABLE = "file_export_rows"


class Mode(StrEnum):
    DRY_RUN = "dry-run"
    SELECT_ONLY = "select-only"
    EXECUTE = "execute"


class TenantIsolationError(RuntimeError):
    """A shared export's result contains rows for another tenant, or none."""


class ExportFailed(RuntimeError):
    def __init__(self, result: RunResult):
        super().__init__(f"{result.export}/{result.recipient}: {result.error}")
        self.result = result


@dataclass
class RunResult:
    export: str
    recipient: str
    mode: Mode
    status: str
    sql: str
    window: ResolvedWindow
    row_count: int | None = None
    files: list[str] = field(default_factory=list)
    error: str | None = None


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


class ExportEngine:
    def __init__(
        self,
        connection,
        output_root: Path,
        *,
        run_log: RunLog | None = None,
        clock: Callable[[], dt.datetime] = utc_now,
        dialect=None,
    ):
        self.connection = connection
        self.output_root = Path(output_root)
        self.run_log = run_log or RunLog(connection)
        self.clock = clock
        self.dialect = dialect or DuckDBDialect()

    # -- window -------------------------------------------------------------

    def resolve_window(
        self, config: ExportConfig, recipient: Recipient
    ) -> ResolvedWindow:
        if config.window is None:
            return ResolvedWindow(None, None)
        bounds = config.window.bounds
        return ResolvedWindow(
            start=self._resolve(bounds.start, config, recipient),
            end=self._resolve(bounds.end, config, recipient),
        )

    def _resolve(self, value, config: ExportConfig, recipient: Recipient) -> str | None:
        today = self.clock().date()
        if value is None:
            return None
        if value == "today":
            return today.isoformat()
        if value == "last_sunday":
            return last_sunday(today).isoformat()
        if value == "last_run_end":
            return self.run_log.last_successful_end(config.name, recipient.name)
        if value == "max_value":
            (maximum,) = self.connection.execute(
                f"SELECT MAX({config.window.column}) FROM {config.source_for(recipient)}"
            ).fetchone()
            return None if maximum is None else str(maximum)
        return value  # a literal date or timestamp

    # -- run ----------------------------------------------------------------

    def run(
        self, config: ExportConfig, recipient_name: str, mode: Mode | str
    ) -> RunResult:
        mode = Mode(mode)
        recipient = config.recipient(recipient_name)
        window = self.resolve_window(config, recipient)
        result = RunResult(
            export=config.name,
            recipient=recipient.name,
            mode=mode,
            status=mode.value,
            sql=build_select(config, recipient, window),
            window=window,
        )
        if mode is Mode.DRY_RUN:
            return result
        rows_sql = build_rows_sql(config, recipient, window)
        if mode is Mode.SELECT_ONLY:
            (result.row_count,) = self.connection.execute(
                f"SELECT COUNT(*) FROM ({rows_sql}) AS export_rows"
            ).fetchone()
            return result
        return self._execute(config, recipient, rows_sql, result)

    def run_all(self, config: ExportConfig, mode: Mode | str) -> list[RunResult]:
        """Every recipient, independently: one recipient's failure is logged
        and reported, and does not stop the others."""
        results = []
        for recipient in config.recipients:
            try:
                results.append(self.run(config, recipient.name, mode))
            except ExportFailed as failed:
                results.append(failed.result)
        return results

    def _execute(
        self,
        config: ExportConfig,
        recipient: Recipient,
        rows_sql: str,
        result: RunResult,
    ) -> RunResult:
        started = self.clock()
        partials: list[tuple[Path, Path]] = []
        try:
            self.connection.execute(
                f"CREATE OR REPLACE TEMP TABLE {ROWS_TABLE} AS {rows_sql}"
            )
            (result.row_count,) = self.connection.execute(
                f"SELECT COUNT(*) FROM {ROWS_TABLE}"
            ).fetchone()

            if config.kind is Kind.SHARED:
                (violations,) = self.connection.execute(
                    tenant_violation_count_sql(
                        ROWS_TABLE, config.tenant_column, recipient.tenant_keys
                    )
                ).fetchone()
                if violations:
                    raise TenantIsolationError(
                        f"{violations} row(s) outside tenant keys {recipient.tenant_keys} "
                        f"(or with NULL {config.tenant_column}); nothing written"
                    )

            export_sql = build_select(
                config, recipient, result.window, from_relation=ROWS_TABLE
            )
            for output in config.outputs:
                final = self._destination(output, config, recipient)
                partial = final.with_name(final.name + ".partial")
                partial.parent.mkdir(parents=True, exist_ok=True)
                partials.append((partial, final))
                self.connection.execute(
                    self.dialect.unload(export_sql, str(partial), output)
                )
                self._add_records(partial, output, config, recipient, result.row_count)
                if output.control_file:
                    control = final.with_name(final.name + ".ctl")
                    control_partial = control.with_name(control.name + ".partial")
                    partials.append((control_partial, control))
                    control_partial.write_text(
                        self._control_text(final, partial, config, recipient, result)
                    )

            for partial, final in partials:
                partial.replace(final)
            result.files = [str(final) for _, final in partials]
            result.status = "success"
        except Exception as exc:  # noqa: BLE001 -- any failure must clean up and be logged
            for partial, _ in partials:
                partial.unlink(missing_ok=True)
            result.status = "failure"
            result.error = f"{type(exc).__name__}: {exc}"
        finally:
            self.connection.execute(f"DROP TABLE IF EXISTS {ROWS_TABLE}")

        self.run_log.record(
            RunLogEntry(
                run_id=str(uuid.uuid4()),
                export_name=config.name,
                recipient=recipient.name,
                status=result.status,
                window_start=result.window.start,
                window_end=result.window.end,
                row_count=result.row_count,
                files=result.files,
                error=result.error,
                started_at=started,
                finished_at=self.clock(),
            )
        )
        if result.status == "failure":
            raise ExportFailed(result)
        return result

    # -- files --------------------------------------------------------------

    def _placeholders(
        self, config: ExportConfig, recipient: Recipient
    ) -> dict[str, str]:
        return {
            "export": config.name,
            "recipient": recipient.name,
            "run_date": self.clock().date().isoformat(),
        }

    def _destination(
        self, output: Output, config: ExportConfig, recipient: Recipient
    ) -> Path:
        relative = output.path.format(**self._placeholders(config, recipient))
        root = self.output_root.resolve()
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise ValueError(f"output path {relative!r} escapes the output root")
        return path

    def _add_records(self, path, output, config, recipient, row_count) -> None:
        if not (output.header_record or output.trailer_record):
            return
        values = {**self._placeholders(config, recipient), "row_count": row_count}
        staged = path.with_name(path.name + ".records")
        with staged.open("w") as out, path.open() as body:
            if output.header_record:
                out.write(output.header_record.format(**values) + "\n")
            shutil.copyfileobj(body, out)
            if output.trailer_record:
                out.write(output.trailer_record.format(**values) + "\n")
        staged.replace(path)

    def _control_text(self, final, partial, config, recipient, result) -> str:
        digest = hashlib.sha256(partial.read_bytes()).hexdigest()
        lines = {
            "export": config.name,
            "recipient": recipient.name,
            "file": final.name,
            "row_count": result.row_count,
            "window_start": result.window.start or "",
            "window_end": result.window.end or "",
            "sha256": digest,
            "generated_at": self.clock().isoformat(),
        }
        return "".join(f"{k}={v}\n" for k, v in lines.items())
