"""Dagster adapter: one job and one schedule per scheduled recipient.

Adding a scheduled export is a YAML edit. Generated definitions follow the
toolkit's schedule invariants: default STOPPED (opt-in activation), UTC,
unique names. Requires the `dagster` extra.

    from file_export.dagster import ExportWarehouse, build_export_definitions

    defs = build_export_definitions(
        "configs/",
        ExportWarehouse(duckdb_path="warehouse.duckdb", output_root="exports"),
    )
"""

from __future__ import annotations

import re
from pathlib import Path

import dagster as dg
import duckdb

from file_export.config import ExportConfig, Recipient, load_configs
from file_export.engine import ExportEngine, Mode

RESOURCE_KEY = "export_warehouse"


class ExportWarehouse(dg.ConfigurableResource):
    duckdb_path: str
    output_root: str


def job_name(config: ExportConfig, recipient: Recipient) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", f"export__{config.name}__{recipient.name}")


def _build_job(config: ExportConfig, recipient: Recipient) -> dg.JobDefinition:
    name = job_name(config, recipient)

    @dg.op(name=f"{name}__op", required_resource_keys={RESOURCE_KEY})
    def export_op(context) -> None:
        warehouse: ExportWarehouse = getattr(context.resources, RESOURCE_KEY)
        with duckdb.connect(warehouse.duckdb_path) as connection:
            engine = ExportEngine(connection, Path(warehouse.output_root))
            result = engine.run(config, recipient.name, Mode.EXECUTE)
        context.log.info(
            f"{result.export}/{result.recipient}: {result.row_count} row(s), "
            f"window ({result.window.start}, {result.window.end}], files {result.files}"
        )

    @dg.job(name=name, tags={"export": config.name, "recipient": recipient.name})
    def export_job() -> None:
        export_op()

    return export_job


def build_jobs_and_schedules(
    configs_dir: str | Path,
) -> tuple[list[dg.JobDefinition], list[dg.ScheduleDefinition]]:
    jobs, schedules = [], []
    for config in load_configs(Path(configs_dir)):
        for recipient in config.recipients:
            if recipient.schedule is None:
                continue
            job = _build_job(config, recipient)
            jobs.append(job)
            schedules.append(
                dg.ScheduleDefinition(
                    name=f"{job.name}__schedule",
                    job=job,
                    cron_schedule=recipient.schedule.cron,
                    execution_timezone="UTC",
                    default_status=dg.DefaultScheduleStatus.STOPPED,
                )
            )
    names = [j.name for j in jobs]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ValueError(f"export job names collide after sanitising: {duplicates}")
    return jobs, schedules


def build_export_definitions(
    configs_dir: str | Path, warehouse: ExportWarehouse
) -> dg.Definitions:
    jobs, schedules = build_jobs_and_schedules(configs_dir)
    return dg.Definitions(
        jobs=jobs, schedules=schedules, resources={RESOURCE_KEY: warehouse}
    )
