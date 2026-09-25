"""Generated Dagster definitions follow the schedule invariants and run."""

import yaml
from dagster import DefaultScheduleStatus

from file_export.dagster import (
    ExportWarehouse,
    build_export_definitions,
    build_jobs_and_schedules,
)
from tests.conftest import CONFIGS_DIR, add_lines, shared_config


def test_one_job_and_schedule_per_scheduled_recipient():
    jobs, schedules = build_jobs_and_schedules(CONFIGS_DIR)
    # orders: north + south; daily-revenue: north; the ad hoc export: none.
    assert sorted(j.name for j in jobs) == [
        "export__daily_revenue__north",
        "export__orders__north",
        "export__orders__south",
    ]
    assert len(schedules) == len(jobs)


def test_schedules_are_stopped_utc_and_uniquely_named():
    _, schedules = build_jobs_and_schedules(CONFIGS_DIR)
    for schedule in schedules:
        assert schedule.default_status == DefaultScheduleStatus.STOPPED, schedule.name
        assert schedule.execution_timezone == "UTC", schedule.name
    names = [s.name for s in schedules]
    assert len(names) == len(set(names))


def test_generated_job_executes_an_export(tmp_path, warehouse):
    add_lines(warehouse, (1, 101, "2026-03-01 10:00"))
    warehouse.close()  # the job opens its own connection
    configs = tmp_path / "configs"
    configs.mkdir()
    data = shared_config(
        recipients=[
            {"name": "north", "tenant_keys": [101], "schedule": {"cron": "0 6 * * *"}}
        ]
    )
    (configs / "lines.yml").write_text(yaml.safe_dump(data))

    defs = build_export_definitions(
        configs,
        ExportWarehouse(
            duckdb_path=str(tmp_path / "warehouse.duckdb"),
            output_root=str(tmp_path / "out"),
        ),
    )
    result = defs.get_job_def("export__lines__north").execute_in_process()
    assert result.success
    assert (tmp_path / "out" / "north" / "lines.csv").exists()
