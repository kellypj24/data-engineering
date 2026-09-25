"""Run-event telemetry: one row per run start and completion.

The writers are Dagster run-status sensors rather than op hooks. A hook runs
inside the run's process, so it never fires when that process dies; Dagster
still marks such a run FAILURE, and ``telemetry_run_failure`` sees that. For
the same reason a completion writer also backfills a missing STARTED row (a
run can fail before it starts), which is the safety net.

Every telemetry error is caught and logged: observability must never fail or
block a pipeline. The sensors only observe, so they default to RUNNING and are
listed in ``OBSERVING_SENSORS`` for the invariant suite.
"""

from __future__ import annotations

import datetime as dt

from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)

from src.telemetry.store import RunEvent, RunEventStore

DBT_COMMAND_TAG = "toolkit/dbt_command"
OBSERVING_SENSORS = {
    "telemetry_run_started",
    "telemetry_run_success",
    "telemetry_run_failure",
}


def utc(timestamp: float | None) -> dt.datetime:
    moment = (
        dt.datetime.fromtimestamp(timestamp, dt.UTC)
        if timestamp
        else dt.datetime.now(dt.UTC)
    )
    return moment.replace(tzinfo=None)


def trigger(tags) -> tuple[str, str | None]:
    if "dagster/schedule_name" in tags:
        return "schedule", tags["dagster/schedule_name"]
    if "dagster/sensor_name" in tags:
        return "sensor", tags["dagster/sensor_name"]
    return "manual", None


def build_event(
    context: RunStatusSensorContext,
    telemetry: RunEventStore,
    status: str,
    *,
    at: float | None,
    completed: bool = False,
    error: str | None = None,
) -> RunEvent:
    run = context.dagster_run
    source, name = trigger(run.tags)
    tests_total = tests_failed = None
    if completed:
        tests_total, tests_failed = check_counts(context)
    return RunEvent(
        run_id=run.run_id,
        job_name=run.job_name,
        status=status,
        trigger_source=source,
        trigger_name=name,
        dbt_command=run.tags.get(DBT_COMMAND_TAG),
        warehouse=telemetry.warehouse,
        tests_total=tests_total,
        tests_failed=tests_failed,
        error=error,
        event_at=utc(at),
    )


def check_counts(context: RunStatusSensorContext) -> tuple[int, int]:
    """Asset-check evaluations in the run. dagster-dbt reports each dbt test
    as an asset check, so on a dbt run these are its tests."""
    records = context.instance.get_records_for_run(
        context.dagster_run.run_id,
        of_type=DagsterEventType.ASSET_CHECK_EVALUATION,
    ).records
    results = [
        r.event_log_entry.dagster_event.event_specific_data.passed for r in records
    ]
    return len(results), results.count(False)


def record_completion(
    context: RunStatusSensorContext,
    telemetry: RunEventStore,
    status: str,
    error: str | None = None,
) -> None:
    try:
        stats = context.instance.get_run_stats(context.dagster_run.run_id)
        if not telemetry.has_started(context.dagster_run.run_id):
            telemetry.record(
                build_event(context, telemetry, "STARTED", at=stats.start_time)
            )
        telemetry.record(
            build_event(
                context,
                telemetry,
                status,
                at=stats.end_time,
                completed=True,
                error=error,
            )
        )
    except Exception as exc:  # noqa: BLE001 -- telemetry must never fail a run
        context.log.warning(f"run telemetry not recorded ({status}): {exc!r}")


@run_status_sensor(
    run_status=DagsterRunStatus.STARTED,
    name="telemetry_run_started",
    default_status=DefaultSensorStatus.RUNNING,
    description="Appends a STARTED row to orchestrator_run_events.",
)
def telemetry_run_started(context: RunStatusSensorContext, telemetry: RunEventStore):
    try:
        stats = context.instance.get_run_stats(context.dagster_run.run_id)
        telemetry.record(
            build_event(context, telemetry, "STARTED", at=stats.start_time)
        )
    except Exception as exc:  # noqa: BLE001 -- telemetry must never fail a run
        context.log.warning(f"run telemetry not recorded (STARTED): {exc!r}")


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="telemetry_run_success",
    default_status=DefaultSensorStatus.RUNNING,
    description="Appends a SUCCESS row (and a missing STARTED row).",
)
def telemetry_run_success(context: RunStatusSensorContext, telemetry: RunEventStore):
    record_completion(context, telemetry, "SUCCESS")


@run_failure_sensor(
    name="telemetry_run_failure",
    default_status=DefaultSensorStatus.RUNNING,
    description="Appends a FAILURE row (and a missing STARTED row).",
)
def telemetry_run_failure(context: RunFailureSensorContext, telemetry: RunEventStore):
    message = context.failure_event.message if context.failure_event else None
    record_completion(context, telemetry, "FAILURE", error=(message or "")[:2000])


def tag_dbt_command(context, args: list[str]) -> None:
    """Record the dbt command on the run, for the telemetry rows. Best effort."""
    try:
        context.instance.add_run_tags(
            context.run_id, {DBT_COMMAND_TAG: "dbt " + " ".join(args)}
        )
    except Exception as exc:  # noqa: BLE001 -- telemetry must never fail a run
        context.log.warning(f"dbt command not tagged on the run: {exc!r}")


telemetry_sensors = [
    telemetry_run_started,
    telemetry_run_success,
    telemetry_run_failure,
]
