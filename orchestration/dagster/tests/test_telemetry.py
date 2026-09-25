"""Run-event telemetry: real in-process runs on an ephemeral instance, handed to
the sensors, written to a temporary duckdb store."""

import csv
from pathlib import Path

import dagster as dg
import duckdb
import pytest

from src.telemetry.sensors import (
    DBT_COMMAND_TAG,
    tag_dbt_command,
    telemetry_run_failure,
    telemetry_run_started,
    telemetry_run_success,
)
from src.telemetry.store import COLUMNS, TABLE, RunEventStore

DBT_SEED = (
    Path(__file__).resolve().parents[3]
    / "transformation/dbt/seeds/example_raw/orchestrator_run_events.csv"
)


@dg.op
def ok():
    return 1


@dg.op
def boom():
    raise RuntimeError("upstream exploded")


@dg.job
def good_job():
    ok()


@dg.job
def bad_job():
    boom()


class RaisingStore(RunEventStore):
    def record(self, event):
        raise ConnectionError("warehouse unreachable")

    def has_started(self, run_id):
        raise ConnectionError("warehouse unreachable")


@pytest.fixture
def instance():
    with dg.instance_for_test() as instance:
        yield instance


@pytest.fixture
def store(tmp_path):
    return RunEventStore(path=str(tmp_path / "telemetry.duckdb"), warehouse="WH_TEST")


def rows(store):
    with duckdb.connect(store.path) as connection:
        return connection.execute(
            f"SELECT status, job_name, trigger_source, warehouse, tests_total, "
            f"tests_failed, error FROM {TABLE} ORDER BY event_at, status DESC"
        ).fetchall()


def run(job, instance, tags=None):
    result = job.execute_in_process(instance=instance, raise_on_error=False, tags=tags)
    return instance.get_run_by_id(result.run_id)


def sensor_context(sensor, instance, dagster_run, event_type):
    [record] = instance.get_records_for_run(
        dagster_run.run_id, of_type=event_type
    ).records
    return dg.build_run_status_sensor_context(
        sensor_name=sensor.name,
        dagster_instance=instance,
        dagster_run=dagster_run,
        dagster_event=record.event_log_entry.dagster_event,
    )


def test_success_writes_started_then_success(instance, store):
    dagster_run = run(good_job, instance, tags={"dagster/schedule_name": "daily"})
    started = sensor_context(
        telemetry_run_started, instance, dagster_run, dg.DagsterEventType.RUN_START
    )
    telemetry_run_started(started, telemetry=store)
    done = sensor_context(
        telemetry_run_success, instance, dagster_run, dg.DagsterEventType.RUN_SUCCESS
    )
    telemetry_run_success(done, telemetry=store)

    assert rows(store) == [
        ("STARTED", "good_job", "schedule", "WH_TEST", None, None, None),
        ("SUCCESS", "good_job", "schedule", "WH_TEST", 0, 0, None),
    ]


def test_failure_backfills_missing_started(instance, store):
    """The STARTED sensor never saw this run; the failure writer adds it."""
    dagster_run = run(bad_job, instance)
    context = sensor_context(
        telemetry_run_failure, instance, dagster_run, dg.DagsterEventType.RUN_FAILURE
    ).for_run_failure()
    telemetry_run_failure(context, telemetry=store)

    [started, failure] = rows(store)
    assert started[:3] == ("STARTED", "bad_job", "manual")
    assert failure[:3] == ("FAILURE", "bad_job", "manual")
    assert failure[6]  # the failure message


def test_raising_store_does_not_raise(instance):
    dagster_run = run(bad_job, instance)
    context = sensor_context(
        telemetry_run_failure, instance, dagster_run, dg.DagsterEventType.RUN_FAILURE
    ).for_run_failure()
    telemetry_run_failure(context, telemetry=RaisingStore())  # no exception


def test_telemetry_error_does_not_fail_the_job(instance):
    """tag_dbt_command runs inside the dbt asset; a failure there is logged."""

    class BrokenInstance:
        def add_run_tags(self, *_):
            raise ConnectionError("instance storage down")

    @dg.op
    def dbt_like(context: dg.OpExecutionContext):
        tag_dbt_command(
            type(
                "Ctx",
                (),
                {"instance": BrokenInstance(), "run_id": "x", "log": context.log},
            )(),
            ["build"],
        )
        tag_dbt_command(context, ["build"])

    @dg.job
    def dbt_job():
        dbt_like()

    result = dbt_job.execute_in_process(instance=instance)
    assert result.success
    tags = instance.get_run_by_id(result.run_id).tags
    assert tags[DBT_COMMAND_TAG] == "dbt build"


def test_ddl_matches_the_dbt_fixture_columns():
    """The dbt seed standing in for this table must have the same columns."""
    with DBT_SEED.open() as f:
        assert next(csv.reader(f)) == COLUMNS
