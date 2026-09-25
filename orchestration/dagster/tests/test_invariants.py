"""Definition invariants: the real code location passes, and each kind of
violation fails with a message naming the offender.

The dbt cases build `@dbt_assets` from the real manifest (`dbt parse` in
transformation/dbt; `just dagster::test` does it first)."""

import os

import dagster as dg
import pytest
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    build_dbt_asset_selection,
    dbt_assets,
)

from src import defs
from src.assets.dbt import DBT_MANIFEST_PATH, DBT_PROJECT_DIR
from src.telemetry.sensors import OBSERVING_SENSORS
from src.utils.invariants import MANUAL_ONLY_TAG, check_definitions


@dg.asset
def orders():
    return 1


orders_job = dg.define_asset_job("orders_job", selection=[orders])


def check(**kwargs):
    resources = {"dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR))}
    return check_definitions(dg.Definitions(resources=resources, **kwargs))


def assert_flags(problems, *fragments):
    assert any(all(f in p for f in fragments) for p in problems), problems


def test_code_location_passes():
    assert check_definitions(defs, observing_sensors=OBSERVING_SENSORS) == []


def test_running_schedule_is_flagged():
    schedule = dg.ScheduleDefinition(
        name="eager_schedule",
        cron_schedule="0 6 * * *",
        execution_timezone="UTC",
        job=orders_job,
        default_status=dg.DefaultScheduleStatus.RUNNING,
    )
    problems = check(assets=[orders], jobs=[orders_job], schedules=[schedule])
    assert_flags(problems, "schedule eager_schedule", "STOPPED")


def test_schedule_without_explicit_utc_is_flagged():
    schedule = dg.ScheduleDefinition(
        name="local_time_schedule",
        cron_schedule="0 6 * * *",
        execution_timezone="America/New_York",
        job=orders_job,
    )
    problems = check(assets=[orders], jobs=[orders_job], schedules=[schedule])
    assert_flags(problems, "schedule local_time_schedule", "execution_timezone")


def test_duplicate_job_name_is_flagged():
    duplicate = dg.define_asset_job("orders_job", selection=[orders], tags={"x": "y"})
    problems = check(assets=[orders], jobs=[orders_job, duplicate])
    assert_flags(problems, "duplicate job name orders_job")


def test_running_launching_sensor_is_flagged():
    @dg.sensor(job=orders_job, default_status=dg.DefaultSensorStatus.RUNNING)
    def eager_sensor():
        return None

    problems = check(assets=[orders], jobs=[orders_job], sensors=[eager_sensor])
    assert_flags(problems, "sensor eager_sensor", "STOPPED")


def test_observing_sensor_must_be_declared():
    @dg.sensor(default_status=dg.DefaultSensorStatus.RUNNING)
    def watch():
        return None

    sensor_defs = dg.Definitions(sensors=[watch])
    assert_flags(check_definitions(sensor_defs), "sensor watch", "observing_sensors")
    assert check_definitions(sensor_defs, observing_sensors={"watch"}) == []


def test_scheduled_manual_only_job_is_flagged():
    repair = dg.define_asset_job(
        "repair_job", selection=[orders], tags={MANUAL_ONLY_TAG: "true"}
    )
    schedule = dg.ScheduleDefinition(
        name="repair_schedule",
        cron_schedule="0 6 * * *",
        execution_timezone="UTC",
        job=repair,
    )
    problems = check(assets=[orders], jobs=[repair], schedules=[schedule])
    assert_flags(problems, "schedule repair_schedule", "manual-only job repair_job")


def _chain(upstream_job):
    @dg.run_status_sensor(
        run_status=dg.DagsterRunStatus.SUCCESS,
        name="after_upstream",
        monitored_jobs=[upstream_job],
        request_job=orders_job,
    )
    def after_upstream(context):
        return dg.RunRequest()

    return after_upstream


def test_chained_job_with_a_schedule_is_flagged():
    upstream = dg.define_asset_job("upstream_job", selection=[orders])
    schedule = dg.ScheduleDefinition(
        name="orders_cron",
        cron_schedule="0 7 * * *",
        execution_timezone="UTC",
        job=orders_job,
    )
    problems = check(
        assets=[orders],
        jobs=[upstream, orders_job],
        schedules=[schedule],
        sensors=[_chain(upstream)],
    )
    assert_flags(problems, "job orders_job", "also scheduled")


def test_chain_with_missing_upstream_is_flagged():
    missing = dg.define_asset_job("missing_job", selection=[orders])
    problems = check(assets=[orders], jobs=[orders_job], sensors=[_chain(missing)])
    assert_flags(problems, "sensor after_upstream", "upstream job missing_job")


# -- dbt ----------------------------------------------------------------------


@pytest.fixture
def manifest():
    if not DBT_MANIFEST_PATH.exists():
        if os.environ.get("DAGSTER_REQUIRE_DBT_MANIFEST") == "1":
            pytest.fail(f"{DBT_MANIFEST_PATH} missing -- run `dbt parse` first")
        pytest.skip("no dbt manifest; run `dbt parse` in transformation/dbt")
    return DBT_MANIFEST_PATH


def test_dbt_assets_excluding_seeds_are_flagged(manifest):
    @dbt_assets(manifest=manifest, exclude="resource_type:seed")
    def no_seeds(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    problems = check(assets=[no_seeds])
    assert_flags(
        problems, "seed.data_warehouse.order_status_codes", "no @dbt_assets definition"
    )


def test_job_selecting_a_model_without_its_seed_is_flagged(manifest):
    @dbt_assets(manifest=manifest)
    def project(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    validation_job = dg.define_asset_job(
        "validation_job",
        selection=build_dbt_asset_selection([project], dbt_select="val_orders"),
    )
    problems = check(assets=[project], jobs=[validation_job])
    assert_flags(
        problems,
        "job validation_job",
        "model.data_warehouse.val_orders",
        "seed.data_warehouse.order_status_codes",
    )


def test_overlapping_dbt_assets_are_flagged(manifest):
    @dbt_assets(manifest=manifest, name="everything")
    def everything(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    @dbt_assets(
        manifest=manifest,
        name="marts",
        select="fct_orders",
        dagster_dbt_translator=_Prefixed(),
    )
    def marts(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    problems = check(assets=[everything, marts])
    assert_flags(problems, "model.data_warehouse.fct_orders", "everything", "marts")


def test_run_then_test_is_flagged(manifest):
    @dbt_assets(manifest=manifest)
    def run_then_test(context: dg.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["run"], context=context).stream()
        yield from dbt.cli(["test"], context=context).stream()

    problems = check(assets=[run_then_test])
    assert_flags(problems, "dbt assets run_then_test", "dbt build")


class _Prefixed(DagsterDbtTranslator):
    """Distinct asset keys, so two definitions can hold the same dbt node
    without Dagster rejecting the duplicate key before the invariant sees it."""

    def get_asset_key(self, dbt_resource_props):
        return super().get_asset_key(dbt_resource_props).with_prefix("copy")
