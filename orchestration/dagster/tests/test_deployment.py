"""Deployment routing: only the production deployment reaches prod."""

import pytest

from src.utils.deployment import (
    DEPLOYMENT_TAG,
    NON_PROD_DATABASE,
    NON_PROD_WAREHOUSES,
    TARGET_DATABASE_TAG,
    Routing,
    current_deployment,
    dbt_routing_env,
    deployment_tags,
    resolve_routing,
    target_database,
    target_warehouse,
)


def test_prod_maps_to_prod():
    assert target_database("prod") == "ANALYTICS_PROD"


def test_stage_maps_to_stage():
    assert target_database("stage") == "ANALYTICS_STAGE"


@pytest.mark.parametrize("deployment", [None, "", "staging", "Prod", "prod ", "dev"])
def test_anything_else_maps_to_non_prod(deployment):
    assert target_database(deployment) == NON_PROD_DATABASE


def test_dagster_cloud_name_wins_over_deployment_env():
    env = {"DAGSTER_CLOUD_DEPLOYMENT_NAME": "stage", "DEPLOYMENT": "prod"}
    assert current_deployment(env) == "stage"
    assert current_deployment({"DEPLOYMENT": "prod"}) == "prod"
    assert current_deployment({"DAGSTER_CLOUD_DEPLOYMENT_NAME": ""}) is None


def test_tags_record_the_choice():
    assert deployment_tags(None) == {
        DEPLOYMENT_TAG: "unset",
        TARGET_DATABASE_TAG: NON_PROD_DATABASE,
    }


def test_code_location_routes_at_run_time_and_tags_for_display():
    """Nothing in RESOURCES pins a database or warehouse at import; the routed
    resource resolves both per connection (tests/test_routing.py). Tests run
    with no deployment set, so the display tags read non-prod."""
    from src.jobs.landing import process_landing_file_job
    from src.resources.connections import RESOURCES
    from src.resources.routed_snowflake import RoutedSnowflakeResource
    from src.schedules.daily import daily_asset_schedule

    assert isinstance(RESOURCES["snowflake"], RoutedSnowflakeResource)
    assert RESOURCES["snowflake"].database is None
    assert RESOURCES["snowflake"].warehouse is None
    assert daily_asset_schedule.tags[TARGET_DATABASE_TAG] == NON_PROD_DATABASE
    assert process_landing_file_job.tags[TARGET_DATABASE_TAG] == NON_PROD_DATABASE


def test_warehouse_size_keys_per_deployment():
    assert target_warehouse("prod", "large") == "TRANSFORMING_LARGE"
    assert target_warehouse("prod") == "TRANSFORMING"


@pytest.mark.parametrize("deployment", [None, "", "staging", "Prod", "dev"])
def test_unknown_deployment_gets_non_prod_warehouses(deployment):
    assert target_warehouse(deployment, "large") == NON_PROD_WAREHOUSES["large"]


def test_unknown_size_is_an_error():
    with pytest.raises(ValueError, match="unknown warehouse size"):
        target_warehouse("prod", "huge")


def test_resolve_routing_reads_the_environment_it_is_given():
    assert resolve_routing({"DEPLOYMENT": "stage"}) == Routing(
        "stage", "ANALYTICS_STAGE", "TRANSFORMING"
    )
    assert resolve_routing({}).database == NON_PROD_DATABASE


def test_dbt_routing_env_only_for_the_deployed_target():
    routing = Routing("prod", "ANALYTICS_PROD", "TRANSFORMING")
    assert dbt_routing_env(routing, {"DBT_TARGET": "snowflake_prod"}) == {
        "SNOWFLAKE_DATABASE": "ANALYTICS_PROD",
        "SNOWFLAKE_WAREHOUSE": "TRANSFORMING",
    }
    assert dbt_routing_env(routing, {"DBT_TARGET": "snowflake"}) == {}
    assert dbt_routing_env(routing, {}) == {}
