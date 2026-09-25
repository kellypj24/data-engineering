"""Deployment routing: only the production deployment reaches prod."""

import pytest

from src.utils.deployment import (
    DEPLOYMENT_TAG,
    NON_PROD_DATABASE,
    TARGET_DATABASE_TAG,
    current_deployment,
    deployment_tags,
    target_database,
)


def test_prod_maps_to_prod():
    assert target_database("prod") == "ANALYTICS"


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


def test_code_location_uses_the_routed_database():
    """Tests run with no deployment set, so everything points at non-prod."""
    from src.jobs.landing import process_landing_file_job
    from src.resources.connections import RESOURCES
    from src.schedules.daily import daily_asset_schedule

    assert RESOURCES["snowflake"].database == NON_PROD_DATABASE
    assert daily_asset_schedule.tags[TARGET_DATABASE_TAG] == NON_PROD_DATABASE
    assert process_landing_file_job.tags[TARGET_DATABASE_TAG] == NON_PROD_DATABASE
