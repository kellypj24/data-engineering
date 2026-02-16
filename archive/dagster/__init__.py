import os

from dagster import Definitions, InMemoryIOManager, IOManager
from dagster_dbt import DbtCliResource
from dagster_slack import SlackResource

from home.assets.dbt_assets import daily_dbt_assets_schedule, dw_dbt_assets


resources = {
    "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
    "slack": SlackResource(token=os.getenv("SLACK_TOKEN")),
}

definitions = Definitions(
    assets=[
         dw_dbt_assets,
    ],
    jobs=[
    ],
    resources=resources,
    schedules=[
        daily_dbt_assets_schedule,
    ],
)
