"""Chained jobs: extract, then transform when extract succeeds.

Only the head (``extract_job``) has a schedule. ``transform_job`` has none: it
is launched by ``transform_after_extract`` (src/sensors/chain.py) when an
extract run succeeds. Staggered crons ("extract at 06:00, transform at 07:00")
encode a guess about how long extract takes. When it runs long, transform reads
half-loaded data; when it fails, transform runs anyway. A chain does neither.

To add a link: define its job here with no schedule, and a run-status sensor
monitoring the job before it. ``tests/test_invariants.py`` fails if a chained
job also gets a cron, or if a sensor monitors a job that does not exist.
"""

from dagster import AssetSelection, define_asset_job

from src.assets.airbyte import airbyte_assets
from src.utils.deployment import current_deployment, deployment_tags

EXTRACT = AssetSelection.assets(*airbyte_assets)

extract_job = define_asset_job(
    "extract_job",
    selection=EXTRACT,
    description="Head of the chain: Airbyte syncs. Scheduled by daily_asset_schedule.",
    tags=deployment_tags(current_deployment()),
)

transform_job = define_asset_job(
    "transform_job",
    # Everything downstream of extraction: the dbt project, seeds included.
    selection=AssetSelection.all() - EXTRACT,
    description="dbt build. No schedule: launched when extract_job succeeds.",
    tags=deployment_tags(current_deployment()),
)
