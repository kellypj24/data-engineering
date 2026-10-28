"""Asset definitions — re-exported from submodules."""

from src.assets.airbyte import airbyte_assets
from src.assets.backups import backup_assets
from src.assets.dbt import dbt_project_assets

# Collect every asset into a single list for the Definitions object.
# Append new asset groups here as the project grows.
all_assets = [
    *airbyte_assets,
    *backup_assets,
    *([dbt_project_assets] if dbt_project_assets is not None else []),
]
