"""Warehouse backups: one Parquet copy of a synced table per run.

``build_backup_asset`` (src/utils/factories.py) writes under
``<root>/<deployment prefix>/<table>/snapshot_ts=<ts>/run_id=<run_id>/`` and
records provenance in the materialization metadata. The ``backup`` resource
(src/resources/connections.py) does the copy.

Customisation
-------------
* One ``build_backup_asset`` per table to keep; ``deps`` is the sync it runs
  after, so materialising it is downstream of that sync.
"""

from dagster import AssetKey

from src.utils.factories import build_backup_asset

raw_orders_backup = build_backup_asset(
    "raw_orders", deps=[AssetKey(["airbyte", "raw_orders"])]
)

backup_assets = [raw_orders_backup]
