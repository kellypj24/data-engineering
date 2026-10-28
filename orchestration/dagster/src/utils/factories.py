"""Asset factory utilities.

Provides factory functions to reduce boilerplate when defining
repetitive asset patterns.

Usage:
    from src.utils.factories import build_backup_asset, build_source_assets

    stripe_assets = build_source_assets(
        name="stripe",
        connection_id="uuid-for-stripe",
        tables=["charges", "customers", "invoices"],
    )
    orders_backup = build_backup_asset("raw.orders", deps=[["airbyte_stripe", "charges"]])
"""

import datetime as dt

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_airbyte import build_airbyte_assets

from src.resources.backup import BackupStorage
from src.utils.deployment import backup_prefix, current_deployment


def build_source_assets(
    name: str,
    connection_id: str,
    tables: list[str],
    key_prefix: str | None = None,
    group_name: str | None = None,
) -> list[AssetsDefinition]:
    """Build a set of Airbyte assets for a single source connection.

    Wraps ``build_airbyte_assets`` with sensible defaults and consistent
    naming conventions.

    Args:
        name: Logical name for the source (e.g. "stripe", "salesforce").
            Used as the default key prefix and group name.
        connection_id: Airbyte connection UUID.
        tables: List of destination table names produced by this connection.
        key_prefix: Optional asset key prefix. Defaults to ``["airbyte", name]``.
        group_name: Optional Dagster group name. Defaults to ``name``.

    Returns:
        A list of ``AssetsDefinition`` objects representing the Airbyte tables.
    """
    prefix = key_prefix or f"airbyte_{name}"
    group = group_name or name

    assets = build_airbyte_assets(
        connection_id=connection_id,
        destination_tables=tables,
        asset_key_prefix=[prefix],
    )

    # Tag with group for UI organization
    for asset_def in assets:
        if hasattr(asset_def, "with_attributes"):
            asset_def = asset_def.with_attributes(
                group_names_by_key={key: group for key in asset_def.keys}
            )

    return assets


def backup_path(
    root: str, prefix: str, table: str, snapshot_ts: str, run_id: str
) -> str:
    return (
        f"{root.rstrip('/')}/{prefix}/{table}/snapshot_ts={snapshot_ts}/run_id={run_id}"
    )


def build_backup_asset(
    table: str,
    deps: list[AssetKey | str | list[str]] | None = None,
    group_name: str = "backups",
) -> AssetsDefinition:
    """An asset that copies ``table`` to Parquet under
    ``<root>/<prefix>/<table>/snapshot_ts=<ts>/run_id=<run_id>/``.

    ``deps`` are the syncs it runs after. The prefix comes from the
    deployment, resolved when the asset runs (src/utils/deployment.py), so an
    unknown or unset deployment writes under non-prod. The copy itself is the
    ``backup`` resource: duckdb by default, ``SnowflakeBackupStorage`` on
    Snowflake. Provenance goes in the materialization metadata.
    """

    @asset(
        name=f"{table.replace('.', '__')}_backup",
        deps=deps or [],
        group_name=group_name,
        description=f"Parquet backup of {table}.",
    )
    def _backup(
        context: AssetExecutionContext, backup: BackupStorage
    ) -> MaterializeResult:
        snapshot = dt.datetime.now(dt.UTC)
        snapshot_ts = snapshot.strftime("%Y%m%dT%H%M%SZ")
        path = backup_path(
            backup.root,
            backup_prefix(current_deployment()),
            table,
            snapshot_ts,
            context.run.run_id,
        )
        rows = backup.copy_to_parquet(table, path)
        context.log.info(f"backed up {rows} rows of {table} to {path}")
        return MaterializeResult(
            metadata={
                "source_relation": table,
                "row_count": rows,
                "snapshot_ts": MetadataValue.timestamp(snapshot),
                "run_id": context.run.run_id,
                "path": MetadataValue.path(path),
            }
        )

    return _backup
