"""Asset factory utilities.

Provides factory functions to reduce boilerplate when defining
repetitive asset patterns.

Usage:
    from src.utils.factories import build_source_assets

    stripe_assets = build_source_assets(
        name="stripe",
        connection_id="uuid-for-stripe",
        tables=["charges", "customers", "invoices"],
    )
"""

from dagster import AssetsDefinition
from dagster_airbyte import build_airbyte_assets


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
