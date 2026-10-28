"""Snowflake extra: unload a table to Parquet in an external stage.

A separate module from ``backup.py`` so that ``src/utils/factories.py`` can
import ``BackupStorage`` without importing the Snowflake resource, which
itself imports ``src/utils``.
"""

from __future__ import annotations

from dagster import ResourceDependency

from src.resources.backup import BackupStorage
from src.resources.routed_snowflake import RoutedSnowflakeResource


class SnowflakeBackupStorage(BackupStorage):
    """``root`` is an external stage, e.g. ``@ANALYTICS_BACKUPS``."""

    root: str = "@ANALYTICS_BACKUPS"
    snowflake: ResourceDependency[RoutedSnowflakeResource]

    def copy_to_parquet(self, table: str, directory: str) -> int:
        with self.snowflake.get_connection() as connection:
            result = (
                connection.cursor().execute(unload_sql(table, directory)).fetchall()
            )
        return sum(int(row[0]) for row in result)  # rows_unloaded per file


def unload_sql(table: str, directory: str) -> str:
    # Unquoted: Snowflake upper-cases unquoted names, quoted ones are exact.
    return (
        f"COPY INTO {directory}/ FROM {table} "
        "FILE_FORMAT = (TYPE = PARQUET) HEADER = TRUE"
    )
