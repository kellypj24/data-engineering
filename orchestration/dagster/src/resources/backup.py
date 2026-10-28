"""Where warehouse backups are written: Parquet under ``root``.

``BackupStorage`` copies a table with duckdb ``COPY ... TO ... (FORMAT
parquet)``, which is enough to run the example and its tests, and writes to
S3 too once duckdb's ``httpfs`` extension has credentials.
``SnowflakeBackupStorage`` (Snowflake extra, ``snowflake_backup.py``)
unloads with ``COPY INTO @stage`` instead, through ``RoutedSnowflakeResource``, so the table is read
from the database routing picks.

Both return the number of rows written. ``build_backup_asset``
(src/utils/factories.py) chooses the path.
"""

from __future__ import annotations

from pathlib import Path

from dagster import ConfigurableResource

PARQUET_FILE = "data.parquet"


def _quote(identifier: str) -> str:
    return ".".join(
        '"' + part.replace('"', '""') + '"' for part in identifier.split(".")
    )


class BackupStorage(ConfigurableResource):
    root: str = "s3://warehouse-backups"
    duckdb_path: str = "warehouse.duckdb"

    def copy_to_parquet(self, table: str, directory: str) -> int:
        import duckdb

        if "://" not in directory:
            Path(directory).mkdir(parents=True, exist_ok=True)
        target = f"{directory}/{PARQUET_FILE}".replace("'", "''")
        with duckdb.connect(self.duckdb_path, read_only=True) as connection:
            rows = connection.execute(
                f"COPY (SELECT * FROM {_quote(table)}) TO '{target}' (FORMAT parquet)"
            ).fetchone()[0]
        return int(rows)
