"""Where warehouse backups are written: Parquet under ``root``.

``BackupStorage`` copies a table with duckdb ``COPY ... TO ... (FORMAT
parquet)``. A local ``root`` is enough to run the example and its tests. An
``s3://`` root loads duckdb's ``httpfs`` extension and creates a temporary S3
secret from ``s3_secret``: by default the AWS credential chain (env vars,
``~/.aws``, instance role), which fails before the copy if no credentials are
found. For MinIO or another S3-compatible endpoint, pass the options instead:

    BackupStorage(
        root="s3://backups",
        s3_secret={"KEY_ID": "...", "SECRET": "...", "ENDPOINT": "minio:9000",
                   "URL_STYLE": "path", "USE_SSL": "false"},
    )

``SnowflakeBackupStorage`` (Snowflake extra, ``snowflake_backup.py``) unloads
with ``COPY INTO @stage`` instead, through ``RoutedSnowflakeResource``, so the
table is read from the database routing picks.

Both return the number of rows written. ``build_backup_asset``
(src/utils/factories.py) chooses the path.
"""

from __future__ import annotations

from pathlib import Path

from dagster import ConfigurableResource
from pydantic import Field

PARQUET_FILE = "data.parquet"


def _quote(identifier: str) -> str:
    return ".".join(
        '"' + part.replace('"', '""') + '"' for part in identifier.split(".")
    )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def s3_setup_sql(s3_secret: dict[str, str]) -> list[str]:
    """Statements that let a duckdb connection write to S3."""
    statements = ["INSTALL httpfs", "LOAD httpfs"]
    if s3_secret.get("PROVIDER") == "credential_chain":
        statements += ["INSTALL aws", "LOAD aws"]
    options = "".join(f", {k} {_literal(v)}" for k, v in s3_secret.items())
    statements.append(
        f"CREATE OR REPLACE TEMPORARY SECRET backup_s3 (TYPE s3{options})"
    )
    return statements


class BackupStorage(ConfigurableResource):
    root: str = "s3://warehouse-backups"
    duckdb_path: str = "warehouse.duckdb"
    s3_secret: dict[str, str] = Field(
        default_factory=lambda: {"PROVIDER": "credential_chain"}
    )

    def copy_to_parquet(self, table: str, directory: str) -> int:
        import duckdb

        if "://" not in directory:
            Path(directory).mkdir(parents=True, exist_ok=True)
        target = _literal(f"{directory}/{PARQUET_FILE}")
        with duckdb.connect(self.duckdb_path, read_only=True) as connection:
            if directory.startswith("s3://"):
                for statement in s3_setup_sql(self.s3_secret):
                    connection.execute(statement)
            rows = connection.execute(
                f"COPY (SELECT * FROM {_quote(table)}) TO {target} (FORMAT parquet)"
            ).fetchone()[0]
        return int(rows)
