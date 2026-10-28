"""Shared resource definitions.

Every resource that ops/assets need at runtime is configured here and
exported via the ``RESOURCES`` dict, which is passed to ``Definitions``.

Secrets are read from environment variables using ``EnvVar`` so that
nothing sensitive is stored in code.

Customisation
-------------
* Adjust ``airbyte_host`` / ``airbyte_port`` if Airbyte is not running at
  the default local address.
* Point ``DbtCliResource`` at the correct ``project_dir``.
* Add additional resources (e.g. Spark, GCS, BigQuery) following the same
  pattern.
"""

from pathlib import Path

from dagster import EnvVar
from dagster_airbyte import AirbyteResource
from dagster_dbt import DbtCliResource

from src.resources.backup import BackupStorage
from src.resources.routed_snowflake import RoutedSnowflakeResource
from src.telemetry.store import RunEventStore

# ---- Relative paths ---------------------------------------------------------
DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[4] / "transformation" / "dbt")

# ---- Resource map -----------------------------------------------------------
RESOURCES: dict = {
    "airbyte": AirbyteResource(
        host="localhost",
        port="8000",
        # Set AIRBYTE_USERNAME / AIRBYTE_PASSWORD in your environment or
        # .env file.
        username=EnvVar("AIRBYTE_USERNAME"),
        password=EnvVar("AIRBYTE_PASSWORD"),
    ),
    "dbt": DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
    ),
    # Database and warehouse are resolved from the deployment on every
    # connection, never here: this module is imported by a code server that
    # may not have the deployment variable yet (src/utils/deployment.py).
    "snowflake": RoutedSnowflakeResource(
        account=EnvVar("SNOWFLAKE_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_USER"),
        password=EnvVar("SNOWFLAKE_PASSWORD"),
        schema=EnvVar("SNOWFLAKE_SCHEMA"),
    ),
    # Run-event telemetry (src/telemetry/). A local duckdb file by default; on
    # a warehouse, subclass RunEventStore and set `warehouse` to record it.
    "telemetry": RunEventStore(path="telemetry.duckdb"),
    # Warehouse backups (src/assets/backups.py): Parquet from the dbt project's
    # duckdb file to S3 via the AWS credential chain. On Snowflake use
    # SnowflakeBackupStorage (src/resources/snowflake_backup.py) instead.
    "backup": BackupStorage(
        root="s3://warehouse-backups",
        duckdb_path=str(Path(DBT_PROJECT_DIR) / "dev.duckdb"),
    ),
}
