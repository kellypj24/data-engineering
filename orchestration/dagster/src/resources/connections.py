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
from dagster_snowflake import SnowflakeResource

# ---- Relative paths ---------------------------------------------------------
DBT_PROJECT_DIR = str(Path(__file__).resolve().parents[3] / "dbt")

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
    "snowflake": SnowflakeResource(
        account=EnvVar("SNOWFLAKE_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_USER"),
        password=EnvVar("SNOWFLAKE_PASSWORD"),
        database=EnvVar("SNOWFLAKE_DATABASE"),
        schema=EnvVar("SNOWFLAKE_SCHEMA"),
        warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    ),
}
