"""Prefect Block definitions for external service connections.

Blocks provide a way to store and manage configuration for external
services. Register blocks with:
    python -m blocks.connections

Then load them in flows with:
    config = AirbyteConnection.load("production")
"""

import os

from prefect.blocks.core import Block


class AirbyteConnection(Block):
    """Connection configuration for an Airbyte instance."""

    _block_type_name = "airbyte-connection"

    server_url: str = "http://localhost:8006/api/public/v1"
    connection_id: str = "your-airbyte-connection-uuid"


class SnowflakeConnection(Block):
    """Connection configuration for Snowflake."""

    _block_type_name = "snowflake-connection"

    account: str = ""
    user: str = ""
    password: str = ""
    database: str = ""
    schema_name: str = ""
    warehouse: str = ""
    role: str = ""


class DbtConfig(Block):
    """Configuration for dbt CLI execution."""

    _block_type_name = "dbt-config"

    project_dir: str = "/opt/dbt"
    target: str = "prod"
    profiles_dir: str = "/opt/dbt"


if __name__ == "__main__":
    # Register example blocks — update values for your environment
    AirbyteConnection(
        server_url=os.environ.get("AIRBYTE_SERVER_URL", "http://localhost:8006/api/public/v1"),
        connection_id=os.environ.get("AIRBYTE_CONNECTION_ID", "your-airbyte-connection-uuid"),
    ).save("production", overwrite=True)

    SnowflakeConnection(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        user=os.environ.get("SNOWFLAKE_USER", ""),
        password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
        database=os.environ.get("SNOWFLAKE_DATABASE", ""),
        schema_name=os.environ.get("SNOWFLAKE_SCHEMA", ""),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    ).save("production", overwrite=True)

    DbtConfig(
        project_dir=os.environ.get("DBT_PROJECT_DIR", "/opt/dbt"),
        target=os.environ.get("DBT_TARGET", "prod"),
    ).save("production", overwrite=True)

    print("Blocks registered successfully")
