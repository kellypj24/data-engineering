"""Identity probe and write-boundary proof. Manual-only: never scheduled
(``MANUAL_ONLY_TAG``; the invariant suite enforces it).

Role separation between stage and prod is usually verified by inference: "the
config says the stage role, so it must be." Only a refused write, from inside
the process that does the work, proves the whole chain (secret -> env var ->
resource -> resolved role).

* ``identity_probe_job`` runs one read-only query and logs who the run is.
* ``write_boundary_job`` tries to create a uniquely named throwaway table in a
  database this identity must NOT be able to write, and passes only if
  Snowflake refuses. The refusal is logged verbatim as evidence. If the table
  is ever created, the job drops it and fails. An active secondary role also
  fails it: secondary roles can satisfy a privilege check through inheritance
  and quietly defeat the boundary.

Snowflake reports both "insufficient privileges" (003001) and "does not exist
or not authorized" (002003) for a write you may not make; either is a refusal.
Any other error (warehouse, network) is inconclusive and fails the job.
"""

import json
import uuid

from dagster import Config, Failure, OpExecutionContext, job, op
from dagster_snowflake import SnowflakeResource
from snowflake.connector.errors import ProgrammingError

from src.utils.deployment import (
    DATABASES,
    PROD_DEPLOYMENT,
    current_deployment,
    deployment_tags,
)
from src.utils.invariants import MANUAL_ONLY_TAG

REFUSAL_ERRNOS = {3001, 2003}
IDENTITY_SQL = (
    "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_SECONDARY_ROLES(), "
    "CURRENT_DATABASE(), CURRENT_WAREHOUSE()"
)
MANUAL_ONLY = {MANUAL_ONLY_TAG: "true"}


def read_identity(connection) -> dict:
    cursor = connection.cursor()
    cursor.execute(IDENTITY_SQL)
    user, role, secondary, database, warehouse = cursor.fetchone()
    return {
        "user": user,
        "role": role,
        "secondary_roles": secondary,
        "database": database,
        "warehouse": warehouse,
    }


def active_secondary_roles(value) -> list[str]:
    """CURRENT_SECONDARY_ROLES() returns JSON like {"roles":"A,B","value":""}."""
    if not value:
        return []
    roles = json.loads(value).get("roles", "") if isinstance(value, str) else ""
    return [r for r in roles.split(",") if r]


def default_forbidden_database(deployment: str | None) -> str:
    """Outside prod, the prod database is the one this identity must not write."""
    if deployment == PROD_DEPLOYMENT:
        raise Failure(
            "write_boundary_job in the prod deployment needs an explicit "
            "forbidden_database: there is no database prod must be refused."
        )
    return DATABASES[PROD_DEPLOYMENT]


@op
def probe_identity(context: OpExecutionContext, snowflake: SnowflakeResource):
    with snowflake.get_connection() as connection:
        identity = read_identity(connection)
    context.log.info(f"identity: {json.dumps(identity)}")
    return identity


class WriteBoundaryConfig(Config):
    forbidden_database: str | None = None  # default: the prod database
    schema_name: str = "PUBLIC"


@op
def assert_write_boundary(
    context: OpExecutionContext,
    config: WriteBoundaryConfig,
    snowflake: SnowflakeResource,
):
    database = config.forbidden_database or default_forbidden_database(
        current_deployment()
    )
    table = f"{database}.{config.schema_name}.WRITE_BOUNDARY_PROBE_{uuid.uuid4().hex[:12].upper()}"

    with snowflake.get_connection() as connection:
        identity = read_identity(connection)
        context.log.info(f"identity: {json.dumps(identity)}")
        secondary = active_secondary_roles(identity["secondary_roles"])
        if secondary:
            raise Failure(
                f"secondary roles {secondary} are active; they can satisfy a "
                "privilege check and defeat the boundary. Disable them for this user."
            )

        cursor = connection.cursor()
        try:
            cursor.execute(f"CREATE TABLE {table} (probe INTEGER)")
        except ProgrammingError as refusal:
            if refusal.errno in REFUSAL_ERRNOS:
                context.log.info(f"write refused, as required. Evidence: {refusal}")
                return str(refusal)
            raise Failure(
                f"inconclusive: unexpected error, not a refusal: {refusal}"
            ) from refusal

        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        raise Failure(
            f"BOUNDARY BREACHED: {identity['role']} created {table} in {database}. "
            "The table was dropped."
        )


@job(
    description="Manual only. Logs the run's Snowflake identity (read-only).",
    tags={**MANUAL_ONLY, **deployment_tags(current_deployment())},
)
def identity_probe_job():
    probe_identity()


@job(
    description="Manual only. Passes only if a write to a forbidden database is refused.",
    tags={**MANUAL_ONLY, **deployment_tags(current_deployment())},
)
def write_boundary_job():
    assert_write_boundary()
