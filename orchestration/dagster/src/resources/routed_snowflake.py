"""A Snowflake resource that resolves its database and warehouse when it
connects, not when the code server imports it.

``SnowflakeResource(database=target_database(current_deployment()))`` decides
at import. A code server that loads without the deployment variable builds the
resource with the non-prod database, and every prod run then writes there, with
no error, because non-prod is the fail-safe. This resource ignores any
configured ``database`` / ``warehouse`` and resolves both from
``src/utils/deployment.py`` on every ``get_connection()``.

Only the default connector is routed. ``connector="sqlalchemy"`` or ``"adbc"``
fails loudly rather than connecting unrouted.
"""

from contextlib import contextmanager

import snowflake.connector
from dagster import Failure
from dagster_snowflake import SnowflakeResource

from src.utils.deployment import Routing, resolve_routing


class RoutedSnowflakeResource(SnowflakeResource):
    warehouse_size: str = "small"  # a logical key in deployment.WAREHOUSES

    def routing(self) -> Routing:
        return resolve_routing(size=self.warehouse_size)

    @contextmanager
    def get_connection(self, raw_conn: bool = True):
        if self.connector:
            raise Failure(
                f"RoutedSnowflakeResource routes the default connector only, not {self.connector!r}."
            )
        routing = self.routing()
        conn = snowflake.connector.connect(
            **{
                **self._connection_args,
                "database": routing.database,
                "warehouse": routing.warehouse,
            }
        )
        try:
            yield conn
            if not self.autocommit:
                conn.commit()
        finally:
            conn.close()
