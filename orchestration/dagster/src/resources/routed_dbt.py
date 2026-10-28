"""Run the dbt CLI against the database the deployment resolves to *now*.

``DbtCliResource`` passes ``os.environ`` to the dbt process, and the deployed
dbt profile reads its database and warehouse from env vars. Routing is
resolved in the op/asset body (``resolve_routing``), applied to the
environment only for the duration of the ``dbt.cli`` call that starts the
process, then restored. Every target is routed (``dbt_routing_env``).
"""

import os
from contextlib import contextmanager

from dagster_dbt import DbtCliResource

from src.utils.deployment import dbt_routing_env, resolve_routing


@contextmanager
def routed_environ(overrides: dict[str, str]):
    """Set ``overrides`` in os.environ for the duration, then restore it."""
    saved = {key: os.environ.get(key) for key in overrides}
    os.environ.update(overrides)
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def routed_dbt_cli(log_context, dbt: DbtCliResource, args: list[str], **cli_kwargs):
    """``dbt.cli(args, **cli_kwargs)``, routed. The process starts inside
    ``dbt.cli``, so the environment it sees is the routed one."""
    routing = resolve_routing()
    log_context.log.info(
        f"routing: deployment={routing.deployment or 'unset'} "
        f"database={routing.database} warehouse={routing.warehouse}"
    )
    with routed_environ(dbt_routing_env(routing)):
        return dbt.cli(args, **cli_kwargs)
