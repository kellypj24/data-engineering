"""Choose the target database from the deployment the code runs in.

"Which database will this write to?" is decided here, in reviewed code, not by
whatever an environment variable happens to say. The safety property: **any
unknown or missing deployment resolves to the non-production database.** Only
the one recognised production deployment name reaches production, so a
misconfigured or locally-run process cannot write prod. Names match exactly:
"Prod" is not "prod".

The deployment comes from ``DAGSTER_CLOUD_DEPLOYMENT_NAME`` (set by Dagster+),
else ``DEPLOYMENT`` (set it yourself elsewhere).

**Resolve when the work runs, never at import.** Call ``resolve_routing()``
inside the op or asset body, right before connecting or calling dbt. A code
server can load without the deployment variable that the run worker later
has; anything resolved at import then routes prod runs to non-prod, silently,
because that is the fail-safe. ``deployment_tags`` are computed at import for
display in the UI only: never route on them.

Warehouses are chosen by a logical size key (``"small"``, ``"large"``) mapped
per deployment, with the same fail-safe: an unknown deployment gets the
non-prod mapping.

Customisation
-------------
* Rename the databases in ``DATABASES`` / ``NON_PROD_DATABASE``, and the
  warehouses in ``WAREHOUSES`` / ``NON_PROD_WAREHOUSES``.
* Add a deployment by adding a key to ``DATABASES`` and ``WAREHOUSES``;
  anything else stays non-prod.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

PROD_DEPLOYMENT = "prod"
DATABASES = {
    PROD_DEPLOYMENT: "ANALYTICS_PROD",
    "stage": "ANALYTICS_STAGE",
}
NON_PROD_DATABASE = "ANALYTICS_DEV"

SIZES = ("small", "large")
WAREHOUSES = {
    PROD_DEPLOYMENT: {"small": "TRANSFORMING", "large": "TRANSFORMING_LARGE"},
    "stage": {"small": "TRANSFORMING", "large": "TRANSFORMING"},
}
NON_PROD_WAREHOUSES = {"small": "TRANSFORMING", "large": "TRANSFORMING"}

DEPLOYMENT_TAG = "toolkit/deployment"
TARGET_DATABASE_TAG = "toolkit/target_database"


def current_deployment(env: Mapping[str, str] = os.environ) -> str | None:
    return env.get("DAGSTER_CLOUD_DEPLOYMENT_NAME") or env.get("DEPLOYMENT") or None


def target_database(deployment: str | None) -> str:
    return DATABASES.get(deployment or "", NON_PROD_DATABASE)


def target_warehouse(deployment: str | None, size: str = "small") -> str:
    if size not in SIZES:
        raise ValueError(f"unknown warehouse size {size!r}; expected one of {SIZES}")
    return WAREHOUSES.get(deployment or "", NON_PROD_WAREHOUSES)[size]


@dataclass(frozen=True)
class Routing:
    deployment: str | None
    database: str
    warehouse: str


def resolve_routing(
    env: Mapping[str, str] = os.environ, size: str = "small"
) -> Routing:
    """Where this run writes, from the environment *now*. Call it in the
    op/asset body, not at import (module docstring)."""
    deployment = current_deployment(env)
    return Routing(
        deployment, target_database(deployment), target_warehouse(deployment, size)
    )


def dbt_routing_env(routing: Routing) -> dict[str, str]:
    """Env overrides for a dbt subprocess so it writes where ``routing`` says.

    Applied on every target. Every deployment mirrors prod's setup and differs
    only in where routing points, so there is no per-target exception to get
    wrong. Personal dev clones (E31) are for the dbt CLI run directly; dbt run
    through Dagster writes where the deployment routes. duckdb ignores these.
    """
    return {
        "SNOWFLAKE_DATABASE": routing.database,
        "SNOWFLAKE_WAREHOUSE": routing.warehouse,
    }


def deployment_tags(deployment: str | None) -> dict[str, str]:
    """Plain-text run tags recording the deployment and the database chosen.
    Display only: they are computed at import, so never route on them."""
    return {
        DEPLOYMENT_TAG: deployment or "unset",
        TARGET_DATABASE_TAG: target_database(deployment),
    }
