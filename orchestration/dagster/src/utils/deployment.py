"""Choose the target database from the deployment the code runs in.

"Which database will this write to?" is decided here, in reviewed code, not by
whatever an environment variable happens to say. The safety property: **any
unknown or missing deployment resolves to the non-production database.** Only
the one recognised production deployment name reaches production, so a
misconfigured or locally-run process cannot write prod. Names match exactly:
"Prod" is not "prod".

The deployment comes from ``DAGSTER_CLOUD_DEPLOYMENT_NAME`` (set by Dagster+),
else ``DEPLOYMENT`` (set it yourself elsewhere). The choice is recorded as run
tags (``deployment_tags``) so the UI shows where each run wrote.

Customisation
-------------
* Rename the databases in ``DATABASES`` / ``NON_PROD_DATABASE``.
* Add a deployment by adding a key to ``DATABASES``; anything else stays
  non-prod.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

PROD_DEPLOYMENT = "prod"
DATABASES = {
    PROD_DEPLOYMENT: "ANALYTICS",
    "stage": "ANALYTICS_STAGE",
}
NON_PROD_DATABASE = "ANALYTICS_DEV"

DEPLOYMENT_TAG = "toolkit/deployment"
TARGET_DATABASE_TAG = "toolkit/target_database"


def current_deployment(env: Mapping[str, str] = os.environ) -> str | None:
    return env.get("DAGSTER_CLOUD_DEPLOYMENT_NAME") or env.get("DEPLOYMENT") or None


def target_database(deployment: str | None) -> str:
    return DATABASES.get(deployment or "", NON_PROD_DATABASE)


def deployment_tags(deployment: str | None) -> dict[str, str]:
    """Plain-text run tags recording the deployment and the database chosen."""
    return {
        DEPLOYMENT_TAG: deployment or "unset",
        TARGET_DATABASE_TAG: target_database(deployment),
    }
