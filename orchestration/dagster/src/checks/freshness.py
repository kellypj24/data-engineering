"""Asset freshness checks.

Verifies that key assets have been materialised within an expected time
window.  If an asset is stale (not materialised recently enough), the check
fails and surfaces a warning in the Dagster UI.

Customisation
-------------
* Change ``asset_key`` to the asset you want to monitor.
* Adjust ``FRESHNESS_THRESHOLD`` to match your SLA.
* Add additional ``@asset_check`` functions for each asset that requires
  freshness monitoring.
"""

import datetime

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    asset_check,
)

# ---- Configuration ---------------------------------------------------------
# Maximum acceptable age of the last materialisation.
FRESHNESS_THRESHOLD = datetime.timedelta(hours=25)

# ---- Check definition -------------------------------------------------------

@asset_check(
    asset=AssetKey(["airbyte", "raw_orders"]),
    description=(
        "Checks that the raw_orders asset was materialised within the last "
        f"{FRESHNESS_THRESHOLD}."
    ),
)
def freshness_check(context) -> AssetCheckResult:
    """Return PASSED if the asset was materialised recently, FAILED otherwise."""

    # Retrieve the latest materialisation event for the target asset.
    events = context.instance.get_latest_materialization_events(
        [AssetKey(["airbyte", "raw_orders"])]
    )
    latest_event = events.get(AssetKey(["airbyte", "raw_orders"]))

    if latest_event is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={"reason": "Asset has never been materialised."},
        )

    last_materialized = datetime.datetime.fromtimestamp(
        latest_event.timestamp,
        tz=datetime.timezone.utc,
    )
    age = datetime.datetime.now(datetime.timezone.utc) - last_materialized

    passed = age <= FRESHNESS_THRESHOLD
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "last_materialized": str(last_materialized),
            "age_hours": round(age.total_seconds() / 3600, 2),
            "threshold_hours": FRESHNESS_THRESHOLD.total_seconds() / 3600,
        },
    )
