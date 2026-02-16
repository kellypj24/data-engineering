"""Airbyte connection assets.

Uses dagster-airbyte's ``build_airbyte_assets`` helper to create Dagster
assets that mirror the tables produced by an Airbyte connection.

Customisation
-------------
* Replace ``AIRBYTE_CONNECTION_ID`` with the UUID of your Airbyte connection.
* Update ``destination_tables`` to match the tables the connection writes.
* Add ``asset_key_prefix`` to namespace the assets (e.g. ["raw", "stripe"]).
* For multiple connections, call ``build_airbyte_assets`` once per connection
  and add the results to the ``airbyte_assets`` list.
"""

from dagster_airbyte import build_airbyte_assets

# ---- Configuration ---------------------------------------------------------
# The UUID of the Airbyte connection you want to materialise.
AIRBYTE_CONNECTION_ID = "your-airbyte-connection-uuid"

# Tables that the connection writes to the destination warehouse.
DESTINATION_TABLES = [
    "raw_orders",
    "raw_customers",
]

# ---- Asset definition -------------------------------------------------------
airbyte_assets = build_airbyte_assets(
    connection_id=AIRBYTE_CONNECTION_ID,
    destination_tables=DESTINATION_TABLES,
    # Optional: prefix every asset key so it appears under a group in the UI.
    asset_key_prefix=["airbyte"],
)
