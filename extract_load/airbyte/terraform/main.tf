# ---------------------------------------------------------------------------
# main.tf — Example source, destination, and connection using custom resources
#
# CUSTOM vs TYPED resources:
#
#   airbyte_source_custom / airbyte_destination_custom
#     - Connector config is a freeform JSON blob.
#     - Survives connector version upgrades without Terraform provider changes.
#     - Best for: connectors that update frequently, custom connectors, or
#       when you want maximum control over the JSON config.
#
#   airbyte_source_<name> / airbyte_destination_<name>  (typed)
#     - Terraform-native attributes with type checking and plan diffs.
#     - Requires the Terraform provider to be updated when the connector
#       schema changes (which can lag behind Airbyte releases).
#     - Best for: stable, well-supported connectors where you want IDE
#       autocompletion and strict validation (e.g., airbyte_source_postgres).
#
# This file uses custom resources as the default pattern because they are
# more resilient to version drift. Decompose into the sources/, destinations/,
# and connections/ subdirectories as your catalog grows.
# ---------------------------------------------------------------------------

# ---- Example Source (custom) ------------------------------------------------

resource "airbyte_source_custom" "example" {
  name         = "example-source"
  workspace_id = var.workspace_id

  # The definition_id is the connector's UUID from the Airbyte connector catalog.
  # Find it in the Airbyte UI or API: GET /v1/source_definitions
  definition_id = "placeholder-source-definition-id"

  configuration = jsonencode({
    # Connector-specific configuration goes here.
    # Refer to the connector's documentation for the full schema.
    # Example (Postgres):
    # host     = "localhost"
    # port     = 5432
    # database = "my_db"
    # username = "airbyte"
    # password = "secret"
  })
}

# ---- Example Destination (custom) ------------------------------------------

resource "airbyte_destination_custom" "example" {
  name         = "example-destination"
  workspace_id = var.workspace_id

  # The definition_id for the destination connector.
  # Find it via: GET /v1/destination_definitions
  definition_id = "placeholder-destination-definition-id"

  configuration = jsonencode({
    # Connector-specific configuration goes here.
    # Example (BigQuery):
    # project_id              = "my-gcp-project"
    # dataset_id              = "raw"
    # credentials_json        = file("service-account.json")
    # loading_method          = { method = "Standard" }
  })
}

# ---- Example Connection -----------------------------------------------------

resource "airbyte_connection" "example" {
  name           = "example-source-to-example-destination"
  source_id      = airbyte_source_custom.example.source_id
  destination_id = airbyte_destination_custom.example.destination_id

  # Schedule — use cron for production, manual for development.
  schedule = {
    schedule_type = "manual"
  }

  # Namespace — controls how destination tables are organized.
  # Options: "source", "destination", "custom_format"
  namespace_definition = "destination"

  # Stream prefix — optional prefix added to all destination table names.
  # prefix = "raw_"

  # Configure individual streams via the configurations block or manage
  # them in the Airbyte UI after the connection is created.
}
