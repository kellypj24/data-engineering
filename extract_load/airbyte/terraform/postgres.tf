# ---------------------------------------------------------------------------
# Postgres Source — POC connector for validating the Terraform setup.
#
# Uses airbyte_source_custom with freeform JSON config (see main.tf header
# for custom vs typed resource trade-offs). The default connection details
# point to a local Postgres instance; override via variables or tfvars.
# ---------------------------------------------------------------------------

resource "airbyte_source_custom" "postgres" {
  name          = "postgres-source"
  workspace_id  = var.workspace_id
  definition_id = var.postgres_source_definition_id

  configuration = jsonencode({
    host     = var.postgres_host
    port     = var.postgres_port
    database = var.postgres_database
    username = var.postgres_username
    password = var.postgres_password
    ssl_mode = { mode = "prefer" }
  })
}
