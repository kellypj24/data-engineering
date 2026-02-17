# Plan-only test with mock Airbyte provider.
# Requires Terraform >= 1.6.

mock_provider "airbyte" {}

variables {
  workspace_id          = "test-workspace-id"
  airbyte_client_id     = "test-client-id"
  airbyte_client_secret = "test-client-secret"
}

run "validate_postgres_source" {
  command = plan

  assert {
    condition     = airbyte_source_custom.postgres.name == "postgres-source"
    error_message = "Source name should be 'postgres-source'"
  }

  assert {
    condition     = airbyte_source_custom.postgres.workspace_id == "test-workspace-id"
    error_message = "Source should use the configured workspace ID"
  }

  assert {
    condition     = airbyte_source_custom.postgres.definition_id == "decd338e-5647-4c0b-adf4-da0e75f5a750"
    error_message = "Source should use the Postgres connector definition ID"
  }
}
