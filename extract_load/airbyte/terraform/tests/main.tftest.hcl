# Plan-only test with mock Airbyte provider.
# Requires Terraform >= 1.6.

mock_provider "airbyte" {}

variables {
  workspace_id          = "test-workspace-id"
  airbyte_client_id     = "test-client-id"
  airbyte_client_secret = "test-client-secret"
}

run "validate_source_resource" {
  command = plan

  assert {
    condition     = airbyte_source_custom.example.name == "example-source"
    error_message = "Source name should be 'example-source'"
  }

  assert {
    condition     = airbyte_source_custom.example.workspace_id == "test-workspace-id"
    error_message = "Source should use the configured workspace ID"
  }
}

run "validate_destination_resource" {
  command = plan

  assert {
    condition     = airbyte_destination_custom.example.name == "example-destination"
    error_message = "Destination name should be 'example-destination'"
  }
}

run "validate_connection_resource" {
  command = plan

  assert {
    condition     = airbyte_connection.example.name == "example-source-to-example-destination"
    error_message = "Connection name should match the source-to-destination pattern"
  }

  assert {
    condition     = airbyte_connection.example.namespace_definition == "destination"
    error_message = "Connection should use destination namespace"
  }
}
