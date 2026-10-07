# Plan-only test of the example account layout with a mocked provider.
# The role and grant rules are tested in modules/snowflake_rbac/tests/.

mock_provider "snowflake" {}

variables {
  dbt_service_public_key = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtest"
}

run "example_layout" {
  command = plan

  assert {
    condition     = output.roles["analytics_prod/WRITE"] == "ANALYTICS_PROD_WRITE"
    error_message = "The example creates ANALYTICS_PROD_WRITE."
  }

  assert {
    condition     = length(output.roles) == 6
    error_message = "OWNER/READ/WRITE for analytics in dev and prod."
  }
}
