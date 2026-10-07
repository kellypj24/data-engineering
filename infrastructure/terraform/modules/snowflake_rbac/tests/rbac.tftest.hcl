# Plan-only tests with a mocked Snowflake provider. Requires Terraform >= 1.6.

mock_provider "snowflake" {}

variables {
  environments = ["dev", "prod"]
  databases = {
    analytics = { schemas = ["raw", "staging", "marts"] }
  }
  warehouses = {
    transforming = { environments = ["prod"] }
  }
  service_users = {
    dbt_prod = {
      rsa_public_key    = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtest"
      database          = "analytics"
      environment       = "prod"
      access            = "write"
      default_warehouse = "transforming"
    }
  }
}

# The precedence rule: a schema that receives a future grant on an object type
# must receive it for EVERY role that holds a future grant on that type in that
# database, or it overrides their database-level grants. Shared by both levels.

run "database_level_future_grants" {
  command = plan

  variables {
    future_grant_level = "database"
  }

  assert {
    condition     = alltrue([for g in output.future_grants : g.schema == null])
    error_message = "At database level no future grant may name a schema."
  }

  assert {
    condition     = length(output.future_grants) == 6
    error_message = "Expected 3 role/object-type grants x 2 databases."
  }
}

run "schema_level_future_grants_cover_every_role" {
  command = plan

  variables {
    future_grant_level = "schema"
  }

  assert {
    condition = alltrue([
      for g in output.future_grants : length(setsubtract(
        toset([for h in output.future_grants : h.role if h.database == g.database && h.object_type == g.object_type]),
        toset([for h in output.future_grants : h.role if h.database == g.database && h.object_type == g.object_type && h.schema == g.schema]),
      )) == 0
    ])
    error_message = "A schema has a future grant on an object type that not every functional role receives."
  }

  assert {
    condition     = alltrue([for g in output.future_grants : g.schema != null])
    error_message = "At schema level every future grant must be pushed down to a schema; mixing levels is the bug."
  }

  assert {
    condition     = length(output.future_grants) == 18
    error_message = "Expected 3 role/object-type grants x 3 schemas x 2 databases."
  }
}

run "role_hierarchy" {
  command = plan

  assert {
    condition     = snowflake_grant_account_role.read_to_write["analytics_prod"].parent_role_name == "ANALYTICS_PROD_WRITE"
    error_message = "READ must be granted to WRITE."
  }

  assert {
    condition     = snowflake_grant_account_role.write_to_owner["analytics_prod"].parent_role_name == "ANALYTICS_PROD_OWNER"
    error_message = "WRITE must be granted to OWNER."
  }

  assert {
    condition     = snowflake_grant_ownership.database["analytics_dev"].account_role_name == "ANALYTICS_DEV_OWNER"
    error_message = "Each database is owned by its OWNER role."
  }

  assert {
    condition     = length(snowflake_grant_privileges_to_account_role.warehouse_usage) == 1
    error_message = "The prod-only warehouse is granted to the prod READ role only."
  }
}

run "service_user_key_pair_and_default_role" {
  command = plan

  assert {
    condition     = snowflake_service_user.this["dbt_prod"].default_role == "ANALYTICS_PROD_WRITE"
    error_message = "A service user's default role is its functional role, set explicitly."
  }

  assert {
    condition     = snowflake_service_user.this["dbt_prod"].rsa_public_key != null
    error_message = "Service users authenticate by key pair."
  }
}

run "rejects_unknown_future_grant_level" {
  command = plan

  variables {
    future_grant_level = "both"
  }

  expect_failures = [var.future_grant_level]
}

run "rejects_service_user_without_a_key" {
  command = plan

  variables {
    service_users = {
      loader = {
        rsa_public_key    = " "
        database          = "analytics"
        environment       = "dev"
        access            = "write"
        default_warehouse = "transforming"
      }
    }
  }

  expect_failures = [var.service_users]
}
