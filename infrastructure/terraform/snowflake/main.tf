# Example account layout. Rename the databases, schemas, and warehouses for
# your project; the role hierarchy and grants come from the module.
module "rbac" {
  source = "../modules/snowflake_rbac"

  environments       = ["dev", "prod"]
  future_grant_level = var.future_grant_level

  databases = {
    analytics = { schemas = ["raw", "staging", "marts"] }
  }

  warehouses = {
    transforming = { size = "XSMALL", environments = ["dev", "prod"] }
  }

  service_users = {
    dbt_prod = {
      rsa_public_key    = var.dbt_service_public_key
      database          = "analytics"
      environment       = "prod"
      access            = "write"
      default_warehouse = "transforming"
    }
  }
}

output "roles" {
  value = module.rbac.roles
}

output "future_grants" {
  value = module.rbac.future_grants
}
