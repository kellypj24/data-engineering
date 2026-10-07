# Snowflake role hierarchy, per database per environment:
#
#   <DB>_<ENV>_READ  -> granted to <DB>_<ENV>_WRITE -> granted to <DB>_<ENV>_OWNER -> SYSADMIN
#
# OWNER owns the database and its schemas. READ and WRITE are the functional
# roles that people and service users are granted. See README.md for the
# future-grant precedence rule this module is built around.

locals {
  db_envs = {
    for pair in setproduct(keys(var.databases), var.environments) :
    "${pair[0]}_${pair[1]}" => {
      name    = upper("${pair[0]}_${pair[1]}")
      env     = pair[1]
      schemas = [for s in var.databases[pair[0]].schemas : upper(s)]
    }
  }

  schemas = merge([
    for key, db in local.db_envs : {
      for schema in db.schemas : "${key}.${schema}" => {
        db_env   = key
        database = db.name
        schema   = schema
      }
    }
  ]...)

  role_kinds = ["OWNER", "READ", "WRITE"]

  roles = merge([
    for key, db in local.db_envs : {
      for kind in local.role_kinds : "${key}/${kind}" => "${db.name}_${kind}"
    }
  ]...)

  # Future-grant privileges per functional role and object type. WRITE also
  # reads, through the READ role it is granted.
  future_privileges = {
    READ  = { TABLES = ["SELECT"], VIEWS = ["SELECT"] }
    WRITE = { TABLES = ["INSERT", "UPDATE", "DELETE", "TRUNCATE"] }
  }

  # Every future grant, at one level only. At "schema" level each grant is
  # pushed down to every schema of the database, for every functional role,
  # so no role's access depends on a grant that a schema-level one overrides.
  future_grants = flatten([
    for key, db in local.db_envs : [
      for role, by_type in local.future_privileges : [
        for object_type, privileges in by_type : [
          for schema in(var.future_grant_level == "schema" ? db.schemas : [null]) : {
            key         = "${key}/${role}/${object_type}/${coalesce(schema, "*")}"
            db_env      = key
            role        = local.roles["${key}/${role}"]
            database    = db.name
            schema      = schema
            object_type = object_type
            privileges  = privileges
          }
        ]
      ]
    ]
  ])

  warehouse_usage = merge([
    for wh, cfg in var.warehouses : {
      for key, db in local.db_envs : "${wh}/${key}" => {
        warehouse = upper(wh)
        role      = local.roles["${key}/READ"]
      } if contains(cfg.environments, db.env)
    }
  ]...)
}

# ---- Roles and hierarchy ----------------------------------------------------

resource "snowflake_account_role" "this" {
  for_each = local.roles
  name     = each.value
}

resource "snowflake_grant_account_role" "read_to_write" {
  for_each         = local.db_envs
  role_name        = snowflake_account_role.this["${each.key}/READ"].name
  parent_role_name = snowflake_account_role.this["${each.key}/WRITE"].name
}

resource "snowflake_grant_account_role" "write_to_owner" {
  for_each         = local.db_envs
  role_name        = snowflake_account_role.this["${each.key}/WRITE"].name
  parent_role_name = snowflake_account_role.this["${each.key}/OWNER"].name
}

resource "snowflake_grant_account_role" "owner_to_sysadmin" {
  for_each         = local.db_envs
  role_name        = snowflake_account_role.this["${each.key}/OWNER"].name
  parent_role_name = "SYSADMIN"
}

# ---- Databases and schemas, owned by the OWNER role ------------------------

resource "snowflake_database" "this" {
  for_each = local.db_envs
  name     = each.value.name
}

resource "snowflake_grant_ownership" "database" {
  for_each          = local.db_envs
  account_role_name = snowflake_account_role.this["${each.key}/OWNER"].name
  on {
    object_type = "DATABASE"
    object_name = snowflake_database.this[each.key].name
  }
}

resource "snowflake_schema" "this" {
  for_each = local.schemas
  database = snowflake_database.this[each.value.db_env].name
  name     = each.value.schema
}

resource "snowflake_grant_ownership" "schema" {
  for_each          = local.schemas
  account_role_name = snowflake_account_role.this["${each.value.db_env}/OWNER"].name
  on {
    object_type = "SCHEMA"
    object_name = "\"${each.value.database}\".\"${each.value.schema}\""
  }
  depends_on = [snowflake_schema.this]
}

# ---- Functional access ------------------------------------------------------

resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  for_each          = local.db_envs
  account_role_name = snowflake_account_role.this["${each.key}/READ"].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.this[each.key].name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  for_each          = local.schemas
  account_role_name = snowflake_account_role.this["${each.value.db_env}/READ"].name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${each.value.database}\".\"${each.value.schema}\""
  }
  depends_on = [snowflake_schema.this]
}

resource "snowflake_grant_privileges_to_account_role" "schema_create" {
  for_each          = local.schemas
  account_role_name = snowflake_account_role.this["${each.value.db_env}/WRITE"].name
  privileges        = ["CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${each.value.database}\".\"${each.value.schema}\""
  }
  depends_on = [snowflake_schema.this]
}

resource "snowflake_grant_privileges_to_account_role" "future" {
  for_each          = { for g in local.future_grants : g.key => g }
  account_role_name = each.value.role
  privileges        = each.value.privileges
  on_schema_object {
    future {
      object_type_plural = each.value.object_type
      in_database        = each.value.schema == null ? each.value.database : null
      in_schema          = each.value.schema == null ? null : "\"${each.value.database}\".\"${each.value.schema}\""
    }
  }
  depends_on = [snowflake_account_role.this, snowflake_database.this, snowflake_schema.this]

  lifecycle {
    precondition {
      # The precedence rule, enforced: a schema-level future grant on an object
      # type must be matched by every role holding a future grant on that type
      # in that database. Fails the plan if a one-off grant is ever added.
      condition = each.value.schema == null || length(setsubtract(
        toset([for g in local.future_grants : g.role if g.db_env == each.value.db_env && g.object_type == each.value.object_type]),
        toset([for g in local.future_grants : g.role if g.db_env == each.value.db_env && g.object_type == each.value.object_type && g.schema == each.value.schema]),
      )) == 0
      error_message = "Schema-level future grant on ${each.value.object_type} in schema ${each.value.schema == null ? "(none)" : each.value.schema} without the matching grant for every role: it would override their database-level future grants. See README.md."
    }
  }
}

# ---- Warehouses -------------------------------------------------------------

resource "snowflake_warehouse" "this" {
  for_each            = var.warehouses
  name                = upper(each.key)
  warehouse_size      = each.value.size
  auto_suspend        = each.value.auto_suspend
  auto_resume         = "true"
  initially_suspended = true
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  for_each          = local.warehouse_usage
  account_role_name = each.value.role
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.this[lower(each.value.warehouse)].name
  }
  depends_on = [snowflake_account_role.this]
}

# ---- Service users: key pair only, explicit default role --------------------

resource "snowflake_service_user" "this" {
  for_each          = var.service_users
  name              = upper(each.key)
  rsa_public_key    = each.value.rsa_public_key
  default_role      = local.roles["${each.value.database}_${each.value.environment}/${upper(each.value.access)}"]
  default_warehouse = upper(each.value.default_warehouse)
}

resource "snowflake_grant_account_role" "service_user" {
  for_each  = var.service_users
  role_name = snowflake_service_user.this[each.key].default_role
  user_name = snowflake_service_user.this[each.key].name
}
