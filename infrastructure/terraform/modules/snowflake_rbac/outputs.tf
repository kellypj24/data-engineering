output "roles" {
  description = "Role name by \"<db>_<env>/<OWNER|READ|WRITE>\"."
  value       = local.roles
}

output "future_grants" {
  description = "Every future grant the module makes: role, database, schema (null at database level), object type, privileges."
  value = [
    for g in local.future_grants : {
      role        = g.role
      database    = g.database
      schema      = g.schema
      object_type = g.object_type
      privileges  = g.privileges
    }
  ]
}

output "functional_roles" {
  description = "READ and WRITE role names per \"<db>_<env>\"."
  value = {
    for key, _ in local.db_envs : key => [local.roles["${key}/READ"], local.roles["${key}/WRITE"]]
  }
}
