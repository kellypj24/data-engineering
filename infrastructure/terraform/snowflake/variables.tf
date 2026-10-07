variable "dbt_service_public_key" {
  description = "RSA public key (PEM body, no header lines) for the dbt service user. Public, not secret."
  type        = string
}

variable "future_grant_level" {
  description = "\"database\" or \"schema\"; see modules/snowflake_rbac/README.md."
  type        = string
  default     = "database"
}
