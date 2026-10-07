variable "environments" {
  description = "Environments each database is created in, e.g. [\"dev\", \"prod\"]. Database names are <DATABASE>_<ENVIRONMENT>."
  type        = list(string)
}

variable "databases" {
  description = "Logical databases and their schemas. Each is created once per environment."
  type = map(object({
    schemas = list(string)
  }))
}

variable "future_grant_level" {
  description = <<-EOT
    Where future grants live: "database" (one grant per database) or "schema"
    (pushed down to every schema, for every functional role). Never both: a
    schema-level future grant overrides the database-level future grants of
    EVERY role on that schema. See README.md.
  EOT
  type        = string
  default     = "database"

  validation {
    condition     = contains(["database", "schema"], var.future_grant_level)
    error_message = "future_grant_level must be \"database\" or \"schema\"."
  }
}

variable "warehouses" {
  description = "Warehouses, and the environments whose READ roles (and so WRITE roles) may use them."
  type = map(object({
    size         = optional(string, "XSMALL")
    auto_suspend = optional(number, 60)
    environments = list(string)
  }))
  default = {}
}

variable "service_users" {
  description = <<-EOT
    Service users. Key-pair auth only: there is no password argument. Each
    gets one functional role, which is also its explicit default role.
  EOT
  type = map(object({
    rsa_public_key    = string
    database          = string
    environment       = string
    access            = string # "read" | "write"
    default_warehouse = string
  }))
  default = {}

  validation {
    condition     = alltrue([for u in values(var.service_users) : contains(["read", "write"], u.access)])
    error_message = "service_users[*].access must be \"read\" or \"write\"."
  }

  validation {
    condition     = alltrue([for u in values(var.service_users) : length(trimspace(u.rsa_public_key)) > 0])
    error_message = "service_users[*].rsa_public_key is required: service users authenticate by key pair."
  }
}
