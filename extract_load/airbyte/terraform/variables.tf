# ---------------------------------------------------------------------------
# Variables — set via terraform.tfvars, environment variables (TF_VAR_*),
# or passed directly on the CLI with -var.
# ---------------------------------------------------------------------------

variable "airbyte_server_url" {
  description = "Base URL of the Airbyte API. For abctl local this is typically http://localhost:8006/api/public/v1."
  type        = string
  default     = "http://localhost:8006/api/public/v1"
}

variable "airbyte_client_id" {
  description = "OAuth client ID for the Airbyte API. Retrieve with: abctl local credentials"
  type        = string
  sensitive   = true
}

variable "airbyte_client_secret" {
  description = "OAuth client secret for the Airbyte API. Retrieve with: abctl local credentials"
  type        = string
  sensitive   = true
}

variable "workspace_id" {
  description = "Airbyte workspace ID. Find in the Airbyte UI under Settings, or via the API."
  type        = string
}

# ---------------------------------------------------------------------------
# Postgres source
# ---------------------------------------------------------------------------

variable "postgres_source_definition_id" {
  description = "Airbyte source definition UUID for the Postgres connector."
  type        = string
  default     = "decd338e-5647-4c0b-adf4-da0e75f5a750"
}

variable "postgres_host" {
  description = "Postgres host for the source connector."
  type        = string
  default     = "host.docker.internal"
}

variable "postgres_port" {
  description = "Postgres port for the source connector."
  type        = number
  default     = 5432
}

variable "postgres_database" {
  description = "Postgres database name for the source connector."
  type        = string
  default     = "postgres"
}

variable "postgres_username" {
  description = "Postgres username for the source connector."
  type        = string
  default     = "postgres"
}

variable "postgres_password" {
  description = "Postgres password for the source connector."
  type        = string
  default     = "postgres"
  sensitive   = true
}
