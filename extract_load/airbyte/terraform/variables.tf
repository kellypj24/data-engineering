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
