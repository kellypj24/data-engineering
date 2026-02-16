terraform {
  required_version = ">= 1.5.0"

  required_providers {
    airbyte = {
      source  = "airbytehq/airbyte"
      version = "~> 0.6"
    }
  }
}

# ---------------------------------------------------------------------------
# Airbyte Terraform Provider — configured for abctl local or EC2 API
#
# Credentials come from variables (which can be set via environment variables):
#   TF_VAR_airbyte_server_url
#   TF_VAR_airbyte_client_id
#   TF_VAR_airbyte_client_secret
#
# To retrieve credentials from a running abctl instance:
#   abctl local credentials
# ---------------------------------------------------------------------------
provider "airbyte" {
  server_url    = var.airbyte_server_url
  client_id     = var.airbyte_client_id
  client_secret = var.airbyte_client_secret
}
