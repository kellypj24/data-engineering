# Airbyte EL (Extract + Load)

Airbyte connectors managed as code via Terraform, running on [abctl](https://docs.airbyte.com/using-airbyte/getting-started/oss-quickstart) (Airbyte's local and EC2 CLI tool).

## Overview

This directory contains Terraform configurations that declaratively manage Airbyte sources, destinations, and connections. The `abctl` tool handles the Airbyte platform lifecycle (install, upgrade, credentials) while Terraform manages the connector catalog and sync configuration on top of it.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- [abctl](https://docs.airbyte.com/using-airbyte/getting-started/oss-quickstart) (installed via `brew install airbytehq/tap/abctl` on macOS)

## abctl Setup

### Local development

```bash
# Install Airbyte locally (runs in Docker via abctl)
abctl local install

# Verify it is running
abctl local status

# Open the UI (default: http://localhost:8006)
open http://localhost:8006
```

### EC2 deployment

```bash
# Install on a remote EC2 instance
abctl local install --host <EC2_PUBLIC_IP>

# For HTTPS with a domain
abctl local install --host <YOUR_DOMAIN> --insecure-cookies=false
```

Refer to the [Airbyte on EC2 guide](https://docs.airbyte.com/deploying-airbyte/on-aws-ec2) for full instructions including security group configuration (ports 8006, 8007, 443).

## Getting Credentials

abctl generates OAuth credentials for the API on install. Retrieve them with:

```bash
abctl local credentials
```

This prints the `client-id` and `client-secret`. Export them as environment variables for Terraform:

```bash
export TF_VAR_airbyte_client_id="<client-id>"
export TF_VAR_airbyte_client_secret="<client-secret>"
export TF_VAR_workspace_id="<workspace-id>"
```

The workspace ID is visible in the Airbyte UI under Settings, or via the API.

## Terraform Workflow

```bash
cd terraform/

# Download the Airbyte provider
terraform init

# Preview changes
terraform plan

# Apply changes to the running Airbyte instance
terraform apply

# Tear down managed resources (does NOT uninstall Airbyte itself)
terraform destroy
```

### State management

For solo/local use the default local state backend is fine. For team or production use, configure a remote backend (S3, GCS, Terraform Cloud) in `providers.tf`.

## Custom vs Typed Connector Resources

The Airbyte Terraform provider offers two styles of resource for sources and destinations:

### Custom resources (`airbyte_source_custom` / `airbyte_destination_custom`)

- Configuration is a freeform JSON blob passed via `jsonencode({...})`.
- Resilient to connector version upgrades -- the Terraform provider does not need to be updated when a connector's config schema changes.
- Best for: connectors that update frequently, community/custom connectors, or when you want to pin exact config without waiting for provider releases.

### Typed resources (`airbyte_source_postgres`, `airbyte_destination_bigquery`, etc.)

- Terraform-native HCL attributes with type checking, autocompletion, and structured plan diffs.
- Requires the `airbytehq/airbyte` Terraform provider version to include that connector's schema (can lag behind Airbyte releases).
- Best for: stable, first-party connectors where you want strict validation and readable diffs.

**This project defaults to custom resources.** Switch to typed resources on a per-connector basis when the stability trade-off makes sense.

## Adding a New Source / Destination / Connection

1. Create a new `.tf` file in the appropriate subdirectory:
   - `terraform/sources/my_source.tf`
   - `terraform/destinations/my_destination.tf`
   - `terraform/connections/my_connection.tf`

2. Use the custom resource pattern from `main.tf` as a template. You will need:
   - The connector's `definition_id` (find it in the Airbyte UI or via the API).
   - The connector-specific configuration JSON (see the connector's docs).

3. Run `terraform plan` to verify, then `terraform apply`.

4. Remove the example resources in `main.tf` once you have real connectors in place.

## Directory Structure

```
extract_load/airbyte/
  README.md              # This file
  terraform/
    providers.tf         # Terraform + Airbyte provider config
    variables.tf         # Shared variables (server URL, credentials, workspace)
    main.tf              # Example resources (source, destination, connection)
    sources/             # One .tf file per source connector
    destinations/        # One .tf file per destination connector
    connections/         # One .tf file per connection (source -> destination)
```

## References

- [Airbyte documentation](https://docs.airbyte.com/)
- [abctl quickstart](https://docs.airbyte.com/using-airbyte/getting-started/oss-quickstart)
- [Airbyte Terraform provider](https://registry.terraform.io/providers/airbytehq/airbyte/latest/docs)
- [Airbyte Terraform provider GitHub](https://github.com/airbytehq/terraform-provider-airbyte)
- [Airbyte on EC2](https://docs.airbyte.com/deploying-airbyte/on-aws-ec2)
- [Airbyte API reference](https://reference.airbyte.com/)
