# Airbyte Extract & Load

## Role
Managed connector-based EL using abctl (local) and Terraform for IaC.

## Key Files

- `terraform/providers.tf` — Airbyte Terraform provider (~> 0.6) with OAuth auth
- `terraform/variables.tf` — server_url, client_id, client_secret, workspace_id
- `terraform/main.tf` — Example custom source, destination, and connection resources
- `terraform/sources/` — Per-source connector files (decompose as catalog grows)
- `terraform/destinations/` — Per-destination connector files
- `terraform/connections/` — Per-connection configuration files
- `terraform/tests/main.tftest.hcl` — Plan-only validation with mock provider

## Commands

```bash
just airbyte::validate  # terraform validate
just airbyte::plan      # terraform plan
just airbyte::apply     # terraform apply
just airbyte::test      # terraform test
```

## Environment Variables

TF_VAR_airbyte_server_url (default: http://localhost:8006/api/public/v1), TF_VAR_airbyte_client_id, TF_VAR_airbyte_client_secret

## Patterns

- Uses `airbyte_source_custom` / `airbyte_destination_custom` (freeform JSON) over typed resources
- Custom resources are more resilient to connector version drift
- `jsonencode()` for connector-specific configuration blocks
- Decompose into sources/, destinations/, connections/ subdirectories as catalog grows
