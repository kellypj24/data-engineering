# ---------------------------------------------------------------------------
# main.tf — Airbyte connector catalog managed via Terraform
#
# CUSTOM vs TYPED resources:
#
#   airbyte_source_custom / airbyte_destination_custom
#     - Connector config is a freeform JSON blob.
#     - Survives connector version upgrades without Terraform provider changes.
#     - Best for: connectors that update frequently, custom connectors, or
#       when you want maximum control over the JSON config.
#
#   airbyte_source_<name> / airbyte_destination_<name>  (typed)
#     - Terraform-native attributes with type checking and plan diffs.
#     - Requires the Terraform provider to be updated when the connector
#       schema changes (which can lag behind Airbyte releases).
#     - Best for: stable, well-supported connectors where you want IDE
#       autocompletion and strict validation (e.g., airbyte_source_postgres).
#
# Connector resources live in this directory as individual .tf files
# (e.g., postgres.tf). Terraform loads all .tf files in the working
# directory automatically. The sources/, destinations/, and connections/
# subdirectories are reserved for future module decomposition.
# ---------------------------------------------------------------------------
