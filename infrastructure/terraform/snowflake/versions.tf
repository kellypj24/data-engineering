terraform {
  required_version = ">= 1.6.0"

  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.21"
    }
  }
}

# Authentication comes from the environment, so no credential is ever in a
# variable file: SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME,
# SNOWFLAKE_USER, SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT, SNOWFLAKE_PRIVATE_KEY.
# Run as a role that can create databases, warehouses, roles, users, and
# grants (e.g. a dedicated Terraform role holding SYSADMIN and SECURITYADMIN).
provider "snowflake" {}
