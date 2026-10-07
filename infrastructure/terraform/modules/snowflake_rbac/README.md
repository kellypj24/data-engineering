# snowflake_rbac

Terraform module for a Snowflake role hierarchy, its databases, warehouses,
grants, and service users. Used by `infrastructure/terraform/snowflake/`.

## What it creates

For each logical database × environment (`ANALYTICS_DEV`, `ANALYTICS_PROD`, ...):

| Role | Gets | Granted to |
|------|------|------------|
| `<DB>_<ENV>_READ` | `USAGE` on the database and schemas; future `SELECT` on tables and views; `USAGE` on the warehouses listed for its environment | `<DB>_<ENV>_WRITE` |
| `<DB>_<ENV>_WRITE` | `CREATE TABLE` / `CREATE VIEW` on schemas; future `INSERT`/`UPDATE`/`DELETE`/`TRUNCATE` on tables | `<DB>_<ENV>_OWNER` |
| `<DB>_<ENV>_OWNER` | ownership of the database and its schemas | `SYSADMIN` |

People and service users get READ or WRITE, never OWNER.

Service users (`snowflake_service_user`) authenticate by **key pair only**:
the variable has no password field, and an empty `rsa_public_key` fails
validation. Each has an explicit `default_role`, the functional role it is
granted, so it never falls back to `PUBLIC`.

## The future-grant precedence rule

> **A schema-level future grant overrides the database-level future grants for
> every role on that schema**, not just the role it names.

Add one schema-level `GRANT SELECT ON FUTURE TABLES IN SCHEMA X TO ROLE A`, and
every other role's `... IN DATABASE` future grant on tables stops applying to
new tables in `X`. Their access to new objects there disappears silently, the
next time something is created.

So the module manages future grants at **one level only**, set by
`future_grant_level`:

- `"database"` (default): one future grant per role and object type per database.
- `"schema"`: the same grants pushed down to **every** schema, for **every**
  functional role.

Two guards:

- A `precondition` on the future-grant resource fails the plan if any schema
  has a future grant on an object type that some other role holding that type
  in the database does not also get there. A hand-added one-off grant fails it.
- `tests/rbac.tftest.hcl` asserts the same rule over the `future_grants` output
  at both levels.

To add access to a new object type, add it to `future_privileges` in
`main.tf`. Never add a separate schema-level future grant beside the module.

## Tests

```bash
cd infrastructure/terraform/modules/snowflake_rbac
terraform init -backend=false && terraform test   # mocked provider, plan only
```
