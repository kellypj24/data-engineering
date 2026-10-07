# Snowflake extras

Macros that rely on Snowflake-only features. Each refuses to execute on other
adapters; dry runs work everywhere.

| Macro | What |
|-------|------|
| `clone_database` | Zero-copy clone, replacing the target; re-applies the replaced database's own grants. Refuses the prod database by name |
| `refresh_environment` | `clone_database` from prod into a shared environment (`environment_databases` var), then restores the preservation manifest |
| `refresh_dev_database` | `clone_database` from prod into `<PROD>_<USERNAME>` (E31) |

## Preservation manifest

A clone has exactly what prod has. Objects that exist only in stage, or must
differ per environment, vanish on every refresh. List them in the
`preservation_manifest` var. `preserve_objects` (in `macros/utils/`, because it
is adapter-neutral) verifies, provisions, or restores them, and
`refresh_environment` restores them after each clone.

```yaml
# dbt_project.yml
vars:
  preservation_manifest:
    - name: landing_stage
      exists_sql: "SHOW STAGES LIKE 'LANDING' IN SCHEMA {database}.RAW"
      ddl:
        - "CREATE OR REPLACE STAGE {database}.RAW.LANDING URL = 's3://example-landing/' STORAGE_INTEGRATION = LANDING_S3"
      grants:
        - "GRANT USAGE ON STAGE {database}.RAW.LANDING TO ROLE {database}_WRITE"
    - name: normalize_email_udf
      exists_sql: "SHOW USER FUNCTIONS LIKE 'NORMALIZE_EMAIL' IN SCHEMA {database}.STAGING"
      ddl:
        - "CREATE OR REPLACE FUNCTION {database}.STAGING.NORMALIZE_EMAIL(e VARCHAR) RETURNS VARCHAR AS 'LOWER(TRIM(e))'"
      grants:
        - "GRANT USAGE ON FUNCTION {database}.STAGING.NORMALIZE_EMAIL(VARCHAR) TO ROLE {database}_READ"
```

`exists_sql` is any query that returns a row when the object exists; `SHOW ...
LIKE` does. `{database}` becomes the database being refreshed. Role names
follow `infrastructure/terraform/modules/snowflake_rbac` (`<DB>_<ENV>_READ`,
`_WRITE`, `_OWNER`).

```bash
dbt run-operation preserve_objects --args '{mode: verify, database: ANALYTICS_STAGE}'
```
