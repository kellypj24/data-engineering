{#-
    Snowflake extra. Ownership drift: schemas, tables, and views in a database
    not owned by its designated owner role, and an owner role that can neither
    create schemas (CREATE SCHEMA) nor owns the database.

        dbt run-operation audit_object_ownership --args '{database: ANALYTICS_STAGE}'
        dbt run-operation normalize_object_ownership --args '{database: ANALYTICS_STAGE}'                  # dry run
        dbt run-operation normalize_object_ownership --args '{database: ANALYTICS_STAGE, dry_run: false}'

    Why: a clone preserves each object's owner. Clone prod into stage and every
    object is still owned by the prod owner role, so stage's dbt role cannot
    `create or replace` an existing model, or create the __dbt_tmp tables that
    incrementals and snapshots need.

    `owner_role` defaults to <DATABASE>_OWNER, the owner role
    infrastructure/terraform/modules/snowflake_rbac creates. The audit and the
    repair share `ownership_drift`, so they cannot disagree about what
    "correct" means.
-#}

{#- What is wrong. `objects`: [{object_type: SCHEMA|TABLE|VIEW, name, owner}];
    `owner_can_create_schemas`: bool. Returns the drifted entries, plus a
    {object_type: 'CREATE SCHEMA'} entry when the owner cannot create schemas. -#}
{% macro ownership_drift(objects, owner_can_create_schemas, owner_role) %}
    {%- set drift = [] -%}
    {%- for object in objects if (object.owner or '') | upper != owner_role | upper -%}
        {%- do drift.append(object) -%}
    {%- endfor -%}
    {%- if not owner_can_create_schemas -%}
        {%- do drift.append({'object_type': 'CREATE SCHEMA', 'name': none, 'owner': none}) -%}
    {%- endif -%}
    {{ return(drift) }}
{% endmacro %}


{#- How to fix it: schemas first, then tables and views. COPY CURRENT GRANTS
    keeps the grants other roles already hold on each object. -#}
{% macro ownership_repair_statements(drift, database, owner_role) %}
    {%- set statements = [] -%}
    {%- for kind in ['CREATE SCHEMA', 'SCHEMA', 'TABLE', 'VIEW'] -%}
        {%- for item in drift if item.object_type == kind -%}
            {%- if kind == 'CREATE SCHEMA' -%}
                {%- do statements.append('GRANT CREATE SCHEMA ON DATABASE ' ~ database ~ ' TO ROLE ' ~ owner_role) -%}
            {%- else -%}
                {%- do statements.append(
                    'GRANT OWNERSHIP ON ' ~ kind ~ ' ' ~ item.name ~ ' TO ROLE ' ~ owner_role ~ ' COPY CURRENT GRANTS'
                ) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    {{ return(statements) }}
{% endmacro %}


{#- Current owners, read from the database's INFORMATION_SCHEMA. Base tables
    (permanent and transient) and views only. -#}
{% macro snowflake_object_owners(database) %}
    {%- set db = database | upper -%}
    {%- set sql -%}
        SELECT 'SCHEMA' AS object_type,
               '"' || catalog_name || '"."' || schema_name || '"' AS name,
               schema_owner AS owner
        FROM "{{ db }}".information_schema.schemata
        WHERE schema_name <> 'INFORMATION_SCHEMA'
        UNION ALL
        SELECT IFF(table_type = 'VIEW', 'VIEW', 'TABLE'),
               '"' || table_catalog || '"."' || table_schema || '"."' || table_name || '"',
               table_owner
        FROM "{{ db }}".information_schema.tables
        WHERE table_schema <> 'INFORMATION_SCHEMA'
          AND table_type IN ('BASE TABLE', 'VIEW')
    {%- endset -%}
    {%- set objects = [] -%}
    {%- for row in run_query(sql).rows -%}
        {%- do objects.append({'object_type': row[0], 'name': row[1], 'owner': row[2]}) -%}
    {%- endfor -%}
    {{ return(objects) }}
{% endmacro %}


{% macro snowflake_role_can_create_schemas(database, role) %}
    {%- for row in run_query('SHOW GRANTS ON DATABASE ' ~ database).rows -%}
        {%- if row['granted_to'] == 'ROLE' and row['grantee_name'] | upper == role | upper
              and row['privilege'] in ['OWNERSHIP', 'CREATE SCHEMA'] -%}
            {{ return(true) }}
        {%- endif -%}
    {%- endfor -%}
    {{ return(false) }}
{% endmacro %}


{% macro _ownership_drift_for(database, owner_role) %}
    {{ return(ownership_drift(
        snowflake_object_owners(database),
        snowflake_role_can_create_schemas(database, owner_role),
        owner_role,
    )) }}
{% endmacro %}


{% macro audit_object_ownership(database, owner_role=none) %}
    {%- set database = database | upper -%}
    {%- set owner_role = (owner_role or database ~ '_OWNER') | upper -%}
    {%- if adapter.type() != 'snowflake' -%}
        {{ exceptions.raise_compiler_error("audit_object_ownership reads Snowflake ownership; adapter is " ~ adapter.type()) }}
    {%- endif -%}
    {%- set drift = _ownership_drift_for(database, owner_role) -%}
    {%- for item in drift -%}
        {%- if item.object_type == 'CREATE SCHEMA' -%}
            {{ log("audit_object_ownership: " ~ owner_role ~ " can neither create schemas in nor owns " ~ database, info=true) }}
        {%- else -%}
            {{ log("audit_object_ownership: " ~ item.object_type ~ " " ~ item.name ~ " owned by " ~ item.owner, info=true) }}
        {%- endif -%}
    {%- endfor -%}
    {{ log("audit_object_ownership: " ~ database ~ ": " ~ drift | length ~ " drifted (expected owner " ~ owner_role ~ ")", info=true) }}
    {{ return(drift) }}
{% endmacro %}


{% macro normalize_object_ownership(database, owner_role=none, dry_run=true) %}
    {%- set database = database | upper -%}
    {%- set owner_role = (owner_role or database ~ '_OWNER') | upper -%}
    {%- if adapter.type() != 'snowflake' -%}
        {%- if dry_run -%}
            {{ log("normalize_object_ownership: DRY RUN, ownership of " ~ database ~ " is read from Snowflake at run time and moved to " ~ owner_role, info=true) }}
            {{ return([]) }}
        {%- endif -%}
        {{ exceptions.raise_compiler_error("normalize_object_ownership runs on Snowflake only; adapter is " ~ adapter.type()) }}
    {%- endif -%}
    {%- set statements = ownership_repair_statements(_ownership_drift_for(database, owner_role), database, owner_role) -%}
    {{ log("normalize_object_ownership: " ~ ("DRY RUN, " if dry_run else "") ~ statements | length ~ " statement(s) for " ~ database, info=true) }}
    {%- for statement in statements -%}
        {{ log("normalize_object_ownership: " ~ statement, info=true) }}
        {%- if not dry_run -%}
            {%- do run_query(statement) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if not dry_run -%}
        {%- do adapter.commit() -%}
    {%- endif -%}
    {{ return(statements) }}
{% endmacro %}
