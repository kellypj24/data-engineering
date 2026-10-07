{#-
    Snowflake extra. Zero-copy clone a database, replacing the target.

        dbt run-operation clone_database --args '{source: ANALYTICS_PROD, target: ANALYTICS_STAGE}'
        dbt run-operation clone_database --args '{source: ANALYTICS_PROD, target: ANALYTICS_STAGE, dry_run: false}'

    dry_run (default true) logs the statements and runs nothing. Executing
    requires a Snowflake target.

    copy_grants (default true). `CREATE OR REPLACE DATABASE ... CLONE` keeps
    grants on the cloned *child* objects, but the grants on the replaced
    database itself are dropped -- the usual way a refresh silently locks out a
    downstream role. Snowflake has no COPY GRANTS for databases, so this macro
    reads `SHOW GRANTS ON DATABASE <target>` first and re-applies them after
    the clone, ownership last.

    (`target` here is the macro argument, which shadows dbt's `target`; the
    adapter check uses adapter.type().)

    Refuses: a missing or identical source/target, and a target equal to the
    production database (`environment_databases.prod`) by name.
-#}

{% macro clone_database(source, target, copy_grants=true, dry_run=true) %}
    {%- set source = (source or '') | upper -%}
    {%- set target_db = (target or '') | upper -%}
    {%- set prod_db = (var('environment_databases', {}).get('prod') or '') | upper -%}

    {%- if not source or not target_db -%}
        {{ exceptions.raise_compiler_error("clone_database: source and target are required") }}
    {%- endif -%}
    {%- if source == target_db -%}
        {{ exceptions.raise_compiler_error("clone_database: source and target are both " ~ source) }}
    {%- endif -%}
    {%- if target_db == prod_db -%}
        {{ exceptions.raise_compiler_error(
            "clone_database: refusing to replace the production database " ~ prod_db
        ) }}
    {%- endif -%}
    {%- if not dry_run and adapter.type() != 'snowflake' -%}
        {{ exceptions.raise_compiler_error(
            "clone_database runs on Snowflake only (adapter is " ~ adapter.type() ~ "); use dry_run"
        ) }}
    {%- endif -%}

    {%- set grants = [] -%}
    {%- if copy_grants and adapter.type() == 'snowflake' -%}
        {%- set grants = snowflake_database_grants(target_db) -%}
    {%- endif -%}

    {%- set statements = clone_database_statements(source, target_db, grants) -%}

    {%- if copy_grants and adapter.type() != 'snowflake' -%}
        {{ log("clone_database: grants on " ~ target_db ~ " are read from Snowflake at run time and re-applied after the clone", info=true) }}
    {%- endif -%}
    {{ log("clone_database: " ~ ("DRY RUN, nothing executed" if dry_run else "executing") ~ ": " ~ source ~ " -> " ~ target_db, info=true) }}
    {%- for statement in statements -%}
        {{ log("clone_database: " ~ statement, info=true) }}
        {%- if not dry_run -%}
            {%- do run_query(statement) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if not dry_run -%}
        {%- do adapter.commit() -%}
    {%- endif -%}
{% endmacro %}


{#- The statements for one clone: the clone, then each captured grant
    re-applied, ownership last (granting ownership first would leave the
    executing role unable to re-grant the rest). -#}
{% macro clone_database_statements(source, target_db, grants) %}
    {%- set statements = ['CREATE OR REPLACE DATABASE ' ~ target_db ~ ' CLONE ' ~ source] -%}
    {%- for grant in grants if grant.privilege != 'OWNERSHIP' -%}
        {%- do statements.append(
            'GRANT ' ~ grant.privilege ~ ' ON DATABASE ' ~ target_db
            ~ ' TO ' ~ grant.grantee_type ~ ' ' ~ grant.grantee
        ) -%}
    {%- endfor -%}
    {%- for grant in grants if grant.privilege == 'OWNERSHIP' -%}
        {%- do statements.append(
            'GRANT OWNERSHIP ON DATABASE ' ~ target_db ~ ' TO ROLE ' ~ grant.grantee ~ ' COPY CURRENT GRANTS'
        ) -%}
    {%- endfor -%}
    {{ return(statements) }}
{% endmacro %}


{#- Grants on a database as [{privilege, grantee_type, grantee}], or [] if it
    does not exist yet. Grants to a database role are skipped: database roles
    live inside the replaced database and are cloned with it. -#}
{% macro snowflake_database_grants(database) %}
    {%- set exists = run_query("SHOW DATABASES LIKE '" ~ database ~ "'") -%}
    {%- if exists.rows | length == 0 -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set result = run_query('SHOW GRANTS ON DATABASE ' ~ database) -%}
    {%- set grants = [] -%}
    {%- for row in result.rows if row['granted_to'] == 'ROLE' -%}
        {%- do grants.append({
            'privilege': row['privilege'],
            'grantee_type': 'ROLE',
            'grantee': row['grantee_name'],
        }) -%}
    {%- endfor -%}
    {{ return(grants) }}
{% endmacro %}


{#-
    Rebuild a shared environment as a clone of production:

        dbt run-operation refresh_environment --args '{env: stage}'                 # dry run
        dbt run-operation refresh_environment --args '{env: stage, dry_run: false}'

    Environments map to databases in the `environment_databases` var. Refuses
    env 'prod', an unknown env, and any env mapped to the production database.
-#}
{% macro refresh_environment(env, dry_run=true) %}
    {%- set databases = var('environment_databases', {}) -%}
    {%- if env == 'prod' -%}
        {{ exceptions.raise_compiler_error("refresh_environment: refusing to refresh prod; it is the clone source") }}
    {%- endif -%}
    {%- if env not in databases -%}
        {{ exceptions.raise_compiler_error(
            "refresh_environment: unknown env " ~ env ~ "; known: " ~ (databases.keys() | list | join(', '))
        ) }}
    {%- endif -%}
    {%- do clone_database(databases['prod'], databases[env], copy_grants=true, dry_run=dry_run) -%}
{% endmacro %}
