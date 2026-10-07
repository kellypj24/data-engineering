{#-
    Objects dbt does not manage but an environment needs -- stages, UDFs,
    event tables, external tables -- recreated after a clone, provisioned when
    missing, or verified.

        dbt run-operation preserve_objects --args '{mode: verify, database: ANALYTICS_STAGE}'
        dbt run-operation preserve_objects --args '{mode: provision, database: ANALYTICS_STAGE, dry_run: false}'
        dbt run-operation preserve_objects --args '{mode: restore, database: ANALYTICS_STAGE, dry_run: false}'

    Modes:
      verify     read-only: runs each entry's `exists_sql`, logs present/missing
      provision  creates only the missing entries
      restore    recreates every entry (use after a clone; `ddl` should be
                 CREATE OR REPLACE)
    provision and restore are dry runs unless `dry_run: false`.

    The manifest is the `preservation_manifest` var: a list of entries

        name         label for the log
        exists_sql   query returning at least one row if the object exists
        ddl          list of statements that create it
        grants       list of GRANT statements (optional)
        post_create  list of follow-up statements (optional)

    `{database}` in any statement is replaced with the `database` argument, so
    one manifest serves every environment. See macros/snowflake/README.md for a
    Snowflake example. Returns {name: 'present' | 'missing'} from verify.
-#}

{% macro preserve_objects(mode, database, dry_run=true) %}
    {%- if mode not in ['verify', 'provision', 'restore'] -%}
        {{ exceptions.raise_compiler_error("preserve_objects: mode must be verify, provision, or restore; got " ~ mode) }}
    {%- endif -%}
    {%- if not database -%}
        {{ exceptions.raise_compiler_error("preserve_objects: database is required") }}
    {%- endif -%}
    {%- set manifest = var('preservation_manifest', []) -%}
    {%- set status = {} -%}

    {%- for entry in manifest -%}
        {%- for key in ['name', 'exists_sql', 'ddl'] if key not in entry -%}
            {{ exceptions.raise_compiler_error("preserve_objects: manifest entry " ~ loop.index ~ " has no `" ~ key ~ "`") }}
        {%- endfor -%}
        {%- set present = run_query(entry.exists_sql | replace('{database}', database)).rows | length > 0 -%}
        {%- do status.update({entry.name: 'present' if present else 'missing'}) -%}
        {{ log("preserve_objects: " ~ entry.name ~ ": " ~ status[entry.name], info=true) }}

        {%- if mode == 'restore' or (mode == 'provision' and not present) -%}
            {%- for statement in entry.ddl + entry.get('grants', []) + entry.get('post_create', []) -%}
                {%- set sql = statement | replace('{database}', database) -%}
                {{ log("preserve_objects: " ~ ("DRY RUN " if dry_run else "") ~ mode ~ " " ~ entry.name ~ ": " ~ sql, info=true) }}
                {%- if not dry_run -%}
                    {%- do run_query(sql) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if mode != 'verify' and not dry_run -%}
        {%- do adapter.commit() -%}
    {%- endif -%}
    {%- set missing = status.values() | select('equalto', 'missing') | list | length -%}
    {{ log("preserve_objects: " ~ mode ~ " on " ~ database ~ ": " ~ (status | length - missing) ~ " present, " ~ missing ~ " missing (before this run)", info=true) }}
    {{ return(status) }}
{% endmacro %}
