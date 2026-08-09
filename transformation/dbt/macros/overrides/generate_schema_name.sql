{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Override the default schema generation to:
        - Non-prod: prefix the custom schema with the target's schema
          (e.g., main_staging on duckdb, analytics_staging on snowflake)
        - Prod: use the custom schema name directly (e.g., staging)
        - Fallback: use the target schema if no custom schema is set

        Environment comes from the `dbt_env` var (DBT_ENV, default 'dev'), not
        from target.name -- the target selects a warehouse, not an environment,
        so branching on it made the prod path unreachable.
    -#}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif var('dbt_env') == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
