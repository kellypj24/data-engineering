{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        Override the default schema generation to:
        - Dev: prefix schemas with the target name (e.g., dev_staging)
        - Prod: use the custom schema name directly (e.g., staging)
        - Fallback: use the target schema if no custom schema is set
    -#}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
