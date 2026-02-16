{% macro clean_string(column_name) -%}
    {#-
        Standardize a string column: lowercase, trim whitespace,
        and convert empty strings to NULL.

        Usage:
            SELECT
                {{ clean_string('email') }} AS email,
                {{ clean_string('first_name') }} AS first_name
            FROM {{ source('raw', 'customers') }}
    -#}
    NULLIF(TRIM(LOWER({{ column_name }})), '')
{%- endmacro %}
