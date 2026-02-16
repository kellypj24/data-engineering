{% macro limit_data_in_dev(column_name, dev_days_of_data=3) -%}
    {#-
        Append a WHERE clause that limits data to the most recent N days
        when running in the dev target. Production runs get all data.

        Usage:
            SELECT *
            FROM {{ source('raw', 'orders') }}
            WHERE 1=1
            {{ limit_data_in_dev('created_at') }}
    -#}
    {%- if target.name == 'dev' -%}
        AND {{ column_name }} >= DATEADD('day', -{{ dev_days_of_data }}, CURRENT_DATE())
    {%- endif -%}
{%- endmacro %}
