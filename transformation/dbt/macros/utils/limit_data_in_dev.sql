{% macro limit_data_in_dev(column_name, dev_days_of_data=3) -%}
    {#-
        A predicate that limits data to the most recent N days outside prod,
        so dev runs stay fast. Production runs get all data.

        Returns a complete boolean expression -- TRUE in prod -- so it is
        always valid on its own and composes with AND. Callers never need a
        `WHERE 1 = 1` anchor.

        Environment comes from the `dbt_env` var (DBT_ENV, default 'dev'),
        not from target.name: the target selects a warehouse, not an
        environment.

        Usage:
            SELECT *
            FROM {{ source('raw', 'orders') }}
            WHERE {{ limit_data_in_dev('created_at') }}

            -- composed with a real predicate
            SELECT *
            FROM {{ source('raw', 'orders') }}
            WHERE {{ limit_data_in_dev('created_at') }}
                AND status IS NOT NULL
    -#}
    {%- if var('dbt_env') == 'prod' -%}
        TRUE
    {%- else -%}
        {{ column_name }} >= {{ dbt.dateadd('day', -dev_days_of_data, 'CURRENT_DATE') }}
    {%- endif -%}
{%- endmacro %}
