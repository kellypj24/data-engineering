{% macro star_except(relation, columns) -%}
    {#-
        Every column of `relation` except `columns`, as a select list:
            SELECT {{ star_except(ref('stg_orders'), ['_grain_rows']) }}
            FROM {{ ref('stg_orders') }}

        duckdb and Snowflake: `* EXCLUDE (...)`; BigQuery: `* EXCEPT (...)`;
        other adapters (Postgres): dbt_utils.star, which lists the remaining
        columns from the warehouse at compile time, so `relation` must be a
        ref or source there, not a CTE.
    -#}
    {%- set columns = [columns] if columns is string else columns -%}
    {{- return(adapter.dispatch('star_except', 'data_warehouse')(relation, columns)) -}}
{%- endmacro %}

{% macro default__star_except(relation, columns) -%}
    {{ dbt_utils.star(from=relation, except=columns) }}
{%- endmacro %}

{% macro duckdb__star_except(relation, columns) -%}
    * EXCLUDE ({{ columns | join(', ') }})
{%- endmacro %}

{% macro snowflake__star_except(relation, columns) -%}
    * EXCLUDE ({{ columns | join(', ') }})
{%- endmacro %}

{% macro bigquery__star_except(relation, columns) -%}
    * EXCEPT ({{ columns | join(', ') }})
{%- endmacro %}
