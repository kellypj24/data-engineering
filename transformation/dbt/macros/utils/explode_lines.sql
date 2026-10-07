{#-
    FROM-clause fragment: one row per line of a newline-separated text column,
    exposed as `exploded.line`.

        SELECT r.run_id, exploded.line
        FROM {{ explode_lines('r', 'files') }}

    default (duckdb, Postgres): unnest(string_to_array(...)) as a lateral
    function. Snowflake: SPLIT_TO_TABLE in a lateral inline view.
-#}

{% macro explode_lines(relation_alias, column) -%}
    {{ return(adapter.dispatch('explode_lines')(relation_alias, column)) }}
{%- endmacro %}


{% macro default__explode_lines(relation_alias, column) -%}
    {{ relation_alias }},
    unnest(string_to_array({{ relation_alias }}.{{ column }}, chr(10))) AS exploded (line)
{%- endmacro %}


{% macro snowflake__explode_lines(relation_alias, column) -%}
    {{ relation_alias }},
    LATERAL (
        SELECT split.value AS line
        FROM TABLE(SPLIT_TO_TABLE({{ relation_alias }}.{{ column }}, '\n')) AS split
    ) AS exploded
{%- endmacro %}
