{% test timestamp_is_utc(model, column_name) %}
{#-
    Fails rows whose timestamp carries a non-UTC offset. Mixed offsets are the
    classic silent join bug: two sources that agree on the wall clock but not
    the zone.

    What it can check depends on how the column stores time:

      string (ISO-8601)        Fails values ending in an offset other than
                               +00:00 / -00:00, e.g. `2026-01-01T09:00:00+02:00`.
                               `Z`, `+00:00`, and values with no offset pass.
      Snowflake TIMESTAMP_TZ   Fails values whose stored offset is not zero.
      anything else            NO-OP. duckdb/Postgres timestamptz normalise to
                               UTC and keep no per-value offset; BigQuery
                               TIMESTAMP is absolute; naive timestamps have no
                               zone to check. The test logs that it checked
                               nothing rather than passing silently.

    Usage:
        columns:
          - name: created_at
            tests:
              - timestamp_is_utc
-#}
{%- set kind = 'unknown' -%}
{#- Columns come from the built relation, so `dbt compile` before the model
    exists sees none; only a built relation missing the column is an error. -#}
{%- set columns = adapter.get_columns_in_relation(model) if execute else [] -%}
{%- if columns -%}
    {%- set matches = columns | selectattr('name', 'equalto', column_name) | list
        or columns | selectattr('name', 'equalto', column_name | upper) | list -%}
    {%- if not matches -%}
        {{ exceptions.raise_compiler_error("timestamp_is_utc: " ~ model ~ " has no column " ~ column_name) }}
    {%- endif -%}
    {%- set kind = adapter.dispatch('timestamp_is_utc_kind', 'data_warehouse')(matches[0]) -%}
    {%- if kind == 'none' -%}
        {{ log(
            "timestamp_is_utc: " ~ model ~ "." ~ column_name ~ " is " ~ matches[0].data_type
            ~ ", which stores no per-value offset on " ~ target.type ~ " -- nothing to check (no-op).",
            info=true
        ) }}
    {%- endif -%}
{%- endif %}

SELECT {{ column_name }}
FROM {{ model }}
{%- if kind == 'string' %}
WHERE
    (
        {{ column_name }} LIKE '%+__:__'
        OR {{ column_name }} LIKE '%-__:__'
    )
    AND RIGHT({{ column_name }}, 6) NOT IN ('+00:00', '-00:00')
{%- elif kind == 'offset' %}
WHERE {{ adapter.dispatch('timestamp_offset_is_nonzero', 'data_warehouse')(column_name) }}
{%- else %}
WHERE FALSE
{%- endif %}
{% endtest %}
