{#-
    Dispatch helpers for the timestamp_is_utc generic test
    (tests/generic/timestamp_is_utc.sql). They classify a column as:
      'string'  -- ISO-8601 text; the test checks the trailing offset
      'offset'  -- a native type that keeps a per-value offset (Snowflake TIMESTAMP_TZ)
      'none'    -- nothing to check; the test logs a no-op
-#}

{% macro default__timestamp_is_utc_kind(column) -%}
    {{- return('string' if column.is_string() else 'none') -}}
{%- endmacro %}


{% macro snowflake__timestamp_is_utc_kind(column) -%}
    {%- if column.is_string() -%}
        {{- return('string') -}}
    {%- elif column.data_type | upper == 'TIMESTAMP_TZ' -%}
        {{- return('offset') -}}
    {%- endif -%}
    {{- return('none') -}}
{%- endmacro %}


{% macro snowflake__timestamp_offset_is_nonzero(column_name) -%}
    (DATE_PART(TIMEZONE_HOUR, {{ column_name }}) <> 0 OR DATE_PART(TIMEZONE_MINUTE, {{ column_name }}) <> 0)
{%- endmacro %}


{% macro default__timestamp_offset_is_nonzero(column_name) -%}
    FALSE
{%- endmacro %}
