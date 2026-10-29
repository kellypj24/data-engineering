{% macro utc_date_trunc(datepart, column) -%}
    {#-
        Truncate a zoned timestamp to `datepart` in UTC, whatever the session
        time zone. Returns a naive timestamp holding the UTC wall-clock time.

        `column` must be zoned: Snowflake TIMESTAMP_LTZ / TIMESTAMP_TZ, or
        timestamptz elsewhere. A naive input is read as session-local time and
        shifted. Apply it once, where a zoned timestamp enters (staging);
        downstream, the result is naive UTC and plain truncation or
        CAST(... AS DATE) is already zone-free.

        Usage:
            {{ utc_date_trunc('hour', 'start_time') }} AS usage_hour
    -#}
    {{- return(adapter.dispatch('utc_date_trunc')(datepart, column)) -}}
{%- endmacro %}


{% macro default__utc_date_trunc(datepart, column) -%}
    {{ dbt.date_trunc(datepart, "timezone('UTC', " ~ column ~ ")") }}
{%- endmacro %}


{% macro snowflake__utc_date_trunc(datepart, column) -%}
    {{ dbt.date_trunc(datepart, "CONVERT_TIMEZONE('UTC', " ~ column ~ ")::TIMESTAMP_NTZ") }}
{%- endmacro %}


{% macro bigquery__utc_date_trunc(datepart, column) -%}
    {#- BigQuery TIMESTAMP is an instant and TIMESTAMP_TRUNC defaults to UTC. -#}
    {{ dbt.date_trunc(datepart, column) }}
{%- endmacro %}
