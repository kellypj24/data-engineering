{#-
    Abort the current node when a guard query returns rows. For a model's
    `pre_hook`, so a period-close guard runs before anything is written
    (docs/patterns/period-close.md):

        {{ config(pre_hook="{{ assert_no_rows(period_close_guard(), 'closed period would be rewritten') }}") }}

    Keep the guard's SQL in a macro and call the same macro from a singular
    test, so the pre-write check and the test cannot disagree.
    Returns no SQL; a no-op at parse time.
-#}
{% macro assert_no_rows(sql, message) %}
    {%- if execute -%}
        {%- set result = run_query('SELECT COUNT(*) FROM (' ~ sql ~ ') AS guard') -%}
        {%- set rows = result.columns[0].values()[0] -%}
        {%- if rows > 0 -%}
            {{ exceptions.raise_compiler_error('assert_no_rows: ' ~ message ~ ' (' ~ rows ~ ' rows)') }}
        {%- endif -%}
    {%- endif -%}
    {{ return('') }}
{% endmacro %}
