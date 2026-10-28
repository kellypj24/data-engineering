{% test within_date_spine(model, column_name, spine=none, spine_column='date_day') %}
{#-
    Fails for every date in `column_name` outside the date spine. A date
    outside the spine does not error; it fails to join and drops out of every
    query that goes through the date dimension. Timestamps are compared by
    their date. NULLs are ignored (not_null's job).

    Default: the bounds dim_date is built from, var('date_spine_start')
    inclusive to var('date_spine_end') EXCLUSIVE. The spine has no gaps, so
    this is the same check as joining to dim_date, without depending on it:
    `dbt build --select +fct_x` would otherwise run this test against a
    dim_date that was never built.

    Pass `spine` (a relation, e.g. a custom date dimension) to check by join.

    Usage:
        columns:
          - name: order_date
            data_tests:
              - within_date_spine
-#}

{%- if spine is none %}
SELECT
    CAST({{ column_name }} AS DATE) AS outside_spine,
    COUNT(*) AS fact_rows
FROM {{ model }}
WHERE
    {{ column_name }} IS NOT NULL
    AND (
        CAST({{ column_name }} AS DATE) < CAST('{{ var("date_spine_start") }}' AS DATE)
        OR CAST({{ column_name }} AS DATE) >= CAST('{{ var("date_spine_end") }}' AS DATE)
    )
GROUP BY CAST({{ column_name }} AS DATE)
{%- else %}
SELECT
    CAST(fact.{{ column_name }} AS DATE) AS outside_spine,
    COUNT(*) AS fact_rows
FROM {{ model }} AS fact
LEFT JOIN {{ spine }} AS spine
    ON CAST(fact.{{ column_name }} AS DATE) = spine.{{ spine_column }}
WHERE fact.{{ column_name }} IS NOT NULL AND spine.{{ spine_column }} IS NULL
GROUP BY CAST(fact.{{ column_name }} AS DATE)
{%- endif %}

{% endtest %}
