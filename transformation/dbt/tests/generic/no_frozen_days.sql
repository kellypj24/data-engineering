{#-
    A history table stamped with `current_date` (one copy of the source per
    day): fails on a date whose rows equal the previous date's across every
    key. Real data that never changes trips it too, so it defaults to warn.
    docs/patterns/durable-facts.md#history-tables-stamped-with-current_date.

        data_tests:
          - no_frozen_days:
              date_column: snapshot_date
              key: customer_id
              value_columns: [status, balance]

    The usual cause is an incremental filter with `>` instead of `>=`, which
    freezes each day at its first run. Repair with `restate_history_date`.
-#}
{% test no_frozen_days(model, date_column, key, value_columns) %}
{{ config(severity='warn') }}
{%- set keys = [key] if key is string else key -%}

WITH days AS (
    SELECT DISTINCT {{ date_column }} AS day FROM {{ model }}
),

pairs AS (
    SELECT
        day,
        LAG(day) OVER (ORDER BY day) AS previous_day
    FROM days
),

today AS (
    SELECT m.*, p.day AS pair_day
    FROM {{ model }} AS m
    INNER JOIN pairs AS p ON m.{{ date_column }} = p.day
    WHERE p.previous_day IS NOT NULL
),

yesterday AS (
    SELECT m.*, p.day AS pair_day
    FROM {{ model }} AS m
    INNER JOIN pairs AS p ON m.{{ date_column }} = p.previous_day
),

changed AS (
    SELECT DISTINCT COALESCE(t.pair_day, y.pair_day) AS day
    FROM today AS t
    FULL OUTER JOIN yesterday AS y
        ON
            t.pair_day = y.pair_day
            {%- for k in keys %}
            AND t.{{ k }} = y.{{ k }}
            {%- endfor %}
    WHERE
        t.pair_day IS NULL
        OR y.pair_day IS NULL
        {%- for column in value_columns %}
        OR t.{{ column }} IS DISTINCT FROM y.{{ column }}
        {%- endfor %}
)

SELECT
    p.day AS frozen_day,
    p.previous_day
FROM pairs AS p
LEFT JOIN changed AS c ON p.day = c.day
WHERE p.previous_day IS NOT NULL AND c.day IS NULL
{% endtest %}
