{% test ties_to_control_total(
    model,
    column_name,
    period_column,
    control,
    control_period_column,
    control_value_column,
    tolerance=0
) %}
{#-
    Ties SUM(column_name) per period to an independent control total.

    The control drives the comparison: every period it contains is a complete
    period, and must match within `tolerance`. Periods only the model has (the
    current, still-open period) are ignored. A period the control has and the
    model lacks fails.

    Fails when ZERO periods are compared, so an empty control or a join that
    matches nothing cannot pass vacuously.

    Usage (column level):
        columns:
          - name: revenue
            tests:
              - ties_to_control_total:
                  period_column: order_date
                  control: source('raw', 'order_daily_totals')
                  control_period_column: order_date
                  control_value_column: revenue
                  tolerance: 0.01
-#}

WITH fact_totals AS (
    SELECT
        {{ period_column }} AS period,
        SUM({{ column_name }}) AS fact_total
    FROM {{ model }}
    GROUP BY {{ period_column }}
),

control_totals AS (
    SELECT
        {{ control_period_column }} AS period,
        SUM({{ control_value_column }}) AS control_total
    FROM {{ control }}
    GROUP BY {{ control_period_column }}
),

compared AS (
    SELECT
        c.period,
        f.fact_total,
        c.control_total
    FROM control_totals AS c
    LEFT JOIN fact_totals AS f
        ON c.period = f.period
)

SELECT
    CAST(period AS {{ dbt.type_string() }}) AS period,
    fact_total,
    control_total,
    'mismatch' AS failure
FROM compared
WHERE fact_total IS NULL OR ABS(fact_total - control_total) > {{ tolerance }}

UNION ALL

SELECT
    CAST(NULL AS {{ dbt.type_string() }}),
    NULL,
    NULL,
    'zero periods compared'
FROM (SELECT COUNT(*) AS n FROM compared) AS counted
WHERE n = 0

{% endtest %}
