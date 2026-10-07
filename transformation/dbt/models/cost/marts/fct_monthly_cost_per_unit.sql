{#-
    Optional: cost per business unit (customer, order, ...). Enabled only when
    the `cost_unit_model` var names a model with columns cost_month and units:

        vars:
          cost_unit_model: monthly_active_customers
-#}

{{ config(enabled=var('cost_unit_model', none) is not none) }}

WITH units AS (

    SELECT
        cost_month,
        units
    FROM {{ ref(var('cost_unit_model', 'fct_monthly_cost')) }}

),

monthly AS (

    SELECT
        cost_month,
        team,
        SUM(cost) AS cost
    FROM {{ ref('fct_monthly_cost') }}
    GROUP BY cost_month, team

)

SELECT
    monthly.cost_month,
    monthly.team,
    monthly.cost,
    units.units,
    monthly.cost / NULLIF(units.units, 0) AS cost_per_unit
FROM monthly
INNER JOIN units ON monthly.cost_month = units.cost_month
