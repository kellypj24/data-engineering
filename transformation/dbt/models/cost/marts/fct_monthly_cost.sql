{#-
    DURABLE FACT (see docs/patterns/durable-facts.md): one row per month,
    team, workload, and warehouse, in the invoice currency. ACCOUNT_USAGE keeps
    365 days; this keeps every month. full_refresh=false, and only the last
    `cost_restate_months` months (default 2) before the latest loaded month are
    reprocessed. Each complete month ties to the invoice within a cent
    (ties_to_control_total).
-#}

{{ config(
    materialized='incremental',
    unique_key='cost_month',
    incremental_strategy='delete+insert',
    full_refresh=false
) }}

WITH allocations AS (

    SELECT
        *,
        CAST({{ dbt.date_trunc('month', 'usage_date') }} AS DATE) AS cost_month
    FROM {{ ref('int_cost_daily_allocation') }}

)

SELECT
    cost_month,
    team,
    workload,
    warehouse_name,
    currency,
    SUM(credits) AS credits,
    SUM(cost) AS cost
FROM allocations
{% if is_incremental() %}
    WHERE cost_month >= (
        SELECT {{ dbt.dateadd('month', -var('cost_restate_months', 2), 'MAX(cost_month)') }}
        FROM {{ this }}
    )
{% endif %}
GROUP BY cost_month, team, workload, warehouse_name, currency
