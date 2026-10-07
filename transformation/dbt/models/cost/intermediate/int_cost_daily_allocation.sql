{#
    Each day's invoiced amount, per usage type, split into (team, workload,
    warehouse) shares. Every share is a fraction of an invoiced amount, so the
    allocations sum to the invoice by construction.

      compute         by query-attributed credits (QUERY_ATTRIBUTION_HISTORY),
                      not by warehouse name. A warehouse-hour's metered credits
                      beyond its queries' attributed credits is idle time:
                      SHARED / IDLE.
      cloud services  SHARED / CLOUD_SERVICES
      storage         by database bytes; team from cost_database_mapping
      anything else   SHARED / OTHER

    Query owner: cost_user_mapping by user. Users flagged resolve_by_database
    take the team of the database the query ran in. Anything unmapped is
    NEEDS_OWNER_REVIEW, never a guess. Invoiced compute or storage on a day
    with nothing to split it by is NEEDS_OWNER_REVIEW too.
#}

WITH users AS (

    SELECT
        team,
        workload,
        resolve_by_database,
        UPPER(user_name) AS user_name
    FROM {{ ref('cost_user_mapping') }}

),

database_owners AS (

    SELECT
        team,
        UPPER(database_name) AS database_name
    FROM {{ ref('cost_database_mapping') }}

),

invoice AS (

    SELECT * FROM {{ ref('stg_snowflake__usage_in_currency_daily') }}

),

query_owners AS (

    SELECT
        query_rows.usage_hour,
        query_rows.warehouse_name,
        query_rows.credits,
        CASE
            WHEN users.user_name IS NULL THEN 'NEEDS_OWNER_REVIEW'
            WHEN users.resolve_by_database THEN COALESCE(database_owners.team, 'NEEDS_OWNER_REVIEW')
            ELSE COALESCE(users.team, 'NEEDS_OWNER_REVIEW')
        END AS team,
        COALESCE(users.workload, 'NEEDS_OWNER_REVIEW') AS workload
    FROM {{ ref('stg_snowflake__query_attribution') }} AS query_rows
    LEFT JOIN users ON query_rows.user_name = users.user_name
    LEFT JOIN database_owners ON query_rows.database_name = database_owners.database_name

),

attributed_per_hour AS (

    SELECT
        usage_hour,
        warehouse_name,
        SUM(credits) AS credits
    FROM query_owners
    GROUP BY usage_hour, warehouse_name

),

idle AS (

    SELECT
        metering.usage_hour,
        metering.warehouse_name,
        'SHARED' AS team,
        'IDLE' AS workload,
        GREATEST(metering.credits_used_compute - COALESCE(attributed.credits, 0), 0) AS credits
    FROM {{ ref('stg_snowflake__warehouse_metering') }} AS metering
    LEFT JOIN attributed_per_hour AS attributed
        ON
            metering.usage_hour = attributed.usage_hour
            AND metering.warehouse_name = attributed.warehouse_name

),

compute_components AS (

    SELECT
        warehouse_name,
        team,
        workload,
        credits,
        CAST(usage_hour AS DATE) AS usage_date
    FROM query_owners
    UNION ALL
    SELECT
        warehouse_name,
        team,
        workload,
        credits,
        CAST(usage_hour AS DATE) AS usage_date
    FROM idle

),

compute_day_credits AS (

    SELECT
        usage_date,
        SUM(credits) AS credits
    FROM compute_components
    GROUP BY usage_date

),

storage_day_bytes AS (

    SELECT
        usage_date,
        SUM(bytes) AS bytes
    FROM {{ ref('stg_snowflake__database_storage') }}
    GROUP BY usage_date

),

compute_cost AS (

    SELECT
        components.usage_date,
        components.warehouse_name,
        components.team,
        components.workload,
        components.credits,
        invoice.currency,
        invoice.usage_in_currency * components.credits / day_total.credits AS cost
    FROM compute_components AS components
    INNER JOIN compute_day_credits AS day_total
        ON components.usage_date = day_total.usage_date
    INNER JOIN invoice
        ON
            components.usage_date = invoice.usage_date
            AND invoice.usage_type = 'compute'
    WHERE day_total.credits > 0

),

storage_cost AS (

    SELECT
        storage_rows.usage_date,
        'NONE' AS warehouse_name,
        COALESCE(database_owners.team, 'NEEDS_OWNER_REVIEW') AS team,
        'STORAGE' AS workload,
        CAST(NULL AS {{ dbt.type_float() }}) AS credits,
        invoice.currency,
        invoice.usage_in_currency * storage_rows.bytes / day_total.bytes AS cost
    FROM {{ ref('stg_snowflake__database_storage') }} AS storage_rows
    INNER JOIN storage_day_bytes AS day_total
        ON storage_rows.usage_date = day_total.usage_date
    INNER JOIN invoice
        ON
            storage_rows.usage_date = invoice.usage_date
            AND invoice.usage_type = 'storage'
    LEFT JOIN database_owners ON storage_rows.database_name = database_owners.database_name
    WHERE day_total.bytes > 0

),

whole_invoice_lines AS (

    SELECT
        invoice.usage_date,
        'NONE' AS warehouse_name,
        CASE
            WHEN invoice.usage_type IN ('compute', 'storage') THEN 'NEEDS_OWNER_REVIEW'
            ELSE 'SHARED'
        END AS team,
        CASE
            WHEN invoice.usage_type IN ('compute', 'storage') THEN 'NEEDS_OWNER_REVIEW'
            WHEN invoice.usage_type = 'cloud services' THEN 'CLOUD_SERVICES'
            ELSE 'OTHER'
        END AS workload,
        invoice.usage AS credits,
        invoice.currency,
        invoice.usage_in_currency AS cost
    FROM invoice
    LEFT JOIN compute_day_credits AS compute_day
        ON invoice.usage_date = compute_day.usage_date
    LEFT JOIN storage_day_bytes AS storage_day
        ON invoice.usage_date = storage_day.usage_date
    WHERE
        invoice.usage_type NOT IN ('compute', 'storage')
        OR (invoice.usage_type = 'compute' AND COALESCE(compute_day.credits, 0) = 0)
        OR (invoice.usage_type = 'storage' AND COALESCE(storage_day.bytes, 0) = 0)

)

SELECT * FROM compute_cost
UNION ALL
SELECT * FROM storage_cost
UNION ALL
SELECT * FROM whole_invoice_lines
