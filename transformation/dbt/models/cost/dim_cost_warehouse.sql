{#-
    Accumulating: WAREHOUSE_METERING_HISTORY keeps 365 days, the monthly fact
    keeps months forever, so a warehouse dropped two years ago must stay here
    or the fact's warehouse_name loses its parent. Each run merges what the
    source still shows into what this table already holds; full_refresh=false
    keeps a --full-refresh from forgetting old warehouses. 'NONE' is the parent
    for costs with no warehouse (storage, cloud services, other).
-#}

{{ config(
    materialized='incremental',
    unique_key='warehouse_name',
    incremental_strategy='delete+insert',
    full_refresh=false
) }}

WITH seen AS (

    SELECT
        warehouse_name,
        MIN(start_time) AS first_seen_at,
        MAX(start_time) AS last_seen_at
    FROM {{ ref('stg_snowflake__warehouse_metering') }}
    GROUP BY warehouse_name
    UNION ALL
    SELECT
        'NONE' AS warehouse_name,
        CAST(NULL AS {{ dbt.type_timestamp() }}) AS first_seen_at,
        CAST(NULL AS {{ dbt.type_timestamp() }}) AS last_seen_at
    {% if is_incremental() %}
        UNION ALL
        SELECT
            warehouse_name,
            first_seen_at,
            last_seen_at
        FROM {{ this }}
    {% endif %}

)

SELECT
    warehouse_name,
    MIN(first_seen_at) AS first_seen_at,
    MAX(last_seen_at) AS last_seen_at
FROM seen
GROUP BY warehouse_name
