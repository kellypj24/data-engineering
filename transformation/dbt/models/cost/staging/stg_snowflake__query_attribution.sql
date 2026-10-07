SELECT
    query_attribution.query_id,
    query_attribution.warehouse_name,
    query_attribution.start_time,
    query_attribution.credits_attributed_compute AS credits,
    UPPER(query_attribution.user_name) AS user_name,
    UPPER(query_history.database_name) AS database_name,
    {{ dbt.date_trunc('hour', 'query_attribution.start_time') }} AS usage_hour
FROM {{ source('snowflake_account_usage', 'query_attribution_history') }} AS query_attribution
LEFT JOIN {{ source('snowflake_account_usage', 'query_history') }} AS query_history
    ON query_attribution.query_id = query_history.query_id
