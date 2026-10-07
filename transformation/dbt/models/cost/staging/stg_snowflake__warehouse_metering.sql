SELECT
    warehouse_name,
    start_time,
    credits_used_compute,
    credits_used_cloud_services,
    {{ dbt.date_trunc('hour', 'start_time') }} AS usage_hour
FROM {{ source('snowflake_account_usage', 'warehouse_metering_history') }}
