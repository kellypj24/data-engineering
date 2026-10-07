SELECT
    usage_date,
    UPPER(database_name) AS database_name,
    COALESCE(average_database_bytes, 0) + COALESCE(average_failsafe_bytes, 0) AS bytes
FROM {{ source('snowflake_account_usage', 'database_storage_usage_history') }}
