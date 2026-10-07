SELECT
    usage_date,
    usage,
    usage_in_currency,
    currency,
    LOWER(usage_type) AS usage_type,
    CAST({{ dbt.date_trunc('month', 'usage_date') }} AS DATE) AS usage_month
FROM {{ source('snowflake_organization_usage', 'usage_in_currency_daily') }}
