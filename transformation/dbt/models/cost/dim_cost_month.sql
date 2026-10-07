{#- Built from a date spine, never from the fact: a dimension derived from the
    fact can never fail its relationships test. -#}

WITH spine AS (

    {{ dbt_utils.date_spine(
        datepart='month',
        start_date="CAST('" ~ var('cost_start_month') ~ "' AS DATE)",
        end_date=dbt.dateadd('month', 1, dbt.current_timestamp())
    ) }}

)

SELECT CAST(date_month AS DATE) AS cost_month
FROM spine
