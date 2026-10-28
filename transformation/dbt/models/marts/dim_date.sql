{#-
    One row per day from var('date_spine_start') up to, but NOT including,
    var('date_spine_end'): dbt_utils.date_spine's end date is EXCLUSIVE, so
    end '2031-01-01' makes 2030-12-31 the last day.

    A fact date outside the spine does not error. It fails to join, and the
    row silently drops out of everything joined through dim_date. Spines are
    set once, too narrow, and not revisited; within_date_spine on every fact
    date column is what notices.
-#}

WITH spine AS (

    {{ dbt_utils.date_spine(
        datepart='day',
        start_date="CAST('" ~ var('date_spine_start') ~ "' AS DATE)",
        end_date="CAST('" ~ var('date_spine_end') ~ "' AS DATE)"
    ) }}

)

SELECT
    CAST(date_day AS DATE) AS date_day,
    CAST({{ dbt.date_trunc('month', 'date_day') }} AS DATE) AS month_start,
    EXTRACT(YEAR FROM date_day) AS calendar_year
FROM spine
