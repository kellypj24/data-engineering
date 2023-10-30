with
{{ import(ref('stg_company__holiday_calendar'), 'holiday_calendar') }},

date_spine as (

{{ dbt_utils.date_spine(
    datepart = "day",
    start_date = "CAST('2010-01-01' AS DATE)",
    end_date = "CAST('2030-01-01' AS DATE)"
) }}

),

base as (

    select cast(date_day as date) as date_day
    from date_spine

),

base_with_holidays as (

    select
        base.date_day,
        holiday_calendar.holiday
    from base
    left join holiday_calendar on
        base.date_day = holiday_calendar.date_key

),

date_attributes as (

    select
        to_char(date_day, 'YYYYMMDD') as date_key,
        date_day as calendar_date,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Dy') as day_name_abbreviated,
        to_char(date_day, 'ID') as day_of_week,
        to_char(date_day, 'DD') as day_of_month,
        to_char(date_day, 'W') as week_of_month,
        cast(datediff('day', date_trunc('qtr', date_day), date_day) + 1 as varchar) as day_of_quarter,
        to_char(date_day, 'DDD') as day_of_year,
        to_char(date_day, 'WW') as week_of_year,
        to_char(date_day, 'MM') as month_of_year,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Mon') as month_name_abbreviated,
        to_char(date_day, 'Q') as quarter_actual,
        to_char(date_day, 'YYYY') as year_actual,
        coalesce(holiday is not null, false) as is_holiday,
        coalesce(holiday, 'NOT A HOLIDAY') as holiday_name,
        case to_char(date_day, 'Day')
            when 'Saturday' then true
            when 'Sunday' then true
            else false
        end as is_weekend
    from base_with_holidays

)

select *
from date_attributes
