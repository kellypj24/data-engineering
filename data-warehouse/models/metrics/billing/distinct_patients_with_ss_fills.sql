with
{{ import(ref('fact_medication_fill_event'), 'fill_events') }},

patient_count AS (
    select count (distinct patient_id)
    from fill_events
    where time_created between to_date(date_part('year' , current_date - interval '365 day')
            || '-' || '12' || '-' || '01', 'yyyy-mm-dd')
    and to_date(date_part('year', current_date)
            || '-' || '12' || '-' || '01', 'yyyy-mm-dd')
    )

select *
from patient_count