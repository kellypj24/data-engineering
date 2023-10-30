with
{{ import(source('redshift_public', 'fact_patient_pdc'), 'pdc') }},

final as (
    select
        patient_id
        , iid
        , category
        , pdc
        , time_created
        , discharge_event_id
        , followup_interval_in_days
        , patient_opportunity_id
    from pdc
    )

select *
from final