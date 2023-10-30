with
{{ import(ref('fact_follow_up'), 'follow_ups') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},


identify_missing as (

    select
        patients.patient_id,
        follow_ups.*,
        case
            when task_sequence_reverse = 1
                and lower(task_disposition) in (
                        'lvm - call back required',
                        'contact reached - call back required',
                        'no answer',
                        'other'
                    )
            then 1
            else 0
        end as missing_follow_up


    from follow_ups
    join opportunities on opportunities.opportunity_id = follow_ups.opportunity_id
    join patients on patients.patient_id = opportunities.patient_id
)

select *
from identify_missing
where missing_follow_up = 1
