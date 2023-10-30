with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_mrp_visit'), 'mrp_events') }},
{{ import(ref('fact_cmr_visit'), 'cmr_events') }},
{{ import(ref('dim_patient'), 'patients') }},

filter_opportunities as (

    select
        opportunities.patient_id,
        opportunities.opportunity_id,
        opportunities.institution_id,
        opportunities.time_created as opportunity_created_date,
        nvl(mrp_events.mrp_completed, 0) as mrp_completed_final,
        nvl(cmr_events.cmr_completed, 0) as cmr_completed_final,
        case when (mrp_completed_final = 1 or cmr_completed_final = 1) then 1 else 0 end as visit_completed
    from
        opportunities
    left join mrp_events on mrp_events.opportunity_id = opportunities.opportunity_id
        and mrp_events.mrp_completed = 1
        and mrp_events.mrp_completed_sequence = 1
    left join cmr_events on cmr_events.opportunity_id = opportunities.opportunity_id
        and cmr_events.cmr_completed = 1
        and cmr_events.cmr_completed_sequence = 1
    join patients on patients.patient_id = opportunities.patient_id
    )

, final as (

    select
        opportunity_created_date,
        institution_id,
        sum(mrp_completed_final) as mrp_completed_sum,
        sum(cmr_completed_final) as cmr_completed_sum,
        sum(visit_completed) as visits_completed_sum,
        (mrp_completed_sum + cmr_completed_sum) as services_completed_sum,
        count(distinct opportunity_id) as unique_opportunity_count
    from
        filter_opportunities
    group by
        opportunity_created_date,
        institution_id
    )

select *
from final