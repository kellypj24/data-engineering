with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_desktop_mrp'), 'desktop_mrp_events') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_medication_problems'), 'med_problems') }},

filter_opportunities as (

    select
        opportunities.opportunity_id,
        patients.patient_id,
        desktop_mrp_events.desktop_mrp_completed,
        med_problems.category,
        med_problems.problem_descriptive as medication_problem

    from opportunities
    join patients on patients.patient_id = opportunities.patient_id
    left join desktop_mrp_events on desktop_mrp_events.opportunity_id = opportunities.opportunity_id
    left join med_problems on patients.patient_id = med_problems.patient_id
    where opportunities.eligibility_status = 'eligible'
        and opportunities.program_type = 'desktop_mrp'
    group by
        opportunities.opportunity_id,
        patients.patient_id,
        desktop_mrp_events.desktop_mrp_completed,
        med_problems.category,
        med_problems.problem_descriptive

)

select *
from filter_opportunities
