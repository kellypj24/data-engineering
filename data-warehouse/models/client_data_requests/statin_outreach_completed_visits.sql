with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_statin_outreach_visit'), 'statin_outreach_events') }},
{{ import(ref('fact_finalized_medlist'), 'finalized_med_lists') }},
{{ import(ref('dim_user_cureatr'), 'users') }},

filter_visits as (

    select
        case
            when patients.institution_id = 'advantasure' then 'bcbsm'
            else patients.institution_id
        end as institution_id,

        statin_outreach_events.opportunity_id,
        'Statin Gap' as program_name,
        patients.plan_name,
        patients.plan_group_id,
        patients.source_patient_id,
        patients.given_name as first_name,
        patients.family_name as last_name,
        patients.date_of_birth,
        statin_outreach_events.statin_outreach_visit_date_est::date as visit_date,
        finalized_med_lists.map_finalized_date_est::date as map_finalized_date,
        users.first_name || ' ' || users.last_name as pharmacist,
        users.npi

    from opportunities
        join patients on opportunities.patient_id = patients.patient_id
        join statin_outreach_events on opportunities.opportunity_id = statin_outreach_events.opportunity_id
            and statin_outreach_completed = 1
        join finalized_med_lists on statin_outreach_events.opportunity_id = finalized_med_lists.opportunity_id
        join users on users.cureatr_user_id = coalesce(finalized_med_lists.coauthor_user_id, finalized_med_lists.pharmacist_user_id)
    where opportunities.program_type = 'statin_gap'
    group by
        patients.institution_id,
        statin_outreach_events.opportunity_id,
        program_name,
        patients.plan_name,
        patients.plan_group_id,
        patients.source_patient_id,
        patients.given_name,
        patients.family_name,
        patients.date_of_birth,
        statin_outreach_events.statin_outreach_visit_date_est,
        finalized_med_lists.map_finalized_date_est,
        pharmacist,
        npi

    ),

final as (

    select
        institution_id,
        opportunity_id,
        program_name,
        plan_name,
        plan_group_id,
        source_patient_id,
        first_name,
        last_name,
        date_of_birth,
        visit_date,
        map_finalized_date,
        pharmacist,
        npi

    from filter_visits
    )

select *
from final
