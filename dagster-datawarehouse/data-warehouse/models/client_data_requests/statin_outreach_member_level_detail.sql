with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_statin_outreach_visit'), 'statin_outreach_events') }},
{{ import(ref('enrollment_funnel'), 'enrollments') }},

filter_enrollments as (

    select
        case
            when patients.institution_id = 'client1' then 'client2'
            else patients.institution_id
        end as institution_id,

        'Statin Gap' as program_name,
        patients.plan_name,
        patients.plan_group_id,
        patients.source_patient_id,
        patients.given_name as first_name,
        patients.family_name as last_name,
        patients.date_of_birth,
        enrollments.ea_1_completed_date as ea1_date,
        enrollments.ea_1_task_disposition as ea1_outcome,
        enrollments.ea_2_completed_date as ea2_date,
        enrollments.ea_2_task_disposition as ea2_outcome,
        enrollments.ea_3_completed_date as ea3_date,
        enrollments.ea_3_task_disposition as ea3_outcome

    from opportunities
        join patients on opportunities.patient_id = patients.patient_id
        left join statin_outreach_events on statin_outreach_events.opportunity_id = opportunities.opportunity_id
        left join enrollments on statin_outreach_events.opportunity_id = enrollments.opportunity_id
    where opportunities.program_type = 'statin_gap'
    group by
        patients.institution_id,
        program_name,
        patients.plan_name,
        patients.plan_group_id,
        patients.source_patient_id,
        patients.given_name,
        patients.family_name,
        patients.date_of_birth,
        enrollments.ea_1_completed_date,
        enrollments.ea_1_task_disposition,
        enrollments.ea_2_completed_date,
        enrollments.ea_2_task_disposition,
        enrollments.ea_3_completed_date,
        enrollments.ea_3_task_disposition

    ),

final as (

    select
        institution_id,
        program_name,
        plan_name,
        plan_group_id,
        source_patient_id,
        first_name,
        last_name,
        date_of_birth,
        ea1_date,
        ea1_outcome,
        ea2_date,
        ea2_outcome,
        ea3_date,
        ea3_outcome

    from filter_enrollments
    )

select *
from final
