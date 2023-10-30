with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_cahps_visit'), 'cahps_visits') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_patient_enrollment'),'enrollments') }},
{{ import(ref('dim_medication'), 'medications') }},
{{ import(ref('fact_cahps_report'), 'cahps_reports') }},


filter_opportunities as (

    select
        patients.source_patient_id as member_id,
        patients.patient_id,
        opportunities.opportunity_id,
        patients.given_name as member_first_name,
        patients.family_name as member_last_name

    from opportunities
    join patients on opportunities.patient_id = patients.patient_id
        and patients.institution_id = 'client3'
    join enrollments on opportunities.opportunity_id = enrollments.opportunity_id
        and enrollments.task_type = 'Enrollment Attempt #1'
    where opportunities.program_type = 'cahps'
    group by
        patients.source_patient_id,
        patients.patient_id,
        opportunities.opportunity_id,
        patients.given_name,
        patients.family_name
),

union_communication as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        enrollments.task_type as communication_type,
        enrollments.task_disposition as communication_disposition,
        enrollments.created_at_est as time_created,
        enrollments.completed_at_est as completed_time

    from filter_opportunities
    join enrollments on filter_opportunities.opportunity_id = enrollments.opportunity_id
    where enrollments.status = 'Completed'

    union

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        cahps_visits.event_type as communication_type,
        cahps_visits.event_disposition as communication_disposition,
        cahps_visits.time_created,
        cahps_visits.cahps_visit_date_est as completed_time

    from filter_opportunities
    join cahps_visits on filter_opportunities.opportunity_id = cahps_visits.opportunity_id
    where cahps_visits.cahps_status = 'Completed'

),

sequence_communication as (

    select
        opportunity_id,
        patient_id,
        communication_type,
        communication_disposition,
        time_created,
        completed_time,
        row_number() over (partition by opportunity_id order by communication_type, completed_time desc) as sequence_number

    from union_communication

),

pull_last_communication as (

    select
        opportunity_id,
        patient_id,
        communication_type,
        communication_disposition,
        time_created,
        completed_time,
        sequence_number

    from sequence_communication
    where sequence_number = 1
),

roll_up_barriers as (

    select
        opportunity_id,
        patient_id,
        drug_id,
        listagg(distinct barrier_name, ', ') within group (order by barrier_name) as barriers


    from cahps_reports
    group by
        opportunity_id,
        patient_id,
        drug_id
),

roll_up_interventions as (

    select
        opportunity_id,
        patient_id,
        drug_id,
        listagg(distinct intervention, ', ') within group (order by intervention) as interventions


    from cahps_reports
    group by
        opportunity_id,
        patient_id,
        drug_id
),

flatten_barriers_interventions as (

    select
        roll_up_barriers.opportunity_id,
        roll_up_barriers.patient_id,
        roll_up_barriers.drug_id,
        roll_up_barriers.barriers,
        roll_up_interventions.interventions
    from roll_up_barriers
    left join roll_up_interventions on roll_up_barriers.opportunity_id = roll_up_interventions.opportunity_id
        and roll_up_barriers.drug_id = roll_up_interventions.drug_id
    group by
        roll_up_barriers.opportunity_id,
        roll_up_barriers.patient_id,
        roll_up_barriers.drug_id,
        roll_up_barriers.barriers,
        roll_up_interventions.interventions
),


final_results as (

    select
        filter_opportunities.patient_id,
        filter_opportunities.opportunity_id,
        filter_opportunities.member_id,
        filter_opportunities.member_first_name,
        filter_opportunities.member_last_name,

        split_part(pull_last_communication.completed_time::date::varchar, '-', 2) || '/'
        || split_part(pull_last_communication.completed_time::date::varchar, '-', 3) || '/'
        || split_part(pull_last_communication.completed_time::date::varchar, '-', 1)  as contact_attempt_date,

        pull_last_communication.communication_disposition as outcome_of_contact_attempt,

        case
            when cahps_visits.cahps_completed = 1 then 'Y'
            when cahps_visits.cahps_completed = 0
                and cahps_visits.cahps_status = 'Completed' then 'N'
            else null
        end as visit_complete,

        case
            when enrollments.completed_at_est is not null then
                split_part(enrollments.completed_at_est::date::varchar, '-', 2) || '/'
                || split_part(enrollments.completed_at_est::date::varchar, '-', 3) || '/'
                || split_part(enrollments.completed_at_est::date::varchar, '-', 1)
            when enrollments.completed_at_est is null and visit_complete = 'Y' then
                split_part(cahps_visits.cahps_visit_date_est::date::varchar, '-', 2) || '/'
                || split_part(cahps_visits.cahps_visit_date_est::date::varchar, '-', 3) || '/'
                || split_part(cahps_visits.cahps_visit_date_est::date::varchar, '-', 1)
            else null
        end as scheduled_date,

        case
            when cahps_visits.cahps_completed = 1
                then
                    split_part(cahps_visits.cahps_visit_date_est::date::varchar, '-', 2) || '/'
                    || split_part(cahps_visits.cahps_visit_date_est::date::varchar, '-', 3) || '/'
                    || split_part(cahps_visits.cahps_visit_date_est::date::varchar, '-', 1)
            else null
        end as visit_completion_date,

        case
            when cahps_visits.cahps_completed = 1
                then
                    cahps_visits.cahps_visit_date
                else null
        end as visit_completion_date_for_tests,

        enrollments.completed_at as scheduled_date_for_tests,

        flatten_barriers_interventions.drug_id,
        flatten_barriers_interventions.barriers as gap_detail,
        flatten_barriers_interventions.interventions as intervention_provided,
        medications.full_gpi_name as medication_name,
        medications.full_gpi_id as medication_gpi


    from filter_opportunities
    left join pull_last_communication on filter_opportunities.opportunity_id = pull_last_communication.opportunity_id
    left join cahps_visits on filter_opportunities.opportunity_id = cahps_visits.opportunity_id
        and cahps_visits.cahps_sequence_desc = 1
    left join enrollments on filter_opportunities.opportunity_id = enrollments.opportunity_id
        and enrollments.task_disposition in ('Encounter Scheduled - Warm Transfer', 'Encounter Scheduled')
        and enrollments.encounter_scheduled_sequence = 1
    left join flatten_barriers_interventions on filter_opportunities.opportunity_id = flatten_barriers_interventions.opportunity_id
    left join medications on medications.drug_id = flatten_barriers_interventions.drug_id

    group by
        filter_opportunities.patient_id,
        filter_opportunities.opportunity_id,
        filter_opportunities.member_id,
        filter_opportunities.member_first_name,
        filter_opportunities.member_last_name,
        pull_last_communication.completed_time,
        pull_last_communication.communication_disposition,
        cahps_visits.cahps_completed,
        cahps_visits.cahps_visit_date_est,
        cahps_visits.cahps_visit_date,
        cahps_visits.cahps_status,
        enrollments.completed_at_est,
        enrollments.completed_at,
        flatten_barriers_interventions.drug_id,
        flatten_barriers_interventions.barriers,
        flatten_barriers_interventions.interventions,
        medications.full_gpi_name,
        medications.full_gpi_id

)

select *
from final_results
