with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_statin_outreach_visit'), 'statin_outreach_visits') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_patient_enrollment'),'enrollments') }},
{{ import(ref('fact_map_send'), 'map_sends') }},
{{ import(ref('fact_patient_map_send'), 'patient_map_sends') }},

filter_opportunities as (

    select
        opportunities.opportunity_id,
        opportunities.time_created_est as opportunity_created_date,
        patients.patient_id,
        opportunities.institution_id,
        patients.plan_group_id

    from opportunities
        join patients on opportunities.patient_id = patients.patient_id
        left join enrollments on opportunities.opportunity_id = enrollments.opportunity_id
    where
        opportunities.program_type = 'statin_gap'
    group by
        opportunities.opportunity_id,
        opportunity_created_date,
        patients.patient_id,
        opportunities.institution_id,
        patients.plan_group_id

),

statin_outreach_events as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.opportunity_created_date,
        statin_outreach_visits.event_disposition,

        case
            when statin_outreach_visits.statin_outreach_completed = 1
            then statin_outreach_visits.statin_outreach_visit_date_est else null
        end as visit_completed_date,

        case
            when statin_outreach_visits.statin_outreach_completed = 1 then 1
            when (statin_outreach_visits.statin_outreach_completed = 0
                and statin_outreach_visits.statin_outreach_status = 'Completed') then 0
            else null
        end as visit_complete,

         case
            when statin_outreach_visits.statin_outreach_completed = 1
            then datediff(day, statin_outreach_visits.time_created, statin_outreach_visits.statin_outreach_visit_date_est)
            else null
        end as days_to_visit_complete_from_scheduled

    from filter_opportunities
        left join statin_outreach_visits on filter_opportunities.opportunity_id = statin_outreach_visits.opportunity_id
            and statin_outreach_visits.statin_outreach_completed = 1

),
enrollment_tasks as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.opportunity_created_date,
        enrollments.task_type,
        enrollments.task_disposition,
        enrollments.created_at_est as ea_created_date,
        enrollments.completed_at_est as ea_completed_date,

        case
            when (enrollments.task_type = 'Enrollment Attempt #1' and ea_created_date is not null)
            then 1 else 0
        end as member_enrolled_flag,

        case
            when (enrollments.task_type = 'Enrollment Attempt #1' and ea_completed_date is not null)
            then 1 else 0
        end as member_attempted_flag,

        case
            when enrollments.task_disposition in ('Encounter Scheduled', 'Encounter Scheduled - Warm Transfer',
                                                  'Refused Service - Do Not Re-Attempt', 'Refused Service - Re-Attempt',
                                                  'Do Not Contact', 'Patient Reached - No MRP or MAP')
            then 1 else 0
        end as member_reached_flag,

        row_number() over (partition by filter_opportunities.opportunity_id order by ea_completed_date desc) as task_sequence

    from filter_opportunities
        left join enrollments on filter_opportunities.opportunity_id = enrollments.opportunity_id
    order by
        filter_opportunities.opportunity_id,
        opportunity_created_date desc

),

ea_base_metrics as (

    select
        opportunity_id,
        patient_id,
        opportunity_created_date,

        case
            when sum(member_enrolled_flag) >= 1 then 1 else 0
        end as member_enrolled,

        case
            when sum(member_attempted_flag) >= 1 then 1 else 0
        end as member_attempted,

        case
            when sum(member_reached_flag) >= 1 then 1 else 0
        end as member_reached

    from enrollment_tasks
    group by
        opportunity_id,
        patient_id,
        opportunity_created_date
),

map_send_metrics as (

    select
        statin_outreach_events.opportunity_id,
        statin_outreach_events.patient_id,
        statin_outreach_events.visit_completed_date,
        patient_map_sends.map_transmission_date_est as patient_map_send_date,
        map_sends.map_transmission_date_est as provider_map_send_date,

        case
            when (statin_outreach_events.visit_complete = 1 and patient_map_send_date is not null)
            then 1 else 0
        end as completed_visit_patient_map_send,

        case
            when (statin_outreach_events.visit_complete = 1 and provider_map_send_date is not null)
            then 1 else 0
        end as completed_visit_provider_map_send,

        case
            when statin_outreach_events.visit_complete = 1
            then datediff(day, statin_outreach_events.visit_completed_date, patient_map_send_date)
            else datediff(day, statin_outreach_events.visit_completed_date, patient_map_send_date)
        end as days_to_patient_map_send_from_visit

    from statin_outreach_events
        left join patient_map_sends on statin_outreach_events.opportunity_id = patient_map_sends.opportunity_id
            and patient_map_sends.map_send_sequence = 1
        left join map_sends on statin_outreach_events.opportunity_id = map_sends.opportunity_id
            and map_sends.map_send_sequence = 1


),

final_results as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.plan_group_id,
        filter_opportunities.institution_id,
        filter_opportunities.opportunity_created_date,

        statin_outreach_events.event_disposition,
        statin_outreach_events.visit_completed_date,
        statin_outreach_events.visit_complete,
        statin_outreach_events.days_to_visit_complete_from_scheduled,

        enrollment_tasks.task_disposition as last_enrollment_disposition,

        ea_base_metrics.member_enrolled,
        ea_base_metrics.member_attempted,
        ea_base_metrics.member_reached,
        case
            when statin_outreach_events.visit_complete = 1
            then 1 else 0
        end as member_engaged,

        map_send_metrics.completed_visit_patient_map_send,
        map_send_metrics.completed_visit_provider_map_send,
        map_send_metrics.patient_map_send_date,
        map_send_metrics.provider_map_send_date,
        map_send_metrics.days_to_patient_map_send_from_visit

    from filter_opportunities
        left join statin_outreach_events on filter_opportunities.opportunity_id = statin_outreach_events.opportunity_id
        left join ea_base_metrics on filter_opportunities.opportunity_id = ea_base_metrics.opportunity_id
        left join enrollment_tasks on filter_opportunities.opportunity_id = enrollment_tasks.opportunity_id
            and enrollment_tasks.task_sequence = 1
        left join map_send_metrics on filter_opportunities.opportunity_id = map_send_metrics.opportunity_id

)

select *
from final_results