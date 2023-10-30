with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_cahps_visit'), 'cahps_visits') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_patient_enrollment'),'enrollments') }},
{{ import(ref('fact_patient_map_send'), 'patient_map_sends') }},


filter_opportunities as (

    select
        opportunities.opportunity_id,
        opportunities.time_created_est as opportunity_created_date,
        patients.patient_id,
        opportunities.institution_id,

        case
            when patients.email is not null or patients.mobile_phone is not null then 1 else 0
        end as sms_email_send

    from opportunities
    join patients on opportunities.patient_id = patients.patient_id
        and patients.institution_id = 'client3'
    left join enrollments on opportunities.opportunity_id = enrollments.opportunity_id
    where
        opportunities.program_type = 'cahps'
        and enrollments.status = 'Completed'
    group by
        opportunities.opportunity_id,
        opportunity_created_date,
        patients.patient_id,
        opportunities.institution_id,
        sms_email_send

),

cahps_events as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.opportunity_created_date,
        cahps_visits.event_disposition,

        case
            when cahps_visits.cahps_completed = 1 then cahps_visits.cahps_visit_date_est else null
        end as visit_completed_date,

        case
            when cahps_visits.cahps_completed = 1 then 1
            when cahps_visits.cahps_completed = 0
                and cahps_visits.cahps_status = 'Completed' then 0
            else null
        end as visit_complete,

         case
            when cahps_visits.cahps_completed = 1 then datediff(day, filter_opportunities.opportunity_created_date, cahps_visits.cahps_visit_date_est)
            else null
        end as days_to_visit_complete_from_eligibility,

        case
            when days_to_visit_complete_from_eligibility <= 7 then 1 else 0
        end as visit_complete_within_7days_eligibility

    from filter_opportunities
    left join cahps_visits on filter_opportunities.opportunity_id = cahps_visits.opportunity_id
        and cahps_visits.cahps_status = 'Completed'

),

enrollment_tasks as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.opportunity_created_date,
        enrollments.task_type,
        enrollments.task_disposition,
        enrollments.created_at_est as ea_created_date,
        enrollments.completed_at as ea_completed_date,

        case
            when (enrollments.task_type = 'Enrollment Attempt #1' and enrollments.completed_at_est is not null)
                or filter_opportunities.sms_email_send = 1 then 1 else 0
        end as member_attempted_flag,

        case
            when enrollments.task_disposition IN ('Encounter Scheduled', 'Encounter Scheduled - Warm Transfer',
                                                  'Refused Service - Do Not Re-Attempt', 'Refused Service - Re-Attempt',
                                                  'Do Not Contact', 'Patient Reached - No MRP or MAP') then 1 else 0
        end as member_reached_flag,

        case
            when enrollments.task_disposition IN ('Encounter Scheduled', 'Encounter Scheduled - Warm Transfer') then 1 else 0
        end as member_accepted_flag,

        case
            when member_reached_flag = 1 and member_accepted_flag = 1 then 1 else 0
        end as member_reached_accepted_flag,

        case
            when member_attempted_flag = 1 then datediff(hour, filter_opportunities.opportunity_created_date, enrollments.created_at_est)
            else datediff(hour, filter_opportunities.opportunity_created_date, enrollments.completed_at)
        end as hours_to_attempt_from_eligibility,

        case
            when hours_to_attempt_from_eligibility <= 24 then 1 else 0
        end as member_attempted_within_24hrs_eligibility_flag,

        row_number() over (partition by filter_opportunities.opportunity_id order by ea_completed_date desc) as task_sequence

    from filter_opportunities
    left join enrollments on filter_opportunities.opportunity_id = enrollments.opportunity_id
    where enrollments.status = 'Completed'
    order by filter_opportunities.opportunity_id, opportunity_created_date desc

),

ea_base_metrics as (

    select
        opportunity_id,
        patient_id,
        opportunity_created_date,

        case
            when sum(member_attempted_flag) >= 1 then 1 else 0
        end as member_attempted,

        case
            when sum(member_reached_flag) >= 1 then 1 else 0
        end as member_reached,

        case
            when sum(member_accepted_flag) >= 1 then 1 else 0
        end as member_accepted,

        case
            when sum(member_reached_accepted_flag) >= 1 then 1 else 0
        end as member_reached_accepted,

        case
            when sum(member_attempted_within_24hrs_eligibility_flag) >= 1 then 1 else 0
        end as member_attempted_within_24hrs_eligibility

        from enrollment_tasks
        group by
            opportunity_id,
            patient_id,
            opportunity_created_date
),

map_send_metrics as (

    select
        cahps_events.opportunity_id,
        cahps_events.patient_id,
        cahps_events.visit_completed_date,
        patient_map_sends.map_transmission_date_est as map_send_date,

        case
            when cahps_events.visit_complete = 1 then datediff(day, cahps_events.visit_completed_date, patient_map_sends.map_transmission_date_est)
            else datediff(day, cahps_events.visit_completed_date, patient_map_sends.map_transmission_date_est)
        end as days_to_MAP_send_from_visit,

        case
            when patient_map_sends.map_transmission_date_est < visit_completed_date then 1 else 0
        end as map_send_before_visit_complete

    from cahps_events
    left join patient_map_sends on cahps_events.opportunity_id = patient_map_sends.opportunity_id
        and patient_map_sends.map_send_sequence = 1

),

final_results as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.institution_id,
        filter_opportunities.opportunity_created_date,

        cahps_events.event_disposition,
        cahps_events.visit_completed_date,
        cahps_events.visit_complete,

        enrollment_tasks.task_disposition as last_enrollment_disposition,

        ea_base_metrics.member_attempted,
        ea_base_metrics.member_reached,
        ea_base_metrics.member_accepted,
        ea_base_metrics.member_reached_accepted,
        ea_base_metrics.member_attempted_within_24hrs_eligibility,

        case
            when cahps_events.visit_complete = 1 then cahps_events.visit_complete_within_7days_eligibility else null
        end as visit_complete_within_7days_eligibility,

        map_send_metrics.map_send_date,
        map_send_metrics.days_to_MAP_send_from_visit

    from filter_opportunities
    left join cahps_events on filter_opportunities.opportunity_id = cahps_events.opportunity_id
    left join ea_base_metrics on filter_opportunities.opportunity_id = ea_base_metrics.opportunity_id
    left join enrollment_tasks on filter_opportunities.opportunity_id = enrollment_tasks.opportunity_id
        and task_sequence = 1
    left join map_send_metrics on filter_opportunities.opportunity_id = map_send_metrics.opportunity_id

)

select *
from final_results