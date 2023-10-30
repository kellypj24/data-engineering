with
{{ import(ref('fact_cahps_visit'), 'events') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_patient_enrollment'), 'enrollments') }},

filter_opportunities as (
    select
        events.opportunity_id,
        events.completed_by_username,
        opportunities.patient_id,
        opportunities.time_created_est as opportunity_created_date,
        case
            when patients.email is not null or patients.mobile_phone is not null then 1 else 0
        end as sms_email_send,
        case
            when (enrollments.task_type = 'Enrollment Attempt #1' and enrollments.completed_at_est is not null)
                or sms_email_send = 1 then 1 else 0
        end as member_contacted_flag,
        case
            when enrollments.task_disposition in ('Encounter Scheduled', 'Encounter Scheduled - Warm Transfer',
                                                  'Refused Service - Do Not Re-Attempt', 'Refused Service - Re-Attempt',
                                                  'Do Not Contact', 'Patient Reached - No MRP or MAP') then 1 else 0
        end as member_reached_flag,
        case
            when enrollments.task_disposition in ('Encounter Scheduled', 'Encounter Scheduled - Warm Transfer') then 1 else 0
        end as member_accepted_flag
    from events
    left join opportunities on events.opportunity_id = opportunities.opportunity_id
    left join patients on opportunities.patient_id = patients.patient_id
        and patients.institution_id = 'humana'
    left join enrollments on events.opportunity_id = enrollments.opportunity_id
    where opportunities.program_type = 'cahps'
    and enrollments.status = 'Completed'
    group by
        events.opportunity_id,
        events.completed_by_username,
        enrollments.completed_at_est,
        enrollments.task_disposition,
        enrollments.task_type,
        opportunities.patient_id,
        opportunities.time_created_est,
        patients.email,
        patients.mobile_phone
),

output_metrics as (
    select
        date(opportunity_created_date) as date_of_outreach,
        completed_by_username,
        sum(member_contacted_flag) as members_contacted,
        sum(member_reached_flag) as members_reached,
        ((members_reached::float / members_contacted::float)*100)::int as reached_percentage,
        sum(member_accepted_flag) as members_accepted,
        ((members_accepted::float / members_contacted::float)*100)::int as accepted_percentage
    from filter_opportunities
    group by date_of_outreach, completed_by_username
    order by date_of_outreach
    )

select * from output_metrics