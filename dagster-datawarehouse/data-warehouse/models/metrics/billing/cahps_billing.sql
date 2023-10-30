with
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_cahps_visit'), 'cahps_events') }},
{{ import(ref('fact_cahps_report'), 'cahps_reports') }},
{{ import(ref('fact_patient_map_send'), 'patient_map_sends') }},
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_patient_enrollment'), 'enrollments') }},



billing as (

    select
        patients.patient_id,
        patients.given_name as first_name,
        patients.family_name as last_name,
        patients.date_of_birth,
        cahps_events.opportunity_id,
        cahps_events.cahps_event_id,
        patients.source_patient_id,
        patients.patient_contact_id,
        patients.institution_id,
        enrollments.created_at_est as enrollment_date,
        cahps_events.cahps_visit_date_est as cahps_visit_date,
        cahps_reports.pharmacist_user_id,
        cahps_reports.map_finalized_date_est as map_finalized_date,
        cahps_reports.map_status,
        patient_map_sends.map_transmission_date_est as map_transmission_date,
        patient_map_sends.map_transmission_method,
        patient_map_sends.map_recipient as sent_to_stakeholder,

        date_diff('day',
                  enrollments.created_at_est,
                  cahps_events.cahps_visit_date_est
        ) as days_between_enrollment_and_visit,

        date_diff('day',
                  cahps_events.cahps_visit_date_est,
                  cahps_reports.map_finalized_date_est
        ) as days_between_visit_and_map_finalized,

        date_diff('day',
                  cahps_reports.map_finalized_date_est,
                  patient_map_sends.map_transmission_date_est
        ) as days_between_map_finalized_and_transmission,

        date_diff('day',
                  enrollments.created_at_est,
                  patient_map_sends.map_transmission_date_est
        ) as days_between_enrollment_and_map_transmission

    from opportunities
    join patients on opportunities.patient_id = patients.patient_id
    join cahps_events on opportunities.opportunity_id = cahps_events.opportunity_id
        and cahps_events.cahps_completed_sequence = 1
    left join enrollments on opportunities.opportunity_id = enrollments.opportunity_id
        and enrollments.status = 'Completed'
        and enrollments.task_type = 'Enrollment Attempt #1'
    left join cahps_reports on opportunities.opportunity_id = cahps_reports.opportunity_id
        and cahps_reports.report_order = 1
    join patient_map_sends on patients.patient_contact_id = patient_map_sends.patient_contact_id
        and opportunities.opportunity_id = patient_map_sends.opportunity_id
        and patient_map_sends.map_send_sequence = 1
    where not patients.is_control
        and patients.institution_id = 'humana'
    group by
        patients.patient_id,
        patients.given_name,
        patients.family_name,
        patients.date_of_birth,
        cahps_events.opportunity_id,
        cahps_events.cahps_event_id,
        patients.source_patient_id,
        patients.patient_contact_id,
        patients.institution_id,
        enrollments.created_at_est,
        cahps_events.cahps_visit_date_est,
        cahps_reports.pharmacist_user_id,
        cahps_reports.map_finalized_date_est,
        cahps_reports.map_status,
        patient_map_sends.map_transmission_date_est,
        patient_map_sends.map_transmission_method,
        patient_map_sends.map_recipient


)

select *
from billing
