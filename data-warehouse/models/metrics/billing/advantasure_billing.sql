with
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_mrp_visit'), 'mrp_events') }},
{{ import(ref('fact_finalized_medlist'), 'finalized_med_lists') }},
{{ import(ref('fact_map_send'), 'map_sends') }},
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},



billing as (

    select
        patients.patient_id,
        patients.given_name as first_name,
        patients.family_name as last_name,
        patients.date_of_birth,
        opportunities.discharge_event_id,
        mrp_events.opportunity_id,
        mrp_events.mrp_event_id,
        patients.source_patient_id,
        patients.patient_contact_id,
        patients.institution_id,
        opportunities.discharge_date_est as discharge_date,
        mrp_events.mrp_visit_date_est as mrp_visit_date,
        finalized_med_lists.med_list_id,
        coalesce(finalized_med_lists.coauthor_user_id, finalized_med_lists.pharmacist_user_id) as pharmacist_user_id,
        finalized_med_lists.map_finalized_date_est as map_finalized_date,
        map_sends.map_transmission_date_est as map_transmission_date,
        map_sends.map_transmission_method,
        map_sends.recipient_provider_npi as provider_npi,
        map_sends.recipient_provider_name as provider_name,

        date_diff('day',
                  opportunities.discharge_date_est,
                  mrp_events.mrp_visit_date_est
        ) as days_between_discharge_and_visit,

        date_diff('day',
                  mrp_events.mrp_visit_date_est,
                  finalized_med_lists.map_finalized_date_est
        ) as days_between_visit_and_map_finalized,

        date_diff('day',
                  map_finalized_date_est,
                  map_sends.map_transmission_date_est
        ) as days_between_map_finalized_and_transmission,

        date_diff('day',
                  opportunities.discharge_date_est,
                  map_sends.map_transmission_date_est
        ) as days_between_discharge_and_map_transmission

    from patients
    join opportunities on opportunities.patient_id = patients.patient_id
    join mrp_events on patients.patient_contact_id = mrp_events.patient_contact_id
        and opportunities.opportunity_id= mrp_events.opportunity_id
        and mrp_events.mrp_completed_sequence = 1
    join finalized_med_lists on patients.patient_id = finalized_med_lists.patient_id
        and opportunities.opportunity_id= finalized_med_lists.opportunity_id
        and finalized_med_lists.map_finalized_sequence = 1
    join map_sends on patients.patient_contact_id = map_sends.patient_contact_id
        and opportunities.opportunity_id= map_sends.opportunity_id
        and map_sends.map_send_sequence = 1
    where opportunities.logical_event_type = 'HD'
        AND NOT patients.is_control
        AND days_between_discharge_and_visit between 0 and 31
        and patients.institution_id = 'advantasure'

)

select *
from billing
