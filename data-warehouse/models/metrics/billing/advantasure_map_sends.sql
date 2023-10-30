with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_mrp_visit'), 'mrp_events') }},
{{ import(ref('fact_finalized_medlist'), 'finalized_med_lists') }},
{{ import(ref('fact_map_send'), 'provider_map_sends') }},
{{ import(ref('fact_patient_map_send'), 'patient_map_sends') }},

combined_map_sends (patient_contact_id, opportunity_id, map_transmission_method, map_recipient_type, map_transmission_date, provider_npi, provider_name) as (

    select
        provider_map_sends.patient_contact_id,
        provider_map_sends.opportunity_id,
        provider_map_sends.map_transmission_method,
        provider_map_sends.map_recipient,
        provider_map_sends.map_transmission_date_est,
        provider_map_sends.recipient_provider_npi,
        provider_map_sends.recipient_provider_name
    from provider_map_sends
    where institution_name = 'Advantasure'
    and opportunity_id IS NOT NULL

    union

    select
        patient_map_sends.patient_contact_id,
        patient_map_sends.opportunity_id,
        patient_map_sends.map_transmission_method,
        patient_map_sends.map_recipient,
        patient_map_sends.map_transmission_date_est,
        null as provider_npi,
        null as provider_name
    from patient_map_sends
    where institution_name = 'Advantasure'
    and opportunity_id IS NOT NULL

),

filter_map_sends as (

    select
        patients.patient_id,
        patients.given_name as first_name,
        patients.family_name as last_name,
        patients.date_of_birth,
        opportunities.discharge_event_id,
        mrp_events.opportunity_id,
        mrp_events.mrp_event_id,
        patients.source_patient_id,
        patients.institution_id,
        opportunities.discharge_date_est as discharge_date,
        mrp_events.mrp_visit_date_est as mrp_visit_date,
        finalized_med_lists.med_list_id,
        finalized_med_lists.user_id as pharmacist_user_id,
        finalized_med_lists.map_finalized_date_est as map_finalized_date,
        combined_map_sends.map_transmission_date,
        combined_map_sends.map_transmission_method,
        combined_map_sends.map_recipient_type,
        combined_map_sends.provider_npi,
        combined_map_sends.provider_name,
        date_diff('day', discharge_date_est, mrp_visit_date_est) as days_between_discharge_and_visit,
        date_diff('day', mrp_visit_date_est, map_finalized_date_est) as days_between_visit_and_map_finalized,
        date_diff('day', map_finalized_date_est, map_transmission_date) as days_between_map_finalized_and_transmission,
        date_diff('day', discharge_date_est, map_transmission_date) as days_between_discharge_and_map_transmission
    from patients
    join opportunities on patients.patient_id = opportunities.patient_id
    join mrp_events on patients.patient_contact_id = mrp_events.patient_contact_id
        and opportunities.opportunity_id = mrp_events.opportunity_id
		and mrp_events.mrp_completed_sequence = 1
    join finalized_med_lists on patients.patient_id = finalized_med_lists.patient_id
        and opportunities.opportunity_id = finalized_med_lists.opportunity_id
    join combined_map_sends on patients.patient_contact_id = combined_map_sends.patient_contact_id
        and opportunities.opportunity_id = combined_map_sends.opportunity_id
    where opportunities.logical_event_type = 'HD'
        AND NOT patients.is_control
        and patients.institution_id = 'advantasure'

)



select *
from filter_map_sends
