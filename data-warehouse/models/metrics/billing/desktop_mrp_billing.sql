with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_desktop_mrp'), 'desktop_mrp_events') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_finalized_medlist'), 'finalized_med_lists') }},
{{ import(ref('fact_map_send'), 'map_sends') }},


billing as (

    select
        patients.patient_id,
        patients.source_patient_id,
        patients.patient_contact_id,
        opportunities.opportunity_id,
        desktop_mrp_events.task_id,
        patients.institution_id,
        opportunities.discharge_date_est as discharge_date,
        desktop_mrp_events.desktop_mrp_completed_date_est as desktop_mrp_completed_date,
        finalized_med_lists.med_list_id,
        coalesce(finalized_med_lists.coauthor_user_id, finalized_med_lists.pharmacist_user_id) as pharmacist_user_id,
        finalized_med_lists.map_finalized_date_est as map_finalized_date,
        map_sends.map_transmission_date_est as map_transmission_date,
        map_sends.map_transmission_method,
        map_sends.recipient_provider_npi as provider_npi,
        map_sends.recipient_provider_name as provider_name,

        date_diff('day',
                  opportunities.discharge_date_est,
                  desktop_mrp_events.desktop_mrp_completed_date_est
        ) as days_between_discharge_and_dmrp_completed,

        date_diff('day',
                  desktop_mrp_events.desktop_mrp_completed_date_est,
                  finalized_med_lists.map_finalized_date_est
        ) as days_between_dmrp_completed_and_map_finalized,

        date_diff('day',
                  map_finalized_date_est,
                  map_sends.map_transmission_date_est
        ) as days_between_map_finalized_and_transmission,

        date_diff('day',
                  opportunities.discharge_date_est,
                  map_sends.map_transmission_date_est
        ) as days_between_discharge_and_map_transmission

    from opportunities
    join patients on patients.patient_id = opportunities.patient_id
    join desktop_mrp_events on desktop_mrp_events.opportunity_id = opportunities.opportunity_id
    join finalized_med_lists on opportunities.opportunity_id= finalized_med_lists.opportunity_id
        and finalized_med_lists.map_sequence = 1
    left join map_sends on opportunities.opportunity_id= map_sends.opportunity_id
    where days_between_discharge_and_dmrp_completed between 0 and 31
        and desktop_mrp_events.desktop_mrp_completed = 1
        and finalized_med_lists.map_finalized_date_est is not null

)

select *
from billing
