with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_desktop_mrp'), 'desktop_mrp_events') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_finalized_medlist'), 'finalized_med_lists') }},
{{ import(ref('fact_map_send'), 'map_sends') }},
{{ import(ref('fact_discharge_summary_needed'), 'ds_needed') }},
{{ import(ref('dim_user_cureatr'), 'users') }},
{{ import(ref('fact_admin_qa'), 'admin_qa') }},
{{ import(ref('fact_provider_urgent_request'), 'pur') }},


filter_opportunities as (

    select
        opportunities.opportunity_id,
        opportunities.time_created_est as opportunity_created_date,
        opportunities.discharge_date_est as discharge_date,
        patients.patient_id,
        patients.source_patient_id,
        patients.institution_id,
        patients.plan_name,
	    patients.contract_id,
        patients.date_of_birth,
        patients.state as patient_state,
        desktop_mrp_events.desktop_mrp_completed_date_est as desktop_mrp_completed_date,
        desktop_mrp_events.completed_by_user_name as desktop_mrp_completed_by,
        desktop_mrp_events.desktop_mrp_disposition,
        desktop_mrp_events.desktop_mrp_status,
        desktop_mrp_events.desktop_mrp_completed,
        desktop_mrp_events.desktop_mrp_canceled,
        finalized_med_lists.med_list_id,
        finalized_med_lists.map_finalized_date_est as map_finalized_date,
        map_sends.map_transmission_date_est as map_transmission_date,
        map_sends.map_transmission_method,
        finalized_med_lists.map_finalized,
        (users.first_name || ' ' || users.last_name) as map_finalized_by,
        map_sends.map_sent,
        map_sends.map_recipient,

        case
            when ds_needed.task_type is not null
            then 'Y' else 'N'
        end as discharge_summary_needed_task_exists,

        case
            when admin_qa.qa_status in ('New','In Progress')
            then 1 else 0
        end as desktop_mrp_in_qa,

        case
            when pur.pur_completed = 1
            then 1 else 0
        end as provider_urgent_request_completed,

        datediff('day',
                opportunities.time_created_est,
                desktop_mrp_events.desktop_mrp_completed_date_est
        ) as days_between_opportunity_created_dmrp_completed,

        date_diff('day',
                  opportunities.discharge_date_est,
                  desktop_mrp_events.desktop_mrp_completed_date_est
        ) as days_between_discharge_dmrp_completed,

        date_diff('day',
                  ds_needed.time_created,
                  ds_needed.ds_needed_completed_date_est
        ) as days_between_ds_needed_created_ds_received,

        datediff('day',
            desktop_mrp_events.desktop_mrp_completed_date_est,
            finalized_med_lists.map_finalized_date_est
        ) as days_between_dmrp_completed_map_finalized,

        datediff('day',
            finalized_med_lists.map_finalized_date_est,
            map_sends.map_transmission_date_est
        ) as days_between_map_finalized_map_sent,

        datediff('day',
            desktop_mrp_events.desktop_mrp_completed_date_est,
            map_sends.map_transmission_date_est
        ) as days_between_dmrp_completed_map_sent

    from opportunities
    join patients on patients.patient_id = opportunities.patient_id
    left join desktop_mrp_events on desktop_mrp_events.opportunity_id = opportunities.opportunity_id
    left join finalized_med_lists on finalized_med_lists.opportunity_id = opportunities.opportunity_id
        and finalized_med_lists.map_sequence = 1
    left join map_sends on map_sends.opportunity_id = opportunities.opportunity_id
    left join users on finalized_med_lists.pharmacist_user_id = users.cureatr_user_id
    left join admin_qa on opportunities.opportunity_id = admin_qa.opportunity_id
    left join ds_needed on opportunities.opportunity_id = ds_needed.opportunity_id
        and ds_needed.task_sequence_desc = 1
    left join pur on opportunities.opportunity_id = pur.opportunity_id
    where opportunities.eligibility_status = 'eligible'
        and opportunities.program_type = 'desktop_mrp'

)

select *
from filter_opportunities
