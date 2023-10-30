with
    {{ import(ref('stg_redshift_public__patient_opportunities'), 'opportunities') }},
    {{ import(ref('stg_redshift_public__discharges'), 'discharges') }},


join_adt_discharge_info as (

    select
         opportunities.opportunity_id,
         opportunities.institution_id,
         opportunities.patient_id,
         opportunities.time_created,
         opportunities.time_created_est,
         opportunities.opportunity_status,
         opportunities.program_type,
         coalesce(opportunities.discharge_date ,discharges.discharge_date) as discharge_date,
         coalesce(opportunities.discharge_date_est ,discharges.discharge_date_est) as discharge_date_est,
         opportunities.facility_name,
         opportunities.closure_status,
         opportunities.closure_type,
         opportunities.enrollment_info_type,
         opportunities.enrollment_task_id,
         opportunities.time_triggered,
         opportunities.discharge_event_id,
         opportunities.eligibility_status,
         discharges.logical_event_type

    from opportunities
    left join discharges on discharges.discharge_event_id = opportunities.discharge_event_id

    )

select *
from join_adt_discharge_info
