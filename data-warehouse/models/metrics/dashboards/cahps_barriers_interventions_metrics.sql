with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_cahps_visit'), 'cahps_visits') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_follow_up'), 'follow_ups') }},
{{ import(ref('dim_medication'), 'medications') }},
{{ import(ref('fact_cahps_report'), 'cahps_reports') }},

filter_opportunities as (

    select
        opportunities.opportunity_id,
        patients.patient_id,
        opportunities.institution_id,
        opportunities.time_created_est as opportunity_created_date,
        cahps_visits.cahps_visit_date_est as visit_completed_date

    from opportunities
    join patients on opportunities.patient_id = patients.patient_id
        and patients.institution_id = 'humana'
    join cahps_visits on opportunities.opportunity_id = cahps_visits.opportunity_id
        and cahps_visits.cahps_completed = 1
    where opportunities.program_type = 'cahps'
    group by
        opportunities.opportunity_id,
        patients.patient_id,
        opportunities.institution_id,
        opportunity_created_date,
        visit_completed_date

),

barriers_interventions_resolutions as (

    select

        cahps_reports.opportunity_id,
        cahps_reports.patient_id,
        cahps_reports.drug_id,
        barrier_name as barrier,

        case
            when resolution = 'Other - ' then 'Other - No Further Detail' else resolution
        end as resolution,

        case
            when intervention is null then 'intervention not provided' else intervention
        end as intervention

    from cahps_reports
    group by
        cahps_reports.opportunity_id,
        cahps_reports.patient_id,
        cahps_reports.drug_id,
        barrier,
        resolution,
        intervention

),

pharmacy_provider_metric as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        drug_id,
        barrier,
        resolution,
        intervention,

        case
            when task_type in ('provider follow-up', 'pharmacy follow-up') then 1 else 0
        end as pharmacy_provider_call_flag

    from filter_opportunities
    join barriers_interventions_resolutions on filter_opportunities.opportunity_id = barriers_interventions_resolutions.opportunity_id
    left join follow_ups on filter_opportunities.opportunity_id = follow_ups.opportunity_id
    group by
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        drug_id,
        barrier,
        resolution,
        intervention,
        pharmacy_provider_call_flag
),

 flatten_metrics as (

    select
        opportunity_id,
        patient_id,
        drug_id,
        barrier,
        resolution,
        intervention,

        case
            when sum(pharmacy_provider_call_flag) >= 1 then 1 else 0
        end as pharmacy_provider_call

    from pharmacy_provider_metric
    group by
        opportunity_id,
        patient_id,
        drug_id,
        barrier,
        resolution,
        intervention

),


final_results as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.institution_id,
        filter_opportunities.opportunity_created_date,
        filter_opportunities.visit_completed_date,
        flatten_metrics.drug_id,
        flatten_metrics.barrier,
        flatten_metrics.resolution,
        flatten_metrics.intervention,
        flatten_metrics.pharmacy_provider_call,
        medications.drug_class_name as medication_class,
        medications.drug_base_name as medication_name,
        medications.full_gpi_id as medication_gpi

    from filter_opportunities
    left join flatten_metrics on filter_opportunities.opportunity_id = flatten_metrics.opportunity_id
    left join medications on flatten_metrics.drug_id = medications.drug_id
    group by
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.institution_id,
        filter_opportunities.opportunity_created_date,
        filter_opportunities.visit_completed_date,
        flatten_metrics.drug_id,
        flatten_metrics.barrier,
        flatten_metrics.resolution,
        flatten_metrics.intervention,
        flatten_metrics.pharmacy_provider_call,
        medications.drug_class_name,
        medications.drug_base_name,
        medications.full_gpi_id
)

select *
from final_results




