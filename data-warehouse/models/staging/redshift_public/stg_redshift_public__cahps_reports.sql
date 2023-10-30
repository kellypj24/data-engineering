with
{{ import(source('redshift_public', 'fact_cahps_report'), 'cahps_reports') }},

extract_barriers as (

    select
        report.patient_id,
        report.iid as institution_id,
        report.patient_opportunity_id::varchar as opportunity_id,
        report.pharmacist_user_id,
        report.document.time_finalized::timestamp as map_finalized_date,
        convert_timezone('EST', report.document.time_finalized::timestamp) as map_finalized_date_est,
        report.document.status::varchar as map_status,
        medications.drug_id::varchar,
        medications.drug_name_fallback::varchar as drug_name,
        barriers.type.name::varchar as barrier,

        case
            when barrier = 'custom' then 'Other' || ' - ' || barriers.type.additional_info::varchar else barrier
        end as barrier_name

    from cahps_reports report,
         report.document.medications medications,
         medications.cahps_barriers barriers


),

extract_interventions as (

    select
        report.patient_id,
        report.iid as institution_id,
        report.patient_opportunity_id::varchar as opportunity_id,
        report.pharmacist_user_id,
        report.document.time_finalized::timestamp as map_finalized_date,
        convert_timezone('EST', report.document.time_finalized::timestamp) as map_finalized_date_est,
        report.document.status::varchar as map_status,
        medications.drug_id::varchar,
        medications.drug_name_fallback::varchar as drug_name,
        barriers.type.name::varchar as barrier_name,
        interventions.name::varchar as intervention_name,

        case
            when intervention_name = 'other' then 'Other' || ' - ' || interventions.additional_info::varchar else intervention_name
        end as intervention

    from cahps_reports report,
         report.document.medications medications,
         medications.cahps_barriers barriers,
         barriers.interventions interventions


),

extract_resolutions as (

    select
        report.patient_id,
        report.iid as institution_id,
        report.patient_opportunity_id::varchar as opportunity_id,
        report.pharmacist_user_id,
        report.document.time_finalized::timestamp as map_finalized_date,
        convert_timezone('EST', report.document.time_finalized::timestamp) as map_finalized_date_est,
        report.document.status::varchar as map_status,
        medications.drug_id::varchar,
        medications.drug_name_fallback::varchar as drug_name,
        barriers.type.name::varchar as barrier_name,
        resolutions.name::varchar as resolution_name,

        case
            when resolution_name = 'other' then 'Other' || ' - ' || resolutions.additional_info::varchar else resolution_name
        end as resolution

    from cahps_reports report,
         report.document.medications medications,
         medications.cahps_barriers barriers,
         barriers.resolutions resolutions


),


final_results as (

    select
        extract_barriers.patient_id,
        extract_barriers.institution_id,
        extract_barriers.opportunity_id,
        extract_barriers.pharmacist_user_id,
        extract_barriers.map_finalized_date,
        extract_barriers.map_finalized_date_est,
        extract_barriers.map_status,
        extract_barriers.drug_id,
        extract_barriers.drug_name,
        extract_barriers.barrier_name,
        extract_interventions.intervention,
        extract_resolutions.resolution,

        rank() over (partition by extract_barriers.opportunity_id
                            order by extract_barriers.map_finalized_date asc, extract_barriers.map_status) as report_order

    from extract_barriers
    left join extract_interventions
        on extract_barriers.opportunity_id = extract_interventions.opportunity_id
        and extract_barriers.drug_id = extract_interventions.drug_id
    left join extract_resolutions
        on extract_barriers.opportunity_id = extract_resolutions.opportunity_id
        and extract_barriers.drug_id = extract_resolutions.drug_id

    group by
        extract_barriers.patient_id,
        extract_barriers.institution_id,
        extract_barriers.opportunity_id,
        extract_barriers.pharmacist_user_id,
        extract_barriers.map_finalized_date,
        extract_barriers.map_finalized_date_est,
        extract_barriers.map_status,
        extract_barriers.drug_id,
        extract_barriers.drug_name,
        extract_barriers.barrier_name,
        extract_interventions.intervention,
        extract_resolutions.resolution

)

select *
from final_results
