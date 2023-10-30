with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_desktop_mrp'), 'desktop_mrp_events') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_finalized_medlist'), 'finalized_med_lists') }},
{{ import(ref('dim_medication'), 'medication_details') }},
{{ import(ref('fact_medlist_med'), 'medications') }},
{{ import(ref('dim_user_company'), 'users') }},
{{ import(ref('fact_medication_problems'), 'problems') }},


medications_details_combined as (

    select
        medications.medlist_id,
        medication_details.full_gpi_name as medication_name,
        medication_details.full_gpi_id as medication_gpi,
        medications.status,
        medications.instructions,
        medications.reason_taking as medication_taken_for,
        problems.patient_med_problems_id,
        problems.problem,
        medications.patient_recommendation,
        medications.provider_recommendation,
        problems.interventions,
        medications.prescriber

    from medications
    left join medication_details on medication_details.drug_id = medications.drug_id
    left join problems on problems.medlist_med_id = medications.id
    group by
        medications.medlist_id,
        medication_details.full_gpi_name,
        medication_details.full_gpi_id,
        medications.status,
        medications.instructions,
        medication_taken_for,
        problems.patient_med_problems_id,
        problems.problem,
        medications.patient_recommendation,
        medications.provider_recommendation,
        problems.interventions,
        medications.prescriber


),


final_results as (

    select
        patients.institution_id,
        patients.patient_id,
        opportunities.opportunity_id,
        medications_details_combined.patient_med_problems_id,
        patients.source_patient_id as primary_patient_id,
        patients.secondary_patient_id,
        patients.given_name || ' ' || patients.family_name as patient_name,
        patients.date_of_birth,
        patients.pcp_name,
        opportunities.discharge_date::date as discharge_date,
        opportunities.facility_name as discharge_facility,
        desktop_mrp_events.desktop_mrp_completed_date::date as mrp_date,
        desktop_mrp_events.completed_by_user_name as mrp_completed_by,
        coauthor.first_name || ' ' || coauthor.last_name as mrp_co_signed_by,
        finalized_med_lists.provider_summary,
        medications_details_combined.medication_gpi,
        medications_details_combined.medication_name,
        medications_details_combined.status,
        medications_details_combined.instructions,
        medications_details_combined.medication_taken_for,
        medications_details_combined.problem,
        medications_details_combined.patient_recommendation,
        medications_details_combined.provider_recommendation,
        medications_details_combined.interventions,
        medications_details_combined.prescriber

    from medications_details_combined
    join finalized_med_lists on finalized_med_lists.med_list_id = medications_details_combined.medlist_id
    join opportunities on opportunities.opportunity_id = finalized_med_lists.opportunity_id
    join desktop_mrp_events on desktop_mrp_events.opportunity_id = opportunities.opportunity_id
    join patients on patients.patient_id = opportunities.patient_id
    left join users coauthor on coauthor.company_user_id = finalized_med_lists.coauthor_user_id

)

select *
from final_results
