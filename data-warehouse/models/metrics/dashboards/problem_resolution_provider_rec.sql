with
{{ import(ref('dim_patient'), 'dim_patient') }},
{{ import(ref('dim_user_cureatr'), 'dim_user') }},
{{ import(ref('fact_finalized_medlist'), 'fact_finalized_medlist') }},
{{ import(ref('fact_medlist_med'), 'fact_medlist_med') }},
{{ import(ref('fact_medication_problems'), 'fact_medication_problems') }},

incoming as (

    select
         fact_medication_problems.patient_med_problems_id
         , fact_medication_problems.category
         , fact_medication_problems.institution_id
         , fact_medication_problems.interventions
         , fact_medication_problems.is_emergent
         , fact_medication_problems.medlist_id
         , fact_medication_problems.medlist_med_id
         , fact_medication_problems.patient_id
         , fact_medication_problems.problem
         , fact_medication_problems.call_provider_flag
         , fact_medication_problems.educate_flag
         , fact_medication_problems.call_pharmacy_flag
         , fact_medication_problems.map_only_flag
         , fact_medication_problems.problem_descriptive
         , fact_medication_problems.status
         , fact_medication_problems.status_descriptive
         , fact_medication_problems.resolved
         , fact_medication_problems.time_created
         , dim_user.cureatr_user_id                                     as pharmacist_user_id
         , concat(dim_user.first_name, concat(' ', dim_user.last_name)) as pharmacist_name
         , fact_medlist_med.drug_id
         , fact_medlist_med.medlist_drug_name
         , fact_medication_problems.prescriber_accepted_flag
         , fact_medication_problems.prescriber_resolved_flag
    from fact_medication_problems
             join fact_finalized_medlist on fact_finalized_medlist.med_list_id = fact_medication_problems.medlist_id
             join fact_medlist_med on fact_medlist_med.id = fact_medication_problems.medlist_med_id
             join dim_user on dim_user.cureatr_user_id = fact_finalized_medlist.user_id
             join dim_patient on dim_patient.patient_id = fact_medication_problems.patient_id and
                                 dim_patient.institution_id = fact_medication_problems.institution_id
    where fact_medication_problems.patient_id not in (select patient_id from datascience.all_test_patients)
        and fact_medication_problems.problem not in ('INCORRECT_ADMINISTRATION','LCA', 'MEDICATION_DISCREPANCY',
                    'MED_REQUIRES_MONITORING', 'NEEDS_IMMUNIZATION', 'NOT_COVERED', 'REQUIRES_MONITORING',
                    'TOO_EXPENSIVE', 'UM_REQUIRED')
        and fact_medication_problems.interventions <> '["EDUCATE"]'

    )

select *
from incoming