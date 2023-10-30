with
{{ import(ref('desktop_mrp_billing'), 'billing') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('dim_user_cureatr'), 'users') }},

filter_med_lists as (

    select
       billing.source_patient_id                      as member_card_id,
       patients.given_name                            as member_first_name,
       patients.family_name                           as member_last_name,
       patients.date_of_birth                         as member_dob,
       patients.sex                                   as member_gender,
       ''                                             as member_ssn,
       ''                                             as member_medicare_id,
       ''                                             as member_medicaid_id,
       users.npi                                      as rendering_provider_npi,
       billing.desktop_mrp_completed_date::date       as service_date,
       ''                                             as revenue_code,
       ''                                             as icd_diagnosis_code_1,
       ''                                             as icd_diagnosis_code_2,
       ''                                             as icd_diagnosis_code_3,
       ''                                             as icd_diagnosis_code_4,
       ''                                             as icd_diagnosis_code_5,
       ''                                             as icd_diagnosis_code_6,
       ''                                             as icd_diagnosis_code_7,
       ''                                             as icd_diagnosis_code_8,
       ''                                             as icd_diagnosis_code_9,
       ''                                             as icd_diagnosis_code_10,
       ''                                             as procedural_icd_code_1,
       ''                                             as procedural_icd_code_2,
       ''                                             as procedural_icd_code_3,
       ''                                             as procedural_icd_code_4,
       ''                                             as procedural_icd_code_5,
       ''                                             as procedural_icd_code_6,
       ''                                             as procedural_icd_code_7,
       ''                                             as procedural_icd_code_8,
       ''                                             as procedural_icd_code_9,
       ''                                             as procedural_icd_code_10,
       ''                                             as icd_version,
       '1111F'                                        as cpt_cptii_hcpcs_code,
       ''                                             as cpt_modifier_1,
       ''                                             as cpt_modifier_2,
       ''                                             as cpt_result,
       ''                                             as loinc_code,
       ''                                             as loinc_result,
       ''                                             as bmi_value,
       ''                                             as member_weight,
       ''                                             as bp_systolic,
       ''                                             as bp_diastolic,
       ''                                             as ndc_code,
       ''                                             as snomed_code,
       ''                                             as prov_rec_id,
       ''                                             as discharge_status_code,
       ''                                             as pot_code,
       ''                                             as cvx_clinical_cd,
       ''                                             as rx_norm_cd
    from billing
    join patients on patients.patient_id = billing.patient_id
    join users on users.cureatr_user_id = billing.pharmacist_user_id
    where billing.map_finalized_date between date_trunc('month', current_date) - interval '1 month'
         and date_trunc('month', current_date) - interval '1 second'

)

select *
from filter_med_lists
