with
{{ import(source('redshift_public', 'fact_medlist_med'), 'medlist_meds') }},

filter_medlist_meds as (

    select
        id,
        medlist_id,
        drug_id,
        drug_name_fallback AS medlist_drug_name,
        status,
        sig as instructions,
        prescriber,
        document.reason_taking_enc::varchar as reason_taking,
        document.patient_recommendation_enc::varchar as patient_recommendation,
        document.provider_recommendation_enc::varchar as provider_recommendation
    from medlist_meds

)

select *
from filter_medlist_meds
