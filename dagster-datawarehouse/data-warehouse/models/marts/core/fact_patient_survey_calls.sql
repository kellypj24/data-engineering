with
    {{ import(ref('stg_redshift_public__patient_survey_calls'), 'survey_calls') }}

select *
from survey_calls