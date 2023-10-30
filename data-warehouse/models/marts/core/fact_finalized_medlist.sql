with
    {{ import(ref('stg_redshift_public__finalized_med_lists'), 'finalized_med_lists') }},
    {{ import(ref('dim_patient'), 'patients') }}

select finalized_med_lists.*
from finalized_med_lists
inner join patients on patients.patient_id = finalized_med_lists.patient_id
