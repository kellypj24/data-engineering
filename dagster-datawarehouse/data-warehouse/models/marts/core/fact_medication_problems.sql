with
    {{ import(ref('stg_redshift_public__patient_medication_problems'), 'med_problems') }}

select *

from med_problems
