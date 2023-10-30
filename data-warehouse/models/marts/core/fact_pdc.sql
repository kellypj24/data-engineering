with
    {{ import(ref('stg_redshift_public__patient_pdc'), 'pdc') }}

select *
from pdc
