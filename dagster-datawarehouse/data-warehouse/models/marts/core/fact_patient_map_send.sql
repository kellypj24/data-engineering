with
{{ import(ref('stg_redshift_public__patient_map_sends'), 'patient_map_sends') }}



select *

from patient_map_sends