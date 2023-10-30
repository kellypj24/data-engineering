with
    {{ import(ref('stg_redshift_public__patient_enrollments'), 'patient_enrollments') }}

select *

from patient_enrollments