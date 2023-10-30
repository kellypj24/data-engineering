with
{{ import(ref('stg_redshift_public__patients'), 'patients') }}

select *
from patients