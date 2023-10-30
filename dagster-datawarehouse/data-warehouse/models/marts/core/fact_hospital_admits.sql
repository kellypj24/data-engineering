with
    {{ import(ref('stg_redshift_public__hospital_admits'), 'admits') }}

select *

from admits