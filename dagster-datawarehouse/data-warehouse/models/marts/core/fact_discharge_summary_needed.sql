with
    {{ import(ref('stg_redshift_public__discharge_summary_needed_tasks'), 'ds_needed') }}

select *
from ds_needed
