with
    {{ import(ref('stg_redshift_public__provider_urgent_request_tasks'), 'pur') }}

select *
from pur
