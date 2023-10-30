with
    {{ import(ref('stg_redshift_public__cahps_events'), 'cahps_events') }}


select *
from cahps_events
