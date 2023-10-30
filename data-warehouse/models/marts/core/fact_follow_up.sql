with
    {{ import(ref('stg_redshift_public__follow_up_tasks'), 'follow_ups') }}


select *
from follow_ups
