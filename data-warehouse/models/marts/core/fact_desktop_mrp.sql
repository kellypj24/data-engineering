with
    {{ import(ref('stg_redshift_public__desktop_mrp_events'), 'desktop_mrp') }}

select *
from desktop_mrp
