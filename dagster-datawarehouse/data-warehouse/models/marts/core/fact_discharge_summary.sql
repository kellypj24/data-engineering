with
    {{ import(ref('stg_redshift_public__discharge_summary'), 'discharge_summaries') }}

select *

from discharge_summaries