with
    {{ import(ref('stg_redshift_public__discharges'), 'discharges') }}

select *

from discharges