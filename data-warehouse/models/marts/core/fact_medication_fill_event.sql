with
{{ import(ref('stg_redshift_public__medication_fill_events'), 'medication_fill_events') }}

select *

from medication_fill_events