with
{{ import(ref('stg_redshift_rxcompanion__call_log'), 'calls') }}


select *

from calls