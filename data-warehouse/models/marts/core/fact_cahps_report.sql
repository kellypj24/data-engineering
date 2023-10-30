with
{{ import(ref('stg_redshift_public__cahps_reports'), 'cahps_reports') }}


select *

from cahps_reports
