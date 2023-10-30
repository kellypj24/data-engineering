with
{{ import(ref('stg_redshift_public__map_sends'), 'map_sends') }}



select *

from map_sends


