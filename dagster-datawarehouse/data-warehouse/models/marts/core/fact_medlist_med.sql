with
    {{ import(ref('stg_redshift_public__medlist_med'), 'medlist_med') }}

select *

from medlist_med
