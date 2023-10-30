with
{{ import(ref('stg_redshift_public__users_cureatr'), 'users') }}

select *
from users