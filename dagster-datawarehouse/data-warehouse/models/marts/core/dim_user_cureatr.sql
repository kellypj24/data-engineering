with
{{ import(ref('stg_redshift_public__users_company'), 'users') }}

select *
from users