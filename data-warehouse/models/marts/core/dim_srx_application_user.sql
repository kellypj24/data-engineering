with
{{ import(ref('stg_redshift_rxcompanion__application_user'), 'users_rxc') }},
filtered_users_rxc as (

    select *

    from users_rxc
    )

select *
from filtered_users_rxc