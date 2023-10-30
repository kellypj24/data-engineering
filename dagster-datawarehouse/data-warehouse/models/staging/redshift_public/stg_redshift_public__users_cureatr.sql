with
{{ import(source('redshift_public', 'dim_user'), 'users') }},

filter_users as (

    select
        id::varchar as company_user_id,
        external_id as user_login,
        iid as institution_id,
        first_name,
        last_name,
        specialty,
        title,
        time_created,
        time_updated,
        npi

    from users
    where iid in (
    {% for iid in var('company_user_institutions') -%}
                '{{ iid }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
    )
)

select *
from filter_users
