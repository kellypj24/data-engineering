with
{{ import(source('redshift_rxcompanion', 'application_user'), 'users') }},

filter_users as (

    select

        userid as user_id,
        username as user_name,
        nullif(title,'') as user_title,
        nullif(firstname,'') as user_first_name,
        nullif(lastname,'') as user_last_name,
        nullif(email,'') as user_email,
        nullif(credential,'') as user_credential,
        nullif(cmscode,'') as user_cms_code,
        departmentid as department_id,
        nullif(inactive,'') as inactive_flag,
        lastactivity as last_activity_date_ts

    from users
    where
        department_id is not null

    )

select *
from filter_users