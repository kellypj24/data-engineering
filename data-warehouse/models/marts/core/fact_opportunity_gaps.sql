with
    {{ import(ref('stg_redshift_public__opportunity_gaps'), 'gaps') }},


gap_information as (

    select
        *
    from
        gaps
    )

select *
from gap_information

