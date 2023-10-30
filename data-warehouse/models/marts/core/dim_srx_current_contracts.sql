with
{{ import(ref('stg_redshift_rxcompanion__contracts'), 'contracts') }},
filtered_contracts as (

    select
        card_name,
        logo_id,
        program_name,
        plan_name

    from contracts
    )

select *
from filtered_contracts
