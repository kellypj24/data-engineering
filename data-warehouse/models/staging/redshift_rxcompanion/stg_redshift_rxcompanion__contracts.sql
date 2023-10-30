with
{{ import(source('redshift_rxcompanion', 'rxc_contracts'), 'contracts') }},

filter_contracts as (

    select

        esi_div as card_name,
        logo_id,
        programname as program_name,
        plan_name

    from contracts

    group by
        esi_div,
        logo_id,
        programname,
        plan_name

    )

select *
from filter_contracts