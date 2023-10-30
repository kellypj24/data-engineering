with
{{ import(source('redshift_rxcompanion', 'rxc_subscriber_perm_optout'), 'permanent_optout') }},

filter_optout as (

    select

        cardid as card_id,
        cardname as card_name,
        nullif(addeddate,'0001-01-01')::date as permanent_optout_date,
        programtype as program_type

    from permanent_optout

    )

select *
from filter_optout