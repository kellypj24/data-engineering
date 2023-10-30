with
{{ import(source('redshift_rxcompanion', 'rxc_sponsor_ref'), 'sponsors') }},

filter_sponsors as (

    select
        sponsor as sponsor_name,
        planname as plan_name,
        cardid_format as card_id_format
    from sponsors
    order by sponsor

    )

select *
from filter_sponsors