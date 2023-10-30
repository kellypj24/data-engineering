with
{{ import(source('redshift_rxcompanion', 'rxc_history_qualification'), 'qualification') }},

filter_qualification as (

    select

        keyid::varchar as key_id,
        programname as program_name,
        cardname as card_name,
        cardid as card_id,
        logo_id,
        qualifieddate as qualified_date,
        qualifieddate_met_criteria as qualified_date_met_criteria,
        qualifieddate_ee as qualified_date_expanded_eligibility,
        qualifieddate_dmp_arb as qualified_date_at_risk_beneficiary,
        howqualified as method_qualified,
        nullif(optout,'0001-01-01') as opt_out_date,
        nullif(notserviced,'') as opt_out_reason

    from qualification

    where
        qualifieddate >= '2021-01-01'
    )

select *
from filter_qualification