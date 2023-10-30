with
{{ import(source('redshift_datascience', 'mf2ndc'), 'medispan_ndc') }},

filter_medispan_ndc as (

    select
        ndc_upc_hri,
        drug_descriptor_id
    from
        medispan_ndc
    )

select *
from filter_medispan_ndc