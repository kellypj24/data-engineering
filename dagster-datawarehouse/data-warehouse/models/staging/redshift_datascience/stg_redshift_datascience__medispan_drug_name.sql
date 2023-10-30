with
{{ import(source('redshift_datascience', 'mf2name'), 'medispan_drug_name') }},

filter_medispan_drug_name as (

    select
        drug_descriptor_id,
        drug_name,
        route_of_administration,
        dosage_form,
        strength,
        strength_unit_of_measure,
        generic_product_identifier
    from
        medispan_drug_name
    )

select *
from filter_medispan_drug_name