with
{{ import(source('redshift_datascience', 'mf2tcgpi'), 'medispan_gpi_tc') }},

filter_medispan_gpi_tc as (

    select
        tcgpi_id,
        tcgpi_name,
        tc_level_code
    from
        medispan_gpi_tc
    )

select *
from filter_medispan_gpi_tc