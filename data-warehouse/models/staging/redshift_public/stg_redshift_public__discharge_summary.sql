with
{{ import(source('redshift_public', 'fact_discharge_summary'),'discharge_summary') }},

filter_discharge_summary as (

    select
        id as discharge_summary_id,
        patient_id,
        discharge_event_id::varchar,
        patient_opportunity_id::varchar,
        state,
        document_source,
        document_user_id,
        document.document_set::bool as document_set,
        ccda_association_id,
        time_created,
        time_updated
    from
        discharge_summary)

select *
from filter_discharge_summary
