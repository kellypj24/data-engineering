with
{{ import(source('redshift_public', 'fact_medication_event'), 'medication_fill_events') }},

filter_medication_fill_events AS (

    select
        patient_id,
        iid as institution_id,
        event_time::date as fill_date,
        convert_timezone('EST', event_time)::date as fill_date_est,
        time_created,
        medication_dispensed.drugcoded.productcode.code::varchar AS ndc,
        medication_dispensed.drugdescription::varchar as medication_description,
        medication_dispensed.drugcoded.strength::varchar as strength,
        medication_dispensed.dayssupply::varchar as days_supply,
        medication_dispensed.directions::varchar as directions,
        medication_dispensed.pharmacy.storename::varchar as pharmacy_name,
        medication_dispensed.pharmacy.identification.npi::varchar as pharmacy_npi,
        medication_dispensed.prescriber.identification.npi::varchar as prescriber_npi,
        medication_dispensed.lastfilldate.date::date as last_fill_date,
        convert_timezone('EST', medication_dispensed.lastfilldate.date::date)::date as last_fill_date_est,
        medication_dispensed.medication_dispensed.writtendate.date::date as prescription_date,
        medication_dispensed.medication_dispensed.quantity.value::integer as amount_dispensed

    from
        medication_fill_events
    where institution_id in (
        {% for iid in var('patient_institution_filter', []) -%}
            '{{ iid }}'
            {%- if not loop.last %},{{ '\n' }}{% endif %}
        {%- endfor %}
        )
    )

select *
from filter_medication_fill_events