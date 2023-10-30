with
{{ import(source('redshift_public', 'salesforce_event'), 'salesforce_events') }},

filter_mrp_events as (

    select
        id as mrp_event_id,
        associated_discharge_event_id as discharge_event_id,
        coalesce(document.associatedpatientopportunityid__c::varchar, associated_discharge_event_id) AS opportunity_id,
        account_id,
        institution_name,
        who_id as patient_contact_id,
        time_start,
        time_end::timestamp as mrp_visit_date,
        convert_timezone('EST', time_end)::timestamp as mrp_visit_date_est,
        event_type,
        event_disposition,
        status as mrp_status,
        time_created,
        time_updated,

        case
            when lower(event_disposition) like 'pt complete%'
            then 1 else 0
        end as mrp_completed,

        row_number() over (partition by opportunity_id order by mrp_visit_date) as mrp_sequence,
        row_number() over (partition by opportunity_id order by mrp_visit_date desc) as mrp_sequence_desc,

        case
            when lower(event_disposition) like 'pt complete%'
            then row_number() over (partition by opportunity_id, event_disposition order by time_end)
        end as mrp_completed_sequence,

        case
            when lower(event_disposition) in (
            {% for disposition in var('salesforce_invalid_phone_dispositions') -%}
                '{{ disposition }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
            )
            then 1
            else 0
        end as mrp_bad_phone_number,

        case
            when lower(event_disposition) in (
            {% for disposition in var('salesforce_no_show_dispositions') -%}
                '{{ disposition }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
            )
            then 1
            else 0
        end as no_show,

        case
            when sum(no_show) over
                    (partition by opportunity_id order by mrp_visit_date rows between unbounded preceding and current row) >= 1
                and mrp_sequence > 1
            then true
            else false
        end as is_re_enroll_mrp


    from salesforce_events
    where lower(event_type) = 'mrp'
    and lower(status) in ('new', 'completed')
    and id not in (
        {% for bad_id in var('salesforce_ignore_event_ids') -%}
            '{{ bad_id }}'
            {%- if not loop.last %},{{ '\n' }}{% endif %}
        {%- endfor %}
    )
)

select *
from filter_mrp_events
