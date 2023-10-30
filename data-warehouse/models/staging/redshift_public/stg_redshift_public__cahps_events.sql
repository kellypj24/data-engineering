with
{{ import(source('redshift_public', 'salesforce_event'), 'salesforce_events') }},

filter_cahps_events as (

    select
        id as cahps_event_id,
        associated_discharge_event_id as discharge_event_id,
        coalesce(document.associatedpatientopportunityid__c::varchar, associated_discharge_event_id) AS opportunity_id,
        account_id,
        institution_name,
        who_id as patient_contact_id,
        time_start,
        time_end::timestamp as cahps_visit_date,
        convert_timezone('EST', time_end)::timestamp as cahps_visit_date_est,
        event_type,
        event_disposition,
        status as cahps_status,
        time_created,
        document.completedbyuserid__c::varchar as completed_by_user_id,
        document.completedbyusername__c::varchar as completed_by_username,
        time_updated,

        case
            when lower(event_disposition) = 'pt complete - no f/u needed'
            then 1 else 0
        end as cahps_completed,

        row_number() over (partition by opportunity_id order by cahps_visit_date) as cahps_sequence,
        row_number() over (partition by opportunity_id order by cahps_visit_date desc) as cahps_sequence_desc,

        case
            when lower(event_disposition) = 'pt complete - no f/u needed'
            then row_number() over (partition by opportunity_id, event_disposition order by time_end)
        end as cahps_completed_sequence,

        case
            when lower(event_disposition) in (
            {% for disposition in var('salesforce_invalid_phone_dispositions') -%}
                '{{ disposition }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
            )
            then 1
            else 0
        end as cahps_bad_phone_number,

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
                    (partition by opportunity_id order by cahps_visit_date rows between unbounded preceding and current row) >= 1
                and cahps_sequence > 1
            then true
            else false
        end as is_re_enroll_cahps

    from salesforce_events
    where lower(event_type) = 'cahps'

)

select *
from filter_cahps_events
