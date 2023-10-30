with
{{ import(source('redshift_public', 'salesforce_task'), 'salesforce_tasks') }},

filter_sent_maps as (

    select
        id as task_id,
        associated_discharge_event_id as discharge_event_id,
        coalesce(document.associatedpatientopportunityid__c::varchar, associated_discharge_event_id) AS opportunity_id,
        what_id as account_id,
        who_id as patient_contact_id,
        institution_name,
        task_disposition as map_transmission_method,
        status,
        pcp_provider_name,
        pcp_provider_npi,
        non_pcp_provider_name,
        non_pcp_provider_npi,
        map_recipient,
        completed_at_date_time,
        completed_by_user_id,
        completed_by_user_name,
        time_created,
        time_updated,
        convert_timezone('UTC', timestamp 'epoch' + document.completeddatetime::bigint / 1000 *
                                interval '1 second') as map_transmission_date,

        convert_timezone('EST', timestamp 'epoch' + document.completeddatetime::bigint / 1000 *
                                interval '1 second') as map_transmission_date_est,

        case
            when map_recipient = 'PCP' then pcp_provider_npi
            when map_recipient = 'NON-PCP PROVIDER' then non_pcp_provider_npi
        end as recipient_provider_npi,

        case
            when map_recipient = 'PCP' then pcp_provider_name
            when map_recipient = 'NON-PCP PROVIDER' then non_pcp_provider_name
        end as recipient_provider_name,

        case
            when map_transmission_date is not null
                then 1
            else 0
        end as map_sent,

        case
            when lower(status) = 'completed'
            then row_number() over (partition by opportunity_id, status order by map_transmission_date)
        end as map_send_sequence

    from salesforce_tasks
    where lower(task_type) in ('document send', 'document resend')
        and lower(map_recipient) in (
            {% for recipient in var('salesforce_map_provider_recipients', []) -%}
                '{{ recipient }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
        )

)

select *
from filter_sent_maps
