with
{{ import(source('redshift_public', 'salesforce_task'), 'salesforce_tasks') }},

filter_sent_patient_maps as (

    select
        id as task_id,
        document.associatedpatientopportunityid__c::varchar AS opportunity_id,
        what_id as account_id,
        who_id as patient_contact_id,
        institution_name,
        task_disposition as map_transmission_method,
        status,
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
        and lower(map_recipient) = 'patient'
        and lower(status) = 'completed'

)

select *
from filter_sent_patient_maps
