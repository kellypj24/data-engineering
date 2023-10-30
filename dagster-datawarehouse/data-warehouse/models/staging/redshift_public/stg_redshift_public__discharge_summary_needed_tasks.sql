with
{{ import(source('redshift_public', 'salesforce_task'), 'salesforce_tasks') }},

filter_discharge_summary_needed_tasks as (

    select
        id as task_id,
        associated_discharge_event_id as discharge_event_id,
        coalesce(document.associatedpatientopportunityid__c::varchar, associated_discharge_event_id) AS opportunity_id,
        what_id as account_id,
        who_id as patient_contact_id,
        institution_name,
        completed_at_date_time::timestamp as ds_needed_completed_date,
        convert_timezone('EST', completed_at_date_time)::timestamp as ds_needed_completed_date_est,
        completed_by_user_name,
        task_type,
        task_disposition as ds_needed_disposition,
        status as ds_needed_status,
        time_created,
        time_updated,

        case
            when lower(task_disposition) like 'complete%'
            then 1 else 0
        end as ds_needed_completed,

        case
            when lower(task_disposition) like 'canceled%'
            then 1 else 0
        end as ds_needed_canceled,

        row_number() over (partition by opportunity_id order by time_created desc) as task_sequence_desc

    from salesforce_tasks
    where lower(task_type) = 'discharge summary needed'

)

select *
from filter_discharge_summary_needed_tasks
