with
{{ import(source('redshift_public', 'salesforce_task'), 'salesforce_tasks') }},

filter_provider_urgent_request_tasks as (

   select
       id as task_id,
       associated_discharge_event_id as discharge_event_id,
       coalesce(document.associatedpatientopportunityid__c::varchar, associated_discharge_event_id) as opportunity_id,
       what_id as account_id,
       who_id as patient_contact_id,
       institution_name,
       completed_at_date_time::timestamp as pur_completed_date,
       convert_timezone('EST', completed_at_date_time)::timestamp as pur_completed_date_est,
       completed_by_user_name,
       task_type,
       task_disposition as pur_disposition,
       status as pur_status,
       time_created,
       time_updated,

       case
           when lower(task_disposition) like 'complete%'
           then 1 else 0
       end as pur_completed,

       case
           when lower(task_disposition) like 'canceled%'
           then 1 else 0
       end as pur_canceled

   from salesforce_tasks
   where lower(task_type) in ('Provider Urgent Request Attempt #1',
                              'Provider Urgent Request Attempt #2',
                              'Provider Urgent Request Attempt #3')

)

select *
from filter_provider_urgent_request_tasks
