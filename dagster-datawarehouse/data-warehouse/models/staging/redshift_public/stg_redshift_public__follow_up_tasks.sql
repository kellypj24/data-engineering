with
{{ import(source('redshift_public', 'salesforce_task'), 'salesforce_tasks') }},


filter_tasks as (

    select
        id as task_id,
        associated_discharge_event_id as discharge_event_id,
        coalesce(document.associatedpatientopportunityid__c::varchar, associated_discharge_event_id) AS opportunity_id,
        what_id as account_id,
        who_id as patient_contact_id,
        institution_name,
        lower(task_type) as task_type,
        task_disposition,
        status,
        time_created,
        time_updated,
        completed_by_user_name,
        completed_by_user_id,
        completed_at_date_time,

        row_number() over (partition by opportunity_id, task_type order by time_created desc) as task_sequence_reverse

    from salesforce_tasks
    where lower(task_type) in (
        {% for task_type in var('follow_up_task_types') -%}
            '{{ task_type }}'
            {%- if not loop.last %},{{ '\n' }}{% endif %}
        {%- endfor %}
    )
)

select *
from filter_tasks
