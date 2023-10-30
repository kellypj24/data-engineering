with
{{ import(source('redshift_public', 'salesforce_task'), 'salesforce_tasks') }},

filter_enrollment_tasks as (

    select
        id as task_id,
        associated_discharge_event_id as discharge_event_id,
        coalesce(document.associatedpatientopportunityid__c::varchar, associated_discharge_event_id) AS opportunity_id,
        what_id as account_id,
        who_id as patient_contact_id,
        created_by_id,
        last_modified_by_id,
        task_type,
        task_disposition,
        status,
        completed_at_date_time as completed_at,
        convert_timezone('EST', completed_at_date_time) as completed_at_est,
        completed_by_user_id,
        completed_by_user_name,
        activity_date,
        convert_timezone('EST', activity_date) as activity_date_est,
        time_created as created_at,
        convert_timezone('EST', time_created) as created_at_est,
        time_updated as updated_at,
        convert_timezone('EST', time_updated) as updated_at_est,
        case lower(task_type)
            when 'enrollment attempt #1' then 'ea_1'
            when 'enrollment attempt #2' then 'ea_2'
            when 'enrollment attempt #3' then 'ea_3'
            when 'enrollment attempt #4' then 'ea_4'
            when 're-enrollment attempt' then 're_enroll'
        end as task_type_slug
    from salesforce_tasks
    where lower(task_type) in (
        {% for task_type in var('salesforce_enrollment_types') -%}
            '{{ task_type }}'
            {%- if not loop.last %},{{ '\n' }}{% endif %}
        {%- endfor %}
    )

),

add_task_metrics as (

    select
        *,
        datediff(hour, created_at, completed_at) as hours_to_complete,
        case
            when hours_to_complete <= 24 then 'within 24 hours'
            when hours_to_complete > 24 then 'more than 24 hours'
        end as completion_category,

        coalesce(lower(task_disposition) in (
            {% for disposition in var('salesforce_invalid_phone_dispositions', []) -%}
                '{{ disposition }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
        ), false) as has_invalid_phone_number,

        coalesce(lower(task_disposition) in (
            {% for disposition in var('salesforce_patient_reached_dispositions', []) -%}
                '{{ disposition }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
        ), false) as is_reached,

        coalesce(lower(status) = 'completed', false) as is_completed,

        coalesce(lower(task_disposition) in (
            {% for disposition in var('salesforce_patient_scheduled_dispositions', []) -%}
                '{{ disposition }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
        ), false) as is_scheduled,

        case
            when lower(task_type) = 're-enrollment attempt'
            then row_number() over (partition by discharge_event_id, task_type order by created_at)
        end as re_enrollment_attempt_count,

        row_number() over (partition by discharge_event_id, task_type order by created_at) as type_sequence_discharge,
        row_number() over (partition by opportunity_id, task_type order by created_at) as type_sequence_opportunity,

        case
            when lower(task_disposition) in ('encounter scheduled', 'encounter scheduled - warm transfer')
            then row_number() over (partition by opportunity_id, task_disposition order by created_at)
        end as encounter_scheduled_sequence

    from filter_enrollment_tasks

)

select *
from add_task_metrics
