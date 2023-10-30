with
{{ import(source('redshift_public', 'fact_patient_opportunity'), 'opportunities') }},


final as (

    select
        id::varchar as opportunity_id,
        iid as institution_id,
        patient_id,
        time_created,
        convert_timezone('EST', time_created)::timestamp as time_created_est,
        status::varchar as opportunity_status,
        document.program_info.program_type::varchar,
        document.opportunity_trigger.discharge_date::timestamp as discharge_date,
        convert_timezone('EST', document.opportunity_trigger.discharge_date::timestamp) as discharge_date_est,
        document.opportunity_trigger.facility_name::varchar,
        document.closure_reason.status::varchar as closure_status,
        document.closure_reason.type::varchar as closure_type,
        document.enrollment_info.type::varchar as enrollment_info_type,
        document.enrollment_info.enrollment_task_id::varchar,
        document.opportunity_trigger.time_triggered::timestamp,

        case when document.opportunity_trigger.type::varchar = '{{ var("discharge_opportunity_trigger_type") }}'
             then document.opportunity_trigger.patient_event_id::varchar end as discharge_event_id,

        case
            when closure_status in (
                {% for closure_status in var('ineligible_opportunity_closure_status', []) -%}
                '{{ closure_status }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
                {%- endfor %}
                )
            or closure_type in (
                {% for closure_type in var('ineligible_opportunity_closure_type', []) -%}
                '{{ closure_type }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
                {%- endfor %}
                )   then 'ineligible'
            when opportunity_status in (
                {% for opportunity_status in var('eligible_opportunity_status', []) -%}
                '{{ opportunity_status }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
                {%- endfor %})
            or enrollment_info_type = '{{ var("eligible_enrollment_info_type") }}'
            or closure_type = '{{ var("eligible_opportunity_closure_type") }}'
                  then 'eligible'
            end as eligibility_status

    from opportunities

    )

select *
from final
