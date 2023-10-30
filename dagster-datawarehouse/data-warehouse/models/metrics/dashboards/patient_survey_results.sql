with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_patient_survey_calls'), 'survey_calls') }},
{{ import(ref('dim_patient'), 'patients') }},

filter_survey_results as (

    select
        survey_calls.patient_id,
	    survey_calls.institution_id,
        opportunities.program_type,
        patients.plan_group_id,
        survey_calls.call_timestamp,
        survey_calls.survey_asked,
        survey_calls.survey_understand,
        survey_calls.survey_satisfied

    from survey_calls
	    join patients on survey_calls.patient_id = patients.patient_id
        join opportunities on survey_calls.patient_id = opportunities.patient_id

    where
        survey_calls.institution_id in (
    {% for institution_id in var('contact_institution_filter') -%}
                '{{ institution_id }}'
                {%- if not loop.last %},{{ '\n' }}{% endif %}
            {%- endfor %}
    )

    group by
        survey_calls.patient_id,
        survey_calls.institution_id,
        opportunities.program_type,
        patients.plan_group_id,
        survey_calls.call_timestamp,
        survey_calls.survey_asked,
        survey_calls.survey_understand,
        survey_calls.survey_satisfied
    )

select *
from filter_survey_results

