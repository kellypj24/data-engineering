{% set task_type_slugs =
    dbt_utils.get_column_values(
        ref('stg_redshift_public__patient_enrollments'), 'task_type_slug'
    )
%}

with
{{ import(ref('fact_discharges'), 'discharges') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('stg_redshift_public__patient_enrollments'), 'patient_enrollments') }},


patient_enrollments_join_discharges as (

	select
		patient_enrollments.opportunity_id,
        patient_enrollments.discharge_event_id,
        discharges.discharge_date_est as discharge_date,
        discharges.logical_event_type as discharge_event_type,
        patients.patient_id,
        patients.source_patient_id,
        patients.institution_id,
        patients.plan_name,
		patients.contract_id,
        patients.date_of_birth,
        patient_enrollments.task_type_slug,
        patient_enrollments.task_disposition,
        patient_enrollments.status,
        patient_enrollments.created_at_est as created_at,
        patient_enrollments.completed_at_est as completed_at,
        patient_enrollments.completion_category,
        patient_enrollments.has_invalid_phone_number,
        patient_enrollments.is_completed,
        patient_enrollments.is_reached,
        patient_enrollments.is_scheduled,
        patient_enrollments.re_enrollment_attempt_count

	from patient_enrollments
	join patients
		on patient_enrollments.patient_contact_id = patients.patient_contact_id
	left join discharges
		on patients.patient_id = discharges.patient_id
        and patient_enrollments.discharge_event_id = discharges.discharge_event_id
	where NOT patients.is_control
	)

,pivot_enrollment_attempts as (

	select
		opportunity_id,
        discharge_event_id,
        discharge_date,
        discharge_event_type,
		patient_id,
        source_patient_id,
        institution_id,
        plan_name,
        contract_id,

        {{- dbt_utils.pivot(
            column='task_type_slug',
            values=task_type_slugs,
            agg="min",
            then_value="created_at::date",
            else_value="null",
            suffix="_created_date"
        ) -}},

        {{- dbt_utils.pivot(
            column='task_type_slug',
            values=task_type_slugs,
            agg="min",
            then_value="task_disposition",
            else_value="null",
            suffix="_task_disposition",
        ) -}},

        {{- dbt_utils.pivot(
            column='task_type_slug',
            values=task_type_slugs,
            agg="min",
            then_value="completed_at::date",
            else_value="null",
            suffix="_completed_date"
        ) -}},

        {{- dbt_utils.pivot(
            column='task_type_slug',
            values=task_type_slugs,
            agg="min",
            then_value="completion_category",
            else_value="null",
            suffix="_completion_category"
        ) -}},

        {{- dbt_utils.pivot(
            column='task_type_slug',
            values=task_type_slugs,
            agg="max",
            then_value="has_invalid_phone_number::integer",
            suffix="_bad_phone_number"
        ) -}},

        {{- dbt_utils.pivot(
            column='task_type_slug',
            values=task_type_slugs,
            agg="max",
            then_value="is_completed::integer",
            suffix="_completed"
        ) -}},

        {{- dbt_utils.pivot(
            column='task_type_slug',
            values=task_type_slugs,
            agg="max",
            then_value="is_reached::integer",
            suffix="_reached"
        ) -}},

        {{- dbt_utils.pivot(
            column='task_type_slug',
            values=task_type_slugs,
            agg="max",
            then_value="is_scheduled::integer",
            suffix="_scheduled"
        ) -}},

         coalesce(
            max(re_enrollment_attempt_count),
            0) as re_enrollment_attempt_count,

        max(is_reached::integer) as ea_reached,
        max(is_scheduled::integer) as ea_scheduled,
        max(is_completed::integer) as ea_completed,
        max(has_invalid_phone_number::integer) as ea_bad_phone_number,

        date_diff('year', date_of_birth, discharge_date)::int as age_at_discharge,

        case
             when age_at_discharge <= 49 then '49 & under'
             when age_at_discharge between 50 and 59 then '50 - 59'
             when age_at_discharge between 60 and 69 then '60 - 69'
             when age_at_discharge between 70 and 79 then '70 - 79'
             when age_at_discharge between 80 and 89 then '80 - 89'
             when age_at_discharge >= 90 then '90+' end as age_at_discharge_grouped

    from patient_enrollments_join_discharges
    where opportunity_id is not null
    group by
		opportunity_id,
        discharge_event_id,
        discharge_date,
        discharge_event_type,
		patient_id,
        source_patient_id,
        institution_id,
        plan_name,
        contract_id,
        date_of_birth

	)

select *
from pivot_enrollment_attempts
