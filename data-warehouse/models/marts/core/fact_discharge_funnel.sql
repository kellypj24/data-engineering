{% set task_type_slugs =
    dbt_utils.get_column_values(
        ref('stg_redshift_public__patient_enrollments'), 'task_type_slug'
    )
%}

with
{{ import(ref('stg_redshift_public__discharges'), 'discharges') }},
{{ import(ref('stg_redshift_public__patients'), 'patients') }},
{{ import(ref('stg_redshift_public__patient_enrollments'), 'patient_enrollments') }},
{{ import(ref('stg_redshift_public__mrp_events'), 'mrp_events') }},
{{ import(ref('stg_redshift_public__finalized_med_lists'), 'finalized_med_lists') }},
{{ import(ref('stg_redshift_public__map_sends'), 'map_sends') }},

patient_enrollment_join_discharges as (

    select
        discharges.discharge_event_id,
        discharges.discharge_date,
        patients.patient_id,
        patients.source_patient_id,
        patients.institution_id,
        patient_enrollments.task_type_slug,
        patient_enrollments.task_disposition,
        patient_enrollments.status,
        patient_enrollments.created_at,
        patient_enrollments.completed_at,
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
    where discharges.discharge_event_id is not null
        and NOT patients.is_control

),

pivot_enrollment_attempts as (

    select
        discharge_event_id,
        discharge_date,
        patient_id,
        source_patient_id,
        institution_id,

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
        max(has_invalid_phone_number::integer) as ea_bad_phone_number

    from patient_enrollment_join_discharges
    where discharge_event_id is not null
    group by
        discharge_event_id,
        discharge_date,
        patient_id,
        source_patient_id,
        institution_id

),

final as (

    select
        pivot_enrollment_attempts.discharge_event_id,
        pivot_enrollment_attempts.discharge_date,
        pivot_enrollment_attempts.patient_id,
        pivot_enrollment_attempts.source_patient_id,
        pivot_enrollment_attempts.institution_id,

        pivot_enrollment_attempts.ea_1_completed_date,
        pivot_enrollment_attempts.ea_1_completion_category,
        pivot_enrollment_attempts.ea_1_created_date,
        pivot_enrollment_attempts.ea_1_bad_phone_number,
        pivot_enrollment_attempts.ea_1_completed,
        pivot_enrollment_attempts.ea_1_reached,
        pivot_enrollment_attempts.ea_1_scheduled,
        pivot_enrollment_attempts.ea_1_task_disposition,

        pivot_enrollment_attempts.ea_2_bad_phone_number,
        pivot_enrollment_attempts.ea_2_completed,
        pivot_enrollment_attempts.ea_2_completed_date,
        pivot_enrollment_attempts.ea_2_completion_category,
        pivot_enrollment_attempts.ea_2_created_date,
        pivot_enrollment_attempts.ea_2_reached,
        pivot_enrollment_attempts.ea_2_scheduled,
        pivot_enrollment_attempts.ea_2_task_disposition,

        pivot_enrollment_attempts.ea_3_bad_phone_number,
        pivot_enrollment_attempts.ea_3_completed,
        pivot_enrollment_attempts.ea_3_completed_date,
        pivot_enrollment_attempts.ea_3_completion_category,
        pivot_enrollment_attempts.ea_3_created_date,
        pivot_enrollment_attempts.ea_3_reached,
        pivot_enrollment_attempts.ea_3_scheduled,
        pivot_enrollment_attempts.ea_3_task_disposition,

        pivot_enrollment_attempts.ea_4_bad_phone_number,
        pivot_enrollment_attempts.ea_4_completed,
        pivot_enrollment_attempts.ea_4_completed_date,
        pivot_enrollment_attempts.ea_4_completion_category,
        pivot_enrollment_attempts.ea_4_created_date,
        pivot_enrollment_attempts.ea_4_reached,
        pivot_enrollment_attempts.ea_4_scheduled,
        pivot_enrollment_attempts.ea_4_task_disposition,

        pivot_enrollment_attempts.ea_bad_phone_number,
        pivot_enrollment_attempts.ea_completed,
        pivot_enrollment_attempts.ea_reached,
        pivot_enrollment_attempts.ea_scheduled,

        pivot_enrollment_attempts.re_enroll_created_date,
        pivot_enrollment_attempts.re_enrollment_attempt_count as re_enroll_attempt_count,
        pivot_enrollment_attempts.re_enroll_task_disposition as re_enroll_attempt_status,
        pivot_enrollment_attempts.re_enroll_bad_phone_number,
        pivot_enrollment_attempts.re_enroll_reached,
        pivot_enrollment_attempts.re_enroll_scheduled,

        first_mrp_event.mrp_visit_date as first_mrp_visit_date,
        first_mrp_event.event_disposition as first_mrp_disposition,
        first_mrp_event.mrp_status as first_mrp_status,
        first_mrp_event.no_show as first_mrp_no_show,
        first_mrp_event.mrp_completed as first_mrp_completed,
        first_mrp_event.mrp_bad_phone_number as first_mrp_bad_phone_number,

        re_enroll_mrp_event.mrp_visit_date as re_enroll_mrp_visit_date,
        re_enroll_mrp_event.event_disposition as re_enroll_mrp_disposition,
        re_enroll_mrp_event.mrp_status as re_enroll_mrp_status,
        re_enroll_mrp_event.mrp_completed as re_enroll_mrp_completed,
        re_enroll_mrp_event.no_show as re_enroll_no_show,

        finalized_med_lists.map_finalized_date,
        map_sends.map_transmission_date,
        map_sends.map_transmission_method,
        finalized_med_lists.map_finalized,
        map_sends.map_sent,

        datediff(day,
            coalesce(
                 re_enroll_mrp_event.mrp_visit_date,
                 first_mrp_event.mrp_visit_date
             ),
            finalized_med_lists.map_finalized_date
        ) as days_mrp_map_finalized,

        datediff(day,
            finalized_med_lists.map_finalized_date,
            map_sends.map_transmission_date
        ) as days_map_finalized_sent,

        datediff(day,
            coalesce(
                 re_enroll_mrp_event.mrp_visit_date,
                 first_mrp_event.mrp_visit_date
             ),
            map_sends.map_transmission_date
        ) as days_mrp_map_sent
    from pivot_enrollment_attempts
    left join mrp_events as first_mrp_event
        on pivot_enrollment_attempts.discharge_event_id = first_mrp_event.discharge_event_id
        and first_mrp_event.mrp_sequence = 1
    left join mrp_events as re_enroll_mrp_event
        on pivot_enrollment_attempts.discharge_event_id = re_enroll_mrp_event.discharge_event_id
        and re_enroll_mrp_event.is_re_enroll_mrp
        and re_enroll_mrp_event.mrp_sequence_desc = 1
    left join finalized_med_lists
        on pivot_enrollment_attempts.discharge_event_id = finalized_med_lists.discharge_event_id
        and finalized_med_lists.map_finalized_sequence = 1
    left join map_sends
        on pivot_enrollment_attempts.discharge_event_id = map_sends.discharge_event_id
        and map_sends.map_send_sequence = 1

)

select *
from final
