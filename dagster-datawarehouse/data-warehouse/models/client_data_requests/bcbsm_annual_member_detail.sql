with
{{ import(ref('fact_discharges'), 'discharges') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_patient_enrollment'), 'patient_enrollments') }},
{{ import(ref('fact_mrp_visit'), 'mrp_events') }},

filter_patients as (

    select *
    from patients
    where patients.institution_id = 'advantasure'

    ),


filter_enrollment as (

    select
        filter_patients.institution_id,
        filter_patients.source_patient_id,
        filter_patients.given_name,
        filter_patients.family_name,
        filter_patients.date_of_birth,
        filter_patients.sex as gender,
        filter_patients.street_address,
        filter_patients.city,
        filter_patients.state,
        filter_patients.zip,
        filter_patients.country,
        filter_patients.email,
        filter_patients.home_phone,
        filter_patients.mobile_phone,
        UPPER(filter_patients.plan_group_id) as plan_group_id,
        patient_enrollments.discharge_event_id,
        discharges.discharge_date_est as discharge_date,
        mrp_events.mrp_visit_date_est::date as mrp_visit_date,
        mrp_events.event_disposition,

        case when
            (case when
                    row_number() over (partition by filter_patients.source_patient_id
                        order by discharges.discharge_date_est) = 1 then filter_patients.original_sms_consent
                when
                    row_number() over (partition by filter_patients.source_patient_id
                        order by discharges.discharge_date_est) > 1 then filter_patients.current_sms_consent end) is true
            and
                filter_patients.mobile_phone is not null
            then
                patient_enrollments.created_at::date
            else
                null
            end as sms_date,

            case when
            (case when
                    row_number() over (partition by filter_patients.source_patient_id
                        order by discharges.discharge_date_est) = 1 then filter_patients.original_email_consent
                when
                    row_number() over (partition by filter_patients.source_patient_id
                        order by discharges.discharge_date_est) > 1 then filter_patients.current_email_consent end) is true
            and
                filter_patients.email is not null
            then
                patient_enrollments.created_at::date
            else
                null
            end as email_date,

            row_number() over (partition by filter_patients.source_patient_id, discharges.discharge_date_est
                                    order by mrp_events.mrp_visit_date_est::date)  as discharge_duplication


    from patient_enrollments
    join filter_patients
        on patient_enrollments.patient_contact_id = filter_patients.patient_contact_id
    join discharges
        on filter_patients.patient_id = discharges.patient_id
        and patient_enrollments.discharge_event_id = discharges.discharge_event_id
    left join mrp_events
        on mrp_events.discharge_event_id = patient_enrollments.discharge_event_id
        and mrp_events.mrp_completed = 1
        and mrp_events.mrp_completed_sequence = 1
    where patient_enrollments.task_type = 'Enrollment Attempt #1'
        and patient_enrollments.type_sequence_discharge = 1
        and date_part('year', discharges.discharge_date_est) = (date_part('year', current_date) - 1)
    group by
       filter_patients.institution_id,
       filter_patients.source_patient_id,
       filter_patients.given_name,
       filter_patients.family_name,
       filter_patients.date_of_birth,
       filter_patients.sex,
       filter_patients.street_address,
       filter_patients.city,
       filter_patients.state,
       filter_patients.zip,
       filter_patients.country,
       filter_patients.email,
       filter_patients.home_phone,
       filter_patients.mobile_phone,
       filter_patients.plan_group_id,
       patient_enrollments.discharge_event_id,
       discharges.discharge_date_est,
       patient_enrollments.created_at,
       original_sms_consent,
       current_sms_consent,
       original_email_consent,
       current_email_consent,
       mrp_events.mrp_visit_date_est,
       mrp_events.event_disposition

    ),

call_count as (

    select
        patient_enrollments.discharge_event_id,
        count(distinct patient_enrollments.task_id) as total_calls

    from patient_enrollments
    join filter_patients
        on patient_enrollments.patient_contact_id = filter_patients.patient_contact_id
    where patient_enrollments.task_type != 'Re-enrollment Attempt'
        and patient_enrollments.type_sequence_discharge = 1
    group by patient_enrollments.discharge_event_id

    ),

contact_blend as (

    select
        patient_enrollments.discharge_event_id,
        patient_enrollments.task_type as contact_type,
        patient_enrollments.task_disposition as contact_outcome,
        patient_enrollments.created_at as contact_date

    from patient_enrollments
    where
        patient_enrollments.type_sequence_discharge = 1
    and patient_enrollments.task_disposition is not null

    union

    select
        mrp_events.discharge_event_id,
        mrp_events.event_type as contact_type,
        mrp_events.event_disposition as contact_outcome,
        mrp_events.mrp_visit_date_est as contact_date
    from mrp_events
    where
        mrp_events.event_disposition is not null

    ),

contact_order as (

    select *,
          row_number() over (partition by discharge_event_id order by contact_date desc,  contact_type ) as contact_order_reverse
    from contact_blend

    ),

final as (

    select
        filter_enrollment.institution_id,
        filter_enrollment.source_patient_id,
        filter_enrollment.given_name,
        filter_enrollment.family_name,
        filter_enrollment.date_of_birth,
        filter_enrollment.gender,
        filter_enrollment.street_address,
        filter_enrollment.city,
        filter_enrollment.state,
        filter_enrollment.zip,
        filter_enrollment.country,
        filter_enrollment.email,
        filter_enrollment.home_phone,
        filter_enrollment.mobile_phone,
        filter_enrollment.plan_group_id,
        filter_enrollment.discharge_date,
        filter_enrollment.mrp_visit_date,

        case
            when
                 contact_order.contact_date::date >= filter_enrollment.mrp_visit_date::date
            then filter_enrollment.mrp_visit_date::date
            else contact_order.contact_date::date
            end  as last_contact_date,

        case
            when
                 contact_order.contact_date::date >= filter_enrollment.mrp_visit_date::date
            then filter_enrollment.event_disposition
            else contact_order.contact_outcome
            end  as last_contact_outcome,

        call_count.total_calls,
        filter_enrollment.sms_date,
        filter_enrollment.email_date
    from filter_enrollment
    join call_count on call_count.discharge_event_id = filter_enrollment.discharge_event_id
    join contact_order on contact_order.discharge_event_id = filter_enrollment.discharge_event_id
        and contact_order.contact_order_reverse = 1
    where filter_enrollment.discharge_duplication = 1
    )

select *
from final
