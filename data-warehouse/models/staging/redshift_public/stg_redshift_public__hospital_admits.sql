with
{{ import(source('redshift_public', 'fact_patient_event'),
    'patient_events') }},
{{ import(source('redshift_public', 'fact_patient_event_matched_patient'),
    'patient_events_matched') }},

filter_admits as (

    select *
    from patient_events
    where logical_event_type = '{{ var("patient_event__hospital_admit") }}'
    and time_created >= '2021-01-01'

),

join_event_patient_ids AS (

    select
        patient_events_matched.patient_id,
        patient_events_matched.iid as institution_id,
        filter_admits.event_type,
        filter_admits.logical_event_type,
        filter_admits.id as admit_event_id,
        filter_admits.time_event::date as admit_date,
        convert_timezone('EST', filter_admits.time_event)::date as admit_date_est,
        ctn_event
    from filter_admits
    inner join patient_events_matched
        on filter_admits.id = patient_events_matched.event_id
    where iid in (
    {% for iid in var('patient_institution_filter', []) -%}
        '{{ iid }}'
        {%- if not loop.last %},{{ '\n' }}{% endif %}
    {%- endfor %}
     )
),

diagnosis_flags AS (

    select
        j.patient_id,
        j.admit_event_id,

        case when sum(
                case when d.code_id like 'I50%'
                    or d.code_id in ('I13.0', 'I11.0', '109.81', 'I97.13') then 1
                else 0 end ) >= 1 then 1 else 0 end   as chf_flag,

        case when sum(
                case when d.code_id like 'E08%' 
                  or  d.code_id like 'E09%' 
                  or  d.code_id like 'E10%' 
                  or  d.code_id like 'E11%'
                  or  d.code_id like 'E13%' then 1
                    else 0 end ) >= 1 then 1 else 0 end as diabetes_flag,

        case when sum(
               case when d.code_id like 'J44%' then 1
                    else 0 end ) >= 1 then 1 else 0 end as copd_flag,

         case when sum(
               case when d.code_id like 'J12%'
                or  d.code_id like 'J13%'
                or  d.code_id like 'J14%'
                or  d.code_id like 'J15%'
                or  d.code_id like 'J16%'
                or  d.code_id like 'J17%'
                or  d.code_id like 'J18%' then 1
                    else 0 end ) >= 1 then 1 else 0 end as pna_flag,

        case when sum(
             case when d.code_id like 'I21%' then 1
                  else 0 end ) >= 1 then 1 else 0 end as ami_flag,

        case when sum(
             case when d.code_id like 'A41%' then 1
                  else 0 end ) >= 1 then 1 else 0 end as sepsis_flag

    from join_event_patient_ids j,
         j.ctn_event.diagnoses d

    where (d.code_system like 'ICD-10%'
    or d.code_system = 'I10')
    and ( -- CHF
            d.code_id like 'I50%'
        or d.code_id in
           ('I13.0', 'I11.0', 'I09.81', 'I97.13'
               )
        -- COPD
        or d.code_id like 'J44%'
        -- PNA
        or d.code_id like 'J12%'
        or d.code_id like 'J13%'
        or d.code_id like 'J14%'
        or d.code_id like 'J15%'
        or d.code_id like 'J16%'
        or d.code_id like 'J17%'
        or d.code_id like 'J18%'
        -- Diabetes
        or d.code_id like 'E08%'
        or d.code_id like 'E09%'
        or d.code_id like 'E10%'
        or d.code_id like 'E11%'
        or d.code_id like 'E13%'
        -- AMI
        or d.code_id like 'I21%'
        -- Sepsis
        or d.code_id like 'A41%')

    group by
        j.patient_id,
        j.admit_event_id
),

final as (

    select
        join_event_patient_ids.patient_id,
        join_event_patient_ids.institution_id,
        join_event_patient_ids.event_type,
        join_event_patient_ids.logical_event_type,
        join_event_patient_ids.admit_event_id,
        join_event_patient_ids.admit_date,
        join_event_patient_ids.admit_date_est,
        coalesce(diagnosis_flags.chf_flag, 0) as chf_flag,
        coalesce(diagnosis_flags.diabetes_flag, 0) as diabetes_flag,
        coalesce(diagnosis_flags.copd_flag, 0) as copd_flag,
        coalesce(diagnosis_flags.pna_flag, 0) as pna_flag,
        coalesce(diagnosis_flags.ami_flag, 0) as ami_flag,
        coalesce(diagnosis_flags.sepsis_flag, 0) as sepsis_flag


    from join_event_patient_ids
    left join diagnosis_flags on diagnosis_flags.patient_id = join_event_patient_ids.patient_id
        and diagnosis_flags.admit_event_id = join_event_patient_ids.admit_event_id

    )

select *
from final