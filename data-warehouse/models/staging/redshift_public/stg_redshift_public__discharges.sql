with
{{ import(source('redshift_public', 'fact_patient_event'),
    'patient_events') }},
{{ import(source('redshift_public', 'fact_patient_event_matched_patient'),
    'patient_events_matched') }},

filter_discharges as (

    select *
    from patient_events
    where event_type = '{{ var("patient_event__discharge") }}'
    and time_event >= '{{ var("discharge_filter_date") }}'

),

join_event_patient_ids as (

    select
        patient_events_matched.patient_id,
        patient_events_matched.iid as institution_id,
        filter_discharges.event_type,
        filter_discharges.logical_event_type,
        filter_discharges.logical_discharge_disposition,
        filter_discharges.discharge_disposition,
        filter_discharges.id::varchar as discharge_event_id,
        filter_discharges.time_event::date as discharge_date,
        convert_timezone('EST', filter_discharges.time_event)::date as discharge_date_est,

        case
            when lower(filter_discharges.discharge_disposition) like '%home health%'
                then 'home health'
            when lower(filter_discharges.discharge_disposition) like '%hh%'
                then 'home health'
            when discharge_disposition like '%06%'
                then 'home_health'
            when discharge_disposition = '6'
                then 'home health'
            else 'home' end as home_health_vs_home,

        ctn_event
    from filter_discharges
    inner join patient_events_matched
        on filter_discharges.id = patient_events_matched.event_id
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
        j.discharge_event_id,

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
        j.discharge_event_id
),
    
final as (

    select
        join_event_patient_ids.patient_id,
        join_event_patient_ids.institution_id,
        join_event_patient_ids.event_type,
        join_event_patient_ids.logical_event_type,
        join_event_patient_ids.logical_discharge_disposition,
        join_event_patient_ids.discharge_disposition,
        join_event_patient_ids.discharge_event_id,
        join_event_patient_ids.discharge_date,
        join_event_patient_ids.discharge_date_est,
        join_event_patient_ids.home_health_vs_home,
        coalesce(diagnosis_flags.chf_flag, 0) as chf_flag,
        coalesce(diagnosis_flags.diabetes_flag, 0) as diabetes_flag,
        coalesce(diagnosis_flags.copd_flag, 0) as copd_flag,
        coalesce(diagnosis_flags.pna_flag, 0) as pna_flag,
        coalesce(diagnosis_flags.ami_flag, 0) as ami_flag,
        coalesce(diagnosis_flags.sepsis_flag, 0) as sepsis_flag

    from join_event_patient_ids
    left join diagnosis_flags on diagnosis_flags.patient_id = join_event_patient_ids.patient_id
        and diagnosis_flags.discharge_event_id = join_event_patient_ids.discharge_event_id

    )


select *
from final
