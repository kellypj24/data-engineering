with
{{ import(ref('fact_discharges'), 'discharges') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_hospital_admits'), 'hospital_admits') }},
{{ import(ref('fact_mrp_visit'), 'visits') }},



filter_institutions as (

    select *
    from patients
    where institution_id IN ('client1', 'client5')

),

filter_discharges as (

    select *
    from discharges
    where logical_event_type = '{{ var("patient_event__hospital_discharge") }}'
        and logical_discharge_disposition = '{{ var("patient_event__discharge_disposition") }}'

),

patients_join_discharges as (

    select
        filter_institutions.patient_id,
        filter_institutions.source_patient_id,
        filter_institutions.date_of_birth,
        filter_institutions.city,
        filter_institutions.state,
        filter_institutions.zip,
        filter_discharges.discharge_date,
        filter_discharges.discharge_date_est,
        filter_discharges.event_type,
        filter_discharges.logical_event_type,
        filter_discharges.discharge_event_id,
        filter_discharges.home_health_vs_home,
        filter_discharges.chf_flag as discharge_chf_flag,
        filter_discharges.diabetes_flag as discharge_diabetes_flag,
        filter_discharges.copd_flag as discharge_copd_flag,
        filter_discharges.pna_flag as discharge_pna_flag,
        filter_discharges.ami_flag as discharge_ami_flag,
        filter_discharges.sepsis_flag as discharge_sepsis_flag

    from filter_discharges
    join filter_institutions on filter_institutions.patient_id
            = filter_discharges.patient_id
),

readmission_flags as (

    select patients_join_discharges.patient_id,
           patients_join_discharges.discharge_event_id,
           patients_join_discharges.discharge_date,
           case when count(distinct case when
                    date_diff('day', patients_join_discharges.discharge_date, hospital_admits.admit_date)
                        between 0 and 30 then hospital_admits.admit_event_id end) >= 1
                            then 1 else 0 end as readmit_30_day_flag,
           case when count(distinct case when
                    date_diff('day', patients_join_discharges.discharge_date, hospital_admits.admit_date)
                        between 0 and 60 then hospital_admits.admit_event_id end) >= 1
                            then 1 else 0 end as readmit_60_day_flag,
           case when count(distinct case when
                    date_diff('day', patients_join_discharges.discharge_date, hospital_admits.admit_date)
                        between 0 and 90 then hospital_admits.admit_event_id end) >= 1
                            then 1 else 0 end as readmit_90_day_flag
    from patients_join_discharges
    left join hospital_admits
        on patients_join_discharges.patient_id = hospital_admits.patient_id
        and hospital_admits.admit_date > patients_join_discharges.discharge_date
    group by patients_join_discharges.patient_id,
           patients_join_discharges.discharge_event_id,
           patients_join_discharges.discharge_date
),

admits_discharge_joined AS (

    select
        patients_join_discharges.patient_id,
        patients_join_discharges.discharge_event_id,
        patients_join_discharges.discharge_date,
        hospital_admits.admit_event_id,
        hospital_admits.admit_date,
        hospital_admits.event_type as admit_event_type,
        hospital_admits.chf_flag as admit_chf_flag,
        hospital_admits.diabetes_flag as admit_diabetes_flag,
        hospital_admits.copd_flag as admit_copd_flag,
        hospital_admits.pna_flag as admit_pna_flag,
        hospital_admits.ami_flag as admit_ami_flag,
        hospital_admits.sepsis_flag as admit_sepsis_flag,
        date_diff('day',hospital_admits.admit_date, patients_join_discharges.discharge_date) as length_of_stay,

        row_number() over (partition by patients_join_discharges.patient_id, patients_join_discharges.discharge_date
                             order by hospital_admits.admit_date DESC) as admit_filter,

        row_number() over (partition by patients_join_discharges.patient_id, hospital_admits.admit_date
                            order by length_of_stay ASC) as admit_dedup
    from patients_join_discharges
    join hospital_admits on hospital_admits.patient_id = patients_join_discharges.patient_id
        and patients_join_discharges.discharge_date >= hospital_admits.admit_date

),

final as (

    select
        patients_join_discharges.*,
        readmission_flags.readmit_30_day_flag,
        readmission_flags.readmit_60_day_flag,
        readmission_flags.readmit_90_day_flag,
        admits_discharge_joined.admit_event_id,
        admits_discharge_joined.admit_date,
        admits_discharge_joined.admit_event_type,
        admits_discharge_joined.admit_chf_flag,
        admits_discharge_joined.admit_diabetes_flag,
        admits_discharge_joined.admit_copd_flag,
        admits_discharge_joined.admit_pna_flag,
        admits_discharge_joined.admit_ami_flag,
        admits_discharge_joined.admit_sepsis_flag,
        admits_discharge_joined.length_of_stay,
        visits.mrp_visit_date,
        case when visits.discharge_event_id is not null then 1 else 0 end as treated,
        case when date_diff('day', patients_join_discharges.discharge_date, current_date) > 30 then true
                else false end as runout_30_day_flag

    from patients_join_discharges
    join readmission_flags on readmission_flags.discharge_event_id = patients_join_discharges.discharge_event_id
    left join admits_discharge_joined on admits_discharge_joined.discharge_event_id = patients_join_discharges.discharge_event_id
        and admits_discharge_joined.admit_filter = 1
        and admits_discharge_joined.admit_dedup = 1
    left join visits on visits.discharge_event_id = patients_join_discharges.discharge_event_id
        and visits.mrp_completed = 1
        and mrp_completed_sequence = 1
    )

select *
from final