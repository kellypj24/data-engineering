with
{{ import(ref('enrollment_funnel'), 'enrollment') }},
{{ import(ref('fact_mrp_visit'), 'mrp_events') }},
{{ import(ref('fact_finalized_medlist'), 'finalized_med_lists') }},
{{ import(ref('fact_map_send'), 'map_sends') }},
{{ import(ref('fact_medication_fill_event'), 'medication_fills') }},
{{ import(ref('dim_medication'), 'medications') }},

discharge_med_counts as (
    select
        enrollment.patient_id,
        enrollment.opportunity_id,
        enrollment.discharge_date,
        count(distinct medications.drug_id) as discharge_med_count
    from
        enrollment
       left join medication_fills
            on medication_fills.patient_id = enrollment.patient_id
       left join medications
            on medications.ndc = medication_fills.ndc
            and medication_fills.fill_date_est < enrollment.discharge_date
    group by
        enrollment.patient_id,
        enrollment.opportunity_id,
        enrollment.discharge_date
    )

, first_mrp_event_altered as (

    select *,
          case
              when mrp_sequence > 1 and mrp_completed_sequence = 1 then 1
              when lag(mrp_disposition_skip) over (partition by opportunity_id order by time_created) is true
                  then 1
              when mrp_disposition_skip is true
                  then 0
              else mrp_sequence
          end as mrp_sequence_altered
    from mrp_events
    where mrp_events.is_re_enroll_mrp is false

    )

,re_enroll_mrp_event_altered as (

    select *
         ,case
            when mrp_completed = 1 and mrp_sequence_desc != 1
                then row_number() over (partition by opportunity_id order by mrp_visit_date_est asc)
            when mrp_completed_sequence != 1 then 0
            else mrp_sequence_desc
         end as mrp_sequence_desc_altered
    from mrp_events
    where mrp_events.is_re_enroll_mrp is true

    )

, final as (
	select
		enrollment.*,

		first_mrp_event.mrp_visit_date_est as first_mrp_visit_date,
        first_mrp_event.event_disposition as first_mrp_disposition,
        first_mrp_event.mrp_status as first_mrp_status,
        first_mrp_event.no_show as first_mrp_no_show,
        first_mrp_event.mrp_completed as first_mrp_completed,
        first_mrp_event.mrp_bad_phone_number as first_mrp_bad_phone_number,

		re_enroll_mrp_event.mrp_visit_date_est as re_enroll_mrp_visit_date,
        re_enroll_mrp_event.event_disposition as re_enroll_mrp_disposition,
        re_enroll_mrp_event.mrp_status as re_enroll_mrp_status,
        re_enroll_mrp_event.mrp_completed as re_enroll_mrp_completed,
        re_enroll_mrp_event.no_show as re_enroll_no_show,
    
        coalesce(
                re_enroll_mrp_event.mrp_completed,
                first_mrp_event.mrp_completed
            ) as mrp_completed,

        case when first_mrp_event.mrp_completed  = 1 or
                    re_enroll_mrp_event.mrp_completed = 1
             then coalesce(
                 re_enroll_mrp_event.mrp_visit_date_est,
                 first_mrp_event.mrp_visit_date_est
             ) end as mrp_completed_date,
    
        datediff(day,
                enrollment.discharge_date,
            coalesce(
                 re_enroll_mrp_event.mrp_visit_date_est,
                 first_mrp_event.mrp_visit_date_est
             )
        ) as days_discharge_mrp,

        finalized_med_lists.med_list_id,
        finalized_med_lists.map_finalized_date_est,
        map_sends.map_transmission_date_est as map_transmission_date,
        map_sends.map_transmission_method,
        finalized_med_lists.map_finalized,
        map_sends.map_sent,

        datediff(day,
            coalesce(
                 re_enroll_mrp_event.mrp_visit_date_est,
                 first_mrp_event.mrp_visit_date_est
             ),
            finalized_med_lists.map_finalized_date_est
        ) as days_mrp_map_finalized,

        datediff(day,
            finalized_med_lists.map_finalized_date_est,
            map_sends.map_transmission_date_est
        ) as days_map_finalized_sent,

        datediff(day,
            coalesce(
                 re_enroll_mrp_event.mrp_visit_date_est,
                 first_mrp_event.mrp_visit_date_est
             ),
            map_sends.map_transmission_date_est
        ) as days_mrp_map_sent,

        discharge_med_counts.discharge_med_count

	from enrollment
	left join first_mrp_event_altered as first_mrp_event
		on enrollment.opportunity_id = first_mrp_event.opportunity_id
		and first_mrp_event.mrp_sequence_altered = 1
	left join re_enroll_mrp_event_altered as re_enroll_mrp_event
        on enrollment.opportunity_id = re_enroll_mrp_event.opportunity_id
        and re_enroll_mrp_event.mrp_sequence_desc_altered = 1
    left join finalized_med_lists
        on enrollment.opportunity_id = finalized_med_lists.opportunity_id
        and finalized_med_lists.map_finalized_sequence = 1
    left join map_sends
        on enrollment.opportunity_id = map_sends.opportunity_id
        and map_sends.map_send_sequence = 1
    left join discharge_med_counts
        on discharge_med_counts.patient_id = enrollment.patient_id
        and discharge_med_counts.opportunity_id = enrollment.opportunity_id
        and discharge_med_counts.discharge_date = enrollment.discharge_date
	)

select *
from final