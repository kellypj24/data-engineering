with
{{ import(ref('enrollment_funnel'), 'enrollment') }},
{{ import(ref('fact_cmr_visit'), 'cmr_events') }},


 first_cmr_event_altered as (

    select *,
          case
              when cmr_sequence > 1 and cmr_completed_sequence = 1 then 1
              when lag(cmr_disposition_skip) over (partition by opportunity_id order by time_created) is true
                  then 1
              when cmr_disposition_skip is true
                  then 0
              else cmr_sequence
          end as cmr_sequence_altered
    from cmr_events
    where cmr_events.is_re_enroll_cmr is false

    )

,re_enroll_cmr_event_altered as (

    select *
         ,case
            when cmr_completed = 1 and cmr_sequence_desc != 1
                then row_number() over (partition by opportunity_id order by cmr_visit_date_est asc)
            when cmr_completed_sequence != 1 then 0
            else cmr_sequence_desc
         end as cmr_sequence_desc_altered
    from cmr_events
    where cmr_events.is_re_enroll_cmr is true

    )
    

, final as (
	select
		enrollment.*,
    
        first_cmr_event.cmr_visit_date_est as first_cmr_visit_date,
        first_cmr_event.event_disposition as first_cmr_disposition,
        first_cmr_event.cmr_status as first_cmr_status,
        first_cmr_event.no_show as first_cmr_no_show,
        first_cmr_event.cmr_completed as first_cmr_completed,
        first_cmr_event.cmr_bad_phone_number as first_cmr_bad_phone_number,

		re_enroll_cmr_event.cmr_visit_date_est as re_enroll_cmr_visit_date,
        re_enroll_cmr_event.event_disposition as re_enroll_cmr_disposition,
        re_enroll_cmr_event.cmr_status as re_enroll_cmr_status,
        re_enroll_cmr_event.cmr_completed as re_enroll_cmr_completed,
        re_enroll_cmr_event.no_show as re_enroll_no_show,

        coalesce(
                re_enroll_cmr_event.cmr_completed,
                first_cmr_event.cmr_completed
            ) as cmr_completed,

        case when first_cmr_event.cmr_completed  = 1 or
                    re_enroll_cmr_event.cmr_completed = 1
             then coalesce(
                 re_enroll_cmr_event.cmr_visit_date_est,
                 first_cmr_event.cmr_visit_date_est
             ) end as cmr_completed_date,

		datediff(day,
                enrollment.discharge_date,
            coalesce(
                 re_enroll_cmr_event.cmr_visit_date_est,
                 first_cmr_event.cmr_visit_date_est
             )
        ) as days_discharge_cmr
    
    from enrollment
    left join first_cmr_event_altered as first_cmr_event
		on enrollment.opportunity_id = first_cmr_event.opportunity_id
		and first_cmr_event.cmr_sequence_altered = 1
	left join re_enroll_cmr_event_altered as re_enroll_cmr_event
        on enrollment.opportunity_id = re_enroll_cmr_event.opportunity_id
        and re_enroll_cmr_event.cmr_sequence_desc_altered = 1
    where enrollment.institution_id IN ('client5')
	)

select *
from final