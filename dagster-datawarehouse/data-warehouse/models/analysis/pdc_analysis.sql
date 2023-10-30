with
{{ import(ref('fact_pdc'), 'pdc') }},
{{ import(ref('dim_patient'), 'patient') }},
{{ import(ref('fact_patient_opportunity'), 'opportunity') }},
{{ import(ref('fact_mrp_visit'), 'visits') }},

discharge_pdc   as (select pdc.patient_id
                              , pdc.patient_opportunity_id
                              , patient.source_patient_id as pid
                              , pdc.iid
                              , pdc.discharge_event_id
                              , pdc.category
                              , pdc.followup_interval_in_days
                              , pdc.pdc
                              , pdc.time_created::date              as discharge_record_created
                              , row_number()
                                over (partition by pdc.patient_id, discharge_event_id, category
	                                order by pdc.time_created asc) as pdc_calc_ascending
                         from pdc
	                              join patient on patient.patient_id = pdc.patient_id
                         where pdc.followup_interval_in_days = 0
	                       and pdc.discharge_event_id is not null
                           and discharge_record_created >= '2022-11-01'
                           and pdc.iid = 'client1')

   , discharge_date  as (select
                              discharge_pdc.patient_id
		                      , discharge_pdc.patient_opportunity_id
                              , discharge_pdc.pid
		                      , discharge_pdc.iid
		                      , discharge_pdc.discharge_event_id
		                      , opportunity.discharge_date
		                      , discharge_pdc.discharge_record_created
		                      , discharge_pdc.category
		                      , discharge_pdc.followup_interval_in_days
		                      , discharge_pdc.pdc                                                    as discharge_pdc
                         from discharge_pdc
	                              inner join opportunity on opportunity.opportunity_id = discharge_pdc.patient_opportunity_id
                         where discharge_pdc.pdc_calc_ascending = 1
                         )

   , followup_joined_30 as (select discharge_date.patient_id
                              , discharge_date.patient_opportunity_id
                              , discharge_date.pid
                              , discharge_date.iid
                              , discharge_date.discharge_event_id
                              , discharge_date.discharge_date
                              , discharge_date.discharge_record_created
                              , discharge_date.category
                              , discharge_date.followup_interval_in_days
                              , discharge_date.discharge_pdc
                              , followup_pdc.pdc                                                    as followup_pdc_30
                              , followup_pdc.time_created                                           as followup_record_created_30
                              , row_number()
                                over (partition by discharge_date.patient_id, discharge_date.discharge_event_id, discharge_date.category
	                                order by followup_pdc.time_created asc)                        as followup_pdc_calc_ascending_30
                              , date_diff('day', discharge_record_created, followup_record_created_30) as days_between_records


                         from discharge_date
	                              join pdc as followup_pdc
	                                   on followup_pdc.discharge_event_id = discharge_date.discharge_event_id
		                                   and followup_pdc.category = discharge_date.category
		                                   and followup_pdc.followup_interval_in_days = 30)

       , followup_joined_90 as (select discharge_date.patient_id
                              , discharge_date.patient_opportunity_id
                              , discharge_date.pid
                              , discharge_date.iid
                              , discharge_date.discharge_event_id
                              , discharge_date.discharge_date
                              , discharge_date.discharge_record_created
                              , discharge_date.category
                              , discharge_date.followup_interval_in_days
                              , discharge_date.discharge_pdc
                              , followup_joined_30.followup_pdc_30
                              , followup_pdc.pdc                                                    as followup_pdc_90
                              , followup_pdc.time_created                                           as followup_record_created_90
                              , row_number()
                                over (partition by discharge_date.patient_id, discharge_date.discharge_event_id, discharge_date.category
	                                order by followup_pdc.time_created asc)                        as followup_pdc_calc_ascending_90
                              , date_diff('day', discharge_date.discharge_record_created, followup_record_created_90) as days_between_records


                         from discharge_date
	                              join pdc as followup_pdc
	                                   on followup_pdc.discharge_event_id = discharge_date.discharge_event_id
		                                   and followup_pdc.category = discharge_date.category
		                                   and followup_pdc.followup_interval_in_days = 90
                                  left join followup_joined_30
	                                   on followup_joined_30.discharge_event_id = discharge_date.discharge_event_id
		                                   and followup_joined_30.category = discharge_date.category
		                 where followup_pdc_calc_ascending_30 = 1
		                                   )


   , visits_joined   as (select followup_joined_90.patient_id
                              , followup_joined_90.patient_opportunity_id
                              , followup_joined_90.pid
                              , followup_joined_90.iid
                              , followup_joined_90.discharge_event_id
                              , followup_joined_90.discharge_date
                              , followup_joined_90.followup_record_created_90
                              , followup_joined_90.discharge_record_created
                              , followup_joined_90.category
                              , followup_joined_90.discharge_pdc
                              , followup_joined_90.followup_pdc_30
                              , followup_joined_90.followup_pdc_90
                              , visits.mrp_visit_date::date                                   as visit_date

                              , case when visits.mrp_visit_date is not null then 1 else 0 end as treated
                              , row_number() over (partition by patient_id, followup_joined_90.discharge_event_id, category
		order by visits.mrp_visit_date::date asc)                                             as visit_dedup
                         from followup_joined_90
	                              LEFT join visits on visits.discharge_event_id =
	                                                            followup_joined_90.discharge_event_id
                         where followup_pdc_calc_ascending_90 = 1
		                   and days_between_records between 89 and 90
		                   )

select visits_joined.patient_opportunity_id
     , visits_joined.patient_id
	 , visits_joined.pid
	 , visits_joined.iid
	 , visits_joined.discharge_event_id
	 , visits_joined.discharge_date
     , visits_joined.followup_record_created_90
     , visits_joined.discharge_record_created
     , date_diff('day', discharge_date, followup_record_created_90) as discharge_90_date
     , date_diff('day', discharge_record_created, followup_record_created_90) as record_created
	 , date_trunc('month', visits_joined.discharge_date)::date as discharge_month
	 , date_trunc('week', visits_joined.discharge_date)::date  as discharge_week
	 , visits_joined.category
	 , visits_joined.discharge_pdc
	 , visits_joined.followup_pdc_30
     , visits_joined.followup_pdc_90
	 , followup_pdc_30 - discharge_pdc                            as pdc_change_30
	 , followup_pdc_90 - discharge_pdc                            as pdc_change_90
	 , visits_joined.visit_date
	 , visits_joined.treated
from visits_joined
WHERE visit_dedup = 1
