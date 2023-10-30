with
{{ import(ref('dim_patient'), 'dim_patient') }},
{{ import(ref('fact_patient_opportunity'), 'opportunity') }},
{{ import(ref('fact_mrp_visit'), 'fact_mrp_visit') }},
{{ import(ref('fact_finalized_medlist'), 'fact_finalized_medlist') }},
{{ import(ref('fact_medlist_med'), 'fact_medlist_med') }},
{{ import(ref('fact_medication_problems'), 'fact_medication_problems') }},
{{ import(ref('fact_medication_fill_event'), 'fact_medication_fill_event') }},
{{ import(ref('dim_medication'), 'dim_medication') }},

    patients as (
        select
            dim_patient.patient_id,
            dim_patient.source_patient_id,
            dim_patient.institution_id,
            dim_patient.initial_discharge_at,
            opportunity.opportunity_id,
            opportunity.discharge_date
        from dim_patient
                 join opportunity on dim_patient.patient_id = opportunity.patient_id
        where dim_patient.institution_id in ('client1')
          and DATE_DIFF('day', opportunity.discharge_date::date, CURRENT_DATE) >= 30)     -- bring discharge date in from fact_discharges and joins on discharge event id

    , completed_mrp_visits as (
        select
            patients.patient_id,
            patients.source_patient_id,
            patients.institution_id,
            patients.opportunity_id,
            patients.discharge_date,
            fact_mrp_visit.mrp_visit_date,
            DATE_DIFF('day', discharge_date, mrp_visit_date) as days_btwn_discharge_mrp
        from patients
	             join fact_mrp_visit on fact_mrp_visit.opportunity_id = patients.opportunity_id-- replace with opportunity id through opportunity
	        and mrp_completed = 1
--		    and completed_mrp_visits.mrp_visit_date > patients.discharge_date -- add as a test, remove from here
    )

    , automatic_med_problems as (
        select
            completed_mrp_visits.patient_id,
            completed_mrp_visits.source_patient_id,
            completed_mrp_visits.institution_id,
            completed_mrp_visits.opportunity_id,
            fact_finalized_medlist.med_list_id          as medlist_id,
            fact_finalized_medlist.time_created::date   as medlist_creation_date,
            fact_finalized_medlist.map_finalized_date_est as medlist_finalized_date,
            fact_medlist_med.drug_id,
            fact_medlist_med.medlist_drug_name,
            dim_medication.strength as medlist_strength,
            fact_medication_problems.patient_med_problems_id,
            fact_medication_problems.problem,
            fact_medication_problems.status,
            fact_medication_problems.time_created::date as problem_id_date,
            fact_medication_problems.interventions,
            completed_mrp_visits.discharge_date,
            completed_mrp_visits.mrp_visit_date
        from completed_mrp_visits -- use fact_enrollment if not getting a lot of data points here as coalesce.
	        join fact_finalized_medlist on fact_finalized_medlist.opportunity_id = completed_mrp_visits.opportunity_id
	        join fact_medlist_med on fact_finalized_medlist.med_list_id = fact_medlist_med.medlist_id
	        join fact_medication_problems
	             on fact_medication_problems.medlist_id = fact_finalized_medlist.med_list_id
	                 and fact_medication_problems.medlist_med_id = fact_medlist_med.id
		             and lower(fact_medication_problems.problem) in
		                 ('no_refills', 'poor_adherence', 'access_barrier',
		                  'um_required', 'unnecessary', 'duplicate', 'duration',
                             'drug_disease', 'drug_drug',
                            'overutilization', 'reaction', 'fill_100', 'too_high', 'too_low')
		             and
		            lower(fact_medication_problems.status) in ('unresolved')
		    join dim_medication on dim_medication.drug_id = fact_medlist_med.drug_id
        --where true
        --      and date_diff('day', mrp_visit_date, fact_finalized_medlist.time_finalized::date) < 7 -- add as a test, remove from here since opp id
		--      and fact_finalized_medlist.time_finalized::date >= discharge_visit_joined.mrp_visit_date -- add as a test, remove from here since opp id
        --      and discharge_visit_joined.discharge_duplicate = 1 -- remove and set tests
		--      and discharge_visit_joined.mrp_duplicate = 1) -- remove and set tests
    )

    , med_fills as (
        select
            fact_medication_fill_event.patient_id,
            fact_medication_fill_event.fill_date_est as fill_date,
 		    fact_medication_fill_event.ndc as ndc,
 		    fact_medication_fill_event.prescription_date,
 		    fact_medication_fill_event.last_fill_date as last_fill_date,
 		    fact_medication_fill_event.amount_dispensed,
 		    fact_medication_fill_event.days_supply as days_supply,
 		    dim_medication.drug_group_name,
 		    dim_medication.drug_class_name,
 		    dim_medication.full_gpi_name,
            dim_medication.strength as retail_strength,
 		    dim_medication.drug_group_id,
 		    dim_medication.drug_class_id,
 		    dim_medication.full_gpi_id,
 		    fact_medication_fill_event.medication_description as drug_description,
 		    dim_medication.generic_risk as high_risk_med,
 		    dim_medication.drug_id
        from fact_medication_fill_event
            join dim_medication
                on fact_medication_fill_event.ndc =  dim_medication.ndc
    )

    , cleaned_strengths as (
        select distinct
             patient_med_problems_id
             , regexp_replace(retail_strength,'[^-.0-9]','') as reg_retail
             , case when split_part(reg_retail, '-', 1) <> '' then split_part(reg_retail, '-', 1)::float(24)
                 when split_part(reg_retail, '-', 1) = '' then null end as primary_retail_strength
             , case when split_part(reg_retail, '-', 2) <> '' then split_part(reg_retail, '-', 2)::float(24)
                 when split_part(reg_retail, '-', 2) = '' then null end as secondary_retail_strength
             , case when split_part(reg_retail, '-', 3) <> '' then split_part(reg_retail, '-', 3)::float(24)
                 when split_part(reg_retail, '-', 3) = '' then null end as tertiary_retail_strength
             , case when split_part(reg_retail, '-', 4) <> '' then split_part(reg_retail, '-', 4)::float(24)
                 when split_part(reg_retail, '-', 4) = '' then null end as quaternary_retail_strength
             , case when split_part(reg_retail, '-', 5) <> '' then split_part(reg_retail, '-', 5)::float(24)
                 when split_part(reg_retail, '-', 5) = '' then null end as quinternary_retail_strength
             , regexp_replace(medlist_strength,'[^-.0-9]','') as reg_medlist
             , case when split_part(reg_medlist, '-', 1) <> '' then split_part(reg_medlist, '-', 1)::float(24)
                 when split_part(reg_medlist, '-', 1) = '' then null end as primary_medlist_strength
             , case when split_part(reg_medlist, '-', 2) <> '' then split_part(reg_medlist, '-', 2)::float(24)
                 when split_part(reg_medlist, '-', 2) = '' then null end as secondary_medlist_strength
             , case when split_part(reg_medlist, '-', 3) <> '' then split_part(reg_medlist, '-', 3)::float(24)
                 when split_part(reg_medlist, '-', 3) = '' then null end as tertiary_medlist_strength
             , case when split_part(reg_medlist, '-', 4) <> '' then split_part(reg_medlist, '-', 4)::float(24)
                 when split_part(reg_medlist, '-', 4) = '' then null end as quaternary_medlist_strength
             , case when split_part(reg_medlist, '-', 5) <> '' then split_part(reg_medlist, '-', 5)::float(24)
                 when split_part(reg_medlist, '-', 5) = '' then null end as quinternary_medlist_strength
        from automatic_med_problems
	        left join med_fills
	            on med_fills.patient_id = automatic_med_problems.patient_id
                  and med_fills.fill_date > automatic_med_problems.mrp_visit_date
                  and automatic_med_problems.drug_id = med_fills.drug_id
    )

    , strength_comparison as (
        select
            patient_med_problems_id
            , reg_medlist
            , reg_retail
            , case
                when primary_retail_strength > primary_medlist_strength then 1
                else 0
              end as primary_strength_increase
            , case
                when secondary_retail_strength > secondary_medlist_strength then 1
                else 0
              end as secondary_strength_increase
            , case
                when tertiary_retail_strength > tertiary_medlist_strength then 1
                else 0
              end as tertiary_strength_increase
            , case
                when quaternary_retail_strength > quaternary_medlist_strength then 1
                else 0
              end as quaternary_strength_increase
            , case
                when quinternary_retail_strength > quinternary_medlist_strength then 1
                else 0
              end as quinternary_strength_increase

            , case
                when primary_retail_strength < primary_medlist_strength then 1
                else 0
              end as primary_strength_decrease
            , case
                when secondary_retail_strength < secondary_medlist_strength then 1
                else 0
              end as secondary_strength_decrease
            , case
                when tertiary_retail_strength < tertiary_medlist_strength then 1
                else 0
              end as tertiary_strength_decrease
            , case
                when quaternary_retail_strength < quaternary_medlist_strength then 1
                else 0
              end as quaternary_strength_decrease
            , case
                when quinternary_retail_strength < quinternary_medlist_strength then 1
                else 0
              end as quinternary_strength_decrease

            , case when (primary_strength_increase + secondary_strength_increase + tertiary_strength_increase
                        + quaternary_strength_increase + quaternary_strength_increase) >= 1 then 1
                   else 0
              end as overall_strength_increase
            , case when (primary_strength_decrease + secondary_strength_decrease + tertiary_strength_decrease
                        + quaternary_strength_decrease + quaternary_strength_decrease) >= 1 then 1
                   else 0
              end as overall_strength_decrease
        from cleaned_strengths

    )

    , final as (
        select
            automatic_med_problems.patient_id,
            automatic_med_problems.source_patient_id,
            automatic_med_problems.opportunity_id,
            automatic_med_problems.institution_id,
            automatic_med_problems.discharge_date,
            automatic_med_problems.mrp_visit_date,
            automatic_med_problems.medlist_creation_date,
            automatic_med_problems.medlist_finalized_date,
            automatic_med_problems.drug_id,
            automatic_med_problems.medlist_drug_name,
            automatic_med_problems.medlist_strength,
            automatic_med_problems.patient_med_problems_id,
            automatic_med_problems.problem,
            automatic_med_problems.status,
            automatic_med_problems.problem_id_date,
            automatic_med_problems.interventions,
            med_fills.fill_date,
            med_fills.ndc as ndc,
            med_fills.prescription_date,
            med_fills.last_fill_date,
            med_fills.amount_dispensed,
            med_fills.days_supply,
            med_fills.drug_group_name,
            med_fills.drug_class_name,
            med_fills.full_gpi_name,
            med_fills.retail_strength,
            med_fills.drug_group_id,
            med_fills.drug_class_id,
            med_fills.full_gpi_id,
            med_fills.drug_description,
            med_fills.high_risk_med,
            date_diff('day', mrp_visit_date, med_fills.fill_date) as days_btwn_visit_fill,
            case
              when days_btwn_visit_fill <= 30 then 1
              else 0
            end as filled_within_30_days,
            case
                when med_fills.days_supply >= 90 and filled_within_30_days = 1 then 1
                else 0
            end                         as ninety_day_supply_filled_within_30_days,
            strength_comparison.overall_strength_increase,
            strength_comparison.overall_strength_decrease,
            case
                when lower(automatic_med_problems.problem) = 'fill_100' and filled_within_30_days = 1
                    and med_fills.days_supply >= 90 then 1
                when lower(automatic_med_problems.problem) in ('no_refills', 'poor_adherence', 'access_barrier',
		                  'um_required') and filled_within_30_days = 1 then 1
                when lower(automatic_med_problems.problem) in ('unnecessary', 'duplicate', 'duration',
                            'drug_disease', 'drug_drug', 'overutilization', 'reaction')
                    and filled_within_30_days = 0 then 1
                when lower(automatic_med_problems.problem) = 'too_low' and overall_strength_increase = 1 then 1
                when lower(automatic_med_problems.problem) = 'too_high' and overall_strength_decrease = 1 then 1
                else 0
            end                         as problem_resolved,
            row_number()
            over (partition by automatic_med_problems.opportunity_id,
	                           automatic_med_problems.drug_id,
	                           automatic_med_problems.problem
	               order by med_fills.fill_date, days_supply)         as fill_date_dedup
        from automatic_med_problems
	        left join med_fills
	            on med_fills.patient_id = automatic_med_problems.patient_id
                  and med_fills.fill_date > automatic_med_problems.mrp_visit_date
                  and automatic_med_problems.drug_id = med_fills.drug_id
            left join strength_comparison
                on strength_comparison.patient_med_problems_id = automatic_med_problems.patient_med_problems_id
    )

select
    final.patient_id,
    final.opportunity_id,
    final.source_patient_id as pid,
    final.institution_id,
    final.discharge_date,
    final.mrp_visit_date::date,
    final.medlist_creation_date,
    final.medlist_finalized_date,
    final.drug_id,
    final.medlist_drug_name,
    final.patient_med_problems_id,
    final.problem,
    final.status,
    final.fill_date,
    final.days_btwn_visit_fill,
    final.filled_within_30_days,
    final.ninety_day_supply_filled_within_30_days,
    final.overall_strength_increase,
    final.overall_strength_decrease,
    final.problem_resolved,
    final.amount_dispensed,
    final.days_supply,
    final.high_risk_med,
    final.problem_id_date,
    final.interventions,
    final.ndc,
    final.prescription_date,
    final.last_fill_date,
    final.drug_group_name,
    final.drug_class_name,
    final.full_gpi_name,
    final.medlist_strength,
    final.retail_strength,
    final.drug_group_id,
    final.drug_class_id,
    final.full_gpi_id,
    final.drug_description,
    final.fill_date_dedup
from final
where fill_date_dedup = 1

