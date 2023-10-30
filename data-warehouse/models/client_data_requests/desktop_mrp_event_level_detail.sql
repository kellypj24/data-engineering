with
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_desktop_mrp'), 'desktop_mrp_events') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_finalized_medlist'), 'finalized_med_lists') }},
{{ import(ref('fact_medlist_med'), 'medications') }},
{{ import(ref('fact_medication_problems'), 'problems') }},
{{ import(ref('fact_map_send'), 'map_sends') }},
{{ import(ref('fact_discharge_summary'), 'discharge_summaries') }},


filter_opportunities as (

    select
        opportunities.opportunity_id,
        patients.patient_id,
        patients.source_patient_id,
        patients.secondary_patient_id,
        opportunities.discharge_date::date as discharge_date,

        case
            when
                discharge_summaries.document_source in ('uploaded','ccda') then 'Y'
            else 'N'
        end as discharge_materials_obtained,

        case
            when
                discharge_summaries.document_source = 'uploaded' then 'Discharge Facility'
            when
                discharge_summaries.document_source = 'ccda' then 'CCDA'
            else null
        end as discharge_materials_source,

        case
            when discharge_summaries.document_source in ('unset', 'default') then null
            else discharge_summaries.time_created::date
        end as discharge_materials_obtained_date,

        case
            when
                patients.pcp_name is not null then 'Y'
            else 'N'
        end as pcp_info_available,

        null as pcp_info_source,

        map_sends.map_transmission_date::date as pcp_info_obtained_date,

        case
            when
                desktop_mrp_events.desktop_mrp_completed = 1 then 'Y'
            when
                desktop_mrp_events.desktop_mrp_completed = 0 then 'N'
            else null
        end as med_rec_successfully_performed,

        desktop_mrp_events.desktop_mrp_completed_date::date as med_rec_performed_date,

        case
            when
                map_sends.map_sent = 1 then 'Y'
            when
                map_sends.map_sent = 0 then 'N'
            else null
        end as med_rec_successfully_delivered_pcp,

        map_sends.map_transmission_date::date as med_rec_transferred_pcp_date

    from opportunities
    join patients on patients.patient_id = opportunities.patient_id
    left join desktop_mrp_events on desktop_mrp_events.opportunity_id = opportunities.opportunity_id
    left join map_sends on map_sends.opportunity_id = opportunities.opportunity_id
    left join discharge_summaries on opportunities.opportunity_id = discharge_summaries.patient_opportunity_id
    where opportunities.eligibility_status = 'eligible'
        and opportunities.program_type = 'desktop_mrp'
),

summarize_problems as (

    select
        filter_opportunities.opportunity_id,

        case
            when
                sum(
                    case when category = 'EFFECTIVENESS' then 1 else 0 end
                    ) > 1 then 'Y'
                else 'N'
        end as identified_effectiveness,

        case
            when
                sum(
                    case when category = 'SAFETY' then 1 else 0 end
                    ) > 1 then 'Y'
                else 'N'
        end as identified_safety,

        case
            when
                sum(
                    case when category = 'ADHERENCE' then 1 else 0 end
                    ) > 1 then 'Y'
                else 'N'
        end as identified_adherence,

        case
            when
                sum(
                    case when category = 'INDICATION' then 1 else 0 end
                    ) > 1 then 'Y'
                else 'N'
        end as identified_indication,

        case
            when
                sum(
                    case when category = 'INSURANCE_FORMULARY' then 1 else 0 end
                    ) > 1 then 'Y'
                else 'N'
        end as identified_insurance_formulary

    from filter_opportunities
    join finalized_med_lists on finalized_med_lists.opportunity_id = filter_opportunities.opportunity_id
    join medications on medications.medlist_id = finalized_med_lists.med_list_id
    join problems on problems.medlist_id = finalized_med_lists.med_list_id
        and problems.medlist_med_id = medications.id
    group by
        filter_opportunities.opportunity_id

),

final_results as (

    select
        filter_opportunities.opportunity_id,
        filter_opportunities.patient_id,
        filter_opportunities.source_patient_id,
        filter_opportunities.secondary_patient_id,
        filter_opportunities.discharge_date,
        filter_opportunities.discharge_materials_obtained,
        filter_opportunities.discharge_materials_source,
        filter_opportunities.discharge_materials_obtained_date,
        filter_opportunities.pcp_info_available,
        filter_opportunities.pcp_info_source,
        filter_opportunities.pcp_info_obtained_date,
        filter_opportunities.med_rec_successfully_performed,
        filter_opportunities.med_rec_performed_date,
        filter_opportunities.med_rec_successfully_delivered_pcp,
        filter_opportunities.med_rec_transferred_pcp_date,
        summarize_problems.identified_effectiveness,
        summarize_problems.identified_safety,
        summarize_problems.identified_adherence,
        summarize_problems.identified_indication,
        summarize_problems.identified_insurance_formulary
    from filter_opportunities
    left join summarize_problems on summarize_problems.opportunity_id = filter_opportunities.opportunity_id
)


select *
from final_results
