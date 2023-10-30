with
{{ import(ref('fact_discharge_summary'), 'discharge_summaries') }},
{{ import(ref('dim_patient'), 'patients') }},
{{ import(ref('fact_patient_opportunity'), 'opportunities') }},
{{ import(ref('fact_mrp_visit'), 'telephonic_mrp') }},
{{ import(ref('fact_desktop_mrp'), 'desktop_mrp') }},
{{ import(ref('fact_cmr_visit'), 'cmr') }},


filter_opportunities as (

    select *
    from opportunities
    where eligibility_status = 'eligible'
    )

, final as (

    select
        filter_opportunities.discharge_event_id,
        filter_opportunities.opportunity_id,
        discharge_summaries.discharge_summary_id,
        discharge_summaries.patient_id,
        patients.institution_id,
        coalesce(telephonic_mrp.event_type, desktop_mrp.task_type, cmr.event_type) as event_type,
        discharge_summaries.state,
        discharge_summaries.document_source,
        discharge_summaries.document_user_id,
        discharge_summaries.document_set,
        discharge_summaries.ccda_association_id,
        discharge_summaries.time_created,
        discharge_summaries.time_updated,
        patients.state as patient_state
    from
        filter_opportunities
        left join discharge_summaries on

                case when discharge_summaries.patient_opportunity_id is null
                    then filter_opportunities.discharge_event_id = discharge_summaries.discharge_event_id
                    else filter_opportunities.opportunity_id = discharge_summaries.patient_opportunity_id
                end

        join patients on patients.patient_id = discharge_summaries.patient_id
        left join telephonic_mrp on telephonic_mrp.opportunity_id = filter_opportunities.opportunity_id
        left join desktop_mrp on desktop_mrp.opportunity_id = filter_opportunities.opportunity_id
        left join cmr on cmr.opportunity_id = filter_opportunities.opportunity_id
    group by
        filter_opportunities.discharge_event_id,
        filter_opportunities.opportunity_id,
        discharge_summaries.discharge_summary_id,
        discharge_summaries.patient_id,
        patients.institution_id,
        telephonic_mrp.event_type,
        desktop_mrp.task_type,
        cmr.event_type,
        discharge_summaries.state,
        discharge_summaries.document_source,
        discharge_summaries.document_user_id,
        discharge_summaries.document_set,
        discharge_summaries.ccda_association_id,
        discharge_summaries.time_created,
        discharge_summaries.time_updated,
        patients.state
    )

select *
from final
