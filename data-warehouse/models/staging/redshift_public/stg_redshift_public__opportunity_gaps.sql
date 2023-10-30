with
{{ import(source('redshift_public', 'fact_patient_opportunity'), 'opportunities') }},


gap_information as (

    select
        opp.id::varchar as opportunity_id,
        opp.iid as institution_id, --should this stay in the opportunity table and just connect via opp?
        opp.patient_id, --should this stay in the opportunity table and just connect via opp?
        convert_timezone('EST', gaps_array.datetime_added::timestamp) as gap_added_date,
        individual_gaps.gap::varchar as gap,
        individual_gaps.pdc::decimal(18,2) as pdc,
        -- type of bonus status
        case
            when lower(opp.status::varchar) = 'standby' then 'potential'
            --when lower(opp.status::varchar) in ('new','open') then 'pending' --completed opportunity won't be closed out so end in pending
            --when lower(status::varchar) = 'completed' then 'earned'
            --when lower(opp.status::varchar) = 'closed' then 'finalized'
            when lower(opp.status::varchar) in ('new','open','closed') then 'actual'
            else ''
        end as gap_payment_status,
        case
            --TODO: HOW DO WE INCORPORATE PMPM RATE SINCE MEMBER LEVEL
            when individual_gaps.gap::varchar = 'controlling blood pressure'
                 and lower(opp.status::varchar) = 'standby' then 100
            when individual_gaps.gap::varchar = 'controlling blood pressure'
                 and lower(opp.status::varchar) in ('new','open','closed') then 100 /* (bp test completed)*25 + (last bp < 140/90 BOTH?) * 75*/
            when individual_gaps.gap::varchar in ('medication adherence for diabetes',
                                                  'medication adherence for hypertension (ace/arb)',
                                                  'medication adherence for cholesterol (statin)')
                 and lower(opp.status::varchar) = 'standby' then 75
            when individual_gaps.gap::varchar in ('medication adherence for diabetes',
                                                  'medication adherence for hypertension (ace/arb)',
                                                  'medication adherence for cholesterol (statin)')
                 and lower(opp.status::varchar) in ('new','open','closed')
                 and individual_gaps.pdc::decimal(18,2) >= .8 then 75
            when individual_gaps.gap::varchar in ('statin therapy for patients with cardiovascular disease',
                                                  'statin use in persons with diabetes',
                                                  'statin therapy for patients with diabetes')
                 and lower(opp.status::varchar) = 'standby' then 80
            when individual_gaps.gap::varchar in ('statin therapy for patients with cardiovascular disease',
                                                  'statin use in persons with diabetes',
                                                  'statin therapy for patients with diabetes')
                 and lower(opp.status::varchar) in ('new','open','closed')
                /*Need to separate SPC and SUPD, need statin claim*/ then 80
            when individual_gaps.gap::varchar = 'hba1c' -- IS THIS THE CBP HBA1C TEST THAT's 75?
                 and lower(opp.status::varchar) = 'standby' then 100
            when individual_gaps.gap::varchar = 'hba1c'
                 and lower(opp.status::varchar) in ('new','open','closed')
                 then 25/* *(hba1c test)+75*(final hba1c < 9% aka controlled)*/
            else 0
        end as gap_payment


    from
        opportunities opp,
        opp.document.opportunity_trigger.gaps as gaps_array,
        gaps_array.gaps as individual_gaps

    order by
        institution_id,
        opportunity_id,
        gap_added_date
    )

select *
from gap_information
