-- want the program and ranking
with {{ import(ref('fact_opportunity_gaps'), 'gaps') }},
     {{ import(ref('fact_patient_opportunity'), 'opportunity')}},

gap_programs as (
    select
        gaps.opportunity_id,
        opportunity.institution_id,
        opportunity.program_type,
        sum(gaps.gap_payment) as opportunity_value

    from
        gaps
        left join opportunity
            on gaps.opportunity_id = opportunity.opportunity_id

    where
        lower(opportunity.opportunity_status) = 'standby'
        and opportunity.program_type = 'cmm'

    group by
        gaps.opportunity_id,
        opportunity.institution_id,
        opportunity.program_type
    ),

prioritization as (
    select
        opportunity_id,
        institution_id,
        program_type,
        row_number() over(partition by institution_id, program_type order by opportunity_value, opportunity_id desc) as opportunity_ranking

    from
        -- Union each program type above with rankings calculated
        gap_programs

)

select
    institution_id,
    program_type,
    opportunity_id,
    opportunity_ranking

from prioritization

order by institution_id, program_type, opportunity_ranking