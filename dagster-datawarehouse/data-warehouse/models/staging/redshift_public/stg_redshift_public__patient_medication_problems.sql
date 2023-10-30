with
{{ import(source('redshift_public', 'fact_patient_medication_problems'), 'med_problems') }},

filter_med_problems as (

    select
        id as patient_med_problems_id,
        iid as institution_id,
        patient_id,
        medlist_id,
        medlist_med_id,
        problem,
        category,
        is_emergent,
        status,
        time_created,
        interventions,
        case
              when lower(problem) = 'no_refills' then 'No Refills Remaining'
              when lower(problem) = 'poor_adherence' then 'Poor Adherence/Refill Gaps'
              when lower(problem) = 'access_barrier'
                   then 'Barriers to Medication Access'
              when lower(problem) = 'too_low' then 'Dose Too Low'
              when lower(problem) = 'suboptimal' then 'Suboptimal Therapy'
              when lower(problem) = 'duration' then 'Duration Inappropriate'
              when lower(problem) in ('requires_monitoring','med_requires_monitoring')
                   then 'Medication Requires Monitoring'
              when lower(problem) = 'duplicate' then 'Duplicate Therapy'
              when lower(problem) = 'unnecessary' then 'Unnecessary Drug'
              when lower(problem) = 'wrong' then 'Wrong Formulation'
              when lower(problem) = 'needs_immunization' then 'Needs Immunization'
              when lower(problem) = 'reaction' then 'Adverse Drug Reaction'
              when lower(problem) = 'needs_additional' then 'Needs Additional Therapy'
              when lower(problem) = 'um_required' then 'Prior Authorization Required'
              when lower(problem) = 'lca' then 'lower Cost Alternative Available'
              when lower(problem) = 'not_covered'
                   then 'Drug Not Covered (Non-Formulary)'
              when lower(problem) = 'too_high' then 'Dose Too High'
              when lower(problem) = 'overutilization' then 'Overutilization'
              when lower(problem) = 'drug_drug' then 'Drug/Drug Interaction'
              when lower(problem) = 'drug_disease' then 'Drug/Disease Interaction'
              when lower(problem) = 'incorrect_administration'
                   then 'Incorrect Administration'
              when lower(problem) = 'medication_discrepancy' then 'General Medication Discrepancy'
        end                                                             as problem_descriptive,
        case
              when lower(status) = 'unresolved' then 'Unresolved'
              when lower(status) = 'patient_accepted' then 'Resolved - Patient Accepted'
              when lower(status) = 'patient_refused' then 'Resolved - Patient Refused'
              when lower(status) = 'prescriber_accepted'
                   then 'Resolved - Prescriber Accepted'
              when lower(status) = 'prescriber_refused'
                   then 'Resolved - Prescriber Refused'
              when lower(status) = 'resolved' then 'Resolved - Other - N/A'
        end                                                             as status_descriptive,
        case
              when lower(status) = 'unresolved' then 0
              else 1
        end                                                             as resolved,
        case
             when status_descriptive = 'Resolved - Prescriber Accepted' then 1
             else 0
         end                                                            as prescriber_accepted_flag,
         case
             when status_descriptive = 'Resolved - Prescriber Accepted' or
              status_descriptive = 'Resolved - Prescriber Refused' then 1
             else 0
         end                                                            as prescriber_resolved_flag
    from med_problems
)

, unpacked as (

               select
                    p.patient_med_problems_id
                    , i::varchar as intervention_unpacked
               from
                    filter_med_problems p
                    , p.interventions i

    )

, create_flags as (

            select
                patient_med_problems_id
                , sum(case
                       when intervention_unpacked = 'CALL_PROVIDER' then 1
                        else 0
                     end)                                                as call_provider_flag
                , sum(case
                       when intervention_unpacked = 'EDUCATE' then 1
                        else 0
                     end)                                                as educate_flag
                , sum(case
                       when intervention_unpacked = 'CALL_PHARMACY' then 1
                        else 0
                     end)                                                as call_pharmacy_flag
                , sum(case
                       when intervention_unpacked = 'CALL_INSURANCE' then 1
                        else 0
                     end)                                                as call_insurance_flag
                , sum(case
                       when intervention_unpacked = 'CALL_PROVIDER' or intervention_unpacked = 'CALL_PHARMACY' or
                            intervention_unpacked = 'CALL_INSURANCE' then 1
                       else 0
                     end)                                                as non_map_flag
            from unpacked
            group by patient_med_problems_id

    )

, final as (

     select filter_med_problems.*
            , create_flags.call_provider_flag
            , create_flags.educate_flag
            , create_flags.call_pharmacy_flag
            , create_flags.call_insurance_flag
            , case
                when non_map_flag = 0 then 1
                else 0
              end                                as map_only_flag
     from filter_med_problems
     join create_flags on create_flags.patient_med_problems_id =
                 filter_med_problems.patient_med_problems_id

)

select *
from final
