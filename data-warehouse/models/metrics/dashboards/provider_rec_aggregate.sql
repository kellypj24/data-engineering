with
{{ import(ref('problem_resolution_provider_rec'), 'filtered_problems') }},

    
aggregation as (
    
    select
         institution_id
         , category
         , problem
         , count(distinct patient_med_problems_id)                                      as actionable_problems
         , ((sum(resolved::decimal(10,1)) /
                count(distinct patient_med_problems_id)) * 100)::decimal(10,1)          as resolution_percentage
         , sum(prescriber_accepted_flag)                                                as accepted_prescriber_requests
         , sum(prescriber_resolved_flag)                                                as resolved_prescriber_requests
          , case 
                when resolved_prescriber_requests > 0 then
                    (((accepted_prescriber_requests::decimal(10,1)) / (nullif(resolved_prescriber_requests,0))) * 100)::decimal(10,1)
                when resolved_prescriber_requests = 0 then null
                else 0 
            end                                                          as prescriber_acceptance_percentage
         , ((sum(case
                    when is_emergent = 'true' then 1 
                    else 0 
                 end)::decimal(10,1))
                    / (count(distinct patient_med_problems_id)) * 100)::decimal(10,1)
                                                                         as emergent_percentage
         , sum(call_provider_flag)                              as provider_call_count
         , sum(educate_flag)                                    as educated
         , sum(call_pharmacy_flag)                              as called_pharmacy
         , sum(map_only_flag)                                   as map_send_only_count
         , case
                when sum(case when prescriber_resolved_flag = 1
                                    and call_provider_flag = 1 then 1 else 0 end) > 0 then
                                ((sum(case when prescriber_accepted_flag = 1
                                    and call_provider_flag = 1 then 1 else 0 end)::decimal(10,1) /
                                (nullif(sum(case when prescriber_resolved_flag = 1
                                    and call_provider_flag = 1 then 1 else 0 end),0))) * 100)::decimal(10,1)
                when sum(case when prescriber_resolved_flag = 1
                        and call_provider_flag = 1 then 1 else 0 end) = 0 then null
                else 0
            end                                                            as call_acceptance_percentage
          , case
                when sum(case when prescriber_resolved_flag = 1
                                    and map_only_flag = 1 then 1
                              else 0
                         end) > 0 then
                                ((sum(case when prescriber_accepted_flag = 1
                                            and map_only_flag = 1 then 1
                                           else 0
                                      end)::decimal(10,1) /
                                (nullif(sum(case when prescriber_resolved_flag = 1
                                                    and map_only_flag = 1 then 1
                                                 else 0
                                            end),0))) * 100)::decimal(10,1)
                when sum(case when prescriber_resolved_flag = 1
                                    and map_only_flag = 1 then 1
                              else 0
                         end) = 0 then null
                else 0
            end                                                            as map_acceptance_percentage
    from filtered_problems
    group by institution_id, category, problem
    order by institution_id, category, problem
    
    )

select institution_id
       , category
       , problem
       , actionable_problems
       , resolution_percentage
       , prescriber_acceptance_percentage
       , provider_call_count
       , call_acceptance_percentage
       , map_send_only_count
       , map_acceptance_percentage
from aggregation