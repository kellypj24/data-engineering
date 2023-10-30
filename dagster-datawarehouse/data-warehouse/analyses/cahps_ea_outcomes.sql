with pop as (
     select
         patients.source_patient_id,
         opportunities.opportunity_id,
         opportunities.time_created_est as opportunity_created_date,
         enrollments.task_type,
         enrollments.completed_at_est   as task_completed_date,
         enrollments.task_disposition
     from
         dim_patient patients
         join fact_patient_opportunity opportunities on patients.patient_id = opportunities.patient_id
        join fact_patient_enrollment enrollments on opportunities.opportunity_id = enrollments.opportunity_id
     where
           opportunities.program_type = 'cahps'
           and enrollments.status = 'Completed'
           and patients.institution_id = 'client3'
     order by
         patients.source_patient_id,
         enrollments.completed_at_est
     )

select *
from pop;