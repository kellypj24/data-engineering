with
{{ import(ref('dim_patient'), 'dim_patient') }},
{{ import(ref('fact_patient_opportunity'), 'opportunity') }},
{{ import(ref('fact_patient_enrollment'), 'enrollment') }},
{{ import(ref('fact_mrp_visit'), 'mrp') }},
{{ import(ref('fact_medication_fill_event'), 'med_event') }},
{{ import(ref('dim_medication'), 'dim_medication') }},

    patients as (
        select distinct
            dim_patient.source_patient_id,
            dim_patient.patient_id,
            dim_patient.institution_id,
            opportunity.opportunity_id,
            dim_patient.control_type,
            opportunity.discharge_date
        from dim_patient
            join opportunity on dim_patient.patient_id = opportunity.patient_id
                 and lower(dim_patient.control_type) in ('intervention', 'control')
        where dim_patient.institution_id in ('advantasure', 'united')
    )

   , ea_indicator as (
        select
            patients.patient_id,
            patients.opportunity_id,
            max(case
                    when lower(enrollment.task_disposition) in
                         ('encounter scheduled',
                          'refused service',
                          'do not contact') then 1
                    else 0 end
                ) as pt_reached
        from patients
                 left join enrollment on enrollment.opportunity_id = patients.opportunity_id
                    and lower(enrollment.task_type) like 'enrollment attempt%%'
        group by patients.patient_id
               , patients.opportunity_id
        )

   , mrp_complete_indicator as (
        select
            patients.patient_id,
            patients.control_type,
            patients.opportunity_id,
            patients.discharge_date,
            mrp.mrp_visit_date::date              as mrp_date,
            row_number()
            over (partition by patients.patient_id, patients.opportunity_id
                order by mrp.mrp_visit_date asc ) as mrp_date_order
        from patients
	             left join mrp on mrp.opportunity_id = patients.opportunity_id
                    and mrp_completed = 1
                    and mrp.mrp_visit_date >= '2022-07-20'
        )

   , med_fills as (
        select
            med_event.patient_id,
            med_event.fill_date_est::DATE as fill_date,
 		    med_event.ndc as ndc,
 		    med_event.prescription_date,
 		    med_event.last_fill_date as last_fill_date,
 		    med_event.amount_dispensed,
 		    med_event.days_supply as days_supply,
 		    dim_medication.drug_group_name,
 		    dim_medication.drug_class_name,
 		    dim_medication.drug_base_name,
 		    dim_medication.full_gpi_name,
 		    dim_medication.drug_group_id,
 		    dim_medication.drug_class_id,
 		    dim_medication.drug_base_name_id,
 		    dim_medication.full_gpi_id,
 		    med_event.medication_description as drug_description,
 		    dim_medication.generic_risk as high_risk_med,
 		    dim_medication.drug_id,
            med_event.pharmacy_npi
        from med_event
            join dim_medication
                on med_event.ndc =  dim_medication.ndc
    )

   , visit_fills as (
        select
            mrp_complete_indicator.patient_id,
            mrp_complete_indicator.opportunity_id,
            mrp_complete_indicator.discharge_date,
            mrp_complete_indicator.control_type,
            mrp_complete_indicator.mrp_date as first_mrp_date,
            med_fills.ndc  as drug_ndc,
            med_fills.drug_id,
            med_fills.drug_group_name,
 		    med_fills.drug_class_name,
 		    med_fills.drug_base_name,
 		    med_fills.full_gpi_name as drug_gpi_name,
 		    med_fills.drug_group_id,
 		    med_fills.drug_class_id,
 		    med_fills.drug_base_name_id,
 		    med_fills.full_gpi_id as drug_gpi,
            case
                   when lower(mrp_complete_indicator.control_type) = 'intervention' and
                        med_fills.fill_date <= mrp_complete_indicator.mrp_date then 1
                   when lower(mrp_complete_indicator.control_type) = 'intervention' and
                        med_fills.fill_date > mrp_complete_indicator.mrp_date then 0
                   else null end as fill_before_visit,
            case
                   when (lower(mrp_complete_indicator.control_type) = 'control' or
                         (lower(mrp_complete_indicator.control_type) = 'intervention' and
                          mrp_complete_indicator.mrp_date is null))
                       and med_fills.fill_date <= mrp_complete_indicator.discharge_date then 1
                   when (lower(mrp_complete_indicator.control_type) = 'control' or
                         (lower(mrp_complete_indicator.control_type) = 'intervention' and
                          mrp_complete_indicator.mrp_date is null))
                       and med_fills.fill_date > mrp_complete_indicator.discharge_date then 0
                   else null end as fill_before_discharge,
            med_fills.fill_date                                         as medfill_date,
            med_fills.days_supply                  as days_supply,
            med_fills.pharmacy_npi as pharm_npi
        from mrp_complete_indicator
                 join med_fills
                      on med_fills.patient_id = mrp_complete_indicator.patient_id
                        --and med_fills.fill_date::DATE > mrp_complete_indicator.mrp_date
        where mrp_complete_indicator.mrp_date_order = 1
        group by
            mrp_complete_indicator.patient_id,
            mrp_complete_indicator.opportunity_id,
            mrp_complete_indicator.discharge_date,
            mrp_complete_indicator.control_type,
            first_mrp_date,
            drug_ndc,
            med_fills.drug_id,
            drug_group_name,
            drug_class_name,
            drug_base_name,
            drug_gpi_name,
            drug_group_id,
            drug_class_id,
            drug_base_name_id,
            drug_gpi,
            fill_before_visit,
            fill_before_discharge,
            medfill_date,
            days_supply,
            pharm_npi)

   , before_visit_fills_gpi as (
        select
            visit_fills.patient_id,
            visit_fills.opportunity_id,
            visit_fills.first_mrp_date,
            visit_fills.drug_gpi,
            visit_fills.drug_gpi_name,
            visit_fills.drug_class_id,
            visit_fills.drug_class_name,
            visit_fills.drug_base_name,
            visit_fills.drug_group_id,
            visit_fills.drug_group_name,
            visit_fills.drug_id,
            visit_fills.drug_base_name_id,
            visit_fills.drug_ndc,
            visit_fills.fill_before_visit,
            visit_fills.fill_before_discharge,
            row_number()
                 over ( partition by patient_id, opportunity_id, drug_base_name_id
                        order by visit_fills.medfill_date desc ) as fill_date_order,
            visit_fills.medfill_date,
            visit_fills.days_supply,
            visit_fills.pharm_npi
        from visit_fills
        where fill_before_visit = 1 or fill_before_discharge = 1
    )

   , after_visit_fills_gpi as (
        select
            visit_fills.patient_id,
            visit_fills.opportunity_id,
            visit_fills.first_mrp_date,
            visit_fills.drug_gpi,
            visit_fills.drug_gpi_name,
            visit_fills.drug_class_id,
            visit_fills.drug_class_name,
            visit_fills.drug_base_name,
            visit_fills.drug_group_id,
            visit_fills.drug_group_name,
            visit_fills.drug_id,
            visit_fills.drug_base_name_id,
            visit_fills.drug_ndc,
            visit_fills.fill_before_visit,
            visit_fills.fill_before_discharge,
            row_number()
                 over ( partition by patient_id, opportunity_id, drug_base_name_id
                        order by visit_fills.medfill_date asc ) as fill_date_order,
            visit_fills.medfill_date,
            visit_fills.days_supply,
            visit_fills.pharm_npi
        from visit_fills
        where fill_before_visit = 0 or fill_before_discharge = 0
    )

   , patient_visit_base as (
        select
            patients.institution_id,
            patients.source_patient_id,
            patients.patient_id,
            patients.opportunity_id,
            patients.control_type,
            patients.discharge_date,
            ea_indicator.pt_reached,
            mrp_complete_indicator.mrp_date
        from patients
                 left join ea_indicator on ea_indicator.patient_id = patients.patient_id
                    and ea_indicator.opportunity_id = patients.opportunity_id

                 left join mrp_complete_indicator on mrp_complete_indicator.patient_id = patients.patient_id
                    and mrp_complete_indicator.opportunity_id = patients.opportunity_id
    )

   , before_after_medfills as (
        select
            patient_visit_base.institution_id,
            patient_visit_base.source_patient_id,
            patient_visit_base.patient_id,
            patient_visit_base.opportunity_id,
            patient_visit_base.control_type,
            patient_visit_base.discharge_date,
            patient_visit_base.pt_reached,
            case
                when lower(patient_visit_base.control_type) = 'control' or
                     (lower(patient_visit_base.control_type) = 'intervention' and
                      patient_visit_base.pt_reached = 0 and
                      COALESCE(after_visit_fills_gpi.first_mrp_date,before_visit_fills_gpi.first_mrp_date) is null)
                    then 'never treated'
                when lower(patient_visit_base.control_type) = 'intervention' and
                     COALESCE(after_visit_fills_gpi.first_mrp_date,before_visit_fills_gpi.first_mrp_date) is not null
                    then 'treated'
                when lower(patient_visit_base.control_type) = 'intervention' and
                     patient_visit_base.pt_reached = 1 and
                     COALESCE(after_visit_fills_gpi.first_mrp_date,before_visit_fills_gpi.first_mrp_date) is null
                    then 'exclude'
                else 'requires research' end            as pt_treated,
            COALESCE(after_visit_fills_gpi.first_mrp_date,before_visit_fills_gpi.first_mrp_date) as first_mrp_date,
            before_visit_fills_gpi.drug_gpi,
            before_visit_fills_gpi.drug_gpi_name,
            before_visit_fills_gpi.drug_class_id,
            before_visit_fills_gpi.drug_class_name,
            before_visit_fills_gpi.drug_base_name_id,
            before_visit_fills_gpi.drug_base_name,
            before_visit_fills_gpi.drug_group_id,
            before_visit_fills_gpi.drug_group_name,
            before_visit_fills_gpi.drug_id              as drug_id_before,
            before_visit_fills_gpi.drug_ndc             as drug_ndc_before,
            COALESCE(before_visit_fills_gpi.fill_before_visit,after_visit_fills_gpi.fill_before_visit) as fill_before_visit,
            COALESCE(before_visit_fills_gpi.fill_before_discharge,after_visit_fills_gpi.fill_before_discharge) as fill_before_discharge,
            before_visit_fills_gpi.fill_date_order      as fill_date_order_before,
            before_visit_fills_gpi.medfill_date         as medfill_date_before,
            before_visit_fills_gpi.days_supply          as days_supply_before,
            before_visit_fills_gpi.pharm_npi            as pharm_npi_before,
            after_visit_fills_gpi.drug_id               as drug_id_after,
            after_visit_fills_gpi.drug_ndc              as drug_ndc_after,
            after_visit_fills_gpi.fill_date_order       as fill_date_order_after,
            after_visit_fills_gpi.medfill_date          as medfill_date_after,
            after_visit_fills_gpi.days_supply           as days_supply_after,
            after_visit_fills_gpi.pharm_npi             as pharm_npi_after
        from patient_visit_base

            join before_visit_fills_gpi
                    on ((before_visit_fills_gpi.patient_id = patient_visit_base.patient_id
                        and before_visit_fills_gpi.opportunity_id = patient_visit_base.opportunity_id
                        and before_visit_fills_gpi.first_mrp_date = patient_visit_base.mrp_date
                        and before_visit_fills_gpi.fill_before_visit = 1
                        and before_visit_fills_gpi.fill_date_order = 1 -- Most recent fill for gpi before mrp
                        )
                        or (before_visit_fills_gpi.patient_id = patient_visit_base.patient_id
                            and before_visit_fills_gpi.opportunity_id = patient_visit_base.opportunity_id
                            and before_visit_fills_gpi.fill_before_discharge = 1
                            and before_visit_fills_gpi.fill_date_order = 1 -- Most recent fill for gpi before discharge
                            )
                        )

            join after_visit_fills_gpi
                    on ((after_visit_fills_gpi.patient_id = patient_visit_base.patient_id
                        and after_visit_fills_gpi.opportunity_id = patient_visit_base.opportunity_id
                        and after_visit_fills_gpi.first_mrp_date = patient_visit_base.mrp_date
                        and after_visit_fills_gpi.fill_before_visit = 0
                        and after_visit_fills_gpi.fill_date_order = 1 -- First fill for gpi after mrp
                        and after_visit_fills_gpi.drug_base_name_id = before_visit_fills_gpi.drug_base_name_id
                        )
                        or (after_visit_fills_gpi.patient_id = patient_visit_base.patient_id
                            and after_visit_fills_gpi.opportunity_id = patient_visit_base.opportunity_id
                            and after_visit_fills_gpi.fill_before_discharge = 0
                            and after_visit_fills_gpi.fill_date_order = 1
                            and after_visit_fills_gpi.drug_base_name_id = before_visit_fills_gpi.drug_base_name_id -- First fill for gpi after discharge
                            )
                        )

        order by patient_visit_base.patient_id,
                 patient_visit_base.opportunity_id,
                 first_mrp_date,
                 before_visit_fills_gpi.drug_gpi,
                 fill_date_order_before,
                 medfill_date_before desc,
                 fill_date_order_after,
                 medfill_date_after asc
               )

select
    before_after_medfills.institution_id,
    before_after_medfills.source_patient_id as pid,
    before_after_medfills.patient_id,
    before_after_medfills.opportunity_id,
    before_after_medfills.control_type,
    before_after_medfills.discharge_date,
    before_after_medfills.pt_reached,
    before_after_medfills.pt_treated,
    before_after_medfills.first_mrp_date,
    before_after_medfills.drug_gpi,
    before_after_medfills.drug_gpi_name,
    before_after_medfills.drug_class_id,
    before_after_medfills.drug_class_name,
    before_after_medfills.drug_base_name_id,
    before_after_medfills.drug_base_name,
    before_after_medfills.drug_group_id,
    before_after_medfills.drug_group_name,
    before_after_medfills.drug_id_before,
    before_after_medfills.drug_ndc_before,
    before_after_medfills.fill_before_visit,
    before_after_medfills.fill_before_discharge,
    before_after_medfills.fill_date_order_before,
    before_after_medfills.medfill_date_before,
    before_after_medfills.days_supply_before,
    before_after_medfills.pharm_npi_before,
    before_after_medfills.drug_id_after,
    before_after_medfills.drug_ndc_after,
    before_after_medfills.fill_date_order_after,
    before_after_medfills.medfill_date_after,
    before_after_medfills.days_supply_after,
    before_after_medfills.pharm_npi_after
from before_after_medfills