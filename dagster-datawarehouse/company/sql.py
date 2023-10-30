from dataclasses import dataclass


@dataclass
class FileGenerationQuery:
    name: str
    query: str
    mapping: dict[str, str] | None = None
    db_type: str | None = "redshift"
    connection_str: str | None = "postgresql+psycopg2://"
    filename_pattern: str | None = None


class FileGenerationQueries:
    client2_SUPPLEMENTAL = FileGenerationQuery(
        name="client2_supplemental",
        query="""
        SELECT *
        FROM datascience.client2_supplemental_file
    """,
        mapping={
            "memberkey": "MemberKey",
            "providerkey": "ProviderKey",
            "dos": "DOS",
            "dosthru": "DOSThru",
            "providertype": "ProviderType",
            "providertaxonomy": "ProviderTaxonomy",
            "cptpx": "CPTPx",
            "hcpcpspx": "HCPCPSPx",
            "loinc": "LOINC",
            "snowmed": "SNOWMED",
            "icddx": "ICDDX",
            "icddx10": "ICDDX10",
            "rxnorm": "RxNorm",
            "cvx": "CVX",
            "modifier": "Modifier",
            "rxproviderflag": "RxProviderFlag",
            "pcpflag": "PCPFlag",
            "quantitydispensed": "QuantityDispensed",
            "icdpx": "ICDPx",
            "icdpx10": "ICDPx10",
            "suppsource": "SuppSource",
            "result": "Result",
        },
    )

    client2_STATIN_OUTREACH_COMPLETED_VISITS = FileGenerationQuery(
        name="client2_statin_completed_visits",
        query="""
        SELECT *
        FROM datascience.statin_outreach_completed_visits
        WHERE institution_id = 'client2'
    """,
        mapping={
            "institution_id": "iid",
            "program_name": "Program",
            "plan_name": "MEMBER.PLAN_NAME",
            "plan_group_id": "plan group id",
            "source_patient_id": "MemberID",
            "first_name": "FirstName",
            "last_name": "LastName",
            "date_of_birth": "DOB",
            "visit_date": "Visit Date",
            "pharmacist": "Pharmacist",
            "npi": "NPI",
        },
    )

    client2_STATIN_OUTREACH_MEMBER_DISPOSITION = FileGenerationQuery(
        name="client2_statin_member_disposition",
        query="""
        SELECT *
        FROM datascience.statin_outreach_member_level_detail
        WHERE institution_id = 'client2'
    """,
        mapping={
            "institution_id": "iid",
            "program_name": "Program",
            "plan_name": "MEMBER.PLAN_NAME",
            "plan_group_id": "plan group id",
            "source_patient_id": "PID",
            "first_name": "FirstName",
            "last_name": "LastName",
            "date_of_birth": "DOB",
            "ea1_date": "EA1 Date",
            "ea1_outcome": "EA1 Outcome",
            "ea2_date": "EA2 Date",
            "ea2_outcome": "EA2 Outcome",
            "ea3_date": "EA3 Date",
            "ea3_outcome": "EA3 Outcome",
        },
    )

    client3_CAHPS_WEEKLY_GND_OUTCOMES = FileGenerationQuery(
        name="client3_cahps_weekly_gnd_outcomes",
        query="""
        SELECT 
            member_id,
            member_first_name,
            member_last_name,
            contact_attempt_date,
            outcome_of_contact_attempt,
            visit_complete,
            scheduled_date,
            visit_completion_date,
            gap_detail,
            intervention_provided,
            medication_name,
            medication_gpi 
        FROM datascience.cahps_gnd_outcomes
    """,
        mapping={
            "member_id": "Member ID",
            "member_first_name": "Member First Name",
            "contact_attempt_date": "Contact Attempt Date",
            "outcome_of_contact_attempt": "Outcome of Contact Attempt",
            "visit_complete": "Visit Complete",
            "scheduled_date": "Scheduled Date",
            "visit_completion_date": "Visit Completion Date",
            "gap_detail": "Gap Detail",
            "intervention_provided": "Intervention Provided",
            "medication_name": "Medication Name",
            "medication_gpi": "Medication GPI",
        },
    )

    DIALER_LOADER_CMR_COMMERCIAL = FileGenerationQuery(
        name="dialer_loader_cmr_commercial",
        query="""
        SELECT *
        FROM srx_reporting.dialer_loader_cmr_commerical
    """,
        db_type="bigquery",
        filename_pattern="dialer_loader_{campaign}",
    )

    client3_DMRP_WEEKLY_EVENT_LEVEL_DETAIL = FileGenerationQuery(
        name="client3_dmpr_weekly_event_level_detail",
        query="""
        SELECT 
            secondary_patient_id,
            source_patient_id,
            discharge_date,
            discharge_materials_obtained,
            discharge_materials_source,
            discharge_materials_obtained_date,
            pcp_info_available,
            pcp_info_source,
            pcp_info_obtained_date,
            med_rec_successfully_performed,
            med_rec_performed_date,
            med_rec_successfully_delivered_pcp,
            med_rec_transferred_pcp_date,
            identified_effectiveness,
            identified_safety,
            identified_adherence,
            identified_indication,
            identified_insurance_formulary               
        FROM datascience.desktop_mrp_event_level_detail
    """,
        mapping={
            "secondary_patient_id": "member.subscriber.id",
            "source_patient_id": "member.pid",
            "discharge_date": "discharge.date",
            "discharge_materials_obtained": "discharge materials obtained",
            "discharge_materials_source": "discharge materials source",
            "discharge_materials_obtained_date": "discharge materials obtained date",
            "pcp_info_available": "PCP info available",
            "pcp_info_source": "PCP info source",
            "pcp_info_obtained_date": "PCP info obtained date",
            "med_rec_successfully_performed": "Med Rec successfully performed",
            "med_rec_performed_date": "Med Rec performed date",
            "med_rec_successfully_delivered_pcp": "Med Rec successfully delivered to PCP performed",
            "med_rec_transferred_pcp_date": "Med Rec delivered to PCP date",
            "identified_effectiveness": "Identified: Effectiveness",
            "identified_safety": "Identified: Safety",
            "identified_adherence": "Identified: Adherence",
            "identified_indication": "Identified: Indication",
            "identified_insurance_formulary": "Identified: Insurance Formulary",
        },
    )

    client3_DMRP_WEEKLY_MAP_EXTRACT = FileGenerationQuery(
        name="client3_dmpr_weekly_map_extract",
        query="""
        SELECT 
            institution_id,
            primary_patient_id,
            secondary_patient_id,
            patient_name,
            date_of_birth,
            pcp_name,
            discharge_date,
            discharge_facility,
            mrp_date,
            mrp_completed_by,
            mrp_co_signed_by,
            provider_summary,
            medication_gpi,
            medication_name,
            status,
            instructions,
            medication_taken_for,
            problem,
            patient_recommendation,
            provider_recommendation,
            prescriber
        FROM datascience.dmrp_map_extract
    """,
        mapping={
            "institution_id": "iid",
            "primary_patient_id": "Primary id",
            "secondary_patient_id": "Seondary id",
            "patient_name": "Pt Name",
            "date_of_birth": "Pt DOB",
            "pcp_name": "Pt PCP",
            "discharge_date": "Discharge Date",
            "discharge_facility": "Discharge Facility",
            "mrp_date": "MRP Date",
            "mrp_completed_by": "MRP Completed by",
            "mrp_co_signed_by": "MRP Co-signed by",
            "provider_summary": "Overall Summary for Provider",
            "medication_gpi": "Medication 1 GPI",
            "medication_name": "Medication 1",
            "status": "Medication 1 Status",
            "instructions": "Medication1 Instructions",
            "medication_taken_for": "Medication 1 Taken For",
            "problem": "Medication 1 Problem",
            "patient_recommendation": "Medication 1 Recommendation for Patient",
            "provider_recommendation": "Medication 1 Recommendation for Provider",
            "prescriber": "Medication 1 Provider",
        },
    )

    client3_DMRP_SUPPLEMENTAL = FileGenerationQuery(
        name="client3_dmpr_supplemental",
        query="""
            SELECT *
            FROM datascience.dmrp_supplemental_file
        """,
        mapping={
            "member_card_id": "Member Card ID",
            "member_first_name": "Member First Name",
            "member_last_name": "Member Last Name",
            "member_dob": "Member DOB",
            "member_gender": "Member Gender",
            "member_ssn": "Member SSN",
            "member_medicare_id": "Member Medicare ID",
            "member_medicaid_id": "Member Medicaid ID",
            "rendering_provider_npi": "Rendering Provider NPI",
            "service_date": "Service Date",
            "revenue_code": "Revenue Code",
            "icd_diagnosis_code_1": "ICD Diagnosis Code 1",
            "icd_diagnosis_code_2": "ICD Diagnosis Code 2",
            "icd_diagnosis_code_3": "ICD Diagnosis Code 3",
            "icd_diagnosis_code_4": "ICD Diagnosis Code 4",
            "icd_diagnosis_code_5": "ICD Diagnosis Code 5",
            "icd_diagnosis_code_6": "ICD Diagnosis Code 6",
            "icd_diagnosis_code_7": "ICD Diagnosis Code 7",
            "icd_diagnosis_code_8": "ICD Diagnosis Code 8",
            "icd_diagnosis_code_9": "ICD Diagnosis Code 9",
            "icd_diagnosis_code_10": "ICD Diagnosis Code 10",
            "procedural_icd_code_1": "Procedural ICD Code 1",
            "procedural_icd_code_2": "Procedural ICD Code 2",
            "procedural_icd_code_3": "Procedural ICD Code 3",
            "procedural_icd_code_4": "Procedural ICD Code 4",
            "procedural_icd_code_5": "Procedural ICD Code 5",
            "procedural_icd_code_6": "Procedural ICD Code 6",
            "procedural_icd_code_7": "Procedural ICD Code 7",
            "procedural_icd_code_8": "Procedural ICD Code 8",
            "procedural_icd_code_9": "Procedural ICD Code 9",
            "procedural_icd_code_10": "Procedural ICD Code 10",
            "icd_version": "ICD Version",
            "cpt_cptii_hcpcs_code": "CPT/CPTII/HCPCS Code",
            "cpt_modifier_1": "CPT Modifier 1",
            "cpt_modifier_2": "CPT Modifier 2",
            "cpt_result": "CPT Result",
            "loinc_code": "LOINC Code",
            "loinc_result": "LOINC Result",
            "bmi_value": "BMI Value",
            "member_weight": "Member Weight",
            "bp_systolic": "BP Systolic",
            "bp_diastolic": "BP Diastolic",
            "ndc_code": "NDC Code",
            "snomed_code": "SNOMED Code",
            "prov_rec_id": "PROV_REC_ID",
            "discharge_status_code": "Discharge Status Code",
            "pot_code": "POT_CODE",
            "cvx_clinical_cd": "CVX_CLINICAL_CD",
            "rx_norm_cd": "RX_NORM_CD",
        },
    )
