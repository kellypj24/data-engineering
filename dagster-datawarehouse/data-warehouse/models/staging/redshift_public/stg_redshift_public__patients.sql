with
{{ import(source('redshift_public', 'dim_patient'), 'patients') }},
{{ import(source('redshift_public', 'salesforce_contact'), 'salesforce_contacts') }},

test_patients as (

    select distinct
        salesforce_contacts.id as salesforce_contact_id,
        salesforce_contacts.patient_id,
        salesforce_contacts.iid,
        null as pid
    from salesforce_contacts
    where salesforce_contacts.patient_id not in (
        select distinct patients.id from patients
    )

    union

    select
        salesforce_contacts.id as salesforce_contact_id,
        patients.id as patient_id,
        patients.iid,
        patients.pid
    from patients
    inner join salesforce_contacts on
        patients.id = salesforce_contacts.patient_id
    where patients.pid like 'test_%'

    union

    select
        salesforce_contacts.id as salesforce_contact_id,
        patients.id as patient_id,
        patients.iid,
        patients.pid
    from patients
    inner join salesforce_contacts on
        patients.id = salesforce_contacts.patient_id
    where patients.id in (
    {% for patient_id in var('duplicate_patients_to_ignore', []) -%}
        '{{ patient_id }}'
        {%- if not loop.last %},{{ '\n' }}{% endif %}
    {%- endfor %}
    )

),

filter_test_patients as (

    select *,
		  document.patient_attributes.contract_id::varchar,
	      document.patient_attributes.plan_name::varchar,
          document.name.given::varchar as given_name,
          document.name.family::varchar as family_name,
          document.dob::date as date_of_birth,
          document.sex::varchar,
          document.address.street_address::varchar,
          UPPER(document.address.state::varchar) as state,
          document.address.city::varchar,
          document.address.zip::varchar,
          document.email::varchar,
          document.home_phone::varchar,
          document.mobile_phone::varchar,
          patient_attributes.subscriber_id::varchar,
          patient_attributes.email_consent::boolean,
          patient_attributes.sms_consent::boolean,
          patient_attributes.plan_group_id::varchar,
          patient_attributes.pcp_name::varchar,
          patient_attributes.pcp_phone::varchar,
          patient_attributes.pcp_npi::varchar,
          document.patient_attributes.pcp_address.street_address::varchar as pcp_street_address,
          document.patient_attributes.pcp_address.city::varchar as pcp_city,
          document.patient_attributes.pcp_address.state::varchar as pcp_state,
          document.patient_attributes.pcp_address.zip::varchar as pcp_zip
    from patients
    where id not in (
        select patient_id
        from test_patients
    )

),

filter_institutions as (

    select *
    from filter_test_patients
    where iid in (
    {% for iid in var('patient_institution_filter', []) -%}
        '{{ iid }}'
        {%- if not loop.last %},{{ '\n' }}{% endif %}
    {%- endfor %}
    )

),

final as (

    select
        filter_institutions.id as patient_id,
        filter_institutions.iid as institution_id,
        filter_institutions.pid as source_patient_id,
        filter_institutions.subscriber_id as secondary_patient_id,
        salesforce_contacts.id as patient_contact_id,
        filter_institutions.time_created as created_at,
        filter_institutions.time_updated as updated_at,
        salesforce_contacts.time_initial_discharge as initial_discharge_at,
        salesforce_contacts.is_control as is_control,
        salesforce_contacts.control_type as control_type,
        filter_institutions.plan_name,
        filter_institutions.given_name,
        filter_institutions.family_name,
        filter_institutions.contract_id,
        filter_institutions.date_of_birth,
        filter_institutions.sex,
        filter_institutions.street_address,
        filter_institutions.city,
        filter_institutions.state,
        filter_institutions.zip,

        case when filter_institutions.state in (
        {% for state in var('us_states', []) -%}
             '{{ state }}'
             {%- if not loop.last %},{{ '\n' }}{% endif %}
        {%- endfor %}
        ) then 'US'
        else null end as country,

        filter_institutions.email,
        filter_institutions.home_phone,
        filter_institutions.mobile_phone,
        filter_institutions.email_consent as original_email_consent,
        filter_institutions.sms_consent as original_sms_consent,

        case when salesforce_contacts.has_email_address = true
                and salesforce_contacts.communication_type_consent like '%Email%' then true
             else false
             end as current_email_consent,

        case when salesforce_contacts.has_mobile_phone = true
                and salesforce_contacts.communication_type_consent like '%SMS%' then true
             else false
             end as current_sms_consent,

        filter_institutions.plan_group_id,
        filter_institutions.pcp_name,
        filter_institutions.pcp_phone,
        filter_institutions.pcp_npi,
        filter_institutions.pcp_street_address,
        filter_institutions.pcp_city,
        filter_institutions.pcp_state,
        filter_institutions.pcp_zip

    from filter_institutions
    left join salesforce_contacts
        on filter_institutions.id = salesforce_contacts.patient_id

)

select *
from final
