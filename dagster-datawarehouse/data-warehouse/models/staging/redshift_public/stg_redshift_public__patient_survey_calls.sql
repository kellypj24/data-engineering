with
{{ import(source('redshift_public', 'salesforce_ctr'), 'contact_trace_record') }},
{{ import(source('redshift_public', 'salesforce_cca'), 'contact_channel_analytics') }},
{{ import(source('redshift_public', 'salesforce_contact'), 'salesforce_contacts') }},

filter_patient_surveys as (

    select
        salesforce_contacts.patient_id,
        salesforce_contacts.iid as institution_id,
        convert_timezone('EST', timestamp 'epoch' + contact_trace_record.document.amazonconnect__agentconnectedtoagenttimestamp__c::bigint /
        1000 * interval '1 second') as call_timestamp,
        contact_trace_record.attributes.surveyasked::varchar as survey_asked,
        contact_trace_record.attributes.surveyunderstand::varchar as survey_understand,
        contact_trace_record.attributes.surveysatisfied::varchar as survey_satisfied

    from contact_trace_record
        join contact_channel_analytics on contact_trace_record.ac_contact_id = contact_channel_analytics.ac_contact_id
        join salesforce_contacts ON salesforce_contacts.id = contact_channel_analytics.sf_contact_id
    where
        (survey_asked is not null
            or survey_understand is not null
	        or survey_satisfied is not null)
    group by
        salesforce_contacts.patient_id,
        salesforce_contacts.iid,
        call_timestamp,
        survey_asked,
        survey_understand,
        survey_satisfied
    )

select *
from filter_patient_surveys
