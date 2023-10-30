with
{{ import(source('redshift_rxcompanion', 'rxc_subscriber'), 'subscribers') }},

filter_subscribers as (

    select

        rxc_subscriber_id::varchar as rxc_subscriber_id,
        patientid as patient_id,
        nullif(altmemberid,'') as alt_member_id,
        nullif(altmemberid4,'') as alt_member_id_4,
        nullif(customerid,'') as customer_id,
        nullif(hicn,'') as hicn,
        nullif(mbi,'') as mbi,
        nullif(hicn_mbi_last,'') as hicn_mbi_last,
        cardname as card_name,
        cardid as card_id,
        nullif(clientid,'') as client_id,
        nullif(groupid,'') as group_id,
        nullif(subgroupid,'') as sub_group_id,
        nullif(contractid,'') as contract_id,
        logo_id,
        nullif(personcode,'') as person_code,
        nullif(membercommercialflag,'') as commercial_flag,
        nullif(memberlastname,'') as last_name,
        nullif(memberfirstname,'') as first_name,
        nullif(membermiddleinitial,'') as middle_initial,
        memberdob as date_of_birth,
        nullif(deceased_date,'0001-01-01') as deceased_date,
        nullif(membergendercode,'') as sex,
        nullif(memberaddress1,'') as street_address_1,
        nullif(memberaddress2,'') as street_address_2,
        nullif(memberaddress3,'') as street_address_3,
        nullif(membercity,'') as city,
        nullif(memberstate,'') as state,
        nullif(memberzip,'') as zip,
        nullif(preferredlanguage,'') as preferred_language,
        nullif(memberpreferredlanguagespoken,'') as preferred_language_spoken,
        nullif(memberpreferredlanguagewritten,'') as preferred_language_written,
        nullif(specialhandling,'') as special_handling,
        nullif(membertelephone,'') as phone,
        nullif(altphonenumber,'') as alt_phone,
        nullif(alt2phonenumber,'') as member_provided_phone,
        nullif(addeddate,'0001-01-01') as added_date,
        nullif(lastupdatedate,'0001-01-01') as last_update_date,
        enrolldate as enroll_date,
        disenrolldate as disenroll_date,
        nullif(returnedmail,'') as returned_mail,
        nullif(donotcontactplan,'') as do_not_contact_plan,
        nullif(donotcontactmtm,'') as do_not_contact_mtm,
        nullif(donotcallplan,'') as do_not_call_plan,
        nullif(donotcallmtm,'') as do_not_call_mtm,
        nullif(cognitivelyimpaired,'') as cognitively_impaired,
        nullif(cognitively_impaired_description,'') as cognitively_impaired_description

    from subscribers
    where
        disenroll_date >= '2021-01-01'
        or disenroll_date = '0001-01-01'

    )

select *
from filter_subscribers