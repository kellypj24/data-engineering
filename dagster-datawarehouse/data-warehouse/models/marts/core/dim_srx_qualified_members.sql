with
{{ import(ref('stg_redshift_rxcompanion__subscribers'), 'subscribers') }},
{{ import(ref('stg_redshift_rxcompanion__qualification'), 'qualification') }},
{{ import(ref('stg_redshift_rxcompanion__sponsors'), 'sponsors') }},
{{ import(ref('stg_redshift_rxcompanion__subscriber_permanent_optout'), 'opt_out') }},

filter_qualified_members as (

    select
        subscribers.rxc_subscriber_id,
        qualification.program_name,
        sponsors.sponsor_name,
        subscribers.contract_id,
        subscribers.group_id,
        subscribers.card_name,
        subscribers.card_id,
        subscribers.patient_id,
        subscribers.alt_member_id,
        subscribers.alt_member_id_4,
        subscribers.mbi,
        subscribers.first_name,
        subscribers.last_name,
        subscribers.date_of_birth,
        extract(year from current_date) - extract(year from subscribers.date_of_birth) as est_age,
        subscribers.sex,
        subscribers.street_address_1,
        subscribers.street_address_2,
        subscribers.city,
        subscribers.state,
        subscribers.zip,
        qualification.qualified_date,
        current_date - qualification.qualified_date as days_qualified,
        qualification.opt_out_date,
        qualification.opt_out_reason,
        subscribers.disenroll_date,
        subscribers.deceased_date,
        subscribers.do_not_contact_mtm,
        subscribers.do_not_contact_plan,
        subscribers.do_not_call_mtm,
        subscribers.do_not_call_plan,
        opt_out.permanent_optout_date

    from qualification
        join subscribers on qualification.card_name = subscribers.card_name
            and qualification.card_id = subscribers.card_id
        join sponsors on subscribers.card_name = sponsors.plan_name
            and qualification.card_name = sponsors.plan_name
        left join opt_out on qualification.card_name = opt_out.card_name
            and qualification.card_id = opt_out.card_id
            and case
                    when upper(qualification.program_name) like 'MTM20%'
                         or upper(qualification.program_name) like 'COAMTM20%' then 'MTM'
                    when upper(qualification.program_name) like 'COA20%' then 'COA'
                    when upper(qualification.program_name) like 'STAR20%'
                         or upper(qualification.program_name) like 'UHCLQ%' then 'STAR'
                    else 'Commercial'
                end
                = upper(opt_out.program_type)

    where
        (subscribers.disenroll_date > qualification.qualified_date
            or disenroll_date = '0001-01-01')
        and (subscribers.deceased_date > qualification.qualified_date
            or subscribers.deceased_date is null)
        and ((qualification.opt_out_date > qualification.qualified_date
              and opt_out.permanent_optout_date > qualification.qualified_date)
            or (qualification.opt_out_date is null and opt_out.permanent_optout_date is null))
    )

select *
from filter_qualified_members