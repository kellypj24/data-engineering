with
{{ import(source('redshift_rxcompanion', 'rxc_call_log'), 'calls') }},

filter_calls as (

    select
        rxc_subscriber_id,
        rxc_communications_id,
        callid as call_id,
        call_date as call_date_ts,
        campaign as call_campaign,
        call_type,
        userid as user_id,
        disposition,
        ani_phone_number,
        dni_phone_number,
        call_time,
        ivr_time,
        queue_wait_time,
        ring_time,
        talk_time,
        hold_time,
        park_time,
        after_call_work_time,
        speed_of_answer_time,
        handle_time,
        manual_time,
        dial_time

    from calls

    where
        call_date >= '2021-01-01'
        and lower(call_type) not like '%test%'
        and lower(campaign) not like '%test%'
        and lower(campaign) not like '%demo%'
        and lower(campaign) not like '%helpdesk%'
        and lower(campaign) not like '%trhc%'
        and lower(campaign) not like '%walmart%'
        and lower(campaign) not like '%xarelto%'
        and campaign <> '[None]'
        and campaign <> '[Deleted]'

    order by call_date

    )

select *
from filter_calls