with
{{ import(source('redshift_public', 'fact_patient_subscription_tag'), 'subscriptions') }},


filter_subscriptions as (

    select subscriptions.patient_id,
           subscriptions.iid,
           subscriptions.tag_ as subscription_tag,
           subscriptions.time_start,
           subscriptions.time_end,
           RANK() OVER (PARTITION BY patient_id ORDER BY time_start ASC) subscription_record_order,
           RANK() OVER (PARTITION BY patient_id ORDER BY time_start DESC) subscription_record_order_reverse

    from subscriptions
    where subscription_tag in (
    {% for subscription_tag in var('patient_subscription_filter', []) -%}
        '{{ subscription_tag }}'
        {%- if not loop.last %},{{ '\n' }}{% endif %}
    {%- endfor %})
)

select *
from filter_subscriptions