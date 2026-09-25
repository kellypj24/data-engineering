{#-
    Append-only history of every validation failure (WARN or FAIL), across all
    rule sets. Each run adds that run's failures -- the key includes the run
    time -- and the post_hook purges rows past each validation's
    retention_days.

    Add every validation model to `validation_models` below.
-#}

{{ config(
    materialized='incremental',
    unique_key='validation_key',
    incremental_strategy='delete+insert',
    post_hook="{{ purge_validation_log(this) }}"
) }}

{%- set validation_models = ['val_orders'] %}

{% for validation_model in validation_models %}
    SELECT
        validation_key,
        validation_name,
        source_table,
        record_id,
        validated_at,
        failed_rules,
        max_failed_severity_rank,
        max_failed_severity,
        validation_result,
        should_notify,
        notification_channel
    FROM {{ ref(validation_model) }}
    WHERE validation_result <> 'PASS'
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
