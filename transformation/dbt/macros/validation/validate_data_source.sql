{% macro validate_data_source(name, source_model, record_id_column, rules, timestamp_column=none) -%}
    {#-
        Evaluate a rule set against every row of `source_model` and emit one
        result row per record. See README.md in this directory.

        Usage (the body of a validation model):

            {{ validate_data_source(
                name='orders',
                source_model=ref('stg_orders'),
                record_id_column='order_id',
                timestamp_column='created_at',
                rules={
                    'order_id_present': {'logic': 'order_id IS NOT NULL', 'severity': 'CRITICAL'},
                    'amount_non_negative': {'logic': 'amount >= 0', 'severity': 'HIGH'},
                }
            ) }}

        A rule's `logic` is a predicate that is TRUE when the row passes. NULL
        counts as a failure: a rule that cannot be evaluated has not passed.

        Columns: validation_key, validation_name, source_table, record_id,
        validated_at; per rule `<rule>_passed` and `<rule>_severity` (the
        rule's severity when it failed, else NULL); failed_rules;
        max_failed_severity_rank / max_failed_severity; validation_result;
        should_notify; notification_channel.

        validation_result: FAIL when any MEDIUM-or-higher rule failed, WARN
        when only LOW rules failed, PASS otherwise.

        Config (get_validation_config): `enabled: false` yields zero rows;
        `lookback_days` limits rows to that many days of `timestamp_column`.
    -#}
    {%- set ranks = validation_severity_ranks() -%}
    {%- if not rules -%}
        {{ exceptions.raise_compiler_error("validate_data_source(" ~ name ~ "): rules is empty") }}
    {%- endif -%}
    {%- for rule_name, rule in rules.items() -%}
        {%- if rule.get('severity') not in ranks -%}
            {{ exceptions.raise_compiler_error(
                "validate_data_source(" ~ name ~ "): rule " ~ rule_name ~ " has severity "
                ~ rule.get('severity') ~ "; expected one of " ~ (ranks.keys() | list | join(', '))
            ) }}
        {%- endif -%}
        {%- if not rule.get('logic') -%}
            {{ exceptions.raise_compiler_error("validate_data_source(" ~ name ~ "): rule " ~ rule_name ~ " has no logic") }}
        {%- endif -%}
    {%- endfor -%}

    {%- set config = get_validation_config(name) -%}
    {%- set failed_rank = [] -%}
    {%- for rule_name, rule in rules.items() -%}
        {%- do failed_rank.append(
            "CASE WHEN NOT " ~ rule_name ~ "_passed THEN " ~ ranks[rule['severity']] ~ " ELSE 0 END"
        ) -%}
    {%- endfor %}

WITH source AS (

    SELECT *
    FROM {{ source_model }}
    WHERE {{ 'TRUE' if config['enabled'] else 'FALSE' }}
    {%- if config['lookback_days'] is not none and timestamp_column %}
        AND {{ timestamp_column }} >= {{ dbt.dateadd('day', -config['lookback_days'], dbt.current_timestamp()) }}
    {%- endif %}

),

checked AS (

    SELECT
        CAST({{ record_id_column }} AS {{ dbt.type_string() }}) AS record_id,
        {%- for rule_name, rule in rules.items() %}
        COALESCE({{ rule['logic'] }}, FALSE) AS {{ rule_name }}_passed{{ ',' if not loop.last }}
        {%- endfor %}
    FROM source

),

ranked AS (

    SELECT
        *,
        {% if failed_rank | length == 1 -%}
        {{ failed_rank[0] }}
        {%- else -%}
        GREATEST(
            {{ failed_rank | join(',\n            ') }}
        )
        {%- endif %} AS max_failed_severity_rank
    FROM checked

),

labelled AS (

    SELECT
        *,
        CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}' AS {{ dbt.type_timestamp() }}) AS validated_at,
        CASE max_failed_severity_rank
            {%- for level, rank in ranks.items() %}
            WHEN {{ rank }} THEN '{{ level }}'
            {%- endfor %}
        END AS max_failed_severity,
        CASE
            WHEN max_failed_severity_rank >= {{ ranks['MEDIUM'] }} THEN 'FAIL'
            WHEN max_failed_severity_rank >= {{ ranks['LOW'] }} THEN 'WARN'
            ELSE 'PASS'
        END AS validation_result
    FROM ranked

)

SELECT
    {{ mint_surrogate_key(["'" ~ name ~ "'", "'" ~ source_model.identifier ~ "'", 'record_id', 'validated_at']) }}
        AS validation_key,
    '{{ name }}' AS validation_name,
    '{{ source_model.identifier }}' AS source_table,
    record_id,
    validated_at,
    {%- for rule_name, rule in rules.items() %}
    {{ rule_name }}_passed,
    CASE WHEN NOT {{ rule_name }}_passed THEN '{{ rule['severity'] }}' END AS {{ rule_name }}_severity,
    {%- endfor %}
    RTRIM(
        {%- for rule_name in rules %}
        CASE WHEN NOT {{ rule_name }}_passed THEN '{{ rule_name }},' ELSE '' END{{ ' ||' if not loop.last }}
        {%- endfor %},
        ','
    ) AS failed_rules,
    max_failed_severity_rank,
    max_failed_severity,
    validation_result,
    {{ should_send_notification('validation_result', 'max_failed_severity', name) }} AS should_notify,
    CASE
        WHEN max_failed_severity IS NOT NULL
            THEN {{ get_notification_channel(name, 'max_failed_severity') }}
    END AS notification_channel
FROM labelled
{%- endmacro %}
