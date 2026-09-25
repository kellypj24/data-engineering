{#-
    Configuration and routing for the validation framework. See README.md in
    this directory.

    Everything tunable lives in vars, so changing behaviour is a config change,
    not a code change:

        vars:
          validation_configs:            # per-validation overrides
            orders:
              lookback_days: 7
              retention_days: 30
              notification_channel: '#orders-oncall'
          high_priority_validations: [orders]
          validation_notification_channels:
            CRITICAL: '#data-alerts'
            HIGH: '#data-alerts'
            MEDIUM: '#data-quality'
            LOW: '#data-quality'
-#}


{% macro validation_severity_ranks() -%}
    {{- return({'LOW': 1, 'MEDIUM': 2, 'HIGH': 3, 'CRITICAL': 4}) -}}
{%- endmacro %}


{% macro get_validation_config(name) -%}
    {#- Defaults, overlaid with var('validation_configs')[name]. -#}
    {%- set config = {
        'enabled': true,
        'lookback_days': none,
        'notification_enabled': true,
        'notification_channel': none,
        'retention_days': 90
    } -%}
    {%- set overrides = var('validation_configs', {}).get(name, {}) -%}
    {%- for key in overrides -%}
        {%- if key not in config -%}
            {{ exceptions.raise_compiler_error(
                "validation_configs." ~ name ~ ": unknown key '" ~ key ~ "'. Known keys: "
                ~ (config.keys() | list | join(', '))
            ) }}
        {%- endif -%}
    {%- endfor -%}
    {%- do config.update(overrides) -%}
    {{- return(config) -}}
{%- endmacro %}


{% macro should_send_notification(result, severity, name, high_priority=none, enabled=none) -%}
    {#-
        SQL boolean: should this validation row raise a notification?

            notifications disabled for `name`  -> never
            CRITICAL                           -> always
            HIGH                               -> on FAIL
            MEDIUM                             -> on FAIL, only if `name` is high priority
            LOW, or no failure                 -> never

        `result` and `severity` are SQL expressions (column names). The
        high-priority list and the enabled flag come from vars; the optional
        arguments override them, which is how the branch test pins them.
    -#}
    {%- set high_priority = var('high_priority_validations', []) if high_priority is none else high_priority -%}
    {%- set enabled = get_validation_config(name)['notification_enabled'] if enabled is none else enabled -%}
    {%- if not enabled -%}
        FALSE
    {%- else -%}
        CASE
            WHEN {{ severity }} = 'CRITICAL' THEN TRUE
            WHEN {{ severity }} = 'HIGH' AND {{ result }} = 'FAIL' THEN TRUE
            WHEN {{ severity }} = 'MEDIUM' AND {{ result }} = 'FAIL' THEN {{ 'TRUE' if name in high_priority else 'FALSE' }}
            ELSE FALSE
        END
    {%- endif -%}
{%- endmacro %}


{% macro get_notification_channel(name, severity) -%}
    {#-
        SQL string: where a notification for this row goes. A per-validation
        `notification_channel` wins; otherwise route by severity through
        var('validation_notification_channels'). NULL when nothing matches.
    -#}
    {%- set override = get_validation_config(name)['notification_channel'] -%}
    {%- if override -%}
        '{{ override | replace("'", "''") }}'
    {%- else -%}
        {%- set channels = var('validation_notification_channels', {}) -%}
        {%- if channels -%}
            CASE {{ severity }}
                {%- for level, channel in channels.items() %}
                WHEN '{{ level }}' THEN '{{ channel | replace("'", "''") }}'
                {%- endfor %}
            END
        {%- else -%}
            CAST(NULL AS {{ dbt.type_string() }})
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
