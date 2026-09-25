{% macro purge_validation_log(relation) -%}
    {#-
        DELETE statement for a validation_log post_hook: drops rows older than
        each validation's `retention_days` (get_validation_config). Validations
        with an override get their own cutoff; every other row uses the default.
    -#}
    {%- set overridden = var('validation_configs', {}).keys() | list -%}
    {%- set default_cutoff = dbt.dateadd(
        'day', -get_validation_config('__default__')['retention_days'], dbt.current_timestamp()
    ) -%}
    DELETE FROM {{ relation }}
    WHERE validated_at <
    {%- if overridden %} CASE validation_name
        {%- for name in overridden %}
        WHEN '{{ name }}' THEN {{ dbt.dateadd('day', -get_validation_config(name)['retention_days'], dbt.current_timestamp()) }}
        {%- endfor %}
        ELSE {{ default_cutoff }}
    END
    {%- else %} {{ default_cutoff }}
    {%- endif %}
{%- endmacro %}
