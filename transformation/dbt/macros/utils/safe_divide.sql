{% macro safe_divide(numerator, denominator) -%}
    {#-
        Perform a division that returns NULL instead of raising an error
        when the denominator is zero or NULL.

        Usage:
            SELECT {{ safe_divide('revenue', 'sessions') }} AS revenue_per_session
    -#}
    CASE
        WHEN {{ denominator }} IS NULL OR {{ denominator }} = 0
            THEN NULL
        ELSE {{ numerator }}::FLOAT / {{ denominator }}::FLOAT
    END
{%- endmacro %}
