{% macro import(from, alias_name) -%}
{{ alias_name }} as (

    select *
    from {{ from }}

)
{% endmacro %}
