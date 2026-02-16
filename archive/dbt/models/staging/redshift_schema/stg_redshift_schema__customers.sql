with
{{ import(source('redshift_schema', 'source_company_customers'), 'customers') }},

filter_customers as (

  select *
  
  from customers
  where customers.id in (
    customers.state in (
        {% for state in var('us_states', []) -%}
             '{{ state }}'
             {%- if not loop.last %},{{ '\n' }}{% endif %}
        {%- endfor %}
    )

  select *
  from filter_customers
 
