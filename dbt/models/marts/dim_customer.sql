with
{{ import(ref('stg_redshift_schema__customers'), 'customers') }}

select *
from customers
