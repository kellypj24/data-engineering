with
    {{ import(ref('stg_redshift_public__admin_qa_tasks'), 'admin_qa') }}

select *
from admin_qa
