-- One row per order. The source for the example exports in
-- delivery/file-export/configs/: customer_id is the tenant key a shared export
-- is checked against.

SELECT
    order_id,
    customer_id,
    status,
    amount,
    created_at
FROM {{ ref('stg_example') }}
