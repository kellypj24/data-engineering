WITH source AS (

    SELECT *
    FROM {{ source('raw', 'orders') }}
    WHERE {{ limit_data_in_dev('created_at') }}

),

renamed AS (

    SELECT
        id AS order_id,
        customer_id,
        {{ clean_string('status') }} AS status,
        amount,
        created_at,
        {{ audit_columns() }}
    FROM source

),

-- One row per order_id. A duplicated id upstream is kept as one row and
-- reported by grain_guard_is_one (warn) on _grain_rows, not hidden.
deduped AS (

    {{ grain_guard('renamed', grain=['order_id'], order_by=['created_at DESC', 'customer_id', 'status', 'amount']) }}

)

SELECT * FROM deduped
