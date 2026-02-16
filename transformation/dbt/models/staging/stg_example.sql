WITH source AS (

    SELECT *
    FROM {{ source('raw', 'orders') }}
    WHERE 1 = 1
    {{ limit_data_in_dev('created_at') }}

),

renamed AS (

    SELECT
        id AS order_id,
        {{ clean_string('status') }} AS status,
        amount,
        created_at,
        {{ audit_columns() }}
    FROM source

)

SELECT * FROM renamed;
