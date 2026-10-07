WITH source AS (

    SELECT *
    FROM {{ source('raw', 'delivery_confirmations') }}

)

SELECT
    recipient,
    file_name,
    confirmed_at,
    transport
FROM source
