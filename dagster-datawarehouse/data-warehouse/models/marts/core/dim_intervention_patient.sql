SELECT *
FROM {{ ref('dim_patient') }}
WHERE NOT is_control